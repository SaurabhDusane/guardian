"""Runner abstraction for the scenario suite.

A runner drives the demo pipeline through one execution mode (standalone, Dagster,
...). Actions (run, status changes, replay, faults) are runner-specific; reads go
through the shared Guardian stores, so assertions are identical for every runner.
"""

from __future__ import annotations

import abc
import dataclasses
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from guardian.core.events import Event, EventKind
from guardian.core.guardian import Guardian
from guardian.core.models import (
    BlockStatus,
    DataRef,
    PipelineSpec,
    QuarantineRecord,
    QuarantineStatus,
    ReplayResult,
)
from guardian.core.refs import load_ref
from guardian.demo.faults import Fault, apply_faults
from guardian.runner.executor import Executor
from guardian.runner.spec_loader import load_spec

DEMO_SPEC_PATH = Path(load_ref("guardian.demo:__file__")).parent / "pipeline.yaml"


def scenario_spec(rows: int = 200) -> PipelineSpec:
    """The demo spec with clean (error-free) input, so every quarantine is a fault."""
    spec = load_spec(DEMO_SPEC_PATH)
    blocks = []
    for block in spec.blocks:
        if block.name == "b1_ingest":
            block = dataclasses.replace(
                block, params={**block.params, "rows": rows, "error_rate": 0.0}
            )
        blocks.append(block)
    return dataclasses.replace(spec, blocks=tuple(blocks))


@dataclass(frozen=True)
class BlockResult:
    outcome: str  # PASS | ROLLBACK | SKIPPED | BLOCKED
    sources: tuple[DataRef, ...] = ()

    @property
    def rerouted(self) -> bool:
        return any(ref.rerouted for ref in self.sources)


RunResult = dict[str, BlockResult]


class ScenarioRunner(abc.ABC):
    name: str

    def __init__(self, spec: PipelineSpec, root: Path) -> None:
        self.spec = spec
        self.root = root
        self.faults: dict[str, tuple[Fault, ...]] = {}
        self._reader: Guardian | None = None

    # -------------------------------------------------------------- actions

    @abc.abstractmethod
    def run(self, run_id: str, only: Sequence[str] | None = None) -> RunResult:
        """Run the pipeline (or just the ``only`` blocks) under Guardian run id ``run_id``."""

    @abc.abstractmethod
    def set_block_status(self, block: str, status: BlockStatus) -> None: ...

    @abc.abstractmethod
    def replay(self, block: str) -> ReplayResult: ...

    def inject_fault(self, block: str, *faults: Fault) -> None:
        self.spec.block(block)
        self.faults[block] = faults

    def clear_faults(self, block: str | None = None) -> None:
        if block is None:
            self.faults.clear()
        else:
            self.faults.pop(block, None)

    # -------------------------------------------------------------- reads

    def close(self) -> None:
        """Release the cached read handle. Runners call this before every action."""
        if self._reader is not None:
            self._reader.close()
            self._reader = None

    def _read(self, fn: Callable[[Guardian], Any]) -> Any:
        if self._reader is None:
            self._reader = Guardian(self.spec, self.root)
        return fn(self._reader)

    def status(self, block: str) -> BlockStatus:
        return self._read(lambda g: g.status(block))

    def last_good(self, block: str) -> DataRef | None:
        return self._read(lambda g: g.snapshots.last_good(block))

    def snapshot(self, block: str, run_id: str) -> pd.DataFrame | None:
        return self._read(
            lambda g: g.snapshots.read(block, run_id) if g.snapshots.exists(block, run_id) else None
        )

    def read_last_good(self, block: str) -> pd.DataFrame:
        return self._read(lambda g: g.read(g.snapshots.last_good(block)))

    def quarantine(
        self,
        block: str | None = None,
        run_id: str | None = None,
        status: QuarantineStatus | None = None,
    ) -> list[QuarantineRecord]:
        return self._read(lambda g: g.quarantine.list(block=block, run_id=run_id, status=status))

    def events(
        self, kind: EventKind | None = None, block: str | None = None, run_id: str | None = None
    ) -> list[Event]:
        return self._read(lambda g: g.events.query(kind=kind, block=block, run_id=run_id))


class StandaloneRunner(ScenarioRunner):
    name = "standalone"

    def _faulted(self) -> tuple[PipelineSpec, dict[str, Any]]:
        return apply_faults(self.spec, self.faults)

    @contextmanager
    def _acting_guardian(self) -> Iterator[Guardian]:
        self.close()
        spec, registry = self._faulted()
        with Guardian(spec, self.root, registry=registry) as g:
            yield g

    def run(self, run_id: str, only: Sequence[str] | None = None) -> RunResult:
        with self._acting_guardian() as g:
            report = Executor(g).run(run_id, only=only)
        return {r.block: BlockResult(r.outcome.value, r.sources) for r in report.blocks}

    def set_block_status(self, block: str, status: BlockStatus) -> None:
        with self._acting_guardian() as g:
            g.set_block_status(block, status)

    def replay(self, block: str) -> ReplayResult:
        with self._acting_guardian() as g:
            return g.replay(block)


class DagsterRunner(ScenarioRunner):
    """Runs the same spec through the Dagster adapter with in-process execution."""

    name = "dagster"

    def _execute(self, job_name: str, **kwargs: Any) -> Any:
        from guardian.adapters.dagster import build_definitions

        self.close()
        spec, registry = StandaloneRunner._faulted(self)  # same fault wiring
        with Guardian(spec, self.root, registry=registry) as g:
            defs = build_definitions(g)
            result = defs.resolve_job_def(job_name).execute_in_process(
                raise_on_error=False, **kwargs
            )
            if not result.success:
                failures = [e.message for e in result.all_events if e.is_failure]
                raise AssertionError(f"dagster job {job_name} failed: {failures}")
            return result

    def run(self, run_id: str, only: Sequence[str] | None = None) -> RunResult:
        import dagster as dg

        from guardian.adapters.dagster import PIPELINE_JOB
        from guardian.adapters.dagster.io_manager import RUN_ID_TAG

        selection = None if only is None else [dg.AssetKey(b) for b in only]
        result = self._execute(PIPELINE_JOB, tags={RUN_ID_TAG: run_id}, asset_selection=selection)
        outcomes = {
            ev.asset_key.to_user_string(): ev.metadata["outcome"].value
            for ev in result.get_asset_check_evaluations()
        }
        resolved = self.events(EventKind.RESOLVE, run_id=run_id) + self.events(
            EventKind.REROUTE, run_id=run_id
        )
        out: RunResult = {}
        for block in self.spec.blocks:
            if only is not None and block.name not in only:
                continue
            by_upstream = {e.data["upstream"]: e for e in resolved if e.block == block.name}
            sources = tuple(
                DataRef(
                    block=e.data["source"],
                    run_id=e.data["source_run_id"],
                    requested=e.data["upstream"],
                    adapter=e.data["adapter"],
                    stale=e.data["stale"],
                )
                for up in block.inputs
                if (e := by_upstream.get(up)) is not None
            )
            out[block.name] = BlockResult(outcomes[block.name], sources)
        return out

    def set_block_status(self, block: str, status: BlockStatus) -> None:
        # A human action, not part of a Dagster run: it goes straight to core.
        self.close()
        with Guardian(self.spec, self.root) as g:
            g.set_block_status(block, status)

    def replay(self, block: str) -> ReplayResult:
        from guardian.adapters.dagster import REPLAY_JOB
        from guardian.adapters.dagster.replay import replay_run_config

        result = self._execute(REPLAY_JOB, run_config=replay_run_config(block))
        summary = result.output_for_node("guardian_replay_op")
        snapshot = (
            DataRef(block, summary["snapshot_run_id"]) if summary["snapshot_run_id"] else None
        )
        return ReplayResult(
            block, summary["replayed"], summary["still_failing"], snapshot, summary["merged"]
        )
