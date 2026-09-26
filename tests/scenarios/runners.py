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
from guardian.core.guardian import Guardian, PromotionResult
from guardian.core.models import (
    BlockStatus,
    DataRef,
    PipelineSpec,
    QuarantineRecord,
    QuarantineStatus,
    ReplayResult,
)
from guardian.core.refs import load_ref
from guardian.core.shadow import Shadow, ShadowRun
from guardian.core.versions import Promotion
from guardian.demo.faults import Fault, apply_faults
from guardian.runner.executor import Executor
from guardian.runner.spec_loader import load_spec

DEMO_SPEC_PATH = Path(load_ref("guardian.demo:__file__")).parent / "pipeline.yaml"


def scenario_spec(rows: int = 200, required_runs: int = 2) -> PipelineSpec:
    """The demo spec with clean (error-free) input, so every quarantine is a fault.

    Source blocks generate ``rows`` rows. Versioned blocks auto-promote after
    ``required_runs`` shadow runs (fewer than the default 3, to keep scenarios fast;
    the scenarios read it from the policy).
    """
    spec = load_spec(DEMO_SPEC_PATH)
    blocks = []
    for block in spec.blocks:
        if not block.inputs:
            block = dataclasses.replace(
                block, params={**block.params, "rows": rows, "error_rate": 0.0}
            )
        if block.versions:
            block = dataclasses.replace(
                block, shadow=dataclasses.replace(block.shadow, required_runs=required_runs)
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

    # -------------------------------------------------------------- faults

    def _faulted(self) -> tuple[PipelineSpec, dict[str, Any]]:
        """The spec with faults wrapping each block's live version."""
        active = self._read(lambda g: {b: g.active_version(b) for b in self.spec.block_names})
        return apply_faults(self.spec, self.faults, active)

    @contextmanager
    def _acting_guardian(self) -> Iterator[Guardian]:
        spec, registry = self._faulted()
        self.close()
        with Guardian(spec, self.root, registry=registry) as g:
            yield g

    # -------------------------------------------------------------- shadow versions
    # Human actions: every runner performs them through core, like `guardian shadow`.

    def shadow_start(self, block: str, version: str, *, expect_diff: bool = False) -> None:
        with self._acting_guardian() as g:
            g.shadow_start(block, version, expect_diff=expect_diff)

    def shadow_stop(self, block: str) -> None:
        with self._acting_guardian() as g:
            g.shadow_stop(block)

    def promote(
        self, block: str, *, approve: bool = False, interrupt: str | None = None
    ) -> PromotionResult:
        """Promote; ``interrupt`` simulates a crash "during_replay" (before quarantine
        records are marked) or "after_replay" (before the registry completes)."""
        with self._acting_guardian() as g:
            if interrupt == "during_replay":
                g.quarantine.mark_replayed = _power_cut  # type: ignore[method-assign]
            elif interrupt == "after_replay":
                g.versions.complete_promotion = _power_cut  # type: ignore[method-assign]
            elif interrupt is not None:
                raise ValueError(interrupt)
            return g.promote(block, approve=approve)

    def rollback_version(self, block: str) -> Promotion:
        with self._acting_guardian() as g:
            return g.rollback_version(block)

    def active_version(self, block: str) -> str | None:
        return self._read(lambda g: g.active_version(block))

    def shadow_runs(self, block: str) -> tuple[Shadow | None, list[ShadowRun]]:
        return self._read(lambda g: g.shadow_runs(block))

    def pending_promotion(self, block: str) -> Promotion | None:
        return self._read(lambda g: g.versions.pending(block))

    def promotions(self, block: str) -> list[Promotion]:
        return self._read(lambda g: g.versions.history(block))

    def candidate_snapshot(self, block: str, version: str, run_id: str) -> pd.DataFrame | None:
        def read(g: Guardian) -> pd.DataFrame | None:
            store = g.candidate_store(version)
            return store.read(block, run_id) if store.exists(block, run_id) else None

        return self._read(read)

    def candidate_provenance(self, block: str, version: str, run_id: str) -> dict | None:
        return self._read(lambda g: g.candidate_store(version).read_provenance(block, run_id))

    def provenance(self, block: str, run_id: str) -> dict | None:
        return self._read(lambda g: g.snapshots.read_provenance(block, run_id))


def _power_cut(*args: Any, **kwargs: Any) -> Any:
    raise RuntimeError("power cut")


class StandaloneRunner(ScenarioRunner):
    name = "standalone"

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

        spec, registry = self._faulted()  # same fault wiring
        self.close()
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
        from guardian.adapters.dagster.checks import CHECK_NAME

        outcomes = {
            ev.asset_key.to_user_string(): ev.metadata["outcome"].value
            for ev in result.get_asset_check_evaluations()
            if ev.check_name == CHECK_NAME
        }
        self.last_result = result  # for tests that inspect Dagster metadata
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
