"""Runner abstraction for the scenario suite.

A runner drives the demo pipeline through one execution mode (standalone, Dagster,
...). Actions (run, status changes, replay, faults) are runner-specific; reads go
through the shared Guardian stores, so assertions are identical for every runner.
"""

from __future__ import annotations

import abc
import dataclasses
from collections.abc import Callable, Iterator
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
from guardian.demo.faults import Fault, inject
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
    def run(self, run_id: str) -> RunResult: ...

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
        """Spec whose faulted blocks point at wrapped functions in a registry."""
        registry: dict[str, Any] = {}
        blocks = []
        for block in self.spec.blocks:
            if block.name in self.faults:
                key = f"__faulted__:{block.name}"
                registry[key] = inject(load_ref(block.fn), *self.faults[block.name])
                block = dataclasses.replace(block, fn=key)
            blocks.append(block)
        return dataclasses.replace(self.spec, blocks=tuple(blocks)), registry

    @contextmanager
    def _acting_guardian(self) -> Iterator[Guardian]:
        self.close()
        spec, registry = self._faulted()
        with Guardian(spec, self.root, registry=registry) as g:
            yield g

    def run(self, run_id: str) -> RunResult:
        with self._acting_guardian() as g:
            report = Executor(g).run(run_id)
        return {r.block: BlockResult(r.outcome.value, r.sources) for r in report.blocks}

    def set_block_status(self, block: str, status: BlockStatus) -> None:
        with self._acting_guardian() as g:
            g.set_block_status(block, status)

    def replay(self, block: str) -> ReplayResult:
        with self._acting_guardian() as g:
            return g.replay(block)
