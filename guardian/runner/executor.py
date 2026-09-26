"""Standalone executor: runs a pipeline in topological order through Guardian hooks.

All repair logic (validation, quarantine, rollback, rerouting) lives in core; this
module only sequences blocks and records what happened.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum

import pandas as pd

from guardian.core.events import EventKind
from guardian.core.guardian import Guardian, new_run_id
from guardian.core.models import Action, BlockStatus, DataRef, Decision, NoSafeInputError
from guardian.runner.spec_loader import topological_order


class Outcome(StrEnum):
    PASS = "PASS"
    ROLLBACK = "ROLLBACK"
    SKIPPED = "SKIPPED"  # block is OUT
    BLOCKED = "BLOCKED"  # no safe input for at least one upstream


@dataclass(frozen=True)
class BlockReport:
    block: str
    outcome: Outcome
    status: BlockStatus
    rows_in: int = 0
    rows_out: int = 0
    quarantined: int = 0
    sources: tuple[DataRef, ...] = ()
    reason: str | None = None
    decision: Decision | None = None
    seconds: float = 0.0

    @property
    def rerouted(self) -> bool:
        return any(ref.rerouted for ref in self.sources)

    @property
    def stale(self) -> bool:
        return any(ref.stale for ref in self.sources)


@dataclass
class RunReport:
    run_id: str
    pipeline: str
    blocks: list[BlockReport] = field(default_factory=list)

    def get(self, block: str) -> BlockReport:
        return next(r for r in self.blocks if r.block == block)

    @property
    def ok(self) -> bool:
        return all(r.outcome is Outcome.PASS for r in self.blocks)


class Executor:
    def __init__(self, guardian: Guardian) -> None:
        self.guardian = guardian
        self.order = topological_order(guardian.spec)

    def run(self, run_id: str | None = None) -> RunReport:
        g = self.guardian
        run_id = run_id or new_run_id()
        report = RunReport(run_id=run_id, pipeline=g.spec.name)
        g.events.emit(EventKind.RUN_STARTED, run_id=run_id, pipeline=g.spec.name)
        for block in self.order:
            report.blocks.append(self._run_one(block, run_id))
        g.events.emit(
            EventKind.RUN_FINISHED,
            run_id=run_id,
            pipeline=g.spec.name,
            outcomes={r.block: r.outcome.value for r in report.blocks},
        )
        return report

    def _run_one(self, block: str, run_id: str) -> BlockReport:
        g = self.guardian
        spec = g.spec.block(block)
        if g.status(block) is BlockStatus.OUT:
            return BlockReport(
                block, Outcome.SKIPPED, BlockStatus.OUT, reason="taken out (status OUT)"
            )

        started = time.perf_counter()
        g.events.emit(EventKind.BLOCK_STARTED, block=block, run_id=run_id)
        refs: list[DataRef] = []
        frames: list[pd.DataFrame] = []
        try:
            for upstream in spec.inputs:
                ref = g.resolve_input(block, upstream, run_id)
                refs.append(ref)
                frames.append(g.read(ref))
        except NoSafeInputError as exc:
            return BlockReport(
                block,
                Outcome.BLOCKED,
                g.status(block),
                sources=tuple(refs),
                reason=str(exc),
                seconds=time.perf_counter() - started,
            )

        decision = g.run_block(block, run_id, frames)
        passed = decision.action is Action.PASS
        report = BlockReport(
            block=block,
            outcome=Outcome.PASS if passed else Outcome.ROLLBACK,
            status=g.status(block),
            rows_in=sum(len(f) for f in frames),
            rows_out=decision.good_rows if passed else 0,
            quarantined=decision.bad_rows if passed else decision.total_rows,
            sources=tuple(refs),
            reason=decision.reason,
            decision=decision,
            seconds=time.perf_counter() - started,
        )
        g.events.emit(
            EventKind.BLOCK_FINISHED,
            block=block,
            run_id=run_id,
            outcome=report.outcome.value,
            rows_in=report.rows_in,
            rows_out=report.rows_out,
            quarantined=report.quarantined,
            seconds=round(report.seconds, 4),
        )
        return report
