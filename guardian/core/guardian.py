"""The Guardian facade: the single entry point runners and adapters call."""

from __future__ import annotations

import functools
import json
import os
import traceback
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import numpy as np
import pandas as pd

from guardian.core.events import EventKind, EventLogger
from guardian.core.models import (
    DEFAULT_STORAGE_ROOT,
    Action,
    BlockStatus,
    DataRef,
    Decision,
    NoSafeInputError,
    PipelineSpec,
    ReplayResult,
    validate_name,
)
from guardian.core.planner import plan_input
from guardian.core.quarantine import (
    DuckDBQuarantineStore,
    QuarantineEntry,
    QuarantineStore,
    restore_dtypes,
    rows_to_payloads,
)
from guardian.core.refs import load_ref
from guardian.core.snapshots import LocalParquetSnapshotStore, SnapshotStore
from guardian.core.validation import (
    REASON_COL,
    RULE_COL,
    PanderaValidator,
    PassthroughValidator,
    ValidationResult,
    Validator,
    schema_failure,
)

ROLLBACK_RULE = "rollback"
CRASH_REASON = "crash"


@runtime_checkable
class BlockStatusStore(Protocol):
    def get(self, block: str) -> BlockStatus: ...

    def set(self, block: str, status: BlockStatus) -> None: ...

    def all(self) -> dict[str, BlockStatus]: ...


class JsonBlockStatusStore:
    """Block statuses in ``<root>/block_status.json``; unknown blocks are HEALTHY."""

    def __init__(self, root: Path | str) -> None:
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / "block_status.json"

    def all(self) -> dict[str, BlockStatus]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return {block: BlockStatus(value) for block, value in raw.items()}

    def get(self, block: str) -> BlockStatus:
        return self.all().get(block, BlockStatus.HEALTHY)

    def set(self, block: str, status: BlockStatus) -> None:
        statuses = {k: v.value for k, v in self.all().items()}
        statuses[validate_name(block, "block name")] = BlockStatus(status).value
        tmp = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(statuses, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


class Guardian:
    def __init__(
        self,
        spec: PipelineSpec,
        root: Path | str = DEFAULT_STORAGE_ROOT,
        *,
        snapshots: SnapshotStore | None = None,
        quarantine: QuarantineStore | None = None,
        events: EventLogger | None = None,
        statuses: BlockStatusStore | None = None,
        registry: Mapping[str, Any] | None = None,
        sample_rate: float = 1.0,
    ) -> None:
        self.spec = spec
        self.root = Path(root)
        self.snapshots = snapshots or LocalParquetSnapshotStore(self.root)
        self.quarantine = quarantine or DuckDBQuarantineStore(self.root)
        self.events = events or EventLogger(self.root, sample_rate=sample_rate)
        self.statuses = statuses or JsonBlockStatusStore(self.root)
        self.registry = dict(registry or {})
        self._validators: dict[str, Validator] = {}

    # ------------------------------------------------------------------ plumbing

    def close(self) -> None:
        self.events.close()

    def __enter__(self) -> Guardian:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def resolve(self, ref: str) -> Any:
        return load_ref(ref, self.registry)

    def block_fn(self, block: str) -> Callable[..., pd.DataFrame]:
        """The block's function with its spec ``params`` bound as keyword arguments."""
        spec = self.spec.block(block)
        fn = self.resolve(spec.fn)
        return functools.partial(fn, **spec.params) if spec.params else fn

    def validator_for(self, block: str) -> Validator:
        if block not in self._validators:
            schema_ref = self.spec.block(block).schema
            if schema_ref is None:
                self._validators[block] = PassthroughValidator()
            else:
                self._validators[block] = _as_validator(self.resolve(schema_ref))
        return self._validators[block]

    # ------------------------------------------------------------------ status

    def status(self, block: str) -> BlockStatus:
        self.spec.block(block)
        return self.statuses.get(block)

    def all_statuses(self) -> dict[str, BlockStatus]:
        stored = self.statuses.all()
        return {b: stored.get(b, BlockStatus.HEALTHY) for b in self.spec.block_names}

    def set_block_status(self, block: str, status: BlockStatus | str) -> None:
        """Human control: take a block OUT for optimization, or return it to HEALTHY."""
        status = BlockStatus(status)
        if status is BlockStatus.DEGRADED:
            raise ValueError("DEGRADED is set by Guardian on rollback, not manually")
        self._set_status(block, status, reason="manual")

    def _set_status(self, block: str, status: BlockStatus, *, reason: str, run_id=None) -> None:
        previous = self.status(block)
        if previous is status:
            return
        self.statuses.set(block, status)
        self.events.emit(
            EventKind.STATUS_CHANGE,
            block=block,
            run_id=run_id,
            previous=previous.value,
            status=status.value,
            reason=reason,
        )

    def _mark_after_run(self, block: str, status: BlockStatus, run_id: str) -> None:
        # A block a human took OUT stays OUT until a human brings it back.
        if self.status(block) is not BlockStatus.OUT:
            self._set_status(block, status, reason="run", run_id=run_id)

    # ------------------------------------------------------------------ outputs

    def run_block(self, block: str, run_id: str, inputs: Sequence[pd.DataFrame] = ()) -> Decision:
        """Call the block's function on ``inputs`` and hand the result to on_output.

        An exception from the function becomes a ROLLBACK with reason "crash".
        """
        try:
            out = self.block_fn(block)(*inputs)
            if not isinstance(out, pd.DataFrame):
                raise TypeError(f"block {block!r} returned {type(out).__name__}, not DataFrame")
        except Exception as exc:
            return self.on_crash(block, run_id, exc)
        return self.on_output(block, run_id, out)

    def on_crash(self, block: str, run_id: str, error: BaseException) -> Decision:
        self.spec.block(block)
        self.events.emit(
            EventKind.ERROR,
            block=block,
            run_id=run_id,
            error=f"{type(error).__name__}: {error}",
            traceback="".join(traceback.format_exception(error))[-4000:],
        )
        return self._rollback(block, run_id, CRASH_REASON, total=0, good=0, bad=0)

    def on_output(self, block: str, run_id: str, df: pd.DataFrame) -> Decision:
        spec = self.spec.block(block)
        validate_name(run_id, "run_id")
        total = len(df)
        try:
            result = self.validator_for(block).validate(df)
        except Exception as exc:
            result = schema_failure(df, f"validator raised {type(exc).__name__}: {exc}")

        n_bad = len(result.bad)
        n_good = total - n_bad
        self.events.emit(
            EventKind.VALIDATION,
            block=block,
            run_id=run_id,
            total=total,
            good=n_good,
            bad=n_bad,
            schema_error=result.schema_error,
        )

        if result.schema_error is not None:
            self._quarantine(block, run_id, result.bad, df)
            return self._rollback(
                block, run_id, result.schema_error, total=total, good=0, bad=total
            )

        fraction = n_bad / total if total else 0.0
        if fraction <= spec.quarantine_threshold:
            ref = self.snapshots.write(block, run_id, result.good)
            self._quarantine(block, run_id, result.bad, df)
            self.snapshots.mark_last_good(block, run_id)
            self.events.emit(
                EventKind.SNAPSHOT, block=block, run_id=run_id, rows=n_good, last_good=True
            )
            self._mark_after_run(block, BlockStatus.HEALTHY, run_id)
            return Decision(block, run_id, Action.PASS, None, total, n_good, n_bad, snapshot=ref)

        # Too many bad rows: nothing is promoted, so the good rows are quarantined too
        # (rule "rollback") to keep the no-silent-loss invariant; replay restores them.
        reason = f"bad-row fraction {fraction:.4f} exceeds threshold {spec.quarantine_threshold}"
        good_rows = _with_rule(df.iloc[~_bad_mask(df, result)], ROLLBACK_RULE, reason)
        self._quarantine(block, run_id, pd.concat([result.bad, good_rows]), df)
        return self._rollback(block, run_id, reason, total=total, good=n_good, bad=n_bad)

    def _rollback(
        self, block: str, run_id: str, reason: str, *, total: int, good: int, bad: int
    ) -> Decision:
        last_good = self.snapshots.last_good(block)
        self._mark_after_run(block, BlockStatus.DEGRADED, run_id)
        self.events.emit(
            EventKind.ROLLBACK,
            block=block,
            run_id=run_id,
            reason=reason,
            last_good_run_id=last_good.run_id if last_good else None,
        )
        return Decision(block, run_id, Action.ROLLBACK, reason, total, good, bad, last_good)

    def _quarantine(
        self, block: str, run_id: str, bad: pd.DataFrame, original: pd.DataFrame
    ) -> None:
        if bad.empty:
            return
        payloads = rows_to_payloads(bad[list(original.columns)])
        entries = [
            QuarantineEntry(block, run_id, str(rule), str(reason), payload)
            for rule, reason, payload in zip(bad[RULE_COL], bad[REASON_COL], payloads, strict=True)
        ]
        self.quarantine.add(entries)
        rules = bad[RULE_COL].value_counts().to_dict()
        self.events.emit(
            EventKind.QUARANTINE, block=block, run_id=run_id, rows=len(entries), rules=rules
        )

    # ------------------------------------------------------------------ inputs

    def resolve_input(self, block: str, upstream: str, run_id: str | None = None) -> DataRef:
        try:
            ref = plan_input(self.spec, block, upstream, self.status, self.snapshots.last_good)
        except NoSafeInputError as exc:
            self.events.emit(
                EventKind.ERROR, block=block, run_id=run_id, upstream=upstream, error=str(exc)
            )
            raise
        self.events.emit(
            EventKind.REROUTE if ref.rerouted else EventKind.RESOLVE,
            block=block,
            run_id=run_id,
            upstream=upstream,
            upstream_status=self.status(upstream).value,
            source=ref.block,
            source_run_id=ref.run_id,
            adapter=ref.adapter,
            stale=ref.stale,
        )
        return ref

    def read(self, ref: DataRef) -> pd.DataFrame:
        """Load the snapshot behind ``ref`` and apply its adapter transform, if any."""
        df = self.snapshots.read(ref.block, ref.run_id)
        if ref.adapter:
            df = self.resolve(ref.adapter)(df)
        return df

    def load_input(self, block: str, upstream: str, run_id: str | None = None) -> pd.DataFrame:
        return self.read(self.resolve_input(block, upstream, run_id))

    # ------------------------------------------------------------------ replay

    def replay(self, block: str, run_id: str | None = None) -> ReplayResult:
        """Re-run ``block``'s function on its QUARANTINED records.

        Rows whose output passes validation are merged with the block's last-good
        snapshot into a new last-good snapshot and marked REPLAYED; the rest stay
        QUARANTINED. The block function must preserve the index (row-wise
        transforms do), which is how output rows map back to quarantine records.
        """
        self.spec.block(block)
        run_id = run_id or new_run_id("replay")
        validate_name(run_id, "run_id")
        ids, frame = self.quarantine.load_frame(block)
        if not ids:
            return ReplayResult(block, replayed=0, still_failing=0)
        frame.index = pd.Index(ids, name=None)
        # Payloads are the block's own output rows; the last-good snapshot of the same
        # block tells us their original dtypes (JSON loses them).
        base_ref = self.snapshots.last_good(block)
        base = self.snapshots.read(base_ref.block, base_ref.run_id) if base_ref else None
        if base is not None:
            frame = restore_dtypes(frame, dict(base.dtypes))

        try:
            out = self.block_fn(block)(frame)
            if not isinstance(out, pd.DataFrame):
                raise TypeError(f"block {block!r} returned {type(out).__name__}, not DataFrame")
            result = self.validator_for(block).validate(out[out.index.isin(ids)])
        except Exception as exc:
            self.events.emit(
                EventKind.ERROR,
                block=block,
                run_id=run_id,
                phase="replay",
                error=f"{type(exc).__name__}: {exc}",
            )
            return self._replay_done(block, run_id, [], len(ids), None)

        if result.schema_error is not None:
            self.events.emit(
                EventKind.ERROR,
                block=block,
                run_id=run_id,
                phase="replay",
                error=result.schema_error,
            )
            return self._replay_done(block, run_id, [], len(ids), None)

        bad_ids = set(result.bad.index)
        passing = [i for i in dict.fromkeys(result.good.index) if i not in bad_ids]
        if not passing:
            return self._replay_done(block, run_id, [], len(ids), None)

        replayed_rows = result.good[result.good.index.isin(passing)]
        if base is not None:
            replayed_rows = restore_dtypes(replayed_rows, dict(base.dtypes))
        merged = pd.concat(
            [*([base] if base is not None else []), replayed_rows], ignore_index=True
        )
        ref = self.snapshots.write(block, run_id, merged)
        self.snapshots.mark_last_good(block, run_id)
        changed = self.quarantine.mark_replayed(passing, replay_run_id=run_id)
        self._mark_after_run(block, BlockStatus.HEALTHY, run_id)
        return self._replay_done(block, run_id, passing, len(ids) - changed, ref, changed)

    def _replay_done(
        self,
        block: str,
        run_id: str,
        passing: list[Any],
        still_failing: int,
        ref: DataRef | None,
        replayed: int | None = None,
    ) -> ReplayResult:
        replayed = len(passing) if replayed is None else replayed
        self.events.emit(
            EventKind.REPLAY,
            block=block,
            run_id=run_id,
            replayed=replayed,
            still_failing=still_failing,
            snapshot_run_id=ref.run_id if ref else None,
        )
        return ReplayResult(block, replayed, still_failing, ref)


def _as_validator(schema: Any) -> Validator:
    """Wrap pandera schemas/models; accept ready-made Validator objects as-is.

    Pandera schemas are checked first because they also have a ``validate`` method.
    """
    try:
        return PanderaValidator(schema)
    except TypeError:
        if isinstance(schema, Validator):
            return schema
        raise


def _bad_mask(df: pd.DataFrame, result: ValidationResult) -> Any:
    """Boolean mask over ``df``'s rows marking the rows the validator rejected."""
    if result.bad_positions is not None:
        mask = np.zeros(len(df), dtype=bool)
        mask[list(result.bad_positions)] = True
        return mask
    if not df.index.is_unique:
        raise ValueError("validator did not report bad_positions and the index is not unique")
    return df.index.isin(result.bad.index)


def _with_rule(rows: pd.DataFrame, rule: str, reason: str) -> pd.DataFrame:
    out = rows.copy()
    out[RULE_COL] = rule
    out[REASON_COL] = reason
    return out


__all__ = [
    "CRASH_REASON",
    "ROLLBACK_RULE",
    "BlockStatusStore",
    "Guardian",
    "JsonBlockStatusStore",
    "ValidationResult",
    "new_run_id",
]
