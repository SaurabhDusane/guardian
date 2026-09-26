"""The Guardian facade: the single entry point runners and adapters call."""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import json
import os
import traceback
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import numpy as np
import pandas as pd

from guardian.core.code import CodeFingerprint, CodeStore, fingerprint
from guardian.core.drift import DRIFT_RULE, DriftLevel, DriftReport, DriftStore
from guardian.core.drift import check as check_drift
from guardian.core.events import EventKind, EventLogger
from guardian.core.models import (
    DEFAULT_STORAGE_ROOT,
    Action,
    BlockCrash,
    BlockSkipped,
    BlockStatus,
    DataRef,
    Decision,
    GuardianError,
    NoSafeInputError,
    PipelineSpec,
    Quality,
    ReplayResult,
    validate_name,
)
from guardian.core.planner import plan_input
from guardian.core.provenance import (
    CANDIDATE,
    LIVE,
    QUALITY_COL,
    BlockRun,
    DuckDBProvenanceStore,
    InputProvenance,
    Provenance,
    ProvenanceStore,
)
from guardian.core.quarantine import (
    DuckDBQuarantineStore,
    QuarantineEntry,
    QuarantineStore,
    restore_dtypes,
    rows_to_payloads,
)
from guardian.core.refs import load_ref
from guardian.core.shadow import (
    Shadow,
    ShadowMode,
    ShadowRun,
    ShadowStatus,
    ShadowStore,
    auto_promotable,
    column_stats,
    compare_rows,
    evaluate,
)
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
from guardian.core.versions import Promotion, PromotionKind, PromotionState, VersionRegistry

ROLLBACK_RULE = "rollback"
CRASH_REASON = "crash"
CANDIDATES_DIR = "candidates"


class PromotionError(GuardianError):
    """A promotion is not allowed (yet), e.g. it needs an explicit approval."""


@dataclass(frozen=True)
class PromotionResult:
    block: str
    from_version: str | None
    to_version: str
    reason: str
    replay: ReplayResult
    resumed: bool


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


def _in_session(method: Callable[..., Any]) -> Callable[..., Any]:
    """Run a Guardian method as one unit of work (see Guardian.session)."""

    @functools.wraps(method)
    def wrapper(self: Guardian, *args: Any, **kwargs: Any) -> Any:
        with self.session():
            return method(self, *args, **kwargs)

    return wrapper


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
        provenance: ProvenanceStore | None = None,
        diagnoser: Callable[[Guardian, str, str], Any] | None = None,
    ) -> None:
        self.spec = spec
        self.root = Path(root)
        self.snapshots = snapshots or LocalParquetSnapshotStore(self.root)
        self.quarantine = quarantine or DuckDBQuarantineStore(self.root)
        self.events = events or EventLogger(self.root, sample_rate=sample_rate)
        self.statuses = statuses or JsonBlockStatusStore(self.root)
        self.registry = dict(registry or {})
        self.versions = VersionRegistry(self.root)
        self.provenance = provenance or DuckDBProvenanceStore(self.root)
        self.code = CodeStore(self.root)
        self.drift = DriftStore(self.root)
        self._code_cache: dict[str, tuple[Any, CodeFingerprint]] = {}
        # Called as diagnoser(guardian, block, run_id) after a ROLLBACK of a block with
        # ``auto_diagnose``. Advisory only: whatever it does or raises, the run goes on.
        self.diagnoser = diagnoser
        self._quality_cache: dict[tuple[str, str], Quality] = {}
        self.shadows = ShadowStore(self.root)
        self._validators: dict[str, Validator] = {}
        self._candidate_stores: dict[str, LocalParquetSnapshotStore] = {}
        self.decisions: dict[tuple[str, str], Decision] = {}
        # Inputs each block resolved per run (with the upstream's status at that time):
        # provenance for the snapshots it writes.
        self._resolved: dict[tuple[str, str], dict[str, tuple[DataRef, str]]] = {}
        # Per-instance caches of the version registry and shadow store (both change
        # only through this instance's own shadow/promotion calls during a run).
        self._active_cache: dict[str, str | None] = {}
        self._shadow_cache: dict[str, Shadow | None] = {}
        # Inputs staged by begin_block for complete_block (same process, same step), and
        # each block's shadow comparison per run, for adapters to report.
        self._staged: dict[tuple[str, str], Any] = {}
        self.shadow_results: dict[tuple[str, str], ShadowRun | None] = {}

    # ------------------------------------------------------------------ plumbing

    def close(self) -> None:
        self.events.close()

    @contextlib.contextmanager
    def session(self) -> Iterator[None]:
        """One unit of work (a pipeline run, a block, a promotion): each store keeps a
        single connection open until it ends, instead of one per operation. Outside a
        session no store holds its file, so other processes can always open it."""
        with contextlib.ExitStack() as stack:
            for store in (self.provenance, self.quarantine, self.versions, self.shadows):
                session = getattr(store, "session", None)
                if session is not None:
                    stack.enter_context(session())
            yield

    def __enter__(self) -> Guardian:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def resolve(self, ref: str) -> Any:
        return load_ref(ref, self.registry)

    def active_version(self, block: str) -> str | None:
        """The live version: the registry's if it has one, else the spec's ``active``.

        During an unfinished promotion (PROMOTING) the previous version stays live.
        """
        spec = self.spec.block(block)
        if block not in self._active_cache:
            registered = self.versions.get(block)
            self._active_cache[block] = (
                registered.version
                if registered is not None and registered.version in spec.versions
                else spec.active
            )
        return self._active_cache[block]

    def _forget_versions(self, block: str) -> None:
        self._active_cache.pop(block, None)
        self._shadow_cache.pop(block, None)

    def version_fn(self, block: str, version: str | None = None) -> Callable[..., pd.DataFrame]:
        """A version's function (default: the live one), with spec ``params`` bound.

        For a source block with ``load``, the params belong to the loader instead and
        every version receives the loaded frame as its only argument.
        """
        spec = self.spec.block(block)
        ref = spec.version_ref(version if version is not None else self.active_version(block))
        fn = self.resolve(ref)
        if spec.params and spec.load is None:
            return functools.partial(fn, **spec.params)
        return fn

    def block_fn(self, block: str) -> Callable[..., pd.DataFrame]:
        """The live version's function."""
        return self.version_fn(block)

    def candidate_store(self, version: str) -> LocalParquetSnapshotStore:
        """Where candidate snapshots of ``version`` live; never read by resolve_input."""
        if version not in self._candidate_stores:
            root = self.root / CANDIDATES_DIR / validate_name(version, "version name")
            self._candidate_stores[version] = LocalParquetSnapshotStore(root)
        return self._candidate_stores[version]

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

    def skip_reason(self, block: str) -> str | None:
        """Why ``block`` must not run this time, or None if it should run."""
        if self.status(block) is BlockStatus.OUT:
            return "taken out (status OUT)"
        return None

    def prepare_inputs(self, block: str, inputs: Sequence[Any] = ()) -> Any:
        """The inputs every version of ``block`` runs on this run.

        For a source block with ``load``, the loader runs once and its frame is the
        single input (so a shadow candidate sees the same loaded data). Returns a
        ``BlockCrash`` if loading fails.
        """
        spec = self.spec.block(block)
        if spec.load is None or inputs:
            return list(inputs)
        try:
            raw = self.resolve(spec.load)(**spec.params)
            if not isinstance(raw, pd.DataFrame):
                raise TypeError(f"loader of {block!r} returned {type(raw).__name__}")
        except Exception as exc:
            return BlockCrash(exc)
        return [raw]

    def compute(self, block: str, inputs: Sequence[Any] = ()) -> Any:
        """Run ``block``'s function; never raises for block failures.

        Returns the output DataFrame, a ``BlockCrash`` if the function raised or did
        not return a DataFrame, or a ``BlockSkipped`` if the block is OUT or any input
        is itself a ``BlockSkipped`` (no safe input could be resolved).
        """
        if isinstance(inputs, BlockCrash):
            return inputs
        reason = self.skip_reason(block)
        if reason is not None:
            return BlockSkipped(reason)
        inputs = self.prepare_inputs(block, inputs)
        if isinstance(inputs, BlockCrash):
            return inputs
        for item in inputs:
            if isinstance(item, BlockSkipped):
                return BlockSkipped(item.reason, blocked=True)
        try:
            out = self.block_fn(block)(*inputs)
            if not isinstance(out, pd.DataFrame):
                raise TypeError(f"block {block!r} returned {type(out).__name__}, not DataFrame")
        except Exception as exc:
            return BlockCrash(exc)
        return out

    def handle_result(self, block: str, run_id: str, result: Any) -> Decision | None:
        """Dispatch a ``compute`` result: DataFrame -> on_output, crash -> on_crash.

        Skipped blocks produce no decision. Decisions are also kept in
        ``self.decisions`` so adapters can report them after the fact.
        """
        if isinstance(result, BlockSkipped):
            return None
        if isinstance(result, BlockCrash):
            decision = self.on_crash(block, run_id, result.error)
        elif isinstance(result, pd.DataFrame):
            decision = self.on_output(block, run_id, result)
        else:
            raise TypeError(f"unexpected result for block {block!r}: {type(result).__name__}")
        self.decisions[(block, run_id)] = decision
        return decision

    def begin_block(self, block: str, run_id: str, frames: Sequence[Any] = ()) -> Any:
        """First half of executing a block on a run; every runner/adapter calls it.

        Prepares the inputs every version runs on (loading a source block's data once),
        stages them for ``complete_block`` and returns the live version's ``compute``
        result. A block that is OUT with no candidate in shadow loads nothing.
        """
        # Re-read this block's version and shadow state: another process (the CLI) may
        # have promoted, rolled back or started a shadow since this Guardian was built.
        self._forget_versions(block)
        runs_anything = self.skip_reason(block) is None or self.shadow_candidate(block) is not None
        blocked = any(isinstance(f, BlockSkipped) for f in frames)
        inputs = (
            self.prepare_inputs(block, frames) if runs_anything and not blocked else list(frames)
        )
        self._staged[(block, run_id)] = inputs
        return self.compute(block, inputs)

    @_in_session
    def complete_block(
        self, block: str, run_id: str, result: Any, inputs: Any = None
    ) -> tuple[Decision | None, ShadowRun | None]:
        """Second half: handle the live result, then run the shadow candidate (if any)
        on the same inputs. Auto-promotion may happen here.

        ``inputs`` defaults to what ``begin_block`` staged for (block, run_id).
        """
        staged = self._staged.pop((block, run_id), None)
        inputs = staged if inputs is None else inputs
        decision = self.handle_result(block, run_id, result)
        if isinstance(result, BlockSkipped):
            outcome = "BLOCKED" if result.blocked else "SKIPPED"
            self._record_run(block, run_id, outcome, result.reason)
        shadow = None if inputs is None else self.run_shadow(block, run_id, inputs, decision)
        self.shadow_results[(block, run_id)] = shadow
        if decision is not None and decision.action is Action.ROLLBACK:
            self._auto_diagnose(block, run_id)
        return decision, shadow

    def _auto_diagnose(self, block: str, run_id: str) -> None:
        """Run the diagnoser for an ``auto_diagnose`` block; never fails the pipeline."""
        if not self.spec.block(block).auto_diagnose:
            return
        if self.diagnoser is None:
            self.events.emit(
                EventKind.WARN,
                block=block,
                run_id=run_id,
                warning="auto_diagnose is set but no diagnoser is configured",
                agent=True,
            )
            return
        try:
            self.diagnoser(self, block, run_id)
        except Exception as exc:  # advisory: a diagnosis failure is logged, never raised
            self.events.emit(
                EventKind.WARN,
                block=block,
                run_id=run_id,
                warning="auto-diagnosis failed",
                error=f"{type(exc).__name__}: {exc}",
                agent=True,
            )

    def run_block(self, block: str, run_id: str, inputs: Sequence[pd.DataFrame] = ()) -> Decision:
        """Call the block's function on ``inputs`` and hand the result to on_output.

        An exception from the function becomes a ROLLBACK with reason "crash".
        """
        result = self.compute(block, inputs)
        if isinstance(result, BlockSkipped):
            raise ValueError(f"block {block!r} cannot run: {result.reason}")
        decision = self.handle_result(block, run_id, result)
        assert decision is not None
        return decision

    @_in_session
    def on_crash(self, block: str, run_id: str, error: BaseException) -> Decision:
        self.spec.block(block)
        self.events.emit(
            EventKind.ERROR,
            block=block,
            run_id=run_id,
            error=f"{type(error).__name__}: {error}",
            traceback="".join(traceback.format_exception(error))[-4000:],
        )
        decision = self._rollback(block, run_id, CRASH_REASON, total=0, good=0, bad=0)
        self._record_run(block, run_id, "ROLLBACK", CRASH_REASON)
        return decision

    @_in_session
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
        provenance = self._run_provenance(block, run_id)
        self.events.emit(
            EventKind.VALIDATION,
            block=block,
            run_id=run_id,
            total=total,
            good=n_good,
            bad=n_bad,
            schema_error=result.schema_error,
            version=provenance.version,
            quality=provenance.quality.value,
        )

        if result.schema_error is not None:
            self._quarantine(block, run_id, result.bad, df)
            self._record_output(provenance, has_snapshot=False)
            decision = self._rollback(
                block, run_id, result.schema_error, total=total, good=0, bad=total
            )
            self._record_run(block, run_id, "ROLLBACK", result.schema_error, rows=(total, 0, total))
            return decision

        fraction = n_bad / total if total else 0.0
        if fraction <= spec.quarantine_threshold and spec.drift is not None:
            report = self._check_drift(block, run_id, result.good)
            if report.level is DriftLevel.FAIL:
                # A soft failure: every row is schema-valid, but the output as a whole no
                # longer looks like this block's output. Treated like a validation
                # failure: nothing is promoted and every row is quarantined.
                reason = report.reason
                drifted = _with_rule(df.iloc[~_bad_mask(df, result)], DRIFT_RULE, reason)
                self._quarantine(block, run_id, pd.concat([result.bad, drifted]), df)
                self._record_output(provenance, has_snapshot=False)
                decision = self._rollback(
                    block, run_id, reason, total=total, good=n_good, bad=n_bad
                )
                self._record_run(block, run_id, "ROLLBACK", reason, rows=(total, 0, total))
                return decision
        if fraction <= spec.quarantine_threshold:
            good = result.good
            if spec.annotate_quality:  # opt-in: user frames are never changed by default
                good = good.assign(**{QUALITY_COL: provenance.quality.value})
            ref = self.snapshots.write(block, run_id, good)
            self._record_output(provenance, has_snapshot=True)
            self._quarantine(block, run_id, result.bad, df)
            self.snapshots.mark_last_good(block, run_id)
            self.events.emit(
                EventKind.SNAPSHOT, block=block, run_id=run_id, rows=n_good, last_good=True
            )
            self._mark_after_run(block, BlockStatus.HEALTHY, run_id)
            self._record_run(block, run_id, "PASS", rows=(total, n_good, n_bad))
            return Decision(block, run_id, Action.PASS, None, total, n_good, n_bad, snapshot=ref)

        # Too many bad rows: nothing is promoted, so the good rows are quarantined too
        # (rule "rollback") to keep the no-silent-loss invariant; replay restores them.
        reason = f"bad-row fraction {fraction:.4f} exceeds threshold {spec.quarantine_threshold}"
        good_rows = _with_rule(df.iloc[~_bad_mask(df, result)], ROLLBACK_RULE, reason)
        self._quarantine(block, run_id, pd.concat([result.bad, good_rows]), df)
        self._record_output(provenance, has_snapshot=False)
        decision = self._rollback(block, run_id, reason, total=total, good=n_good, bad=n_bad)
        self._record_run(block, run_id, "ROLLBACK", reason, rows=(total, 0, total))
        return decision

    def drift_reference(self, block: str, run_id: str) -> list[tuple[str, pd.DataFrame]]:
        """The block's last ``drift.window`` promoted run snapshots before ``run_id``."""
        policy = self.spec.block(block).drift
        window = policy.window if policy is not None else 0
        runs = [
            r.run_id
            for r in self.provenance.runs(block)
            if r.outcome == "PASS" and r.run_id != run_id and self.snapshots.exists(block, r.run_id)
        ]
        return [(r, self.snapshots.read(block, r)) for r in runs[-window:]] if window else []

    def _check_drift(self, block: str, run_id: str, good: pd.DataFrame) -> DriftReport:
        report = check_drift(
            self.spec.block(block), run_id, good, self.drift_reference(block, run_id)
        )
        self.drift.write(report)
        if report.level in (DriftLevel.WARN, DriftLevel.FAIL):
            self.events.emit(
                EventKind.DRIFT,
                block=block,
                run_id=run_id,
                level=report.level.value,
                reason=report.reason,
                columns={
                    c.column: {"psi": c.psi, "z": c.z, "level": c.level.value}
                    for c in report.drifted
                },
                reference_runs=list(report.reference_runs),
            )
        return report

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
        upstream_status = self.status(upstream).value
        if run_id is not None:
            self._resolved.setdefault((block, run_id), {})[upstream] = (ref, upstream_status)
        self.events.emit(
            EventKind.REROUTE if ref.rerouted else EventKind.RESOLVE,
            block=block,
            run_id=run_id,
            upstream=upstream,
            upstream_status=upstream_status,
            source=ref.block,
            source_run_id=ref.run_id,
            adapter=ref.adapter,
            stale=ref.stale,
            quality=self._input_quality(ref).value,
        )
        return ref

    # ------------------------------------------------------------------ provenance

    def snapshot_quality(self, block: str, run_id: str) -> Quality:
        """A live snapshot's recorded quality (FRESH if none was recorded)."""
        key = (block, run_id)
        if key not in self._quality_cache:
            record = self.provenance.get(block, run_id)
            self._quality_cache[key] = record.quality if record else Quality.FRESH
        return self._quality_cache[key]

    def _input_quality(self, ref: DataRef) -> Quality:
        """How good one input is: the read itself, and the snapshot it read."""
        return Quality.worst([ref.read_quality, self.snapshot_quality(ref.block, ref.run_id)])

    def input_provenance(self, block: str, run_id: str) -> tuple[InputProvenance, ...]:
        """The live snapshots ``block`` read on ``run_id``, in input order."""
        resolved = self._resolved.get((block, run_id), {})
        inputs = []
        for upstream in self.spec.block(block).inputs:
            if upstream not in resolved:
                continue
            ref, upstream_status = resolved[upstream]
            inputs.append(
                InputProvenance(
                    upstream=upstream,
                    source_block=ref.block,
                    source_run_id=ref.run_id,
                    adapter=ref.adapter,
                    read=ref.read_quality,
                    quality=self._input_quality(ref),
                    upstream_status=upstream_status,
                )
            )
        return tuple(inputs)

    def _run_provenance(
        self,
        block: str,
        run_id: str,
        *,
        version: str | None = None,
        kind: str = "run",
        store: str = LIVE,
    ) -> Provenance:
        inputs = self.input_provenance(block, run_id)
        return Provenance(
            block=block,
            run_id=run_id,
            kind=kind,
            store=store,
            version=version if version is not None else self.active_version(block),
            quality=Quality.worst(i.quality for i in inputs),
            inputs=inputs,
        )

    def _record_output(self, provenance: Provenance, *, has_snapshot: bool) -> None:
        provenance = dataclasses.replace(provenance, has_snapshot=has_snapshot)
        self.provenance.record(provenance)
        if provenance.store == LIVE and has_snapshot:
            self._quality_cache[(provenance.block, provenance.run_id)] = provenance.quality

    def _record_run(
        self,
        block: str,
        run_id: str,
        outcome: str,
        reason: str | None = None,
        *,
        rows: tuple[int, int, int] | None = None,
    ):
        """Record what ``block`` did on ``run_id`` and announce it (BLOCK_OUTCOME).

        ``rows`` is (output rows, promoted, quarantined). Every outcome of every runner
        passes through here, which makes BLOCK_OUTCOME the one event observability
        exporters need to describe a block run.
        """
        code = self.code_fingerprint(block)
        status = self.status(block).value
        version = self.active_version(block)
        self.provenance.record_run(
            BlockRun(
                block, run_id, outcome, status, version, reason, code=code.sha if code else None
            )
        )
        inputs = self.input_provenance(block, run_id)
        total, promoted, quarantined = rows if rows is not None else (0, 0, 0)
        self.events.emit(
            EventKind.BLOCK_OUTCOME,
            block=block,
            run_id=run_id,
            pipeline=self.spec.name,
            outcome=outcome,
            status=status,
            version=version,
            reason=reason,
            quality=Quality.worst(i.quality for i in inputs).value,
            rows_out=total,
            rows_promoted=promoted,
            rows_quarantined=quarantined,
            inputs=[i.as_dict() for i in inputs],
        )

    def code_fingerprint(self, block: str, version: str | None = None) -> CodeFingerprint | None:
        """Fingerprint of a version's code (default: the live one); its source is stored."""
        spec = self.spec.block(block)
        try:
            ref = spec.version_ref(version if version is not None else self.active_version(block))
            fn = self.resolve(ref)
        except Exception:
            return None
        cached = self._code_cache.get(ref)
        if cached is not None and cached[0] is fn:
            return cached[1]
        fp = fingerprint(fn, ref)
        self.code.put(fp)
        self._code_cache[ref] = (fn, fp)
        return fp

    def read(self, ref: DataRef) -> pd.DataFrame:
        """Load the snapshot behind ``ref`` and apply its adapter transform, if any."""
        df = self.snapshots.read(ref.block, ref.run_id)
        if ref.adapter:
            df = self.resolve(ref.adapter)(df)
        return df

    def load_input(self, block: str, upstream: str, run_id: str | None = None) -> pd.DataFrame:
        return self.read(self.resolve_input(block, upstream, run_id))

    # ------------------------------------------------------------------ replay

    @_in_session
    def replay(
        self,
        block: str,
        run_id: str | None = None,
        *,
        version: str | None = None,
        promote: bool = True,
        on_snapshot: Callable[[DataRef], None] | None = None,
    ) -> ReplayResult:
        """Re-run ``block``'s function on its QUARANTINED records.

        Rows whose output passes validation are "recovered" and their records marked
        REPLAYED; the rest stay QUARANTINED. The block function must preserve the index
        (row-wise transforms do), which is how output rows map back to records.

        - With a ``merge_key``: recovered rows are upserted into a copy of the last-good
          snapshot (recovered rows win on key conflict; among recovered rows the newest
          quarantine record wins). The result is written and promoted to last-good.
        - Without one: nothing is merged. Recovered rows are written to their own
          snapshot, last-good is left alone, and a WARN event is emitted.

        Idempotent: only QUARANTINED records are replayed, so a second call finds
        nothing to do and changes nothing. If a replay dies after writing its snapshot
        but before marking records, re-running it upserts the same keys again.

        ``version`` replays through that version instead of the live one. With
        ``promote=False`` the upserted snapshot is written but neither made last-good
        nor used to mark the block HEALTHY (a promotion does that when it completes).
        ``on_snapshot`` is called with the written snapshot before records are marked.
        """
        spec = self.spec.block(block)
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
            out = self.version_fn(block, version)(frame)
            if not isinstance(out, pd.DataFrame):
                raise TypeError(f"block {block!r} returned {type(out).__name__}, not DataFrame")
            result = self.validator_for(block).validate(out[out.index.isin(ids)])
        except Exception as exc:
            return self._replay_failed(block, run_id, len(ids), f"{type(exc).__name__}: {exc}")
        if result.schema_error is not None:
            return self._replay_failed(block, run_id, len(ids), result.schema_error)

        bad_ids = set(result.bad.index)
        passing = [i for i in dict.fromkeys(result.good.index) if i not in bad_ids]
        if not passing:
            return self._replay_done(block, run_id, [], len(ids), None)

        recovered = result.good[result.good.index.isin(passing)].sort_index()
        if base is not None:
            recovered = restore_dtypes(recovered, dict(base.dtypes))
        replay_provenance = self._replay_provenance(block, run_id, version, base_ref, ids, passing)
        if spec.annotate_quality:
            recovered = recovered.assign(**{QUALITY_COL: replay_provenance.quality.value})

        key = list(spec.merge_key or ())
        missing = [
            c
            for c in key
            if c not in recovered.columns or (base is not None and c not in base.columns)
        ]
        if missing:
            return self._replay_failed(
                block, run_id, len(ids), f"merge_key column(s) {missing} not in the block output"
            )

        if key:
            # Newest record wins among recovered rows (index = quarantine id, ascending).
            recovered = recovered.drop_duplicates(subset=key, keep="last")
            parts = [recovered.reset_index(drop=True)]
            if base is not None:
                replaced = pd.MultiIndex.from_frame(base[key]).isin(
                    pd.MultiIndex.from_frame(recovered[key])
                )
                parts.insert(0, base.loc[~replaced].reset_index(drop=True))
            ref = self.snapshots.write(block, run_id, pd.concat(parts, ignore_index=True))
            if promote:
                self.snapshots.mark_last_good(block, run_id)
        else:
            ref = self.snapshots.write(block, run_id, recovered.reset_index(drop=True))
            self.events.emit(
                EventKind.WARN,
                block=block,
                run_id=run_id,
                phase="replay",
                message=(
                    f"merge skipped: block {block!r} declares no merge_key, so "
                    f"{len(recovered)} replayed rows were written to snapshot {run_id!r} "
                    "and not merged into the last-good snapshot"
                ),
                snapshot_run_id=run_id,
                last_good_run_id=base_ref.run_id if base_ref else None,
            )

        self._record_output(replay_provenance, has_snapshot=True)
        if on_snapshot is not None:
            on_snapshot(ref)
        changed = self.quarantine.mark_replayed(passing, replay_run_id=run_id)
        if key and promote:
            self._mark_after_run(block, BlockStatus.HEALTHY, run_id)
        return self._replay_done(
            block, run_id, passing, len(ids) - changed, ref, changed, merged=bool(key)
        )

    def _replay_provenance(
        self,
        block: str,
        run_id: str,
        version: str | None,
        base_ref: DataRef | None,
        ids: list[int],
        passing: list[Any],
    ) -> Provenance:
        """A replay's quality is the worst of its base snapshot and the runs its
        recovered records came from."""
        passing_ids = set(passing)
        source_runs = sorted(
            {
                r.run_id
                for r in self.quarantine.list(block=block)
                if r.id in passing_ids and r.id in set(ids)
            }
        )
        qualities = [self._output_quality(block, r) for r in source_runs]
        if base_ref is not None:
            qualities.append(self.snapshot_quality(base_ref.block, base_ref.run_id))
        return Provenance(
            block=block,
            run_id=run_id,
            kind="replay",
            version=version if version is not None else self.active_version(block),
            quality=Quality.worst(qualities),
            base_run_id=base_ref.run_id if base_ref else None,
            replayed_from=tuple(source_runs),
        )

    def _output_quality(self, block: str, run_id: str) -> Quality:
        """Quality of an output, promoted or not (e.g. a rolled-back run being replayed)."""
        record = self.provenance.get(block, run_id)
        return record.quality if record else Quality.FRESH

    def _replay_failed(self, block: str, run_id: str, n: int, error: str) -> ReplayResult:
        self.events.emit(EventKind.ERROR, block=block, run_id=run_id, phase="replay", error=error)
        return self._replay_done(block, run_id, [], n, None)

    def _replay_done(
        self,
        block: str,
        run_id: str,
        passing: list[Any],
        still_failing: int,
        ref: DataRef | None,
        replayed: int | None = None,
        *,
        merged: bool = False,
    ) -> ReplayResult:
        replayed = len(passing) if replayed is None else replayed
        self.events.emit(
            EventKind.REPLAY,
            block=block,
            run_id=run_id,
            replayed=replayed,
            still_failing=still_failing,
            snapshot_run_id=ref.run_id if ref else None,
            merged=merged,
        )
        return ReplayResult(block, replayed, still_failing, ref, merged)

    # ------------------------------------------------------------------ shadow versions

    def shadow_candidate(self, block: str) -> Shadow | None:
        self.spec.block(block)
        if block not in self._shadow_cache:
            self._shadow_cache[block] = self.shadows.get(block)
        return self._shadow_cache[block]

    def shadow_start(self, block: str, version: str, *, expect_diff: bool = False) -> Shadow:
        """Start running ``version`` of any block as a shadow candidate on every run."""
        spec = self.spec.block(block)
        spec.version_ref(version)  # KeyError for an unknown version
        if version == self.active_version(block):
            raise PromotionError(f"{version!r} is already the active version of {block!r}")
        if not spec.merge_key:
            raise PromotionError(
                f"block {block!r} declares no merge_key; shadow comparison needs one"
            )
        if not spec.inputs and spec.load is None:
            raise PromotionError(
                f"source block {block!r} must declare 'load' so its candidate runs on "
                "the same loaded source data"
            )
        if self.versions.pending(block) is not None:
            raise PromotionError(
                f"block {block!r} has a promotion in progress; resume it with "
                "`guardian shadow promote` first"
            )
        shadow = self.shadows.start(block, version, expect_diff=expect_diff)
        self._forget_versions(block)
        self.events.emit(
            EventKind.SHADOW,
            block=block,
            action="start",
            version=version,
            active=self.active_version(block),
            expect_diff=expect_diff,
        )
        return shadow

    def shadow_stop(self, block: str) -> Shadow:
        shadow = self.shadow_candidate(block)
        if shadow is None:
            raise PromotionError(f"block {block!r} has no candidate in shadow")
        self.shadows.end(shadow.id, ShadowStatus.STOPPED)
        self._forget_versions(block)
        self.events.emit(EventKind.SHADOW, block=block, action="stop", version=shadow.version)
        return shadow

    def shadow_runs(self, block: str) -> tuple[Shadow | None, list[ShadowRun]]:
        shadow = self.shadow_candidate(block)
        return shadow, ([] if shadow is None else self.shadows.runs(shadow.id))

    def run_shadow(
        self, block: str, run_id: str, inputs: Any, decision: Decision | None
    ) -> ShadowRun | None:
        """Run ``block``'s shadow candidate (if any) on the inputs the live version used.

        ``decision`` is the live version's decision on this run (None if it did not
        run, e.g. OUT). The candidate's validated output goes to the candidate store,
        which consumers never read. May auto-promote the candidate.
        """
        shadow = self.shadow_candidate(block)
        if shadow is None or isinstance(inputs, BlockCrash):
            return None
        if any(isinstance(item, BlockSkipped) for item in inputs):
            return None
        spec = self.spec.block(block)
        policy = spec.shadow_policy()
        mode = (
            ShadowMode.PARITY
            if decision is not None and decision.action is Action.PASS
            else ShadowMode.ABSOLUTE
        )
        notes: list[str] = []
        try:
            out = self.version_fn(block, shadow.version)(*inputs)
            if not isinstance(out, pd.DataFrame):
                raise TypeError(f"returned {type(out).__name__}, not DataFrame")
            result = self.validator_for(block).validate(out)
        except Exception as exc:
            out, result = None, None
            notes.append(f"candidate crashed: {type(exc).__name__}: {exc}")

        rows_in = 0 if out is None else len(out)
        if result is None or result.schema_error is not None:
            if result is not None:
                notes.append(f"schema: {result.schema_error}")
            good = pd.DataFrame()
            passed = 0
        else:
            good = result.good
            passed = len(good)
        pass_rate = passed / rows_in if rows_in else (0.0 if notes else 1.0)

        store = self.candidate_store(shadow.version)
        if not store.exists(block, run_id):
            store.write(block, run_id, good.reset_index(drop=True))
            self.provenance.record(
                self._run_provenance(
                    block, run_id, version=shadow.version, kind="candidate", store=CANDIDATE
                )
            )

        comparison = None
        stats: dict[str, Any] = {"candidate": column_stats(good)}
        if mode is ShadowMode.PARITY:
            live = self.snapshots.read(block, run_id)
            stats["active"] = column_stats(live)
            try:
                comparison = compare_rows(live, good, list(spec.merge_key or ()))
            except GuardianError as exc:
                notes.append(str(exc))
        if mode is ShadowMode.PARITY and comparison is None:
            within, reasons = False, ["no row-level comparison possible"]
        elif notes and passed == 0:
            within, reasons = False, list(notes)
        else:
            within, reasons = evaluate(policy, mode, pass_rate, comparison)
        run = ShadowRun(
            shadow_id=shadow.id,
            block=block,
            version=shadow.version,
            run_id=run_id,
            mode=mode,
            rows_in=rows_in,
            rows_passed=passed,
            pass_rate=pass_rate,
            comparison=comparison,
            stats=stats,
            within_tolerance=within,
            notes=tuple(notes + [r for r in reasons if r not in notes]),
        )
        self.shadows.record(run)
        self.events.emit(
            EventKind.SHADOW,
            block=block,
            run_id=run_id,
            action="run",
            version=shadow.version,
            active=self.active_version(block),
            mode=mode.value,
            pass_rate=round(pass_rate, 6),
            changed_fraction=None if comparison is None else comparison.changed_fraction,
            changed_columns=[] if comparison is None else list(comparison.changed_columns),
            within_tolerance=within,
            candidate_snapshot=f"{CANDIDATES_DIR}/{shadow.version}/{block}/{run_id}",
        )
        if (
            not shadow.expect_diff
            and self.versions.pending(block) is None
            and auto_promotable(self.shadows.runs(shadow.id), policy)
        ):
            self.promote(block, reason="auto")
        return run

    @_in_session
    def promote(
        self,
        block: str,
        *,
        approve: bool = False,
        reason: str | None = None,
    ) -> PromotionResult:
        """Make ``block``'s shadow candidate the active version.

        Without ``approve`` this is only allowed when the auto-promotion policy holds
        (``required_runs`` consecutive PARITY runs within tolerance, no expect_diff).
        With ``approve`` the latest shadow run must still meet ``min_pass_rate``.

        Crash-safe and resumable: the promotion is recorded as PROMOTING first; the
        new version becomes active, the replay snapshot last-good and the block
        HEALTHY only at the end. If anything fails in between, calling ``promote``
        again resumes it (the replay is idempotent via merge_key).
        """
        self.spec.block(block)
        pending = self.versions.pending(block)
        if pending is not None:
            return self._finish_promotion(pending, resumed=True)

        shadow = self.shadow_candidate(block)
        if shadow is None:
            raise PromotionError(f"block {block!r} has no candidate in shadow")
        runs = self.shadows.runs(shadow.id)
        if not runs:
            raise PromotionError(
                f"{block!r} candidate {shadow.version!r} has not run yet; run the pipeline first"
            )
        policy = self.spec.block(block).shadow_policy()
        auto_ok = not shadow.expect_diff and auto_promotable(runs, policy)
        if not auto_ok and not approve:
            raise PromotionError(self._why_approval_needed(shadow, runs, policy))
        last = runs[-1]
        assert policy.min_pass_rate is not None
        if last.pass_rate < policy.min_pass_rate:
            raise PromotionError(
                f"{block!r} candidate {shadow.version!r} fails validation on its latest run "
                f"(pass rate {last.pass_rate:.4f} < {policy.min_pass_rate}); "
                "not promotable, even with approval"
            )
        last_good = self.snapshots.last_good(block)
        self._forget_versions(block)
        promotion = self.versions.begin_promotion(
            block,
            self.active_version(block),
            shadow.version,
            reason=reason or ("approved" if approve else "policy"),
            approved=approve,
            last_good_before=last_good.run_id if last_good else None,
        )
        self.events.emit(
            EventKind.PROMOTION,
            block=block,
            promotion_kind=PromotionKind.PROMOTE.value,
            state=PromotionState.PROMOTING.value,
            promotion_id=promotion.id,
            from_version=promotion.from_version,
            to_version=promotion.to_version,
            reason=promotion.reason,
            approved=approve,
            mode=last.mode.value,
        )
        return self._finish_promotion(promotion, resumed=False)

    def _why_approval_needed(self, shadow: Shadow, runs: list[ShadowRun], policy) -> str:
        block = shadow.block
        if shadow.expect_diff:
            why = "the shadow was started with --expect-diff (an intended behaviour change)"
        elif runs[-1].mode is ShadowMode.ABSOLUTE:
            why = "the active version is not healthy, so there is no baseline (absolute mode)"
        else:
            streak = 0
            for r in reversed(runs):
                if r.mode is not ShadowMode.PARITY or not r.within_tolerance:
                    break
                streak += 1
            why = (
                f"it has {streak} consecutive parity run(s) within tolerance; "
                f"{policy.required_runs} are required"
            )
            if not runs[-1].within_tolerance:
                why += f"; latest run: {'; '.join(runs[-1].notes)}"
        return (
            f"promoting {block!r} to {shadow.version!r} needs approval: {why}. "
            f"Use `guardian shadow promote {block} --approve`."
        )

    def _finish_promotion(self, promotion: Promotion, *, resumed: bool) -> PromotionResult:
        block = promotion.block
        replay = self.replay(
            block,
            version=promotion.to_version,
            promote=False,
            on_snapshot=lambda ref: self.versions.set_replay_snapshot(promotion.id, ref.run_id),
        )
        promotion = self.versions.promotion(promotion.id)
        if promotion.replay_run_id is not None:
            self.snapshots.mark_last_good(block, promotion.replay_run_id)
        self.versions.complete_promotion(promotion.id)
        self._forget_versions(block)
        shadow = self.shadows.get(block)
        if shadow is not None and shadow.version == promotion.to_version:
            self.shadows.end(shadow.id, ShadowStatus.PROMOTED)
        self._forget_versions(block)
        self._set_status(block, BlockStatus.HEALTHY, reason="promotion")
        self.events.emit(
            EventKind.PROMOTION,
            block=block,
            promotion_kind=PromotionKind.PROMOTE.value,
            state=PromotionState.COMPLETED.value,
            promotion_id=promotion.id,
            from_version=promotion.from_version,
            to_version=promotion.to_version,
            reason=promotion.reason,
            resumed=resumed,
            replayed=replay.replayed,
            still_failing=replay.still_failing,
            replay_snapshot=promotion.replay_run_id,
        )
        return PromotionResult(
            block,
            promotion.from_version,
            promotion.to_version,
            promotion.reason,
            replay,
            resumed,
        )

    @_in_session
    def rollback_version(self, block: str) -> Promotion:
        """Make the previous version active again. No snapshot is rewritten.

        If the block's last-good snapshot is still the one its last promotion's replay
        produced, the last-good pointer returns to the pre-promotion snapshot too.
        """
        self.spec.block(block)
        last_promotion = next(
            (
                p
                for p in reversed(self.versions.history(block))
                if p.kind is PromotionKind.PROMOTE and p.state is PromotionState.COMPLETED
            ),
            None,
        )
        rollback = self.versions.rollback(block)
        self._forget_versions(block)
        restored = None
        last_good = self.snapshots.last_good(block)
        if (
            last_promotion is not None
            and last_promotion.replay_run_id is not None
            and last_promotion.last_good_before is not None
            and last_good is not None
            and last_good.run_id == last_promotion.replay_run_id
        ):
            self.snapshots.mark_last_good(block, last_promotion.last_good_before)
            restored = last_promotion.last_good_before
        self.events.emit(
            EventKind.PROMOTION,
            block=block,
            state=PromotionState.COMPLETED.value,
            promotion_kind=PromotionKind.ROLLBACK.value,
            promotion_id=rollback.id,
            from_version=rollback.from_version,
            to_version=rollback.to_version,
            restored_last_good=restored,
        )
        return rollback


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
    "CANDIDATES_DIR",
    "CRASH_REASON",
    "ROLLBACK_RULE",
    "BlockStatusStore",
    "Guardian",
    "JsonBlockStatusStore",
    "PromotionError",
    "PromotionResult",
    "ValidationResult",
    "new_run_id",
]
