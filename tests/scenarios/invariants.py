"""The no-silent-loss invariant, checked from the shared stores (runner-agnostic)."""

from __future__ import annotations

from collections import Counter

from guardian.core.events import EventKind
from guardian.core.models import QuarantineStatus

from ..helpers.profile import block_profiles
from ..helpers.roles import dependents
from .runners import ScenarioRunner


def _quarantine_by_block(runner: ScenarioRunner, run_id: str) -> dict[str, list]:
    """All of a run's quarantine records, fetched once and grouped by block."""
    grouped: dict[str, list] = {}
    for record in runner.quarantine(run_id=run_id):
        grouped.setdefault(record.block, []).append(record)
    return grouped


def _row_preserving(runner: ScenarioRunner) -> set[str]:
    """Blocks with one output row per input row (derived from the spec, not named)."""
    return {b for b, p in block_profiles(runner.spec).items() if p.row_preserving}


def assert_no_silent_loss(runner: ScenarioRunner, run_id: str) -> None:
    validations = {e.block: e for e in runner.events(EventKind.VALIDATION, run_id=run_id)}
    resolves = runner.events(EventKind.RESOLVE, run_id=run_id) + runner.events(
        EventKind.REROUTE, run_id=run_id
    )
    records = _quarantine_by_block(runner, run_id)
    for block in runner.spec.block_names:
        snapshot = runner.snapshot(block, run_id)
        promoted = 0 if snapshot is None else len(snapshot)
        quarantined = len(records.get(block, []))
        event = validations.get(block)
        if event is None:
            # Crashed, skipped or blocked: nothing reached on_output, nothing stored.
            assert promoted == 0 and quarantined == 0, block
            continue
        total = event.data["total"]
        assert total == promoted + quarantined, (
            f"{block}: {total} rows handed over, {promoted} promoted + {quarantined} quarantined"
        )
        if block in _row_preserving(runner):
            refs = [e for e in resolves if e.block == block]
            assert len(refs) == 1, block
            source = runner.snapshot(refs[0].data["source"], refs[0].data["source_run_id"])
            assert total == len(source), f"{block}: read {len(source)} rows, emitted {total}"
    assert_exactly_once(runner, run_id)


def _key(values) -> tuple[str, ...]:
    """A merge key as strings: quarantine payloads hold rows as the block emitted them
    (e.g. an id as text) while promoted snapshots hold schema-coerced values."""
    return tuple(str(v) for v in values)


def assert_exactly_once(runner: ScenarioRunner, run_id: str) -> None:
    """Every row handed to on_output in ``run_id`` is held exactly once, by merge_key.

    A row is held by the run's promoted snapshot, by a still-QUARANTINED record, or,
    once REPLAYED, by the replay snapshot its record points at (which must contain
    the key exactly once).
    """
    validations = {e.block: e for e in runner.events(EventKind.VALIDATION, run_id=run_id)}
    records = _quarantine_by_block(runner, run_id)
    for block, event in validations.items():
        key = list(runner.spec.block(block).merge_key or ())
        assert key, f"{block} declares no merge_key"
        held: list[tuple] = []
        snapshot = runner.snapshot(block, run_id)
        if snapshot is not None:
            held += [_key(row) for row in snapshot[key].itertuples(index=False)]
        for record in records.get(block, []):
            # A schema-drifted payload may lack key columns: its key is then unknown.
            row_key = _key(record.payload().get(c) for c in key)
            if record.status is QuarantineStatus.REPLAYED:
                replay = runner.snapshot(block, record.replay_run_id)
                assert replay is not None, record.replay_run_id
                keys = Counter(_key(r) for r in replay[key].itertuples(index=False))
                assert keys[row_key] == 1, f"{block}: key {row_key} x{keys[row_key]} in replay"
            held.append(row_key)
        assert len(held) == event.data["total"], block
        dupes = [k for k, n in Counter(held).items() if n > 1 and "None" not in k]
        assert not dupes, f"{block} run {run_id}: rows held more than once: {dupes[:5]}"


def assert_end_to_end_accounting(runner: ScenarioRunner, run_id: str) -> None:
    """Every row a source ingested is in the last snapshot of each row-preserving chain or
    quarantined somewhere along it.

    Chains start at each source block that ran and follow dependents that are
    row-preserving and read this run's (fresh) snapshot of the previous block.
    """
    spec = runner.spec
    preserving = _row_preserving(runner)
    reads = {
        (e.block, e.data["upstream"]): e
        for kind in (EventKind.RESOLVE, EventKind.REROUTE)
        for e in runner.events(kind, run_id=run_id)
    }

    def fresh_read(block: str, upstream: str) -> bool:
        e = reads.get((block, upstream))
        return (
            e is not None
            and e.data["source"] == upstream
            and e.data["source_run_id"] == run_id
            and not e.data["stale"]
        )

    def rows(block: str) -> int:
        snap = runner.snapshot(block, run_id)
        return 0 if snap is None else len(snap)

    records = _quarantine_by_block(runner, run_id)

    def quarantined(block: str) -> int:
        return len(records.get(block, []))

    validated = {e.block for e in runner.events(EventKind.VALIDATION, run_id=run_id)}

    def walk(path: list[str], ingested: int) -> None:
        last = path[-1]
        assert ingested == rows(last) + sum(quarantined(b) for b in path), path
        for d in dependents(spec, last):
            # A dependent that crashed, was skipped or was blocked never received these
            # rows: they are still held by `last`'s snapshot, so the chain ends there.
            if d in preserving and d in validated and fresh_read(d, last):
                walk([*path, d], ingested)

    for source in (b.name for b in spec.blocks if not b.inputs):
        if runner.snapshot(source, run_id) is not None or records.get(source):
            walk([source], rows(source) + quarantined(source))
