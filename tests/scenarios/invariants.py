"""The no-silent-loss invariant, checked from the shared stores (runner-agnostic)."""

from __future__ import annotations

from collections import Counter

from guardian.core.events import EventKind
from guardian.core.models import QuarantineStatus

from .runners import ScenarioRunner

# Blocks whose output has one row per input row (so rows handed to on_output must
# equal the rows of the snapshot they read).
ROW_PRESERVING = {"b2_parse", "b3_standardize", "b4_clean", "b5_normalize", "b6_enrich"}


def assert_no_silent_loss(runner: ScenarioRunner, run_id: str) -> None:
    validations = {e.block: e for e in runner.events(EventKind.VALIDATION, run_id=run_id)}
    resolves = runner.events(EventKind.RESOLVE, run_id=run_id) + runner.events(
        EventKind.REROUTE, run_id=run_id
    )
    for block in runner.spec.block_names:
        snapshot = runner.snapshot(block, run_id)
        promoted = 0 if snapshot is None else len(snapshot)
        quarantined = len(runner.quarantine(block=block, run_id=run_id))
        event = validations.get(block)
        if event is None:
            # Crashed, skipped or blocked: nothing reached on_output, nothing stored.
            assert promoted == 0 and quarantined == 0, block
            continue
        total = event.data["total"]
        assert total == promoted + quarantined, (
            f"{block}: {total} rows handed over, {promoted} promoted + {quarantined} quarantined"
        )
        if block in ROW_PRESERVING:
            refs = [e for e in resolves if e.block == block]
            assert len(refs) == 1, block
            source = runner.snapshot(refs[0].data["source"], refs[0].data["source_run_id"])
            assert total == len(source), f"{block}: read {len(source)} rows, emitted {total}"
    assert_exactly_once(runner, run_id)


def assert_exactly_once(runner: ScenarioRunner, run_id: str) -> None:
    """Every row handed to on_output in ``run_id`` is held exactly once, by merge_key.

    A row is held by the run's promoted snapshot, by a still-QUARANTINED record, or,
    once REPLAYED, by the replay snapshot its record points at (which must contain
    the key exactly once).
    """
    validations = {e.block: e for e in runner.events(EventKind.VALIDATION, run_id=run_id)}
    for block, event in validations.items():
        key = list(runner.spec.block(block).merge_key or ())
        assert key, f"{block} declares no merge_key"
        held: list[tuple] = []
        snapshot = runner.snapshot(block, run_id)
        if snapshot is not None:
            held += [tuple(row) for row in snapshot[key].itertuples(index=False)]
        for record in runner.quarantine(block=block, run_id=run_id):
            row_key = tuple(record.payload()[c] for c in key)
            if record.status is QuarantineStatus.REPLAYED:
                replay = runner.snapshot(block, record.replay_run_id)
                assert replay is not None, record.replay_run_id
                keys = Counter(tuple(r) for r in replay[key].itertuples(index=False))
                assert keys[row_key] == 1, f"{block}: key {row_key} x{keys[row_key]} in replay"
            held.append(row_key)
        assert len(held) == event.data["total"], block
        dupes = [k for k, n in Counter(held).items() if n > 1 and None not in k]
        assert not dupes, f"{block} run {run_id}: rows held more than once: {dupes[:5]}"


def assert_end_to_end_accounting(runner: ScenarioRunner, run_id: str, last: str) -> None:
    """Every ingested row is in ``last``'s snapshot or quarantined somewhere upstream."""
    ingested = len(runner.snapshot("b1_ingest", run_id)) + len(
        runner.quarantine(block="b1_ingest", run_id=run_id)
    )
    chain = ["b2_parse", "b3_standardize", "b4_clean", "b5_normalize", "b6_enrich"]
    chain = chain[: chain.index(last) + 1]
    quarantined = sum(len(runner.quarantine(block=b, run_id=run_id)) for b in chain)
    assert ingested == len(runner.snapshot(last, run_id)) + quarantined
