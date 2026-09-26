"""The no-silent-loss invariant, checked from the shared stores (runner-agnostic)."""

from __future__ import annotations

from guardian.core.events import EventKind

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


def assert_end_to_end_accounting(runner: ScenarioRunner, run_id: str, last: str) -> None:
    """Every ingested row is in ``last``'s snapshot or quarantined somewhere upstream."""
    ingested = len(runner.snapshot("b1_ingest", run_id)) + len(
        runner.quarantine(block="b1_ingest", run_id=run_id)
    )
    chain = ["b2_parse", "b3_standardize", "b4_clean", "b5_normalize", "b6_enrich"]
    chain = chain[: chain.index(last) + 1]
    quarantined = sum(len(runner.quarantine(block=b, run_id=run_id)) for b in chain)
    assert ingested == len(runner.snapshot(last, run_id)) + quarantined
