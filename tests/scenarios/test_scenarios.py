"""Fault-injection scenarios, run against every runner in conftest.RUNNERS."""

import pytest

from guardian.core.events import EventKind
from guardian.core.models import BlockStatus, QuarantineStatus
from guardian.demo.faults import corrupt_rows, crash, null_burst, schema_drift

from .invariants import assert_end_to_end_accounting, assert_no_silent_loss
from .runners import ScenarioRunner

ALL_BLOCKS = [
    "b1_ingest",
    "b2_parse",
    "b3_standardize",
    "b4_clean",
    "b5_normalize",
    "b6_enrich",
    "b8_aggregate",
]
ROWS = 200


def assert_normal_path(runner: ScenarioRunner, result, run_id: str) -> None:
    b8 = result["b8_aggregate"]
    assert b8.outcome == "PASS" and not b8.rerouted
    assert [ref.block for ref in b8.sources] == ["b6_enrich"]
    assert b8.sources[0].run_id == run_id


def assert_rerouted_to_b5(runner: ScenarioRunner, result, run_id: str) -> None:
    b8 = result["b8_aggregate"]
    assert b8.outcome == "PASS"
    (ref,) = b8.sources
    assert (ref.block, ref.requested, ref.adapter) == (
        "b5_normalize",
        "b6_enrich",
        "demo.blocks:b5_to_b6_shape",
    )
    assert ref.run_id == run_id
    # b8's promoted output came through the adapter
    out = runner.snapshot("b8_aggregate", run_id)
    assert out is not None and set(out["segment"]) == {"unassigned"}
    reroutes = runner.events(EventKind.REROUTE, block="b8_aggregate", run_id=run_id)
    assert [e.data["source"] for e in reroutes] == ["b5_normalize"]


# ---------------------------------------------------------------- 1


def test_clean_run(runner: ScenarioRunner) -> None:
    result = runner.run("r1")
    assert {b: r.outcome for b, r in result.items()} == dict.fromkeys(ALL_BLOCKS, "PASS")
    assert runner.quarantine() == []
    assert all(runner.status(b) is BlockStatus.HEALTHY for b in ALL_BLOCKS)
    assert_normal_path(runner, result, "r1")
    assert len(runner.snapshot("b6_enrich", "r1")) == ROWS
    assert_no_silent_loss(runner, "r1")
    assert_end_to_end_accounting(runner, "r1", last="b6_enrich")


# ---------------------------------------------------------------- 2


def test_minor_corruption_under_threshold(runner: ScenarioRunner) -> None:
    runner.inject_fault("b4_clean", corrupt_rows(0.05, columns=["amount_usd"], seed=1))
    result = runner.run("r1")
    assert all(r.outcome == "PASS" for r in result.values())
    assert runner.status("b4_clean") is BlockStatus.HEALTHY

    bad = runner.quarantine(block="b4_clean", run_id="r1")
    assert len(bad) == 10  # 5% of 200
    assert {r.rule_name for r in bad} == {"amount_usd:greater_than(0)"}
    assert all(r.payload()["amount_usd"] == -999_999 for r in bad)
    assert runner.quarantine(block="b5_normalize") == []

    # downstream sees only the good rows and carries on normally
    assert len(runner.snapshot("b4_clean", "r1")) == ROWS - 10
    assert len(runner.snapshot("b6_enrich", "r1")) == ROWS - 10
    assert_normal_path(runner, result, "r1")
    assert_no_silent_loss(runner, "r1")
    assert_end_to_end_accounting(runner, "r1", last="b6_enrich")


# ---------------------------------------------------------------- 3 & 4 (+ other b6 failure modes)

B6_FAILURES = {
    "heavy_corruption": (
        corrupt_rows(0.5, columns=["region", "segment"], seed=2),
        "exceeds threshold",
    ),
    "crash": (crash("b6 exploded"), "crash"),
    "schema_drift": (schema_drift(rename={"region": "area"}), "missing column 'region'"),
    "null_burst": (null_burst("segment", 0.3, seed=3), "exceeds threshold"),
}


@pytest.mark.parametrize("failure", list(B6_FAILURES))
def test_b6_failure_rolls_back_and_b8_reroutes(runner: ScenarioRunner, failure: str) -> None:
    fault, reason = B6_FAILURES[failure]
    runner.inject_fault("b6_enrich", fault)
    result = runner.run("r1")

    assert result["b6_enrich"].outcome == "ROLLBACK"
    assert runner.status("b6_enrich") is BlockStatus.DEGRADED
    assert runner.last_good("b6_enrich") is None  # nothing promoted
    rollback = runner.events(EventKind.ROLLBACK, block="b6_enrich", run_id="r1")
    assert len(rollback) == 1 and reason in rollback[0].data["reason"]

    # the rest of the pipeline completed, b8 via the fallback edge
    assert all(result[b].outcome == "PASS" for b in ALL_BLOCKS if b != "b6_enrich")
    assert_rerouted_to_b5(runner, result, "r1")

    quarantined = runner.quarantine(block="b6_enrich", run_id="r1")
    assert len(quarantined) == (0 if failure == "crash" else ROWS)
    assert_no_silent_loss(runner, "r1")
    assert_end_to_end_accounting(runner, "r1", last="b5_normalize")


def test_b6_heavy_corruption(runner: ScenarioRunner) -> None:
    """Scenario 3, spelled out: 50% corrupt rows > 10% threshold."""
    runner.inject_fault("b6_enrich", corrupt_rows(0.5, columns=["region", "segment"], seed=2))
    result = runner.run("r1")
    assert result["b6_enrich"].outcome == "ROLLBACK"
    records = runner.quarantine(block="b6_enrich", run_id="r1")
    assert sum(r.rule_name == "rollback" for r in records) == ROWS // 2
    assert sum(r.rule_name.startswith("region:") for r in records) == ROWS // 2
    assert_rerouted_to_b5(runner, result, "r1")
    assert_no_silent_loss(runner, "r1")


def test_b6_crash(runner: ScenarioRunner) -> None:
    """Scenario 4, spelled out: an exception in b6."""
    runner.inject_fault("b6_enrich", crash())
    result = runner.run("r1")
    assert result["b6_enrich"].outcome == "ROLLBACK"
    errors = runner.events(EventKind.ERROR, block="b6_enrich", run_id="r1")
    assert errors and "injected crash" in errors[0].data["error"]
    assert_rerouted_to_b5(runner, result, "r1")
    assert_no_silent_loss(runner, "r1")


def test_b6_failure_after_good_run_keeps_serving_last_good(runner: ScenarioRunner) -> None:
    runner.run("r1")
    runner.inject_fault("b6_enrich", crash())
    runner.run("r2")
    assert runner.last_good("b6_enrich").run_id == "r1"
    assert_no_silent_loss(runner, "r2")


# ---------------------------------------------------------------- 5


def test_b6_taken_out_then_restored(runner: ScenarioRunner) -> None:
    runner.run("r1")
    runner.set_block_status("b6_enrich", BlockStatus.OUT)

    result = runner.run("r2")
    assert result["b6_enrich"].outcome == "SKIPPED"
    assert runner.status("b6_enrich") is BlockStatus.OUT
    assert runner.snapshot("b6_enrich", "r2") is None
    assert runner.last_good("b6_enrich").run_id == "r1"
    assert_rerouted_to_b5(runner, result, "r2")
    assert_no_silent_loss(runner, "r2")

    runner.set_block_status("b6_enrich", BlockStatus.HEALTHY)
    result = runner.run("r3")
    assert result["b6_enrich"].outcome == "PASS"
    assert_normal_path(runner, result, "r3")
    assert set(runner.snapshot("b8_aggregate", "r3")["segment"]) != {"unassigned"}
    assert_no_silent_loss(runner, "r3")


# ---------------------------------------------------------------- 6


def test_replay_after_fixing_b6(runner: ScenarioRunner) -> None:
    runner.inject_fault("b6_enrich", corrupt_rows(0.5, columns=["region", "segment"], seed=2))
    runner.run("r1")
    assert runner.status("b6_enrich") is BlockStatus.DEGRADED
    quarantined = runner.quarantine(block="b6_enrich", status=QuarantineStatus.QUARANTINED)
    assert len(quarantined) == ROWS

    runner.clear_faults("b6_enrich")  # "fix" the block
    result = runner.replay("b6_enrich")
    assert (result.replayed, result.still_failing) == (ROWS, 0)

    # records are kept and marked REPLAYED, never deleted
    records = runner.quarantine(block="b6_enrich")
    assert len(records) == ROWS
    assert all(r.status is QuarantineStatus.REPLAYED for r in records)
    assert len({r.replay_run_id for r in records}) == 1

    # a new snapshot was promoted and the block is healthy again
    ref = runner.last_good("b6_enrich")
    assert ref == result.snapshot and ref.run_id == records[0].replay_run_id
    replayed = runner.read_last_good("b6_enrich")
    assert len(replayed) == ROWS
    assert "__corrupt__" not in set(replayed["region"])
    assert sorted(replayed["order_id"]) == sorted(runner.snapshot("b5_normalize", "r1")["order_id"])
    assert runner.status("b6_enrich") is BlockStatus.HEALTHY

    # replay accounting: every replayed record is in the new snapshot
    assert len(replayed) == result.replayed
    assert_no_silent_loss(runner, "r1")

    # the next run takes the normal path again
    result = runner.run("r2")
    assert_normal_path(runner, result, "r2")
    assert_no_silent_loss(runner, "r2")


def test_replay_merges_with_previous_last_good(runner: ScenarioRunner) -> None:
    runner.run("r1")
    runner.inject_fault("b6_enrich", corrupt_rows(0.05, columns=["region"], seed=4))
    runner.run("r2")  # PASS with 10 rows quarantined
    before = runner.read_last_good("b6_enrich")
    runner.clear_faults()
    result = runner.replay("b6_enrich")
    assert (result.replayed, result.still_failing) == (10, 0)
    assert len(runner.read_last_good("b6_enrich")) == len(before) + 10
    assert_no_silent_loss(runner, "r2")


def test_replay_while_still_broken_changes_nothing(runner: ScenarioRunner) -> None:
    runner.inject_fault("b6_enrich", corrupt_rows(1.0, columns=["region"], seed=2))
    runner.run("r1")
    result = runner.replay("b6_enrich")  # fault still injected: every replayed row fails
    assert (result.replayed, result.still_failing) == (0, ROWS)
    assert runner.quarantine(status=QuarantineStatus.REPLAYED) == []
    assert runner.last_good("b6_enrich") is None
