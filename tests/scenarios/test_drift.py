"""Scenario 11: statistical drift, for every DAG role, under every runner.

The `drift` fault skews a block's output while every row stays schema-valid. Schema
validation alone lets it through; a drift policy catches it (FAIL -> ROLLBACK, with
the usual containment downstream) or reports it (WARN -> event, output promoted).
"""

import dataclasses

import pytest

from guardian.agent.evidence import build_evidence
from guardian.core.drift import DRIFT_RULE
from guardian.core.events import EventKind
from guardian.core.guardian import Guardian
from guardian.core.models import BlockStatus, DriftPolicy, DriftThresholds
from guardian.demo.faults import drift

from ..helpers.roles import representative
from .test_scenarios import ROLES, SPEC, assert_dependents_follow_rules, check_invariants

by_role = pytest.mark.parametrize("role", ROLES)


def with_drift(runner, block: str, policy: DriftPolicy | None) -> None:
    """Give ``block`` exactly ``policy`` and every other block none, so each scenario is
    about one block (None: no drift detection anywhere, schema validation alone)."""
    runner.spec = dataclasses.replace(
        runner.spec,
        blocks=tuple(
            dataclasses.replace(b, drift=policy if b.name == block else None)
            for b in runner.spec.blocks
        ),
    )


def warm_up(runner) -> None:
    """Three clean runs: the drift baseline needs min_history promoted snapshots."""
    for run_id in ("r1", "r2", "r3"):
        result = runner.run(run_id)
        assert all(r.outcome == "PASS" for r in result.values()), run_id


@by_role
def test_11a_schema_validation_alone_misses_drift(runner, role: str) -> None:
    block = representative(SPEC, role)
    with_drift(runner, block, None)
    warm_up(runner)
    runner.inject_fault(block, drift())
    result = runner.run("r4")
    # Every row is schema-valid: nothing is quarantined, the skewed output is promoted.
    assert result[block].outcome == "PASS"
    assert runner.quarantine(block=block, run_id="r4") == []
    assert runner.status(block) is BlockStatus.HEALTHY
    assert runner.events(EventKind.DRIFT) == []
    before, after = runner.snapshot(block, "r3"), runner.snapshot(block, "r4")
    assert len(before) == len(after) and not before.equals(after)  # but the data moved


@by_role
def test_11b_drift_detection_catches_it(runner, role: str) -> None:
    block = representative(SPEC, role)
    with_drift(runner, block, DriftPolicy())
    warm_up(runner)
    runner.inject_fault(block, drift())
    result = runner.run("r4")

    assert result[block].outcome == "ROLLBACK"
    assert runner.status(block) is BlockStatus.DEGRADED
    assert_dependents_follow_rules(runner, result, "r4", block, "r3")
    check_invariants(runner, "r1", "r2", "r3", "r4")
    records = runner.quarantine(block=block, run_id="r4")
    assert records and {r.rule_name for r in records} == {DRIFT_RULE}  # all rows valid
    (event,) = runner.events(EventKind.DRIFT, block=block)
    assert event.data["level"] == "FAIL" and event.data["reference_runs"] == ["r1", "r2", "r3"]
    (rollback,) = runner.events(EventKind.ROLLBACK, block=block, run_id="r4")
    assert rollback.data["reason"].startswith("distribution drift:")

    # The agent's evidence carries the drift details.
    runner.close()
    with Guardian(runner.spec, runner.root) as g:
        bundle = build_evidence(g, block, "r4", git=False)
    (item,) = bundle.of_kind("drift")
    assert item.data["level"] == "FAIL" and item.data["columns"][0]["level"] == "FAIL"
    assert item.data["thresholds"]["fail"] == {"psi": 0.25, "z": 6.0}

    # If the new distribution is right after all, replay is the human override: it
    # re-validates the rows (all valid) without re-checking drift.
    replayed = runner.replay(block)
    assert (replayed.replayed, replayed.still_failing) == (len(records), 0)


@by_role
def test_11c_drift_below_fail_threshold_only_warns(runner, role: str) -> None:
    block = representative(SPEC, role)
    lenient = DriftPolicy(fail=DriftThresholds(psi=1e6, z=1e9))
    with_drift(runner, block, lenient)
    warm_up(runner)
    runner.inject_fault(block, drift())
    result = runner.run("r4")
    assert result[block].outcome == "PASS"
    assert runner.quarantine(block=block, run_id="r4") == []
    (event,) = runner.events(EventKind.DRIFT, block=block)
    assert event.data["level"] == "WARN" and event.data["columns"]
    check_invariants(runner, "r4")
