"""Smoke scenario for EVERY DAG role, standalone runner: part of the default (fast) run.

The exhaustive scenario files run each role under both runners in the full suite; here
one compact scenario per role keeps every role covered in the default run:

clean run -> heavy corruption of the role's block: ROLLBACK, quarantine, dependents
follow the fallback/stale rules -> replay (every held row recovered or still held) ->
the next run is PASS and FRESH end to end -> no row was ever silently lost.
"""

import pytest

from guardian.core.models import BlockStatus, Quality

from ..helpers.roles import descendants, representative
from .runners import StandaloneRunner
from .test_scenarios import (
    ROLES,
    SPEC,
    assert_dependents_follow_rules,
    assert_rest_unaffected,
    check_invariants,
    corrupt,
)


@pytest.mark.parametrize("role", ROLES)
def test_smoke_contain_replay_recover(tmp_path, role: str) -> None:
    block = representative(SPEC, role)
    runner = StandaloneRunner(SPEC, tmp_path / "guardian")
    try:
        assert all(r.outcome == "PASS" for r in runner.run("r1").values())

        runner.inject_fault(block, corrupt(block, 0.6, seed=3))
        r2 = runner.run("r2")
        assert r2[block].outcome == "ROLLBACK"
        assert runner.status(block) is BlockStatus.DEGRADED
        assert runner.quarantine(block=block, run_id="r2")
        assert_dependents_follow_rules(runner, r2, "r2", block, "r1")
        others = {b: r for b, r in r2.items() if b not in descendants(SPEC, block)}
        assert_rest_unaffected(runner, others, "r2", block)

        runner.clear_faults(block)
        held = len(runner.quarantine(block=block, run_id="r2"))
        replay = runner.replay(block)  # every held row is either recovered or kept
        assert replay.replayed + replay.still_failing == held
        r3 = runner.run("r3")
        assert all(r.outcome == "PASS" for r in r3.values())
        for b in (block, *descendants(SPEC, block)):
            assert runner.provenance(b, "r3")["quality"] == Quality.FRESH.value, b
        check_invariants(runner, "r1", "r2", "r3")
    finally:
        runner.close()
