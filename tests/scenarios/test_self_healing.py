"""Scenario 10: the self-healing loop, for every DAG role (standalone runner, FakeClient).

detect (code_bug -> ROLLBACK) -> contain (dependents follow the fallback/stale rules)
-> diagnose (code_bug) -> propose (dry run: a new version, verified in a worktree)
-> shadow (the proposal, as if its PR were merged, runs in shadow while the buggy
version is still live) -> approve -> promote (+ replay) -> FRESH output downstream.

The agent's steps never promote: promotion here is the human reviewer's action.
"""

import pytest

from guardian.agent.diagnose import ACCEPTED, FakeClient, diagnose
from guardian.agent.propose import READY, propose, with_proposed_version
from guardian.core.guardian import PromotionError
from guardian.core.models import BlockStatus, Quality
from guardian.core.shadow import ShadowMode
from guardian.demo.faults import code_bug

from ..agent.helpers import answer, revert_fix
from ..helpers.profile import block_profiles
from ..helpers.project_repo import SPEC_REL, repo_state
from ..helpers.roles import descendants, representative
from .runners import StandaloneRunner
from .test_scenarios import ROLES, SPEC, assert_dependents_follow_rules, check_invariants

by_role = pytest.mark.parametrize("role", ROLES)
PROFILES = block_profiles(SPEC)


@by_role
def test_10_self_healing_loop(tmp_path, project_repo, role: str) -> None:
    block = representative(SPEC, role)
    runner = StandaloneRunner(SPEC, tmp_path / "guardian")

    # Detect and contain.
    runner.run("r1")
    # A bad deploy of the live version. Like the other scenarios' corruption, it garbles
    # the columns the block derives itself, so replaying its quarantined output through
    # a fixed version can re-derive them (a corrupted merge key would re-key the row).
    runner.inject_fault(block, code_bug(columns=PROFILES[block].derived_columns))
    r2 = runner.run("r2")
    assert r2[block].outcome == "ROLLBACK"
    assert runner.status(block) is BlockStatus.DEGRADED
    assert_dependents_follow_rules(runner, r2, "r2", block, "r1")

    # Diagnose and propose (the agent: read-only, dry run).
    client = FakeClient(
        {
            f"{block}/r2": answer("code_bug", ("E1",)),
            f"{block}/r2:fix": revert_fix(SPEC, block, ("E1",)),
        }
    )
    before = repo_state(project_repo)
    with runner._acting_guardian() as g:
        diagnosis = diagnose(g, block, "r2", client=client, git=False).diagnosis
        assert (diagnosis.root_cause, diagnosis.status) == ("code_bug", ACCEPTED)
        proposal = propose(
            g, block, "r2", client=client, spec_path=project_repo / SPEC_REL, git_history=False
        )
    assert proposal.status == READY, proposal.reasons
    assert repo_state(project_repo) == before
    assert runner.active_version(block) != proposal.version  # nothing was promoted

    # A human merges the PR; the new version runs in shadow while the bug is live.
    runner.spec, runner.registry = with_proposed_version(runner.spec, proposal)
    runner.shadow_start(block, proposal.version)
    r3 = runner.run("r3")
    assert r3[block].outcome == "ROLLBACK"  # the live version is still the buggy one
    _, runs = runner.shadow_runs(block)
    assert runs[-1].mode is ShadowMode.ABSOLUTE and runs[-1].within_tolerance
    assert runs[-1].pass_rate == 1.0

    # It would be approved: promotion needs (and gets) an explicit approval.
    with pytest.raises(PromotionError, match="needs approval"):
        runner.promote(block)
    result = runner.promote(block, approve=True)
    assert (result.to_version, runner.active_version(block)) == (proposal.version,) * 2
    assert runner.status(block) is BlockStatus.HEALTHY

    # The buggy deploy is gone: the next run is FRESH end to end.
    runner.clear_faults(block)
    r4 = runner.run("r4")
    assert all(r.outcome == "PASS" for r in r4.values())
    for b in (block, *descendants(SPEC, block)):
        assert runner.provenance(b, "r4")["quality"] == Quality.FRESH.value, b
    check_invariants(runner, "r1", "r2", "r3", "r4")
