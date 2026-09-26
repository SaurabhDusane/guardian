"""Phase 7 shadow promotion scenarios (7a-7g), parametrized over every DAG role.

Each role's representative block declares ``v1`` (active), ``v2`` (an improvement that
is identical on well-formed data) and ``v_bad`` (an off-by-one). Nothing here names a
block: blocks come from their role, expectations from the spec. Every scenario runs
under both runners (the ``runner`` fixture in conftest.py).
"""

import pandas as pd
import pytest

from guardian.core.events import EventKind
from guardian.core.guardian import PromotionError
from guardian.core.models import BlockStatus, Quality, QuarantineStatus
from guardian.core.shadow import ShadowMode
from guardian.core.versions import PromotionKind, PromotionState

from ..helpers.roles import ancestors, dependents, descendants, representative
from .test_scenarios import (
    PROFILES,
    ROLES,
    SPEC,
    assert_fresh,
    check_invariants,
    corrupt,
)

by_role = pytest.mark.parametrize("role", ROLES)


def versioned(role: str) -> str:
    x = representative(SPEC, role)
    assert {"v1", "v2", "v_bad"} <= set(SPEC.block(x).versions), (role, x)
    assert SPEC.block(x).active == "v1"
    return x


def required_runs(block: str) -> int:
    return SPEC.block(block).shadow_policy().required_runs


def by_key(df: pd.DataFrame, block: str) -> pd.DataFrame:
    return df.sort_values(list(SPEC.block(block).merge_key), ignore_index=True)


def assert_same_output(runner, block: str, run_a: str, run_b: str) -> None:
    pd.testing.assert_frame_equal(
        by_key(runner.snapshot(block, run_a), block), by_key(runner.snapshot(block, run_b), block)
    )


def assert_normal_edges(runner, result, block: str, run_id: str) -> None:
    """``block`` passed on its live version and every dependent read it fresh."""
    assert result[block].outcome == "PASS"
    assert runner.events(EventKind.REROUTE, run_id=run_id) == []
    for d in dependents(SPEC, block):
        (ref,) = [r for r in result[d].sources if r.requested == block]
        assert (ref.block, ref.run_id, ref.stale, ref.adapter) == (block, run_id, False, None)
    assert runner.provenance(block, run_id)["quality"] == Quality.FRESH.value


# ---------------------------------------------------------------- 7a


@by_role
def test_7a_out_block_absolute_shadow_approved_promotion(runner, role: str) -> None:
    x = versioned(role)
    runner.run("r0")
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    runner.run("r1")  # live v1 goes bad: rolled back, all rows quarantined
    n = len(runner.quarantine(block=x, run_id="r1"))
    runner.clear_faults(x)
    runner.set_block_status(x, BlockStatus.OUT)

    runner.shadow_start(x, "v2")
    for i in range(required_runs(x) + 1):  # more than enough runs: still no auto-promotion
        result = runner.run(f"s{i}")
        assert result[x].outcome == "SKIPPED"
        _, runs = runner.shadow_runs(x)
        assert runs[-1].run_id == f"s{i}"
        assert runs[-1].mode is ShadowMode.ABSOLUTE and runs[-1].within_tolerance
        assert runs[-1].comparison is None and runs[-1].pass_rate == 1.0
    assert runner.active_version(x) == "v1"
    assert runner.status(x) is BlockStatus.OUT

    with pytest.raises(PromotionError, match="absolute mode"):
        runner.promote(x)
    promoted = runner.promote(x, approve=True)

    assert (promoted.from_version, promoted.to_version, promoted.resumed) == ("v1", "v2", False)
    assert runner.active_version(x) == "v2"
    assert runner.status(x) is BlockStatus.HEALTHY
    assert runner.shadow_runs(x)[0] is None  # the shadow ended
    (promotion,) = runner.promotions(x)
    assert promotion.state is PromotionState.COMPLETED and promotion.approved

    # the quarantine was replayed through v2
    assert promoted.replay.replayed + promoted.replay.still_failing == n
    records = runner.quarantine(block=x, run_id="r1")
    assert all(r.status is QuarantineStatus.REPLAYED for r in records if r.rule_name == "rollback")
    assert sum(r.status is QuarantineStatus.REPLAYED for r in records) == promoted.replay.replayed
    assert runner.provenance(x, promotion.replay_run_id)["version"] == "v2"

    # dependents are back on normal edges (a leaf: just the block itself is FRESH again)
    result = runner.run("r2")
    assert runner.provenance(x, "r2")["version"] == "v2"
    assert_normal_edges(runner, result, x, "r2")
    check_invariants(runner, "r1", "r2")


# ---------------------------------------------------------------- 7b


@by_role
def test_7b_healthy_parity_shadow_auto_promotes(runner, role: str) -> None:
    x = versioned(role)
    req = required_runs(x)
    runner.run("r0")
    runner.shadow_start(x, "v2")
    for i in range(1, req + 1):
        assert runner.active_version(x) == "v1"  # not before the required runs
        runner.run(f"r{i}")
    assert runner.active_version(x) == "v2"
    (promotion,) = runner.promotions(x)
    assert promotion.reason == "auto" and not promotion.approved
    assert promotion.state is PromotionState.COMPLETED

    _, runs = runner.shadow_runs(x)
    assert runs == []  # shadow ended
    events = [e for e in runner.events(EventKind.SHADOW, block=x) if e.data["action"] == "run"]
    assert len(events) == req
    assert all(e.data["mode"] == "PARITY" and e.data["within_tolerance"] for e in events)
    assert all(e.data["changed_fraction"] == 0.0 for e in events)

    result = runner.run("after")
    assert runner.provenance(x, "after")["version"] == "v2"
    assert_normal_edges(runner, result, x, "after")
    for b in [x, *descendants(SPEC, x)]:  # v2 is identical on well-formed data
        assert_same_output(runner, b, "r0", "after")
    check_invariants(runner, "r0", "after")


# ---------------------------------------------------------------- 7c


@by_role
def test_7c_bad_candidate_is_never_promoted(runner, role: str) -> None:
    x = versioned(role)
    policy = SPEC.block(x).shadow_policy()
    runner.run("r0")
    runner.shadow_start(x, "v_bad")
    runs_n = required_runs(x) + 1
    for i in range(1, runs_n + 1):
        runner.run(f"r{i}")

    assert runner.active_version(x) == "v1"
    assert runner.promotions(x) == []
    assert runner.events(EventKind.PROMOTION) == []
    shadow, runs = runner.shadow_runs(x)
    assert shadow.version == "v_bad" and len(runs) == runs_n
    for r in runs:
        assert r.mode is ShadowMode.PARITY and not r.within_tolerance
        assert r.changed_fraction > policy.max_changed_fraction
        assert r.comparison.changed_columns
    with pytest.raises(PromotionError, match="needs approval"):
        runner.promote(x)

    # live output unchanged, here and downstream; the candidate really was different
    for i in range(1, runs_n + 1):
        for b in [x, *descendants(SPEC, x)]:
            assert_same_output(runner, b, "r0", f"r{i}")
    candidate = runner.candidate_snapshot(x, "v_bad", "r1")
    assert not by_key(candidate, x).equals(by_key(runner.snapshot(x, "r1"), x))
    check_invariants(runner, *[f"r{i}" for i in range(runs_n + 1)])


# ---------------------------------------------------------------- 7d


@by_role
def test_7d_promote_then_rollback_restores_previous_version(runner, role: str) -> None:
    x = versioned(role)
    runner.run("r0")
    runner.shadow_start(x, "v2")
    runner.run("r1")
    with pytest.raises(PromotionError, match="needs approval"):
        runner.promote(x)  # fewer runs than the policy requires
    runner.promote(x, approve=True)
    runner.run("r2")
    assert runner.provenance(x, "r2")["version"] == "v2"

    rollback = runner.rollback_version(x)
    assert (rollback.kind, rollback.from_version, rollback.to_version) == (
        PromotionKind.ROLLBACK,
        "v2",
        "v1",
    )
    assert runner.active_version(x) == "v1"
    assert [p.kind for p in runner.promotions(x)] == [PromotionKind.PROMOTE, PromotionKind.ROLLBACK]

    runner.run("r3")
    assert runner.provenance(x, "r3")["version"] == "v1"
    for b in [x, *descendants(SPEC, x)]:  # identical to before the promotion
        assert_same_output(runner, b, "r0", "r3")
    check_invariants(runner, "r0", "r1", "r2", "r3")


# ---------------------------------------------------------------- 7e


@by_role
def test_7e_no_consumer_ever_reads_a_candidate(runner, role: str) -> None:
    x = versioned(role)
    runner.run("r0")
    runner.shadow_start(x, "v_bad")  # a candidate whose output differs from live
    for run_id in ["r1", "r2"]:
        runner.run(run_id)
        # every read in the run was of a live snapshot...
        reads = runner.events(EventKind.RESOLVE, run_id=run_id) + runner.events(
            EventKind.REROUTE, run_id=run_id
        )
        assert reads
        for e in reads:
            assert runner.snapshot(e.data["source"], e.data["source_run_id"]) is not None
        # ...and every snapshot's provenance names only live inputs
        for b in SPEC.block_names:
            prov = runner.provenance(b, run_id)
            assert prov["kind"] == "run"
            for inp in prov["inputs"]:
                assert inp["store"] == "live"
                assert runner.snapshot(inp["block"], inp["run_id"]) is not None
        # the candidate read the same live inputs as the live version
        cand = runner.candidate_provenance(x, "v_bad", run_id)
        assert cand["kind"] == "candidate" and cand["version"] == "v_bad"
        assert cand["inputs"] == runner.provenance(x, run_id)["inputs"]
        # and no consumer saw its (different) data
        live, candidate = runner.snapshot(x, run_id), runner.candidate_snapshot(x, "v_bad", run_id)
        assert not by_key(candidate, x).equals(by_key(live, x))
        for b in descendants(SPEC, x):
            assert_same_output(runner, b, "r0", run_id)
    check_invariants(runner, "r1", "r2")


# ---------------------------------------------------------------- 7f


def _shadow_pair(role: str) -> tuple[str, str]:
    """(upstream, downstream): the role's block and its nearest versioned relative."""
    x = versioned(role)
    for d in descendants(SPEC, x):
        if SPEC.block(d).versions:
            return x, d
    for a in reversed(ancestors(SPEC, x)):
        if SPEC.block(a).versions:
            return a, x
    raise LookupError(f"no versioned block related to {x}")


@by_role
def test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently(
    runner, role: str
) -> None:
    up, down = _shadow_pair(role)
    req = required_runs(up)
    assert required_runs(down) == req
    runner.run("r0")
    runner.shadow_start(up, "v2")
    runner.run("r1")
    runner.shadow_start(down, "v2")  # one run later: they must promote at different runs
    for i in range(2, req + 2):
        runner.run(f"r{i}")
        # the downstream candidate read exactly the live inputs its live version read,
        # never the upstream candidate
        cand = runner.candidate_provenance(down, "v2", f"r{i}")
        assert cand["inputs"] == runner.provenance(down, f"r{i}")["inputs"]
        assert all(inp["store"] == "live" for inp in cand["inputs"])
        if i == req:
            assert runner.active_version(up) == "v2", "upstream promotes after its runs"
            assert runner.active_version(down) == "v1", "downstream is one run behind"
    assert runner.active_version(up) == "v2" and runner.active_version(down) == "v2"
    auto = [p for b in (up, down) for p in runner.promotions(b)]
    assert [(p.block, p.reason) for p in auto] == [(up, "auto"), (down, "auto")]
    assert auto[0].completed_at < auto[1].completed_at
    check_invariants(runner, *[f"r{i}" for i in range(req + 2)])


# ---------------------------------------------------------------- 7g


@by_role
@pytest.mark.parametrize("interrupt", ["during_replay", "after_replay"])
def test_7g_interrupted_promotion_is_resumable(runner, role: str, interrupt: str) -> None:
    x = versioned(role)
    runner.run("r0")
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    runner.run("r1")
    n = len(runner.quarantine(block=x, run_id="r1"))
    assert n == PROFILES[x].rows
    runner.clear_faults(x)
    runner.shadow_start(x, "v2")
    runner.run("r2")
    assert runner.last_good(x).run_id == "r2"

    with pytest.raises(RuntimeError, match="power cut"):
        runner.promote(x, approve=True, interrupt=interrupt)

    # the registry does not claim a completed promotion
    pending = runner.pending_promotion(x)
    assert pending is not None and pending.state is PromotionState.PROMOTING
    assert (pending.from_version, pending.to_version) == ("v1", "v2")
    assert runner.active_version(x) == "v1"
    assert runner.shadow_runs(x)[0] is not None  # still in shadow
    records = runner.quarantine(block=x, run_id="r1")
    if interrupt == "during_replay":
        assert all(r.status is QuarantineStatus.QUARANTINED for r in records)
        assert runner.last_good(x).run_id == "r2"

    resumed = runner.promote(x)  # resumable without re-approving
    assert resumed.resumed and resumed.to_version == "v2"
    assert runner.pending_promotion(x) is None
    assert runner.active_version(x) == "v2"
    assert runner.status(x) is BlockStatus.HEALTHY
    (promotion,) = runner.promotions(x)  # one promotion, completed once
    assert promotion.state is PromotionState.COMPLETED
    assert runner.last_good(x).run_id == promotion.replay_run_id
    merged = runner.read_last_good(x)
    key = list(SPEC.block(x).merge_key)
    assert not merged.duplicated(subset=key).any()
    records = runner.quarantine(block=x, run_id="r1")
    assert all(r.status is QuarantineStatus.REPLAYED for r in records if r.rule_name == "rollback")
    with pytest.raises(PromotionError, match="no candidate in shadow"):
        runner.promote(x)

    result = runner.run("r3")
    assert runner.provenance(x, "r3")["version"] == "v2"
    assert_normal_edges(runner, result, x, "r3")
    check_invariants(runner, "r1", "r2", "r3")
    assert_fresh(result, "r3")
