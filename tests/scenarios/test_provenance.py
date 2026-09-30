"""Provenance and blast radius, for every DAG role, under both runners.

Blocks come from their role; expected qualities and impact sets are derived from the
spec's DAG and the resolution rules, never from block names.
"""

import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from guardian.core.models import BlockStatus, Quality
from guardian.demo.faults import crash

from ..helpers.roles import dependents, descendants, representative, role_params
from .test_scenarios import ROLES, SPEC, check_invariants, corrupt
from .test_shadow import versioned

by_role = pytest.mark.parametrize("role", role_params(ROLES))
FRESH, STALE, FALLBACK = Quality.FRESH.value, Quality.STALE.value, Quality.FALLBACK.value


def quality(runner, block: str, run_id: str) -> str:
    return runner.provenance(block, run_id)["quality"]


def read_of(runner, block: str, run_id: str, upstream: str) -> dict:
    (i,) = [i for i in runner.provenance(block, run_id)["inputs"] if i["upstream"] == upstream]
    return i


def impacted(runner, block: str, since: str | None = None) -> tuple[set, set]:
    entries = runner.impact(block, since)
    self_runs = {(e.block, e.run_id) for e in entries if e.relation == "self"}
    downstream = {(e.block, e.run_id) for e in entries if e.relation == "downstream"}
    return self_runs, downstream


def expected_read(block: str, upstream: str) -> str:
    """How a dependent reads an unhealthy upstream when everything else is healthy."""
    return FALLBACK if SPEC.block(block).fallback_for(upstream) else STALE


# ---------------------------------------------------------------- quality propagation


@by_role
def test_fault_quality_propagates_and_impact_lists_it(runner, role: str) -> None:
    x = representative(SPEC, role)
    runner.run("r0")
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    runner.run("r1")

    downstream = set(descendants(SPEC, x))
    for b in SPEC.block_names:
        if b == x:
            assert runner.provenance(b, "r1")["has_snapshot"] is False  # rolled back
        elif b in downstream:
            assert quality(runner, b, "r1") != FRESH, b
        else:
            assert quality(runner, b, "r1") == FRESH, b
    for d in dependents(SPEC, x):
        read = read_of(runner, d, "r1", x)
        assert read["read"] == expected_read(d, x)
        assert quality(runner, d, "r1") == expected_read(d, x)
        if read["read"] == STALE:
            assert (read["block"], read["run_id"], read["adapter"]) == (x, "r0", None)

    self_runs, touched = impacted(runner, x)
    assert self_runs == {(x, "r1")}
    assert touched == {(b, "r1") for b in downstream}
    if role == "leaf":
        assert touched == set()  # empty apart from the block itself
    if role == "fallback_protected":  # the fallback reader is FALLBACK
        assert any(quality(runner, d, "r1") == FALLBACK for d in dependents(SPEC, x))
    if role == "unprotected":  # dependents are STALE
        assert all(quality(runner, d, "r1") == STALE for d in dependents(SPEC, x))
    if role == "source":  # the whole downstream subgraph degrades
        assert downstream == set(SPEC.block_names) - {x}
    check_invariants(runner, "r1")


def test_fallback_reader_is_marked_fallback_and_listed(runner) -> None:
    x = representative(SPEC, "fallback_protected")
    d = next(d for d in dependents(SPEC, x) if SPEC.block(d).fallback_for(x))
    edge = SPEC.block(d).fallback_for(x)
    runner.run("r0")
    runner.inject_fault(x, crash())
    runner.run("r1")
    read = read_of(runner, d, "r1", x)
    assert (read["block"], read["run_id"], read["adapter"], read["read"]) == (
        edge.source,
        "r1",
        edge.adapter,
        FALLBACK,
    )
    assert quality(runner, d, "r1") == FALLBACK
    assert (d, "r1") in impacted(runner, x)[1]


def test_unprotected_dependents_are_stale(runner) -> None:
    x = representative(SPEC, "unprotected")
    runner.run("r0")
    runner.set_block_status(x, BlockStatus.OUT)
    runner.run("r1")
    for d in dependents(SPEC, x):
        assert quality(runner, d, "r1") == STALE
        assert read_of(runner, d, "r1", x)["upstream_status"] == "OUT"
    self_runs, touched = impacted(runner, x)
    assert self_runs == {(x, "r1")}
    assert {(d, "r1") for d in dependents(SPEC, x)} <= touched


def test_source_fault_degrades_the_whole_subgraph(runner) -> None:
    x = representative(SPEC, "source")
    runner.run("r0")
    runner.inject_fault(x, crash())
    runner.run("r1")
    for b in descendants(SPEC, x):
        assert quality(runner, b, "r1") == STALE, b  # no fallback replaces a source here
    assert impacted(runner, x)[1] == {(b, "r1") for b in descendants(SPEC, x)}


# ---------------------------------------------------------------- two blocks down


def test_fallback_ignored_when_source_degraded(runner) -> None:
    x = representative(SPEC, "fallback_protected")
    d = next(d for d in dependents(SPEC, x) if SPEC.block(d).fallback_for(x))
    s = SPEC.block(d).fallback_for(x).source
    runner.run("r0")
    runner.inject_fault(x, crash())
    runner.inject_fault(s, crash())
    runner.run("r1")
    read = read_of(runner, d, "r1", x)
    assert (read["block"], read["run_id"], read["adapter"], read["read"]) == (x, "r0", None, STALE)
    assert quality(runner, d, "r1") == STALE
    assert (d, "r1") in impacted(runner, x)[1]
    # the fallback source's own other dependents read it stale and are in its impact
    others = [o for o in dependents(SPEC, s) if o != x and not SPEC.block(o).fallback_for(s)]
    assert others
    for o in others:
        assert quality(runner, o, "r1") == STALE
        assert (o, "r1") in impacted(runner, s)[1]
    check_invariants(runner, "r1")


# ---------------------------------------------------------------- recovery


@by_role
def test_next_run_is_fresh_after_promotion_and_replay(runner, role: str) -> None:
    x = versioned(role)
    runner.run("r0")
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    runner.run("r1")
    runner.clear_faults(x)
    runner.set_block_status(x, BlockStatus.OUT)
    runner.shadow_start(x, "v2")
    runner.run("r2")  # x OUT: dependents read it stale, the candidate runs
    assert all(quality(runner, d, "r2") != FRESH for d in dependents(SPEC, x))
    runner.shadow_promote(x, approve=True)  # replays x's quarantine through v2
    replay = runner.promotions(x)[-1].replay_run_id
    if replay is not None:
        assert quality(runner, x, replay) == FRESH

    runner.run("r3")
    for b in SPEC.block_names:
        assert quality(runner, b, "r3") == FRESH, b
    assert runner.provenance(x, "r3")["version"] == "v2"
    assert impacted(runner, x, since="r3") == (set(), set())
    check_invariants(runner, "r3")


# ---------------------------------------------------------------- impact


FAULTS = ("crash", "corrupt", "out")


def _independent_impact(records, block: str) -> set[tuple[str, str]]:
    """Upward search: a snapshot is impacted iff its lineage reaches a non-fresh read of
    ``block``. (``impact`` itself searches downward from those reads.)"""
    by_id = {f"{p.block}@{p.run_id}": p for p in records if p.has_snapshot}
    memo: dict[str, bool] = {}

    def touches(sid: str) -> bool:
        if sid not in memo:
            memo[sid] = False  # guards against (impossible) cycles
            p = by_id[sid]
            memo[sid] = any(
                (i.upstream == block and i.read is not Quality.FRESH)
                or (i.source_id in by_id and touches(i.source_id))
                for i in p.inputs
            )
        return memo[sid]

    return {(p.block, p.run_id) for sid, p in by_id.items() if touches(sid)}


def _8f_settings(examples: int):
    return settings(
        max_examples=examples,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    )


@pytest.mark.slow
@_8f_settings(6)
@given(data=st.data())
def test_impact_matches_an_independent_computation(runner, data) -> None:
    check_8f(runner, data)


@_8f_settings(2)
@given(data=st.data())
def test_impact_matches_an_independent_computation_quick(runner, data) -> None:
    """The same property, fewer examples (the default run); the full run has 6."""
    check_8f(runner, data)


def check_8f(runner, data) -> None:
    blocks = list(SPEC.block_names)
    plan = [
        data.draw(
            st.dictionaries(st.sampled_from(blocks), st.sampled_from(FAULTS), max_size=2),
            label=f"faults in run r{k}",
        )
        for k in (1, 2, 3)
    ]
    with tempfile.TemporaryDirectory() as root:
        r = type(runner)(runner.spec, Path(root))
        try:
            r.run("r0")
            outcomes: dict[str, dict[str, str]] = {}
            for k, faults in enumerate(plan, start=1):
                for b in blocks:  # blocks taken OUT last run come back unless OUT again
                    if r.status(b) is BlockStatus.OUT and faults.get(b) != "out":
                        r.set_block_status(b, BlockStatus.HEALTHY)
                for b, kind in faults.items():
                    if kind == "out":
                        r.set_block_status(b, BlockStatus.OUT)
                    else:
                        r.inject_fault(b, crash() if kind == "crash" else corrupt(b, 0.5, seed=k))
                result = r.run(f"r{k}")
                outcomes[f"r{k}"] = {b: res.outcome for b, res in result.items()}
                r.clear_faults()

            records = r.provenance_records()
            for x in blocks:
                self_runs, touched = impacted(r, x)
                # 1. equals an upward search over the recorded lineage
                assert touched == _independent_impact(records, x), x
                # 2. the block's own degraded runs, from the runner's own outcomes
                assert self_runs == {
                    (x, run) for run, o in outcomes.items() if o[x] in ("ROLLBACK", "SKIPPED")
                }
                # 3. consistent with the DAG: only descendants, and every dependent that
                #    ran while x was degraded is touched
                assert {b for b, _ in touched} <= set(descendants(SPEC, x))
                for run, o in outcomes.items():
                    if o[x] in ("ROLLBACK", "SKIPPED"):
                        for d in dependents(SPEC, x):
                            if o[d] == "PASS":
                                assert (d, run) in touched, (x, d, run)
                # 4. everything touched is non-fresh
                for b, run in touched:
                    assert quality(r, b, run) != FRESH
            check_invariants(r, *outcomes)
        finally:
            r.close()
