"""Fault-injection scenarios, run against every runner and parametrized by DAG role.

No block is named here: each scenario picks the representative block of a role
(tests/helpers/roles.py) and derives its expectations from the pipeline spec, so the
suite holds for any spec that has these roles.
"""

import pandas as pd
import pytest

from guardian.core.events import EventKind
from guardian.core.models import BlockStatus, QuarantineStatus
from guardian.demo.faults import corrupt_rows, crash, schema_drift

from ..helpers.profile import block_profiles
from ..helpers.roles import dependents, descendants, present_roles, representative
from .invariants import assert_end_to_end_accounting, assert_no_silent_loss
from .runners import BlockResult, ScenarioRunner, scenario_spec

SPEC = scenario_spec()
ROLES = present_roles(SPEC)
PROFILES = block_profiles(SPEC)
by_role = pytest.mark.parametrize("role", ROLES)


# ---------------------------------------------------------------- helpers


def corrupt(block: str, fraction: float, seed: int = 0):
    """Corrupt the columns ``block`` itself derives (so a fixed block can re-derive them)."""
    return corrupt_rows(fraction, columns=PROFILES[block].derived_columns, seed=seed, unique=True)


def keys(df: pd.DataFrame, block: str) -> set[tuple]:
    """Merge keys as strings (payloads hold emitted values, snapshots coerced ones)."""
    key = list(SPEC.block(block).merge_key or ())
    return {tuple(str(v) for v in row) for row in df[key].itertuples(index=False)}


def record_key(record, block: str) -> tuple:
    return tuple(str(record.payload().get(c)) for c in SPEC.block(block).merge_key or ())


def check_invariants(runner: ScenarioRunner, *run_ids: str) -> None:
    for run_id in run_ids:
        assert_no_silent_loss(runner, run_id)
        assert_end_to_end_accounting(runner, run_id)


def assert_fresh(result: dict[str, BlockResult], run_id: str, blocks=None) -> None:
    """Each block read its inputs as produced in ``run_id``: no stale or rerouted reads."""
    for block in blocks if blocks is not None else result:
        for ref in result[block].sources:
            assert (ref.block, ref.run_id, ref.stale, ref.adapter) == (
                ref.requested,
                run_id,
                False,
                None,
            ), (block, ref)


def assert_rest_unaffected(runner, result, run_id: str, failed: str) -> None:
    """Every other block passed, quarantined nothing, and (unless it consumes the
    failed block) read fresh inputs."""
    direct = set(dependents(SPEC, failed))
    for block, r in result.items():
        if block == failed:
            continue
        assert r.outcome == "PASS", (block, r.outcome)
        assert runner.quarantine(block=block, run_id=run_id) == [], block
        if block not in direct:
            assert_fresh(result, run_id, [block])


def assert_dependents_follow_rules(runner, result, run_id: str, failed: str, prior: str):
    """Dependents of an unhealthy block: healthy fallback edge -> adapter; else STALE."""
    for d in dependents(SPEC, failed):
        refs = {ref.requested: ref for ref in result[d].sources}
        ref = refs.pop(failed)
        edge = SPEC.block(d).fallback_for(failed)
        if edge is not None:
            assert runner.status(edge.source) is BlockStatus.HEALTHY
            assert ref.rerouted
            assert (ref.block, ref.run_id, ref.adapter, ref.stale) == (
                edge.source,
                run_id,
                edge.adapter,
                False,
            )
        else:
            assert not ref.rerouted
            assert (ref.block, ref.run_id, ref.adapter, ref.stale) == (failed, prior, None, True)
        assert all(r.run_id == run_id and not r.stale for r in refs.values()), d


def assert_role_expectations(runner, result, run_id: str, role: str, failed: str) -> None:
    """What CLAUDE.md promises for each role when the block is unhealthy."""
    reroutes = runner.events(EventKind.REROUTE, run_id=run_id)
    if role == "leaf":
        # nothing reroutes; everything else ran on this run's data
        assert reroutes == []
        assert_fresh(result, run_id, [b for b in result if b != failed])
    if role == "unprotected":
        for d in dependents(SPEC, failed):
            (ref,) = [r for r in result[d].sources if r.requested == failed]
            assert ref.stale and not ref.rerouted
    if role == "fallback_protected":
        assert {e.block for e in reroutes} == {
            d for d in dependents(SPEC, failed) if SPEC.block(d).fallback_for(failed)
        }
    if role == "multi_dependent":
        assert len(dependents(SPEC, failed)) >= 2
        assert all(result[d].outcome == "PASS" for d in dependents(SPEC, failed))


# ---------------------------------------------------------------- 1. clean run


def test_clean_run(runner: ScenarioRunner) -> None:
    result = runner.run("r1")
    assert {b: r.outcome for b, r in result.items()} == dict.fromkeys(SPEC.block_names, "PASS")
    assert runner.quarantine() == []
    assert runner.events(EventKind.REROUTE) == []
    assert_fresh(result, "r1")
    for block in SPEC.block_names:
        assert runner.status(block) is BlockStatus.HEALTHY
        assert len(runner.snapshot(block, "r1")) == PROFILES[block].rows
    check_invariants(runner, "r1")


# ---------------------------------------------------------------- 2. minor corruption


@by_role
def test_minor_corruption_under_threshold(runner: ScenarioRunner, role: str) -> None:
    x = representative(SPEC, role)
    fraction = SPEC.block(x).quarantine_threshold / 2
    n = PROFILES[x].rows
    k = round(fraction * n)
    assert k >= 1, f"{x}: threshold too small to corrupt a row at {n} rows"

    runner.inject_fault(x, corrupt(x, fraction, seed=1))
    result = runner.run("r1")

    assert result[x].outcome == "PASS"
    assert runner.status(x) is BlockStatus.HEALTHY
    assert len(runner.quarantine(block=x, run_id="r1")) == k
    assert len(runner.snapshot(x, "r1")) == n - k
    # downstream is fine: every block passed on this run's data, nothing rerouted
    assert_rest_unaffected(runner, result, "r1", x)
    assert_fresh(result, "r1")
    assert runner.events(EventKind.REROUTE, run_id="r1") == []
    check_invariants(runner, "r1")


# ---------------------------------------------------------------- 3. heavy failure


@by_role
@pytest.mark.parametrize("mode", ["corrupt", "schema_drift"])
def test_heavy_failure_rolls_back_and_dependents_follow_rules(
    runner: ScenarioRunner, role: str, mode: str
) -> None:
    x = representative(SPEC, role)
    runner.run("r0")
    fault = (
        corrupt(x, 0.5, seed=2)
        if mode == "corrupt"
        else schema_drift(drop=PROFILES[x].derived_columns)
    )
    runner.inject_fault(x, fault)
    result = runner.run("r1")

    assert result[x].outcome == "ROLLBACK"
    assert runner.status(x) is BlockStatus.DEGRADED
    assert runner.last_good(x).run_id == "r0"
    assert len(runner.quarantine(block=x, run_id="r1")) == PROFILES[x].rows
    assert len(runner.events(EventKind.ROLLBACK, block=x, run_id="r1")) == 1

    assert_dependents_follow_rules(runner, result, "r1", x, prior="r0")
    assert_rest_unaffected(runner, result, "r1", x)
    assert_role_expectations(runner, result, "r1", role, x)
    check_invariants(runner, "r0", "r1")


# ---------------------------------------------------------------- 4. crash


@by_role
def test_crash_rolls_back_and_dependents_follow_rules(runner: ScenarioRunner, role: str) -> None:
    x = representative(SPEC, role)
    runner.run("r0")
    runner.inject_fault(x, crash(f"{role} block exploded"))
    result = runner.run("r1")

    assert result[x].outcome == "ROLLBACK"
    assert runner.status(x) is BlockStatus.DEGRADED
    assert runner.last_good(x).run_id == "r0"
    assert runner.quarantine(block=x, run_id="r1") == []
    errors = runner.events(EventKind.ERROR, block=x, run_id="r1")
    assert errors and "exploded" in errors[0].data["error"]

    assert_dependents_follow_rules(runner, result, "r1", x, prior="r0")
    assert_rest_unaffected(runner, result, "r1", x)
    assert_role_expectations(runner, result, "r1", role, x)
    check_invariants(runner, "r0", "r1")


# ---------------------------------------------------------------- 5. taken OUT


@by_role
def test_taken_out_then_restored(runner: ScenarioRunner, role: str) -> None:
    x = representative(SPEC, role)
    runner.run("r0")
    runner.set_block_status(x, BlockStatus.OUT)

    result = runner.run("r1")
    assert result[x].outcome == "SKIPPED"
    assert runner.status(x) is BlockStatus.OUT
    assert runner.snapshot(x, "r1") is None
    assert runner.last_good(x).run_id == "r0"
    assert_dependents_follow_rules(runner, result, "r1", x, prior="r0")
    assert_rest_unaffected(runner, result, "r1", x)
    assert_role_expectations(runner, result, "r1", role, x)
    check_invariants(runner, "r1")

    runner.set_block_status(x, BlockStatus.HEALTHY)
    result = runner.run("r2")
    assert all(r.outcome == "PASS" for r in result.values())
    assert_fresh(result, "r2")
    assert runner.events(EventKind.REROUTE, run_id="r2") == []
    check_invariants(runner, "r2")


# ---------------------------------------------------------------- 6. replay after fix


@by_role
def test_replay_after_fix(runner: ScenarioRunner, role: str) -> None:
    """Replay recovers what the fixed block can re-derive, exactly once, idempotently.

    Rows rolled back only because their batch failed (rule ``rollback``) are always
    recovered. Rows whose corruption the block cannot re-derive (a source cannot
    regenerate its data; a summary block cannot rebuild itself from its own output)
    stay QUARANTINED.
    """
    x = representative(SPEC, role)
    runner.run("r0")
    base_keys = keys(runner.snapshot(x, "r0"), x)
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    runner.run("r1")
    assert runner.status(x) is BlockStatus.DEGRADED
    n = len(runner.quarantine(block=x, run_id="r1"))
    assert n == PROFILES[x].rows

    runner.clear_faults(x)  # "fix" the block
    result = runner.replay(x)
    assert result.merged and result.replayed + result.still_failing == n
    assert result.replayed > 0

    records = runner.quarantine(block=x, run_id="r1")
    assert len(records) == n  # never deleted
    replayed = [r for r in records if r.status is QuarantineStatus.REPLAYED]
    assert len(replayed) == result.replayed
    assert all(r.status is QuarantineStatus.REPLAYED for r in records if r.rule_name == "rollback")

    # the upserted snapshot is promoted and holds each key once
    assert runner.last_good(x) == result.snapshot
    merged = runner.read_last_good(x)
    assert len(merged) == len(keys(merged, x))
    assert keys(merged, x) == base_keys | {record_key(r, x) for r in replayed}
    assert runner.status(x) is BlockStatus.HEALTHY
    check_invariants(runner, "r1")

    # a second replay changes nothing
    before = (runner.last_good(x), [(r.id, r.status) for r in runner.quarantine(block=x)])
    again = runner.replay(x)
    assert (again.replayed, again.still_failing, again.snapshot) == (0, result.still_failing, None)
    assert (runner.last_good(x), [(r.id, r.status) for r in runner.quarantine(block=x)]) == before

    # and the next run takes the normal path
    after = runner.run("r2")
    assert all(r.outcome == "PASS" for r in after.values())
    assert_fresh(after, "r2")
    check_invariants(runner, "r2")


# ---------------------------------------------------------------- 7. recompute after replay


def _sorted(df: pd.DataFrame, block: str) -> pd.DataFrame:
    return df.sort_values(list(SPEC.block(block).merge_key), ignore_index=True)


@pytest.mark.parametrize("prior_clean_run", [False, True], ids=["first_run", "after_clean_run"])
def test_descendants_after_replay_match_clean_run(
    runner: ScenarioRunner, tmp_path, prior_clean_run: bool
) -> None:
    """Outage in a fallback-protected block, fix, replay, recompute its descendants:
    the results equal an uninterrupted run."""
    x = representative(SPEC, "fallback_protected")
    downstream = descendants(SPEC, x)
    clean = type(runner)(runner.spec, tmp_path / "clean")
    clean.run("c1")
    expected = {b: _sorted(clean.snapshot(b, "c1"), b) for b in downstream}
    clean.close()

    if prior_clean_run:
        runner.run("r0")
    runner.inject_fault(x, corrupt(x, 0.5, seed=2))
    result = runner.run("r1")
    assert any(result[d].rerouted for d in dependents(SPEC, x))

    runner.clear_faults()
    replay = runner.replay(x)
    assert (replay.replayed, replay.still_failing, replay.merged) == (PROFILES[x].rows, 0, True)

    result = runner.run("r2", only=downstream)
    assert list(result) == downstream
    for d in dependents(SPEC, x):
        (ref,) = [r for r in result[d].sources if r.requested == x]
        assert (ref.block, ref.run_id, ref.stale) == (x, replay.snapshot.run_id, False)
    for b in downstream:
        pd.testing.assert_frame_equal(_sorted(runner.snapshot(b, "r2"), b), expected[b])

    assert runner.replay(x).snapshot is None  # idempotent
    runner.run("r3", only=downstream)
    for b in downstream:
        pd.testing.assert_frame_equal(_sorted(runner.snapshot(b, "r3"), b), expected[b])
    for run_id in ["r1", "r2", "r3"]:
        assert_no_silent_loss(runner, run_id)


# ---------------------------------------------------------------- 8. several blocks unhealthy


def _protected_pair() -> tuple[str, str, str]:
    """(replaced upstream, its consumer with a fallback edge, the fallback source)."""
    x = representative(SPEC, "fallback_protected")
    d = next(d for d in dependents(SPEC, x) if SPEC.block(d).fallback_for(x))
    return x, d, SPEC.block(d).fallback_for(x).source


def _make_unhealthy(runner: ScenarioRunner, block: str, how: str) -> None:
    if how == "out":
        runner.set_block_status(block, BlockStatus.OUT)
    else:
        runner.inject_fault(block, crash(f"{block} down"))


@pytest.mark.parametrize("source_state", ["crash", "out"])
def test_fallback_source_also_unhealthy_reads_stale_without_adapter(
    runner: ScenarioRunner, source_state: str
) -> None:
    x, d, s = _protected_pair()
    runner.run("r0")
    runner.inject_fault(x, crash(f"{x} down"))
    _make_unhealthy(runner, s, source_state)
    result = runner.run("r1")

    assert runner.status(x) is BlockStatus.DEGRADED
    assert runner.status(s) is (BlockStatus.OUT if source_state == "out" else BlockStatus.DEGRADED)
    assert result[d].outcome == "PASS"
    (ref,) = [r for r in result[d].sources if r.requested == x]
    assert (ref.block, ref.run_id, ref.adapter, ref.stale, ref.rerouted) == (
        x,
        "r0",
        None,
        True,
        False,
    )
    assert runner.events(EventKind.REROUTE, run_id="r1") == []
    if SPEC.block(d).inputs == (x,):  # same stale input -> same output as in r0
        pd.testing.assert_frame_equal(
            _sorted(runner.snapshot(d, "r1"), d), _sorted(runner.snapshot(d, "r0"), d)
        )
    # the fallback source's own dependents read it stale too
    for dep in dependents(SPEC, s):
        if dep != x and SPEC.block(dep).fallback_for(s) is None:
            (sref,) = [r for r in result[dep].sources if r.requested == s]
            assert (sref.block, sref.run_id, sref.stale) == (s, "r0", True)
    check_invariants(runner, "r1")


@pytest.mark.parametrize("source_state", ["crash", "out"])
def test_no_last_good_anywhere_blocks_the_consumer(
    runner: ScenarioRunner, source_state: str
) -> None:
    x, d, s = _protected_pair()
    runner.inject_fault(x, crash(f"{x} down"))
    _make_unhealthy(runner, s, source_state)
    result = runner.run("r1")  # first run: nothing has ever been promoted for x or s

    assert result[d].outcome == "BLOCKED"
    assert runner.snapshot(d, "r1") is None
    assert runner.events(EventKind.ERROR, block=d, run_id="r1")
    affected = {x, s, *descendants(SPEC, x), *descendants(SPEC, s)}
    for block, r in result.items():
        if block not in affected:
            assert r.outcome == "PASS", block
    check_invariants(runner, "r1")
