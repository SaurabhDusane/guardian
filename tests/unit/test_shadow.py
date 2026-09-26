import math

import pandas as pd
import pytest

from guardian.core.models import ShadowPolicy
from guardian.core.shadow import (
    Comparison,
    ShadowError,
    ShadowMode,
    ShadowRun,
    ShadowStatus,
    ShadowStore,
    auto_promotable,
    column_stats,
    compare_rows,
    evaluate,
)


def frame(**cols) -> pd.DataFrame:
    return pd.DataFrame(cols)


def test_identical_outputs_do_not_differ() -> None:
    a = frame(id=[1, 2, 3], x=[1.0, None, 3.0], s=["a", "b", None])
    c = compare_rows(a, a.iloc[::-1].reset_index(drop=True), ["id"])  # order-insensitive
    assert (c.added, c.removed, c.changed, c.changed_columns) == (0, 0, 0, ())
    assert c.changed_fraction == 0.0


def test_added_removed_changed_rows_and_columns() -> None:
    a = frame(id=[1, 2, 3, 4], x=[1, 2, 3, 4], s=["a", "b", "c", "d"])
    c = frame(id=[2, 3, 4, 5], x=[2, 30, 4, 5], s=["b", "c", "D", "e"])
    diff = compare_rows(a, c, ["id"])
    assert (diff.rows_active, diff.rows_candidate) == (4, 4)
    assert (diff.added, diff.removed, diff.changed) == (1, 1, 2)
    assert diff.changed_columns == ("x", "s")
    assert diff.changed_fraction == pytest.approx(4 / 4)


def test_int_vs_float_equal_values_are_not_changes() -> None:
    a = frame(id=[1, 2], x=[1, 2])
    c = frame(id=[1, 2], x=[1.0, 2.0])
    assert compare_rows(a, c, ["id"]).changed == 0


def test_schema_change_marks_every_common_row_changed() -> None:
    a = frame(id=[1, 2], x=[1, 2])
    c = frame(id=[1, 2], x=[1, 2], extra=[0, 0])
    diff = compare_rows(a, c, ["id"])
    assert diff.changed == 2 and diff.changed_columns == ("extra",)


def test_composite_key() -> None:
    a = frame(d=["x", "x"], r=["a", "b"], v=[1, 2])
    c = frame(d=["x", "x"], r=["a", "b"], v=[1, 3])
    assert compare_rows(a, c, ["d", "r"]).changed == 1


def test_compare_rejects_non_unique_or_missing_keys() -> None:
    with pytest.raises(ShadowError, match="not unique"):
        compare_rows(frame(id=[1, 1]), frame(id=[1]), ["id"])
    with pytest.raises(ShadowError, match="missing"):
        compare_rows(frame(id=[1]), frame(other=[1]), ["id"])


def test_column_stats() -> None:
    stats = column_stats(frame(x=[1.0, None, 3.0], s=["a", "a", None], b=[True, False, True]))
    assert stats["x"]["null_rate"] == pytest.approx(1 / 3)
    assert stats["x"]["mean"] == 2.0 and stats["x"]["distinct"] == 2
    assert stats["s"]["mean"] is None and stats["s"]["distinct"] == 1
    assert stats["b"]["mean"] is None
    assert column_stats(frame(x=[]))["x"] == {"null_rate": 0.0, "mean": None, "distinct": 0}


POLICY = ShadowPolicy(required_runs=2, max_changed_fraction=0.1, min_pass_rate=0.9)


def cmp(changed: int, rows: int = 100) -> Comparison:
    return Comparison(rows, rows, 0, 0, changed, ("x",) if changed else ())


@pytest.mark.parametrize(
    "mode,pass_rate,comparison,ok",
    [
        (ShadowMode.PARITY, 1.0, cmp(10), True),  # at the tolerance
        (ShadowMode.PARITY, 1.0, cmp(11), False),  # just above
        (ShadowMode.PARITY, 0.89, cmp(0), False),  # fails validation too often
        (ShadowMode.ABSOLUTE, 0.9, None, True),  # no diff in absolute mode
        (ShadowMode.ABSOLUTE, 0.5, None, False),
    ],
)
def test_evaluate(mode, pass_rate, comparison, ok) -> None:
    within, reasons = evaluate(POLICY, mode, pass_rate, comparison)
    assert within is ok and bool(reasons) is not ok


def run(mode: ShadowMode, ok: bool, run_id: str = "r") -> ShadowRun:
    return ShadowRun(1, "b", "v2", run_id, mode, 10, 10, 1.0, None, {}, ok)


def test_auto_promotable_needs_consecutive_parity_runs_within_tolerance() -> None:
    P, A = ShadowMode.PARITY, ShadowMode.ABSOLUTE
    assert not auto_promotable([run(P, True)], POLICY)
    assert auto_promotable([run(P, True), run(P, True)], POLICY)
    assert auto_promotable([run(P, False), run(P, True), run(P, True)], POLICY)
    assert not auto_promotable([run(P, True), run(P, False)], POLICY)
    assert not auto_promotable([run(P, True), run(A, True)], POLICY)  # absolute breaks it


def test_policy_defaults_and_validation() -> None:
    p = ShadowPolicy()
    assert (p.required_runs, p.max_changed_fraction, p.min_pass_rate) == (3, 0.01, None)
    for bad in [dict(required_runs=0), dict(max_changed_fraction=2), dict(min_pass_rate=-1)]:
        with pytest.raises(ValueError):
            ShadowPolicy(**bad)


def test_store_lifecycle(tmp_path) -> None:
    store = ShadowStore(tmp_path)
    s = store.start("b", "v2", expect_diff=True)
    assert store.get("b") == s and s.expect_diff and s.status is ShadowStatus.ACTIVE
    with pytest.raises(ShadowError, match="already shadowing"):
        store.start("b", "v3", expect_diff=False)
    r = ShadowRun(s.id, "b", "v2", "r1", ShadowMode.PARITY, 10, 9, 0.9, cmp(1, 10),
                  {"candidate": {"x": {"mean": 1.0}}}, True, ("note",))  # fmt: skip
    store.record(r)
    (back,) = store.runs(s.id)
    assert (back.run_id, back.mode, back.pass_rate, back.within_tolerance) == (
        "r1",
        ShadowMode.PARITY,
        0.9,
        True,
    )
    assert back.comparison == r.comparison and back.notes == ("note",)
    assert math.isclose(back.changed_fraction, 0.1)
    assert [x.block for x in store.active()] == ["b"]
    store.end(s.id, ShadowStatus.PROMOTED)
    assert store.get("b") is None and store.by_id(s.id).status is ShadowStatus.PROMOTED
    store.start("b", "v3", expect_diff=False)  # a new shadow after the old one ended
