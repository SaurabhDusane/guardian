"""Drift statistics: profiles, PSI, z-scores and levels (block-agnostic, small frames)."""

import math

import numpy as np
import pandas as pd
import pytest

from guardian.core.drift import (
    NULL,
    DriftLevel,
    DriftProfile,
    DriftStore,
    check,
    psi,
)
from guardian.core.models import BlockSpec, DriftPolicy, DriftThresholds


def frame(n: int = 400, seed: int = 0, shift: float = 0.0, cat_bias: float = 0.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    cats = rng.choice(["a", "b", "c", "d"], size=n, p=[0.25, 0.25, 0.25, 0.25])
    if cat_bias:
        cats[: int(n * cat_bias)] = "a"
    return pd.DataFrame(
        {
            "id": np.arange(n),
            "amount": rng.normal(100, 10, n) + shift,
            "cat": cats,
            "email": [f"u{i}@x.com" for i in range(n)],
            "ts": pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC"),
        }
    )


SPEC = BlockSpec("blk", "m:f", merge_key=("id",), drift=DriftPolicy(min_history=2))
REFERENCE = [("r1", frame(seed=1)), ("r2", frame(seed=2))]


def test_psi_basics() -> None:
    assert psi({"a": 0.5, "b": 0.5}, {"a": 0.5, "b": 0.5}) == 0.0
    expected = (0.9 - 0.5) * math.log(0.9 / 0.5) + (0.1 - 0.5) * math.log(0.1 / 0.5)
    assert psi({"a": 0.5, "b": 0.5}, {"a": 0.9, "b": 0.1}) == pytest.approx(expected)
    assert math.isfinite(psi({"a": 1.0}, {"b": 1.0}))  # empty bins are floored


def test_profile_tracks_distributions_not_identities() -> None:
    profile = DriftProfile.learn(REFERENCE, SPEC.drift, SPEC.merge_key)
    tracked = {c.column: c for c in profile.columns}
    assert set(tracked) == {"amount", "cat"}
    assert "merge key" in profile.skipped["id"]
    assert "distinct" in profile.skipped["email"]
    assert profile.skipped["ts"] == "datetime"
    assert tracked["amount"].kind == "numeric" and tracked["amount"].mean == pytest.approx(
        100, abs=1
    )
    assert tracked["cat"].shares.keys() == {"a", "b", "c", "d"}
    only = DriftProfile.learn(REFERENCE, DriftPolicy(columns=("cat",)), ())
    assert [c.column for c in only.columns] == ["cat"]
    without = DriftProfile.learn(REFERENCE, DriftPolicy(exclude=("amount",)), ())
    assert "amount" not in {c.column for c in without.columns}


def test_same_distribution_is_ok() -> None:
    report = check(SPEC, "r3", frame(seed=3), REFERENCE)
    assert report.level is DriftLevel.OK and report.drifted == []
    assert all(c.psi < 0.1 for c in report.columns)
    assert report.reference_runs == ("r1", "r2") and report.reason == "no drift"


def test_numeric_shift_fails_by_psi_and_z() -> None:
    report = check(SPEC, "r3", frame(seed=3, shift=15), REFERENCE)
    amount = next(c for c in report.columns if c.column == "amount")
    assert amount.level is DriftLevel.FAIL and amount.psi > 0.25 and amount.z > 6
    assert amount.mean_now == pytest.approx(amount.mean_ref + 15, abs=2)
    assert report.level is DriftLevel.FAIL and "amount" in report.reason


def test_category_shift_and_null_burst() -> None:
    skewed = check(SPEC, "r3", frame(seed=3, cat_bias=0.6), REFERENCE)
    cat = next(c for c in skewed.columns if c.column == "cat")
    assert cat.level is DriftLevel.FAIL and cat.z is None and cat.top_now[0][0] == "a"

    nulls = frame(seed=3)
    nulls.loc[:150, "amount"] = None
    report = check(SPEC, "r3", nulls, REFERENCE)
    amount = next(c for c in report.columns if c.column == "amount")
    assert amount.null_rate_now > 0.35 and amount.level is DriftLevel.FAIL
    assert NULL in dict(amount.top_now)

    unseen = frame(seed=3)
    unseen.loc[:200, "cat"] = "zzz"  # a category the profile never saw
    assert check(SPEC, "r3", unseen, REFERENCE).level is DriftLevel.FAIL


def test_warn_and_fail_thresholds_and_history() -> None:
    lenient = BlockSpec(
        "blk",
        "m:f",
        drift=DriftPolicy(
            min_history=2, warn=DriftThresholds(psi=0.05), fail=DriftThresholds(psi=50)
        ),
    )
    assert check(lenient, "r3", frame(seed=3, shift=15), REFERENCE).level is DriftLevel.WARN
    few = check(SPEC, "r2", frame(seed=3), REFERENCE[:1])
    assert few.level is DriftLevel.NO_BASELINE and "baseline" in few.reason
    windowed = BlockSpec("blk", "m:f", drift=DriftPolicy(window=2, min_history=1))
    report = check(windowed, "r9", frame(seed=9), [*REFERENCE, ("r3", frame(seed=3))])
    assert report.reference_runs == ("r2", "r3")


def test_policy_validation() -> None:
    for kwargs in (
        {"window": 0},
        {"min_history": 6, "window": 5},
        {"warn": DriftThresholds(psi=0.5), "fail": DriftThresholds(psi=0.2)},
        {"bins": True},
    ):
        with pytest.raises(ValueError):
            DriftPolicy(**kwargs)
    with pytest.raises(ValueError):
        DriftThresholds(psi=-1)


def test_store_round_trip(tmp_path) -> None:
    report = check(SPEC, "r3", frame(seed=3, shift=15), REFERENCE)
    store = DriftStore(tmp_path)
    store.write(report)
    saved = store.get("blk", "r3")
    assert saved["level"] == "FAIL" and saved["reason"] == report.reason
    assert store.get("blk", "nope") is None
