import pandas as pd
import pytest

from guardian.demo.faults import (
    CORRUPT_NUMBER,
    CORRUPT_TEXT,
    FaultInjected,
    corrupt_rows,
    crash,
    inject,
    null_burst,
    pick_positions,
    schema_drift,
)


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "n": range(20),
            "x": [1.0] * 20,
            "s": ["a"] * 20,
            "t": pd.to_datetime(["2024-01-01"] * 20, utc=True),
        }
    )


def test_pick_positions_exact_and_deterministic() -> None:
    assert len(pick_positions(200, 0.05, seed=1)) == 10
    assert (pick_positions(200, 0.05, 1) == pick_positions(200, 0.05, 1)).all()
    assert len(set(pick_positions(10, 1.0, 0))) == 10
    assert len(pick_positions(0, 0.5, 0)) == 0
    with pytest.raises(ValueError):
        pick_positions(10, 1.5, 0)


def test_corrupt_rows_preserves_dtypes_and_counts(df) -> None:
    out = corrupt_rows(0.25, seed=3)(df)
    assert out.dtypes.equals(df.dtypes)
    hit = out["s"] == CORRUPT_TEXT
    assert hit.sum() == 5
    assert (out.loc[hit, "n"] == CORRUPT_NUMBER).all()
    assert (out.loc[hit, "x"] == CORRUPT_NUMBER).all()
    assert out.loc[hit, "t"].isna().all()
    assert df["s"].eq("a").all()  # input untouched


def test_corrupt_rows_limited_columns(df) -> None:
    out = corrupt_rows(0.5, columns=["s"])(df)
    assert (out["s"] == CORRUPT_TEXT).sum() == 10
    pd.testing.assert_series_equal(out["x"], df["x"])


def test_schema_drift(df) -> None:
    out = schema_drift(rename={"s": "text"}, drop=["x"])(df)
    assert list(out.columns) == ["n", "text", "t"]
    with pytest.raises(ValueError):
        schema_drift()


def test_null_burst(df) -> None:
    out = null_burst("n", 0.3, seed=1)(df)
    assert out["n"].isna().sum() == 6
    assert null_burst("s", 0.5)(df)["s"].isna().sum() == 10


def test_crash_and_inject(df) -> None:
    def fn(frame: pd.DataFrame, *, k: int) -> pd.DataFrame:
        return frame.head(k)

    wrapped = inject(fn, schema_drift(drop=["x"]), null_burst("n", 0.5))
    out = wrapped(df, k=10)
    assert "x" not in out.columns and out["n"].isna().sum() == 5
    assert wrapped.__name__ == "fn"
    with pytest.raises(FaultInjected, match="boom"):
        inject(fn, crash("boom"))(df, k=1)
