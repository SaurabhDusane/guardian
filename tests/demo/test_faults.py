import pandas as pd
import pytest

from guardian.demo.faults import (
    CORRUPT_NUMBER,
    CORRUPT_TEXT,
    FaultInjected,
    code_bug,
    corrupt_rows,
    crash,
    inject,
    null_burst,
    parse_fault,
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


@pytest.mark.parametrize(
    "text,block,name",
    [
        ("b6_enrich:corrupt:0.5:region,segment", "b6_enrich", "corrupt_rows(0.5)"),
        ("b4_clean:corrupt:0.1", "b4_clean", "corrupt_rows(0.1)"),
        ("b6_enrich:null:segment:0.3", "b6_enrich", "null_burst(segment, 0.3)"),
        ("b6_enrich:crash", "b6_enrich", "crash"),
        ("b2_parse:code_bug", "b2_parse", "code_bug(0.5)"),
        ("b2_parse:code_bug:0.25", "b2_parse", "code_bug(0.25)"),
    ],
)
def test_parse_fault(text, block, name) -> None:
    parsed_block, fault = parse_fault(text)
    assert parsed_block == block and fault.name == name


def test_parse_fault_drift(df) -> None:
    _, drop = parse_fault("b:drop:x,s")
    assert list(drop(df).columns) == ["n", "t"]
    _, rename = parse_fault("b:rename:s=text")
    assert "text" in rename(df).columns


@pytest.mark.parametrize(
    "text",
    ["b6", "b6:explode", "b6:corrupt:lots", "b6:crash:now", "b:rename:x", "b:code_bug:1:2"],
)
def test_parse_fault_rejects(text) -> None:
    with pytest.raises(ValueError):
        parse_fault(text)


def test_corrupt_rows_unique_values(df) -> None:
    out = corrupt_rows(0.25, seed=3, unique=True)(df)
    hit = out["s"].str.startswith(CORRUPT_TEXT)
    assert hit.sum() == 5
    assert out.loc[hit, "s"].is_unique and out.loc[hit, "n"].is_unique
    assert (out.loc[hit, "n"] <= CORRUPT_NUMBER).all()
    assert out.dtypes.equals(df.dtypes)


def test_code_bug_swaps_the_implementation(df) -> None:
    """A new implementation (its code fingerprint differs), wrong in exactly
    round(fraction * n) rows: numbers negated, text padded, timestamps lost."""
    from guardian.core.code import fingerprint

    def block(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.assign(x=frame["x"] * 2)

    buggy = inject(block, code_bug(0.25, seed=3))
    out, good = buggy(df), block(df)
    rows = pick_positions(len(df), 0.25, 3)
    assert (out["x"].iloc[rows] == -good["x"].iloc[rows]).all()
    assert (out["s"].iloc[rows] == "a ").all() and out["t"].iloc[rows].isna().all()
    untouched = [i for i in range(len(df)) if i not in set(rows)]
    pd.testing.assert_frame_equal(out.iloc[untouched], good.iloc[untouched])
    assert fingerprint(buggy, "r").sha != fingerprint(block, "r").sha
    # Data faults wrap the implementation: the fingerprint is unchanged.
    assert fingerprint(inject(block, corrupt_rows(0.5)), "r").sha == fingerprint(block, "r").sha


def test_drift_fault_skews_with_existing_values(df) -> None:
    from guardian.core.models import BlockSpec
    from guardian.demo.faults import drift

    frame = df.assign(n=range(20), x=[float(i) for i in range(20)], s=list("ab" * 10))
    out = drift(0.5, seed=1)(frame)
    rows = pick_positions(20, 0.5, 1)
    assert (out["x"].iloc[rows] == 19.0).all() and (out["n"].iloc[rows] == 19).all()
    assert out["s"].iloc[rows].nunique() == 1 and out["s"].iloc[rows].iloc[0] in ("a", "b")
    assert out["t"].equals(frame["t"])  # datetimes untouched
    assert set(out["s"]) <= set(frame["s"]) and out["x"].max() <= frame["x"].max()

    bound = drift(0.5, seed=1).for_block(BlockSpec("b", "m:f", merge_key=("n",)))
    kept = bound(frame)
    assert kept["n"].equals(frame["n"])  # the merge key is row identity: left alone
    _, parsed = parse_fault("b:drift:0.3")
    assert parsed.name == "drift(0.3)" and parsed.for_block is not None
