from collections.abc import Iterator

import pandas as pd
import pandera.pandas as pa
import pytest

from guardian.core.guardian import Guardian
from guardian.core.models import BlockSpec, FallbackEdge, PipelineSpec

AMOUNT_SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(int),
        "amount": pa.Column(float, pa.Check.ge(0), coerce=True),
    }
)


def identity(df: pd.DataFrame) -> pd.DataFrame:
    return df


def fix_amount(df: pd.DataFrame) -> pd.DataFrame:
    """A 'fixed' block: negative amounts become their absolute value."""
    out = df.copy()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").abs()
    return out


def boom(*_: pd.DataFrame) -> pd.DataFrame:
    raise RuntimeError("block exploded")


def b5_to_b6_shape(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"value": "amount"})
    out["via_fallback"] = True
    return out


REGISTRY = {
    "identity": identity,
    "fix_amount": fix_amount,
    "boom": boom,
    "schema": AMOUNT_SCHEMA,
    "b5_to_b6_shape": b5_to_b6_shape,
}


def make_spec(threshold: float = 0.2, *, fallback: bool = True) -> PipelineSpec:
    fallbacks = (FallbackEdge("b6", "b5", adapter="b5_to_b6_shape"),) if fallback else ()
    return PipelineSpec(
        name="test",
        blocks=(
            BlockSpec("b1", fn="identity", schema="schema", quarantine_threshold=threshold),
            BlockSpec("b5", fn="identity"),
            BlockSpec("b6", fn="identity", inputs=("b1",)),
            BlockSpec("b8", fn="identity", inputs=("b6",), fallbacks=fallbacks),
            BlockSpec("crashy", fn="boom", inputs=("b1",)),
        ),
    )


@pytest.fixture
def guardian(tmp_path) -> Iterator[Guardian]:
    with Guardian(make_spec(), tmp_path, registry=REGISTRY) as g:
        yield g


def frame(amounts: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"id": range(len(amounts)), "amount": amounts})
