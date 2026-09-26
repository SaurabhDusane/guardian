"""Property test for the core invariant: no input row is ever silently lost.

Every row handed to on_output ends up in exactly one of: the promoted snapshot for
that run, or quarantine for that run.
"""

import tempfile

import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from guardian.core.guardian import Guardian
from guardian.core.models import Action

from .conftest import REGISTRY, make_spec

value = st.one_of(
    st.floats(min_value=0, max_value=1e6, allow_nan=False),  # good
    st.floats(max_value=-0.001, min_value=-1e6, allow_nan=False),  # bad: negative
    st.just(float("nan")),  # bad: null
    st.sampled_from(["oops", "", "1e3", "-7"]),  # uncoercible / coercible strings
)


@st.composite
def frames(draw) -> pd.DataFrame:
    amounts = draw(st.lists(value, min_size=0, max_size=40))
    n = len(amounts)
    index = draw(
        st.one_of(
            st.just(list(range(n))),
            st.lists(st.integers(0, 5), min_size=n, max_size=n),  # duplicate labels
        )
    )
    return pd.DataFrame(
        {"id": list(range(n)), "amount": pd.Series(amounts, dtype=object)}, index=index
    )


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(df=frames(), threshold=st.sampled_from([0.0, 0.1, 0.25, 0.5, 1.0]), drop=st.booleans())
def test_no_silent_loss(df: pd.DataFrame, threshold: float, drop: bool) -> None:
    if drop:  # occasionally a schema-level failure (missing column)
        df = df.drop(columns=["amount"])
    with (
        tempfile.TemporaryDirectory() as root,
        Guardian(make_spec(threshold=threshold), root, registry=REGISTRY) as g,
    ):
        decision = g.on_output("b1", "run1", df)

        promoted_ids: list[int] = []
        if decision.action is Action.PASS:
            promoted_ids = g.snapshots.read("b1", "run1")["id"].tolist()
        else:
            assert not g.snapshots.exists("b1", "run1")
        quarantined_ids = [r.payload()["id"] for r in g.quarantine.list(block="b1", run_id="run1")]

        assert len(df) == len(promoted_ids) + len(quarantined_ids)
        assert sorted(promoted_ids + quarantined_ids) == sorted(df["id"].tolist())
        assert decision.good_rows + decision.bad_rows == decision.total_rows == len(df)
