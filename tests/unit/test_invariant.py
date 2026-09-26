"""Property test for the core invariant: no input row is ever silently lost.

Every row handed to on_output ends up in exactly one of: the promoted snapshot for
that run, or quarantine for that run.
"""

import tempfile

import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from guardian.core.guardian import Guardian
from guardian.core.models import Action, QuarantineStatus

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


def _held_ids(g: Guardian, run_id: str) -> list[int]:
    """Where each input row of ``run_id`` is held now, one entry per row.

    The run's promoted snapshot, its still-QUARANTINED records, and for each REPLAYED
    record the replay snapshot it points at (which must contain its key exactly once).
    """
    ids: list[int] = []
    if g.snapshots.exists("b1", run_id):
        ids += g.snapshots.read("b1", run_id)["id"].tolist()
    for record in g.quarantine.list(block="b1", run_id=run_id):
        key = record.payload()["id"]
        if record.status is QuarantineStatus.REPLAYED:
            replayed = g.snapshots.read("b1", record.replay_run_id)["id"].tolist()
            assert replayed.count(key) == 1, (key, record.replay_run_id)
        ids.append(key)
    return ids


@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    df=frames(),
    threshold=st.sampled_from([0.0, 0.25, 1.0]),
    keyed=st.booleans(),
    prior=st.booleans(),
)
def test_exactly_once_through_replay(
    df: pd.DataFrame, threshold: float, keyed: bool, prior: bool
) -> None:
    """After a run and two replays, every input row is held exactly once."""
    spec = make_spec(threshold=threshold, merge_key=("id",) if keyed else None)
    with (
        tempfile.TemporaryDirectory() as root,
        Guardian(spec, root, registry=dict(REGISTRY)) as g,
    ):
        inputs: dict[str, list[int]] = {}
        if prior:  # an earlier clean run, so a keyed replay merges into a base snapshot
            base = pd.DataFrame({"id": [1000, 1001], "amount": [1.0, 2.0]})
            g.on_output("b1", "run0", base)
            inputs["run0"] = [1000, 1001]
        g.on_output("b1", "run1", df)
        inputs["run1"] = df["id"].tolist()

        def check() -> None:
            for run_id, ids in inputs.items():
                assert sorted(_held_ids(g, run_id)) == sorted(ids), run_id

        check()
        g.registry["identity"] = REGISTRY["fix_amount"]  # "fix" the block
        first = g.replay("b1", run_id="replay-1")
        check()
        if first.snapshot is not None:
            assert first.merged is keyed
            last_good = g.snapshots.last_good("b1")
            assert (last_good is not None and last_good.run_id == "replay-1") is keyed
            if keyed:  # the upserted snapshot holds each key exactly once
                merged_ids = g.snapshots.read("b1", "replay-1")["id"].tolist()
                assert len(merged_ids) == len(set(merged_ids))

        second = g.replay("b1", run_id="replay-2")
        assert second.replayed == 0 and second.snapshot is None
        assert not g.snapshots.exists("b1", "replay-2")
        check()
