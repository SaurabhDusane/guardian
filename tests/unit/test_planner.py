import pytest

from guardian.core.models import BlockStatus, DataRef, NoSafeInputError
from guardian.core.planner import plan_input

from .conftest import make_spec


def lookups(statuses: dict[str, BlockStatus], last_good: dict[str, str]):
    return (
        lambda b: statuses.get(b, BlockStatus.HEALTHY),
        lambda b: DataRef(b, last_good[b]) if b in last_good else None,
    )


def test_healthy_reads_upstream_last_good() -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({}, {"b6": "r2", "b5": "r1"}))
    assert ref == DataRef("b6", "r2", requested="b6")
    assert not ref.rerouted and not ref.stale


def test_healthy_without_snapshot_raises() -> None:
    with pytest.raises(NoSafeInputError):
        plan_input(make_spec(), "b8", "b6", *lookups({}, {"b5": "r1"}))


@pytest.mark.parametrize("status", [BlockStatus.DEGRADED, BlockStatus.OUT])
def test_unavailable_upstream_uses_fallback(status) -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({"b6": status}, {"b6": "r1", "b5": "r3"}))
    assert ref == DataRef("b5", "r3", requested="b6", adapter="b5_to_b6_shape")
    assert ref.rerouted and not ref.stale


def test_fallback_source_degraded_is_marked_stale() -> None:
    statuses = {"b6": BlockStatus.OUT, "b5": BlockStatus.DEGRADED}
    ref = plan_input(make_spec(), "b8", "b6", *lookups(statuses, {"b5": "r3"}))
    assert ref.block == "b5" and ref.stale


def test_fallback_without_snapshot_falls_back_to_stale_upstream() -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({"b6": BlockStatus.DEGRADED}, {"b6": "r1"}))
    assert ref == DataRef("b6", "r1", requested="b6", stale=True)


def test_no_fallback_edge_reads_stale_upstream() -> None:
    ref = plan_input(make_spec(), "b6", "b1", *lookups({"b1": BlockStatus.DEGRADED}, {"b1": "r1"}))
    assert ref == DataRef("b1", "r1", requested="b1", stale=True)


def test_nothing_safe_raises() -> None:
    with pytest.raises(NoSafeInputError, match=r"b6.*b5"):
        plan_input(make_spec(), "b8", "b6", *lookups({"b6": BlockStatus.DEGRADED}, {}))


def test_upstream_must_be_an_input() -> None:
    with pytest.raises(ValueError):
        plan_input(make_spec(), "b8", "b1", *lookups({}, {"b1": "r1"}))
