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


@pytest.mark.parametrize("upstream_status", [BlockStatus.DEGRADED, BlockStatus.OUT])
@pytest.mark.parametrize("source_status", [BlockStatus.DEGRADED, BlockStatus.OUT])
def test_unhealthy_fallback_source_is_not_used(upstream_status, source_status) -> None:
    """Both the replaced upstream and the fallback source unhealthy: stale, no adapter."""
    statuses = {"b6": upstream_status, "b5": source_status}
    ref = plan_input(make_spec(), "b8", "b6", *lookups(statuses, {"b6": "r1", "b5": "r3"}))
    assert ref == DataRef("b6", "r1", requested="b6", stale=True)
    assert ref.adapter is None and not ref.rerouted


def test_both_unhealthy_and_upstream_never_promoted_raises() -> None:
    """The unhealthy fallback's snapshot is not a safe input, even if it exists."""
    statuses = {"b6": BlockStatus.DEGRADED, "b5": BlockStatus.DEGRADED}
    with pytest.raises(NoSafeInputError, match="fallback source 'b5' is DEGRADED"):
        plan_input(make_spec(), "b8", "b6", *lookups(statuses, {"b5": "r3"}))


def test_both_unhealthy_and_no_last_good_anywhere_raises() -> None:
    statuses = {"b6": BlockStatus.OUT, "b5": BlockStatus.OUT}
    with pytest.raises(NoSafeInputError):
        plan_input(make_spec(), "b8", "b6", *lookups(statuses, {}))


def test_healthy_fallback_source_without_snapshot_reads_stale_upstream() -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({"b6": BlockStatus.DEGRADED}, {"b6": "r1"}))
    assert ref == DataRef("b6", "r1", requested="b6", stale=True)


@pytest.mark.parametrize("status", [BlockStatus.DEGRADED, BlockStatus.OUT])
def test_unhealthy_source_block_dependents_read_stale(status) -> None:
    """b1 is a source block (no inputs); its dependent b6 has no fallback for it."""
    ref = plan_input(make_spec(), "b6", "b1", *lookups({"b1": status}, {"b1": "r1"}))
    assert ref == DataRef("b1", "r1", requested="b1", stale=True)


def test_fallback_without_snapshot_falls_back_to_stale_upstream() -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({"b6": BlockStatus.DEGRADED}, {"b6": "r1"}))
    assert ref == DataRef("b6", "r1", requested="b6", stale=True)


def test_no_fallback_edge_reads_stale_upstream() -> None:
    ref = plan_input(make_spec(), "b6", "b1", *lookups({"b1": BlockStatus.DEGRADED}, {"b1": "r1"}))
    assert ref == DataRef("b1", "r1", requested="b1", stale=True)


def test_nothing_safe_raises() -> None:
    with pytest.raises(
        NoSafeInputError, match=r"'b6' has no last-good snapshot; fallback source 'b5'"
    ):
        plan_input(make_spec(), "b8", "b6", *lookups({"b6": BlockStatus.DEGRADED}, {}))


def test_upstream_must_be_an_input() -> None:
    with pytest.raises(ValueError):
        plan_input(make_spec(), "b8", "b1", *lookups({}, {"b1": "r1"}))
