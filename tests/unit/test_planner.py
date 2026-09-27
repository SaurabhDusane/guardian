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


@pytest.mark.parametrize("status", [BlockStatus.DEGRADED, BlockStatus.OUT])
def test_unavailable_upstream_uses_fallback(status) -> None:
    ref = plan_input(make_spec(), "b8", "b6", *lookups({"b6": status}, {"b6": "r1", "b5": "r3"}))
    assert ref == DataRef("b5", "r3", requested="b6", adapter="b5_to_b6_shape")
    assert ref.rerouted and not ref.stale


@pytest.mark.parametrize("upstream_status", [BlockStatus.DEGRADED, BlockStatus.OUT])
@pytest.mark.parametrize("source_status", [BlockStatus.DEGRADED, BlockStatus.OUT])
def test_unhealthy_fallback_source_is_not_used(upstream_status, source_status) -> None:
    statuses = {"b6": upstream_status, "b5": source_status}
    ref = plan_input(make_spec(), "b8", "b6", *lookups(statuses, {"b6": "r1", "b5": "r3"}))
    assert ref == DataRef("b6", "r1", requested="b6", stale=True)
    assert ref.adapter is None and not ref.rerouted


@pytest.mark.parametrize(
    ("block", "upstream", "statuses", "last_good"),
    [
        pytest.param(
            "b8", "b6", {"b6": BlockStatus.DEGRADED}, {"b6": "r1"}, id="fallback_source_no_snapshot"
        ),
        pytest.param(
            "b6", "b1", {"b1": BlockStatus.DEGRADED}, {"b1": "r1"}, id="no_fallback_edge_degraded"
        ),
        pytest.param("b6", "b1", {"b1": BlockStatus.OUT}, {"b1": "r1"}, id="no_fallback_edge_out"),
    ],
)
def test_reads_stale_upstream_when_no_fallback_applies(
    block, upstream, statuses, last_good
) -> None:
    ref = plan_input(make_spec(), block, upstream, *lookups(statuses, last_good))
    assert ref == DataRef(upstream, last_good[upstream], requested=upstream, stale=True)


@pytest.mark.parametrize(
    ("statuses", "last_good", "match"),
    [
        pytest.param({}, {"b5": "r1"}, "no last-good snapshot yet", id="healthy_no_snapshot"),
        pytest.param(
            {"b6": BlockStatus.DEGRADED},
            {},
            r"'b6' has no last-good snapshot; fallback source 'b5'",
            id="degraded_nothing_anywhere",
        ),
        pytest.param(
            # the unhealthy fallback's snapshot is not a safe input, even if it exists
            {"b6": BlockStatus.DEGRADED, "b5": BlockStatus.DEGRADED},
            {"b5": "r3"},
            "fallback source 'b5' is DEGRADED",
            id="both_degraded_upstream_never_promoted",
        ),
        pytest.param(
            {"b6": BlockStatus.OUT, "b5": BlockStatus.OUT},
            {},
            "no last-good",
            id="both_out_nothing",
        ),
    ],
)
def test_no_safe_input_raises(statuses, last_good, match) -> None:
    with pytest.raises(NoSafeInputError, match=match):
        plan_input(make_spec(), "b8", "b6", *lookups(statuses, last_good))


def test_upstream_must_be_an_input() -> None:
    with pytest.raises(ValueError):
        plan_input(make_spec(), "b8", "b1", *lookups({}, {"b1": "r1"}))
