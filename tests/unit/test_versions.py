import pytest

from guardian.core.versions import (
    PromotionKind,
    PromotionState,
    VersionError,
    VersionRegistry,
    VersionState,
)


@pytest.fixture
def reg(tmp_path) -> VersionRegistry:
    return VersionRegistry(tmp_path)


def test_empty_registry(reg) -> None:
    assert reg.get("b") is None and reg.all() == {} and reg.pending("b") is None
    assert reg.history() == []


def test_promotion_is_pending_until_completed(reg) -> None:
    p = reg.begin_promotion(
        "b", "v1", "v2", reason="approved", approved=True, last_good_before="r1"
    )
    assert p.state is PromotionState.PROMOTING and p.kind is PromotionKind.PROMOTE
    active = reg.get("b")
    # the live version is unchanged while PROMOTING
    assert (active.version, active.state, active.pending_version) == (
        "v1",
        VersionState.PROMOTING,
        "v2",
    )
    assert reg.pending("b").id == p.id
    with pytest.raises(VersionError, match="in progress"):
        reg.begin_promotion("b", "v1", "v3", reason="x", approved=True, last_good_before=None)

    reg.set_replay_snapshot(p.id, "replay-1")
    active = reg.complete_promotion(p.id)
    assert (active.version, active.previous_version, active.state) == (
        "v2",
        "v1",
        VersionState.ACTIVE,
    )
    done = reg.promotion(p.id)
    assert done.state is PromotionState.COMPLETED and done.replay_run_id == "replay-1"
    assert done.completed_at is not None and done.last_good_before == "r1"
    assert reg.pending("b") is None
    with pytest.raises(VersionError, match="already completed"):
        reg.complete_promotion(p.id)


def test_rollback_restores_previous_and_toggles(reg) -> None:
    with pytest.raises(VersionError, match="no previous version"):
        reg.rollback("b")
    p = reg.begin_promotion("b", "v1", "v2", reason="auto", approved=False, last_good_before=None)
    reg.complete_promotion(p.id)
    rb = reg.rollback("b")
    assert (rb.kind, rb.from_version, rb.to_version, rb.state) == (
        PromotionKind.ROLLBACK,
        "v2",
        "v1",
        PromotionState.COMPLETED,
    )
    assert (reg.get("b").version, reg.get("b").previous_version) == ("v1", "v2")
    assert [h.kind for h in reg.history("b")] == [PromotionKind.PROMOTE, PromotionKind.ROLLBACK]


def test_rollback_refused_while_promoting(reg) -> None:
    p = reg.begin_promotion("b", "v1", "v2", reason="auto", approved=False, last_good_before=None)
    reg.complete_promotion(p.id)
    reg.begin_promotion("b", "v2", "v3", reason="auto", approved=False, last_good_before=None)
    with pytest.raises(VersionError, match="in progress"):
        reg.rollback("b")


def test_registry_persists_across_instances(tmp_path) -> None:
    p = VersionRegistry(tmp_path).begin_promotion(
        "b", "v1", "v2", reason="auto", approved=False, last_good_before=None
    )
    assert VersionRegistry(tmp_path).pending("b").id == p.id
