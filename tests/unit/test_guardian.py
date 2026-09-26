import pandas as pd
import pytest

from guardian.core.events import EventKind
from guardian.core.guardian import CRASH_REASON, ROLLBACK_RULE, Guardian
from guardian.core.models import Action, BlockStatus, NoSafeInputError, QuarantineStatus
from guardian.core.refs import load_ref

from .conftest import REGISTRY, frame, make_spec

# ---------------------------------------------------------------- on_output


def test_clean_output_passes_and_promotes(guardian) -> None:
    decision = guardian.on_output("b1", "r1", frame([1.0, 2.0, 3.0]))
    assert decision.action is Action.PASS
    assert (decision.total_rows, decision.good_rows, decision.bad_rows) == (3, 3, 0)
    assert guardian.snapshots.last_good("b1").run_id == "r1"
    assert guardian.status("b1") is BlockStatus.HEALTHY
    assert guardian.quarantine.count() == 0


def test_threshold_boundary_at_threshold_passes(guardian) -> None:
    # 2 bad of 10 == 0.2 == threshold -> PASS
    decision = guardian.on_output("b1", "r1", frame([1.0] * 8 + [-1.0] * 2))
    assert decision.action is Action.PASS
    assert decision.bad_fraction == pytest.approx(0.2)
    assert len(guardian.snapshots.read("b1", "r1")) == 8
    records = guardian.quarantine.list(block="b1")
    assert len(records) == 2
    assert {r.rule_name for r in records} == {"amount:greater_than_or_equal_to(0)"}
    assert all(r.run_id == "r1" and r.status is QuarantineStatus.QUARANTINED for r in records)
    assert sorted(r.payload()["amount"] for r in records) == [-1.0, -1.0]


def test_threshold_boundary_just_above_rolls_back(tmp_path) -> None:
    # 21 bad of 100 = 0.21 > 0.2 -> ROLLBACK; 20 of 100 would pass.
    with Guardian(make_spec(threshold=0.2), tmp_path, registry=REGISTRY) as g:
        g.on_output("b1", "r0", frame([1.0] * 10))
        decision = g.on_output("b1", "r1", frame([1.0] * 79 + [-1.0] * 21))
        assert decision.action is Action.ROLLBACK
        assert "exceeds threshold" in decision.reason
        assert g.status("b1") is BlockStatus.DEGRADED
        assert not g.snapshots.exists("b1", "r1")
        # downstream still points at the previous last-good
        assert g.snapshots.last_good("b1").run_id == "r0"
        assert decision.snapshot.run_id == "r0"
        # nothing lost: all 100 rows quarantined; the 79 good ones under "rollback"
        records = g.quarantine.list(block="b1", run_id="r1")
        assert len(records) == 100
        assert sum(r.rule_name == ROLLBACK_RULE for r in records) == 79
        assert len(g.events.query(kind=EventKind.ROLLBACK)) == 1


def test_threshold_zero_rejects_any_bad_row(tmp_path) -> None:
    with Guardian(make_spec(threshold=0.0), tmp_path, registry=REGISTRY) as g:
        assert g.on_output("b1", "r1", frame([1.0] * 3)).action is Action.PASS
        assert g.on_output("b1", "r2", frame([1.0, -1.0])).action is Action.ROLLBACK


def test_schema_level_failure_rolls_back(guardian) -> None:
    guardian.on_output("b1", "r0", frame([1.0]))
    decision = guardian.on_output("b1", "r1", pd.DataFrame({"id": [1, 2]}))
    assert decision.action is Action.ROLLBACK
    assert "missing column 'amount'" in decision.reason
    assert guardian.status("b1") is BlockStatus.DEGRADED
    assert guardian.quarantine.count(block="b1") == 2
    assert guardian.snapshots.last_good("b1").run_id == "r0"


def test_pass_after_degraded_restores_health(guardian) -> None:
    guardian.on_output("b1", "r1", pd.DataFrame({"id": [1]}))
    assert guardian.status("b1") is BlockStatus.DEGRADED
    guardian.on_output("b1", "r2", frame([1.0]))
    assert guardian.status("b1") is BlockStatus.HEALTHY


def test_empty_output_passes(guardian) -> None:
    decision = guardian.on_output("b1", "r1", frame([]))
    assert decision.action is Action.PASS
    assert guardian.snapshots.read("b1", "r1").empty


def test_rerunning_same_run_id_is_rejected(guardian) -> None:
    guardian.on_output("b1", "r1", frame([1.0]))
    with pytest.raises(Exception, match="already exists"):
        guardian.on_output("b1", "r1", frame([2.0]))


# ---------------------------------------------------------------- crash


def test_crash_is_rollback_with_reason_crash(guardian) -> None:
    guardian.on_output("b1", "r1", frame([1.0]))
    decision = guardian.run_block("crashy", "r1", [frame([1.0])])
    assert decision.action is Action.ROLLBACK
    assert decision.reason == CRASH_REASON
    assert guardian.status("crashy") is BlockStatus.DEGRADED
    errors = guardian.events.query(kind=EventKind.ERROR, block="crashy")
    assert "block exploded" in errors[0].data["error"]
    assert len(guardian.events.query(kind=EventKind.ROLLBACK, block="crashy")) == 1


def test_non_dataframe_return_is_a_crash(tmp_path) -> None:
    registry = {**REGISTRY, "identity": lambda *_: "oops"}
    with Guardian(make_spec(), tmp_path, registry=registry) as g:
        assert g.run_block("b5", "r1").reason == CRASH_REASON


def test_run_block_passes_output_to_on_output(guardian) -> None:
    decision = guardian.run_block("b1", "r1", [frame([1.0, -1.0, 2.0, 3.0, 4.0])])
    assert decision.action is Action.PASS and decision.bad_rows == 1


# ---------------------------------------------------------------- resolve_input


def _seed(g: Guardian) -> None:
    g.on_output("b1", "r1", frame([1.0, 2.0]))
    g.on_output("b5", "r1", pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]}))
    g.on_output("b6", "r1", frame([1.0, 2.0]))


def test_resolve_healthy_reads_latest_last_good(guardian) -> None:
    _seed(guardian)
    guardian.on_output("b6", "r2", frame([5.0]))
    ref = guardian.resolve_input("b8", "b6", run_id="r3")
    assert (ref.block, ref.run_id, ref.rerouted) == ("b6", "r2", False)
    events = guardian.events.query(kind=EventKind.RESOLVE, block="b8")
    assert events[-1].data["source"] == "b6"


def test_fallback_resolution_applies_adapter(guardian) -> None:
    _seed(guardian)
    guardian.on_output("b6", "r2", pd.DataFrame({"id": [1]}))  # schema-less: passes
    guardian.set_block_status("b6", BlockStatus.OUT)
    ref = guardian.resolve_input("b8", "b6", run_id="r3")
    assert ref.block == "b5" and ref.requested == "b6" and ref.adapter == "b5_to_b6_shape"
    df = guardian.read(ref)
    assert df["amount"].tolist() == [10.0, 20.0]
    assert df["via_fallback"].all()
    reroutes = guardian.events.query(kind=EventKind.REROUTE)
    assert reroutes[-1].data["source"] == "b5"
    assert reroutes[-1].data["upstream"] == "b6"
    assert reroutes[-1].data["upstream_status"] == "OUT"


def test_fallback_on_degraded_upstream(guardian) -> None:
    _seed(guardian)
    guardian.run_block("b6", "r2", [frame([1.0])])  # passes
    guardian.on_crash("b6", "r3", RuntimeError("x"))
    df = guardian.load_input("b8", "b6")
    assert "via_fallback" in df.columns


def test_stale_read_when_no_fallback_exists(guardian) -> None:
    guardian.on_output("b1", "r1", frame([1.0, 2.0]))
    guardian.on_output("b1", "r2", frame([-1.0] * 5))  # rollback
    assert guardian.status("b1") is BlockStatus.DEGRADED
    ref = guardian.resolve_input("b6", "b1", run_id="r2")
    assert (ref.block, ref.run_id, ref.stale, ref.adapter) == ("b1", "r1", True, None)
    assert guardian.read(ref)["amount"].tolist() == [1.0, 2.0]
    event = guardian.events.query(kind=EventKind.RESOLVE, block="b6")[-1]
    assert event.data["stale"] is True and event.data["source_run_id"] == "r1"


def test_no_safe_input_error(guardian) -> None:
    guardian.on_output("b1", "r1", pd.DataFrame({"id": [1]}))  # schema failure, no last-good
    with pytest.raises(NoSafeInputError):
        guardian.resolve_input("b6", "b1", run_id="r1")
    assert guardian.events.query(kind=EventKind.ERROR, block="b6")


def test_no_safe_input_error_with_fallback_but_no_snapshots(guardian) -> None:
    guardian.set_block_status("b6", BlockStatus.OUT)
    with pytest.raises(NoSafeInputError):
        guardian.resolve_input("b8", "b6")


def test_no_safe_input_for_healthy_upstream_never_run(guardian) -> None:
    with pytest.raises(NoSafeInputError):
        guardian.resolve_input("b6", "b1")


# ---------------------------------------------------------------- status


def test_set_block_status(guardian) -> None:
    guardian.set_block_status("b6", BlockStatus.OUT)
    assert guardian.status("b6") is BlockStatus.OUT
    guardian.set_block_status("b6", "HEALTHY")
    assert guardian.status("b6") is BlockStatus.HEALTHY
    with pytest.raises(ValueError):
        guardian.set_block_status("b6", BlockStatus.DEGRADED)
    with pytest.raises(KeyError):
        guardian.set_block_status("nope", BlockStatus.OUT)
    changes = guardian.events.query(kind=EventKind.STATUS_CHANGE, block="b6")
    assert [e.data["status"] for e in changes] == ["OUT", "HEALTHY"]


def test_out_block_stays_out_after_runs(guardian) -> None:
    guardian.set_block_status("b1", BlockStatus.OUT)
    guardian.on_output("b1", "r1", frame([1.0]))
    assert guardian.status("b1") is BlockStatus.OUT
    guardian.on_output("b1", "r2", pd.DataFrame({"id": [1]}))
    assert guardian.status("b1") is BlockStatus.OUT


def test_status_persists_across_instances(tmp_path) -> None:
    with Guardian(make_spec(), tmp_path, registry=REGISTRY) as g:
        g.set_block_status("b6", BlockStatus.OUT)
    with Guardian(make_spec(), tmp_path, registry=REGISTRY) as g:
        assert g.status("b6") is BlockStatus.OUT
        assert g.all_statuses()["b1"] is BlockStatus.HEALTHY


# ---------------------------------------------------------------- replay


def test_replay_after_fix_merges_and_marks_replayed(tmp_path) -> None:
    with Guardian(make_spec(threshold=0.5), tmp_path, registry=REGISTRY) as g:
        g.on_output("b1", "r1", frame([1.0, -2.0, 3.0, -4.0]))
        assert g.quarantine.count(block="b1") == 2
        # Replay with the unfixed block: nothing passes, nothing changes.
        result = g.replay("b1", run_id="replay-1")
        assert (result.replayed, result.still_failing) == (0, 2)
        # "Fix" the block and replay again.
        g.registry["identity"] = REGISTRY["fix_amount"]
        result = g.replay("b1", run_id="replay-2")
        assert (result.replayed, result.still_failing) == (2, 0)
        assert g.snapshots.last_good("b1").run_id == "replay-2"
        merged = g.read(result.snapshot)
        assert sorted(merged["amount"].tolist()) == [1.0, 2.0, 3.0, 4.0]
        # replayed records are kept, never deleted
        assert g.quarantine.count(block="b1") == 2
        assert g.quarantine.count(block="b1", status=QuarantineStatus.REPLAYED) == 2
        # a second replay finds nothing left to do
        assert g.replay("b1").replayed == 0


def test_replay_partial(tmp_path) -> None:
    def fix_some(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.loc[out["amount"] > -3, "amount"] = out["amount"].abs()
        return out

    with Guardian(make_spec(threshold=1.0), tmp_path, registry=REGISTRY) as g:
        g.on_output("b1", "r1", frame([1.0, -2.0, -5.0]))
        g.registry["identity"] = fix_some
        result = g.replay("b1")
        assert (result.replayed, result.still_failing) == (1, 1)
        remaining = g.quarantine.list(block="b1", status=QuarantineStatus.QUARANTINED)
        assert [r.payload()["amount"] for r in remaining] == [-5.0]


def test_replay_restores_rolled_back_rows_and_health(tmp_path) -> None:
    with Guardian(make_spec(threshold=0.2), tmp_path, registry=REGISTRY) as g:
        g.on_output("b1", "r1", frame([1.0, -1.0, -1.0]))
        assert g.status("b1") is BlockStatus.DEGRADED
        g.registry["identity"] = REGISTRY["fix_amount"]
        result = g.replay("b1")
        assert result.replayed == 3 and result.still_failing == 0
        assert g.status("b1") is BlockStatus.HEALTHY
        assert len(g.read(result.snapshot)) == 3


def test_replay_crashing_fn_leaves_records_quarantined(tmp_path) -> None:
    with Guardian(make_spec(threshold=1.0), tmp_path, registry=REGISTRY) as g:
        g.on_output("b1", "r1", frame([-1.0]))
        g.registry["identity"] = REGISTRY["boom"]
        result = g.replay("b1")
        assert (result.replayed, result.still_failing) == (0, 1)
        assert g.quarantine.count(status=QuarantineStatus.QUARANTINED) == 1
        assert g.events.query(kind=EventKind.ERROR, block="b1")


def test_replay_nothing_quarantined(guardian) -> None:
    result = guardian.replay("b1")
    assert (result.replayed, result.still_failing, result.snapshot) == (0, 0, None)


# ---------------------------------------------------------------- refs


def test_load_ref_prefers_registry_then_imports() -> None:
    assert load_ref("identity", REGISTRY) is REGISTRY["identity"]
    assert load_ref("guardian.core.refs:load_ref") is load_ref
    # "demo.x" resolves under the guardian package
    assert load_ref("core.refs:load_ref") is load_ref
    with pytest.raises(ValueError):
        load_ref("no_colon")
