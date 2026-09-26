"""Guardian facade: versions, shadow runs, promotion policy and provenance."""

from collections.abc import Iterator

import pandas as pd
import pytest

from guardian.core.events import EventKind
from guardian.core.guardian import Guardian, PromotionError
from guardian.core.models import (
    BlockSpec,
    BlockStatus,
    FallbackEdge,
    PipelineSpec,
    Quality,
    ShadowPolicy,
)
from guardian.core.shadow import ShadowMode
from guardian.runner.executor import Executor, Outcome

from .conftest import AMOUNT_SCHEMA


def load() -> pd.DataFrame:
    return pd.DataFrame({"id": range(10), "amount": [float(i + 1) for i in range(10)]})


def ident(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


def ident_v2(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


def plus_one(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["amount"] = out["amount"] + 1
    return out


def negative(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["amount"] = -out["amount"]
    return out


def boom(df: pd.DataFrame) -> pd.DataFrame:
    raise RuntimeError("down")


REGISTRY = {
    "load": load,
    "ident": ident,
    "ident_v2": ident_v2,
    "plus_one": plus_one,
    "negative": negative,
    "boom": boom,
    "schema": AMOUNT_SCHEMA,
}
VERSIONS = {"v1": "ident", "v2": "ident_v2", "v_bad": "plus_one", "v_neg": "negative"}


def make_spec(required_runs: int = 2) -> PipelineSpec:
    policy = ShadowPolicy(required_runs=required_runs)
    return PipelineSpec(
        name="t",
        blocks=(
            BlockSpec("src", "ident", load="load", versions=VERSIONS, active="v1",
                      merge_key=("id",), schema="schema", shadow=policy),
            BlockSpec("b", "ident", inputs=("src",), versions=VERSIONS, active="v1",
                      merge_key=("id",), schema="schema", quarantine_threshold=0.2,
                      shadow=policy),
            BlockSpec("c", "ident", inputs=("b",), merge_key=("id",),
                      fallbacks=(FallbackEdge("b", "src", adapter="ident"),)),
            BlockSpec("d", "ident", inputs=("c",), merge_key=("id",)),
            BlockSpec("nokey", "ident", inputs=("src",), versions=VERSIONS, active="v1"),
        ),
    )  # fmt: skip


@pytest.fixture
def g(tmp_path) -> Iterator[Guardian]:
    with Guardian(make_spec(), tmp_path, registry=dict(REGISTRY)) as guardian:
        yield guardian


def run(g: Guardian, run_id: str):
    return Executor(g).run(run_id)


# ---------------------------------------------------------------- versions


def test_active_version_defaults_to_spec_and_block_without_versions(g) -> None:
    assert g.active_version("b") == "v1"
    assert g.active_version("c") is None
    assert g.version_fn("b", "v_bad") is plus_one


def test_source_block_runs_every_version_on_the_loaded_frame(g) -> None:
    calls = []
    g.registry["load"] = lambda: calls.append(1) or load()
    g.shadow_start("src", "v2")
    report = run(g, "r1")
    assert report.get("src").outcome is Outcome.PASS
    assert calls == [1]  # loaded once, shared by the live version and the candidate
    assert report.get("src").shadow.mode is ShadowMode.PARITY


def test_loader_crash_is_a_block_crash(g) -> None:
    g.registry["load"] = lambda: (_ for _ in ()).throw(RuntimeError("source offline"))
    report = run(g, "r1")
    assert report.get("src").outcome is Outcome.ROLLBACK and report.get("src").reason == "crash"


# ---------------------------------------------------------------- shadow start


def test_shadow_start_validates(g) -> None:
    with pytest.raises(KeyError, match="no block"):
        g.shadow_start("nope", "v2")
    with pytest.raises(KeyError, match="no version 'v9'"):
        g.shadow_start("b", "v9")
    with pytest.raises(KeyError, match="no version"):
        g.shadow_start("c", "v2")  # c has no versions
    with pytest.raises(PromotionError, match="already the active version"):
        g.shadow_start("b", "v1")
    with pytest.raises(PromotionError, match="merge_key"):
        g.shadow_start("nokey", "v2")
    g.shadow_start("b", "v2")
    with pytest.raises(Exception, match="already shadowing"):
        g.shadow_start("b", "v_bad")
    g.shadow_stop("b")
    g.shadow_start("b", "v_bad")


def test_source_block_without_load_cannot_be_shadowed(tmp_path) -> None:
    spec = PipelineSpec(
        name="t",
        blocks=(BlockSpec("s", "ident", versions={"v1": "ident", "v2": "ident_v2"},
                          active="v1", merge_key=("id",)),),
    )  # fmt: skip
    with (
        Guardian(spec, tmp_path, registry=REGISTRY) as g,
        pytest.raises(PromotionError, match="must declare 'load'"),
    ):
        g.shadow_start("s", "v2")


# ---------------------------------------------------------------- promotion policy


def test_promote_needs_a_candidate_and_a_run(g) -> None:
    with pytest.raises(PromotionError, match="no candidate in shadow"):
        g.promote("b")
    g.shadow_start("b", "v2")
    with pytest.raises(PromotionError, match="has not run yet"):
        g.promote("b", approve=True)


def test_auto_promotion_after_required_parity_runs(g) -> None:
    g.shadow_start("b", "v2")
    run(g, "r1")
    assert g.active_version("b") == "v1"
    run(g, "r2")
    assert g.active_version("b") == "v2"
    promotion = [
        e for e in g.events.query(kind=EventKind.PROMOTION) if e.data["state"] == "COMPLETED"
    ]
    assert promotion[0].data["reason"] == "auto"


def test_expect_diff_is_never_auto_promoted(g) -> None:
    g.shadow_start("b", "v2", expect_diff=True)
    for i in range(4):
        run(g, f"r{i}")
    assert g.active_version("b") == "v1"
    with pytest.raises(PromotionError, match="expect-diff"):
        g.promote("b")
    assert g.promote("b", approve=True).to_version == "v2"


def test_approval_cannot_promote_a_candidate_failing_validation(g) -> None:
    g.shadow_start("b", "v_neg")
    run(g, "r1")
    (last,) = g.shadow_runs("b")[1]
    assert last.pass_rate == 0.0 and not last.within_tolerance
    with pytest.raises(PromotionError, match="fails validation"):
        g.promote("b", approve=True)
    assert g.active_version("b") == "v1"


def test_bad_candidate_diff_names_changed_columns(g) -> None:
    g.shadow_start("b", "v_bad")
    run(g, "r1")
    (last,) = g.shadow_runs("b")[1]
    assert last.comparison.changed == 10 and last.comparison.changed_columns == ("amount",)
    assert "changed fraction 1.0000" in " ".join(last.notes)


def test_candidate_crash_is_recorded_not_raised(g) -> None:
    g.registry["ident_v2"] = boom
    g.shadow_start("b", "v2")
    report = run(g, "r1")
    assert report.get("b").outcome is Outcome.PASS  # the live version is unaffected
    (last,) = g.shadow_runs("b")[1]
    assert not last.within_tolerance and "candidate crashed" in last.notes[0]


def test_promotion_of_an_out_block_marks_it_healthy_and_restores_edges(g) -> None:
    run(g, "r0")
    g.set_block_status("b", BlockStatus.OUT)
    g.shadow_start("b", "v2")
    report = run(g, "r1")
    assert report.get("b").outcome is Outcome.SKIPPED
    assert report.get("b").shadow.mode is ShadowMode.ABSOLUTE
    assert report.get("c").rerouted  # b is OUT: c uses its fallback
    g.promote("b", approve=True)
    assert g.status("b") is BlockStatus.HEALTHY
    report = run(g, "r2")
    assert not report.get("c").rerouted and report.get("c").sources[0].block == "b"


def test_rollback_without_promotion_is_refused(g) -> None:
    with pytest.raises(Exception, match="no previous version"):
        g.rollback_version("b")


# ---------------------------------------------------------------- provenance


def test_provenance_records_inputs_version_and_quality(g) -> None:
    run(g, "r1")
    prov = g.snapshots.read_provenance("c", "r1")
    assert prov["quality"] == "FRESH" and prov["version"] is None
    (inp,) = prov["inputs"]
    assert (inp["block"], inp["run_id"], inp["store"], inp["quality"]) == (
        "b",
        "r1",
        "live",
        "FRESH",
    )
    assert g.snapshots.read_provenance("b", "r1")["version"] == "v1"


def test_quality_propagates_as_the_worst_input(tmp_path) -> None:
    """src -> b -> c -> d; c falls back from b to src."""
    registry = {**REGISTRY, "b_live": ident}
    spec = make_spec()
    spec = PipelineSpec(
        name=spec.name,
        blocks=tuple(
            BlockSpec("b", "b_live", inputs=("src",), versions={**VERSIONS, "v1": "b_live"},
                      active="v1", merge_key=("id",), schema="schema")
            if blk.name == "b"
            else blk
            for blk in spec.blocks
        ),
    )  # fmt: skip
    with Guardian(spec, tmp_path, registry=registry) as g:
        run(g, "r0")
        g.registry["b_live"] = boom  # b's live version breaks
        report = run(g, "r1")
        assert report.get("b").outcome is Outcome.ROLLBACK
        # c reads src through the fallback edge (src is healthy): FALLBACK, inherited by d
        assert g.snapshots.read_provenance("c", "r1")["quality"] == Quality.FALLBACK.value
        d_input = g.snapshots.read_provenance("d", "r1")["inputs"][0]
        assert (d_input["read"], d_input["quality"]) == ("FRESH", "FALLBACK")
        assert g.snapshots.read_provenance("d", "r1")["quality"] == Quality.FALLBACK.value

        g.set_block_status("src", BlockStatus.OUT)  # the fallback source is unhealthy too
        run(g, "r2")
        c_input = g.snapshots.read_provenance("c", "r2")["inputs"][0]
        assert (c_input["block"], c_input["run_id"], c_input["read"]) == ("b", "r0", "STALE")
        assert g.snapshots.read_provenance("c", "r2")["quality"] == Quality.STALE.value
        assert g.snapshots.read_provenance("d", "r2")["quality"] == Quality.STALE.value


def test_worst_quality_order() -> None:
    assert Quality.worst([]) is Quality.FRESH
    assert Quality.worst(["FRESH", "STALE"]) is Quality.STALE
    assert Quality.worst(["STALE", "FALLBACK", "FRESH"]) is Quality.FALLBACK


def test_long_lived_guardian_sees_promotions_made_elsewhere(tmp_path) -> None:
    """A Guardian kept across runs (e.g. a Dagster resource) must not cache versions."""
    with (
        Guardian(make_spec(), tmp_path, registry=dict(REGISTRY)) as long_lived,
        Guardian(make_spec(), tmp_path, registry=dict(REGISTRY)) as cli,
    ):
        run(long_lived, "r0")
        cli.shadow_start("b", "v2")
        run(long_lived, "r1")  # the long-lived instance runs the candidate started elsewhere
        cli.promote("b", approve=True)
        report = run(long_lived, "r2")
        assert report.get("b").version == "v2"
        assert long_lived.snapshots.read_provenance("b", "r2")["version"] == "v2"
