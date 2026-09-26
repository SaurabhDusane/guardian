from collections.abc import Iterator

import pandas as pd
import pytest

from guardian.core.guardian import Guardian
from guardian.core.models import BlockSpec, BlockStatus, FallbackEdge, PipelineSpec, Quality
from guardian.core.provenance import (
    CANDIDATE,
    QUALITY_COL,
    BlockRun,
    DuckDBProvenanceStore,
    InputProvenance,
    Provenance,
    ProvenanceStore,
    impact,
    lineage,
)
from guardian.core.shadow import compare_rows
from guardian.runner.executor import Executor

from .conftest import AMOUNT_SCHEMA

F, S, B = Quality.FRESH, Quality.STALE, Quality.FALLBACK


def inp(upstream, block, run, read=F, quality=None, adapter=None) -> InputProvenance:
    return InputProvenance(upstream, block, run, adapter, read, quality or read, "HEALTHY")


@pytest.fixture
def store(tmp_path) -> DuckDBProvenanceStore:
    return DuckDBProvenanceStore(tmp_path)


def test_protocol_and_roundtrip(store) -> None:
    assert isinstance(store, ProvenanceStore)
    p = Provenance("b", "r1", "run", "v1", S, (inp("a", "a", "r0", S),))
    store.record(p)
    back = store.get("b", "r1")
    assert (back.block, back.run_id, back.version, back.quality, back.kind) == (
        "b",
        "r1",
        "v1",
        S,
        "run",
    )
    assert back.inputs == p.inputs and back.snapshot_id == "b@r1" and back.ts is not None
    assert back.inputs[0].source_id == "a@r0" and back.inputs[0].how == "stale a@r0"
    assert store.get("b", "r1", store=CANDIDATE) is None
    assert store.get("nope", "r1") is None


def test_records_are_immutable(store) -> None:
    store.record(Provenance("b", "r1", "run", None, F))
    store.record(Provenance("b", "r1", "run", None, B))  # ignored
    assert store.get("b", "r1").quality is F
    store.record(Provenance("b", "r1", "candidate", "v2", S, store=CANDIDATE))
    assert store.get("b", "r1", store=CANDIDATE).version == "v2"
    assert [p.store for p in store.records()] == ["live"]


def test_block_runs(store) -> None:
    store.record_run(BlockRun("b", "r1", "PASS", "HEALTHY", "v1"))
    store.record_run(BlockRun("b", "r2", "ROLLBACK", "DEGRADED", "v1", "crash"))
    store.record_run(BlockRun("c", "r2", "SKIPPED", "OUT", None))
    assert [(r.run_id, r.outcome) for r in store.runs("b")] == [("r1", "PASS"), ("r2", "ROLLBACK")]
    assert len(store.runs()) == 3


def build_history(store) -> None:
    """a -> b -> c -> d, e reads a; c falls back from b to a. b degraded at r1."""
    store.record_run(BlockRun("b", "r0", "PASS", "HEALTHY", None))
    for blk, ins in [
        ("a", ()),
        ("b", (inp("a", "a", "r0"),)),
        ("c", (inp("b", "b", "r0"),)),
        ("d", (inp("c", "c", "r0"),)),
        ("e", (inp("a", "a", "r0"),)),
    ]:
        store.record(Provenance(blk, "r0", "run", None, F, ins))
    store.record(Provenance("a", "r1", "run", None, F))
    store.record(Provenance("b", "r1", "run", None, F, (inp("a", "a", "r1"),), has_snapshot=False))
    store.record_run(BlockRun("b", "r1", "ROLLBACK", "DEGRADED", None, "bad rows"))
    store.record(
        Provenance("c", "r1", "run", None, B, (inp("b", "a", "r1", B, adapter="m:adapt"),))
    )
    store.record(Provenance("d", "r1", "run", None, B, (inp("c", "c", "r1", F, B),)))
    store.record(Provenance("e", "r1", "run", None, F, (inp("a", "a", "r1"),)))
    # r2: b is OUT, c has no fallback healthy -> stale b@r0
    store.record_run(BlockRun("b", "r2", "SKIPPED", "OUT", None))
    store.record(Provenance("a", "r2", "run", None, F))
    store.record(Provenance("c", "r2", "run", None, S, (inp("b", "b", "r0", S),)))
    store.record(Provenance("d", "r2", "run", None, S, (inp("c", "c", "r2", F, S),)))  # fmt: skip


def test_impact_lists_self_and_transitive_downstream(store) -> None:
    build_history(store)
    entries = impact(store, "b")
    got = [(e.block, e.run_id, e.relation, e.quality) for e in entries]
    assert got == [
        ("b", "r1", "self", "ROLLBACK"),
        ("c", "r1", "downstream", "FALLBACK"),
        ("d", "r1", "downstream", "FALLBACK"),
        ("b", "r2", "self", "SKIPPED"),
        ("c", "r2", "downstream", "STALE"),
        ("d", "r2", "downstream", "STALE"),
    ]
    assert "via m:adapt" in entries[1].reason and entries[2].reason == "read c@r1"


def test_impact_since_and_unaffected_blocks(store) -> None:
    build_history(store)
    assert [(e.block, e.run_id) for e in impact(store, "b", since="r2")] == [
        ("b", "r2"), ("c", "r2"), ("d", "r2"),
    ]  # fmt: skip
    assert impact(store, "e") == []  # a healthy leaf touched nothing
    assert impact(store, "a") == []  # a was never degraded
    with pytest.raises(KeyError, match="no run 'zz'"):
        impact(store, "b", since="zz")


def test_lineage_tree(store) -> None:
    build_history(store)
    root = lineage(store, "d", "r1")
    assert root.snapshot_id == "d@r1" and root.provenance.quality is B
    (c,) = root.parents
    assert c.snapshot_id == "c@r1" and c.via.upstream == "c"
    (a,) = c.parents
    assert a.snapshot_id == "a@r1" and a.via.read is B and a.via.upstream == "b"
    assert a.parents == []
    with pytest.raises(KeyError, match="no provenance"):
        lineage(store, "d", "r9")


def test_replay_lineage_includes_its_base(store) -> None:
    store.record(Provenance("b", "r0", "run", "v1", F))
    store.record(Provenance("b", "rp", "replay", "v2", F, base_run_id="r0", replayed_from=("r1",)))
    node = lineage(store, "b", "rp")
    assert [p.snapshot_id for p in node.parents] == ["b@r0"] and node.parents[0].via is None


# ---------------------------------------------------------------- through Guardian


def ident(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


def load() -> pd.DataFrame:
    return pd.DataFrame({"id": range(5), "amount": [1.0, 2.0, 3.0, 4.0, 5.0]})


def boom(df: pd.DataFrame) -> pd.DataFrame:
    raise RuntimeError("down")


def make_spec(annotate: bool) -> PipelineSpec:
    return PipelineSpec(
        name="t",
        blocks=(
            BlockSpec("src", "ident", load="load", merge_key=("id",)),
            BlockSpec("mid", "mid_fn", inputs=("src",), merge_key=("id",), schema="schema"),
            BlockSpec("out", "ident", inputs=("mid",), merge_key=("id",),
                      fallbacks=(FallbackEdge("mid", "src", adapter="ident"),),
                      annotate_quality=annotate),
        ),
    )  # fmt: skip


@pytest.fixture
def g(tmp_path) -> Iterator[Guardian]:
    registry = {"ident": ident, "load": load, "mid_fn": ident, "schema": AMOUNT_SCHEMA}
    with Guardian(make_spec(annotate=True), tmp_path, registry=registry) as guardian:
        yield guardian


def test_every_output_gets_a_record_and_runs_are_logged(g) -> None:
    Executor(g).run("r0")
    g.registry["mid_fn"] = boom
    Executor(g).run("r1")
    assert g.provenance.get("mid", "r1") is None  # crashed: nothing produced
    assert [(r.outcome, r.reason) for r in g.provenance.runs("mid")] == [
        ("PASS", None),
        ("ROLLBACK", "crash"),
    ]
    out = g.provenance.get("out", "r1")
    (i,) = out.inputs
    assert (i.upstream, i.source_block, i.read, i.upstream_status) == ("mid", "src", B, "DEGRADED")
    assert out.quality is B


def test_annotate_quality_is_opt_in(g, tmp_path) -> None:
    Executor(g).run("r0")
    g.registry["mid_fn"] = boom
    Executor(g).run("r1")
    assert set(g.snapshots.read("out", "r0")[QUALITY_COL]) == {"FRESH"}
    assert set(g.snapshots.read("out", "r1")[QUALITY_COL]) == {"FALLBACK"}
    # blocks without the flag never get the column
    assert QUALITY_COL not in g.snapshots.read("mid", "r0").columns
    assert QUALITY_COL not in g.snapshots.read("src", "r1").columns


def test_quality_column_is_not_a_shadow_difference() -> None:
    live = pd.DataFrame({"id": [1, 2], "x": [1, 2], QUALITY_COL: ["FRESH", "FRESH"]})
    candidate = pd.DataFrame({"id": [1, 2], "x": [1, 2]})
    diff = compare_rows(live, candidate, ["id"])
    assert (diff.changed, diff.changed_columns) == (0, ())


def test_status_out_source_makes_dependents_stale(g) -> None:
    Executor(g).run("r0")
    g.set_block_status("src", BlockStatus.OUT)
    Executor(g).run("r1")
    assert g.provenance.get("mid", "r1").quality is S
    assert g.provenance.get("out", "r1").quality is S  # propagated
    assert [(e.block, e.relation) for e in impact(g.provenance, "src")] == [
        ("src", "self"),
        ("mid", "downstream"),
        ("out", "downstream"),
    ]
