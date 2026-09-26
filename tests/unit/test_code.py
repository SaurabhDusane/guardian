"""Code fingerprints: which implementation produced each run, and its stored source."""

import functools

import duckdb
import pandas as pd

from guardian.core.code import CodeStore, fingerprint
from guardian.core.guardian import Guardian
from guardian.core.models import BlockSpec, PipelineSpec
from guardian.core.provenance import BlockRun, DuckDBProvenanceStore


def double(df: pd.DataFrame, factor: int = 2) -> pd.DataFrame:
    return df * factor


def triple(df: pd.DataFrame) -> pd.DataFrame:
    return df * 3


def test_fingerprint_sees_through_partials_and_wraps() -> None:
    base = fingerprint(double, "m:double")
    assert base.qualname.endswith(":double") and "return df * factor" in base.source
    assert fingerprint(functools.partial(double, factor=5), "x").sha == base.sha

    @functools.wraps(double)
    def wrapper(*a, **k):
        return double(*a, **k)

    assert fingerprint(wrapper, "x").sha == base.sha
    assert fingerprint(triple, "x").sha != base.sha
    assert fingerprint(len, "builtins:len").source is None  # no source: still a fingerprint


def test_code_store_round_trip(tmp_path) -> None:
    store = CodeStore(tmp_path)
    fp = fingerprint(double, "m:double")
    store.put(fp)
    store.put(fp)  # idempotent
    assert store.get(fp.sha) == fp
    assert store.get("nope") is None and store.get("") is None


def test_every_block_run_records_the_code_that_ran(tmp_path) -> None:
    def spec(fn: str) -> PipelineSpec:
        return PipelineSpec("p", (BlockSpec("a", fn),))

    frame = pd.DataFrame({"v": [1, 2]})
    registry = {"double": double, "triple": triple}
    with Guardian(spec("double"), tmp_path, registry=registry) as g:
        g.run_block("a", "r1", [frame])
    with Guardian(spec("triple"), tmp_path, registry=registry) as g:
        g.run_block("a", "r2", [frame])
        runs = {r.run_id: r for r in g.provenance.runs("a")}
        assert runs["r1"].code != runs["r2"].code
        assert g.code.get(runs["r1"].code).qualname.endswith(":double")
        assert "df * 3" in g.code.get(runs["r2"].code).source


def test_provenance_store_upgrades_an_old_block_runs_table(tmp_path) -> None:
    with duckdb.connect(str(tmp_path / "provenance.duckdb")) as con:
        con.execute(
            "CREATE TABLE block_runs (block VARCHAR NOT NULL, run_id VARCHAR NOT NULL, "
            "outcome VARCHAR NOT NULL, status VARCHAR NOT NULL, version VARCHAR, "
            "reason VARCHAR, ts TIMESTAMP NOT NULL, PRIMARY KEY (block, run_id))"
        )
        con.execute(
            "INSERT INTO block_runs VALUES ('a', 'old', 'PASS', 'HEALTHY', NULL, NULL, now())"
        )
    store = DuckDBProvenanceStore(tmp_path)
    store.record_run(BlockRun("a", "new", "PASS", "HEALTHY", None, code="abc"))
    assert [(r.run_id, r.code) for r in store.runs("a")] == [("old", None), ("new", "abc")]
