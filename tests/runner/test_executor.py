from collections.abc import Iterator

import pandas as pd
import pytest

from guardian.core.guardian import Guardian
from guardian.core.models import BlockStatus
from guardian.runner.executor import Executor, Outcome
from guardian.runner.spec_loader import parse_spec


def source() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3], "value": [1.0, 2.0, 3.0]})


def double(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["value"] = out["value"] * 2
    return out


def adapt(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["adapted"] = True
    return out


def total(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"total": [df["value"].sum()], "adapted": ["adapted" in df.columns]})


def boom(df: pd.DataFrame) -> pd.DataFrame:
    raise RuntimeError("boom")


REGISTRY = {"source": source, "double": double, "adapt": adapt, "total": total, "boom": boom}

SPEC = {
    "name": "t",
    "blocks": [
        {"name": "a", "fn": "source"},
        {"name": "b", "fn": "double", "inputs": ["a"]},
        {"name": "c", "fn": "double", "inputs": ["b"]},
        {
            "name": "d",
            "fn": "total",
            "inputs": ["c"],
            "fallbacks": [{"replaces": "c", "source": "b", "adapter": "adapt"}],
        },
    ],
}


@pytest.fixture
def g(tmp_path) -> Iterator[Guardian]:
    with Guardian(parse_spec(SPEC), tmp_path, registry=dict(REGISTRY)) as guardian:
        yield guardian


def test_happy_path(g) -> None:
    report = Executor(g).run("r1")
    assert report.ok
    assert [r.block for r in report.blocks] == ["a", "b", "c", "d"]
    assert g.read(g.snapshots.last_good("d"))["total"].item() == 24.0
    b = report.get("b")
    assert (b.rows_in, b.rows_out, b.quarantined) == (3, 3, 0)
    assert b.sources[0].block == "a" and b.sources[0].run_id == "r1"


def test_crash_reroutes_dependent_through_fallback(g) -> None:
    Executor(g).run("r1")
    g.registry["double"] = lambda df: (
        boom(df) if len(df) and df["value"].iloc[0] == 2.0 else double(df)
    )
    report = Executor(g).run("r2")
    assert report.get("c").outcome is Outcome.ROLLBACK
    assert report.get("c").reason == "crash"
    d = report.get("d")
    assert d.outcome is Outcome.PASS and d.rerouted
    assert d.sources[0].block == "b" and d.sources[0].adapter == "adapt"
    out = g.read(g.snapshots.last_good("d"))
    assert bool(out["adapted"].item()) is True


def test_out_block_is_skipped_and_dependents_reroute(g) -> None:
    Executor(g).run("r1")
    g.set_block_status("c", BlockStatus.OUT)
    report = Executor(g).run("r2")
    assert report.get("c").outcome is Outcome.SKIPPED
    assert not g.snapshots.exists("c", "r2")
    assert report.get("d").rerouted


def test_first_run_crash_blocks_dependents_without_fallback(g) -> None:
    g.registry["source"] = lambda: boom(pd.DataFrame())
    report = Executor(g).run("r1")
    assert report.get("a").outcome is Outcome.ROLLBACK
    assert report.get("b").outcome is Outcome.BLOCKED
    assert "no last-good" in report.get("b").reason
    assert report.get("d").outcome is Outcome.BLOCKED
    assert not report.ok


def test_stale_read_after_upstream_rollback(g) -> None:
    Executor(g).run("r1")
    g.registry["source"] = lambda: boom(pd.DataFrame())
    report = Executor(g).run("r2")
    b = report.get("b")
    assert b.outcome is Outcome.PASS
    assert b.stale and b.sources[0].run_id == "r1"


def test_generated_run_id(g) -> None:
    report = Executor(g).run()
    assert report.run_id.startswith("run-")
