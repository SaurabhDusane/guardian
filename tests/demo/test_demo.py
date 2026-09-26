from pathlib import Path

import pandas as pd

from guardian.core.guardian import Guardian
from guardian.core.models import Action, QuarantineStatus
from guardian.demo import blocks
from guardian.demo.data_gen import RAW_COLUMNS, CsvLoader, DatasetLoader, SyntheticOrdersLoader
from guardian.runner.executor import Executor, Outcome
from guardian.runner.spec_loader import load_spec

DEMO_SPEC = Path(blocks.__file__).parent / "pipeline.yaml"


def test_synthetic_loader_is_deterministic_and_messy() -> None:
    loader = SyntheticOrdersLoader(rows=200, seed=1)
    assert isinstance(loader, DatasetLoader)
    a, b = loader.load(), SyntheticOrdersLoader(rows=200, seed=1).load()
    pd.testing.assert_frame_equal(a, b)
    assert tuple(a.columns) == RAW_COLUMNS and len(a) == 200
    assert a["amount"].str.contains(r"\$", na=False).any()
    assert a["currency"].str.islower().any()
    assert not SyntheticOrdersLoader(rows=200, seed=2).load().equals(a)


def test_csv_loader_swaps_in(tmp_path) -> None:
    raw = SyntheticOrdersLoader(rows=20, seed=3).load()
    path = tmp_path / "orders.csv"
    raw.to_csv(path, index=False)
    loaded = blocks.load_orders(loader="guardian.demo.data_gen:CsvLoader", path=str(path))
    assert isinstance(CsvLoader(path), DatasetLoader)
    assert list(loaded.columns) == list(RAW_COLUMNS) and len(loaded) == 20


def test_row_wise_blocks_are_idempotent_on_their_output() -> None:
    raw = SyntheticOrdersLoader(rows=100, seed=5).load()
    for fn in (blocks.parse, blocks.standardize, blocks.clean):
        once = fn(raw)
        pd.testing.assert_frame_equal(fn(once), once)
        raw = once
    valid = raw.dropna(
        subset=["order_id", "order_time", "amount_usd", "quantity_value", "country_code"]
    )
    norm = blocks.normalize(valid[valid["quantity_value"] >= 1])
    pd.testing.assert_frame_equal(blocks.normalize(norm), norm)


def test_adapter_produces_b6_shape_for_aggregate() -> None:
    raw = SyntheticOrdersLoader(rows=50, seed=7, error_rate=0).load()
    b5 = blocks.normalize(blocks.clean(blocks.standardize(blocks.parse(raw))))
    via_fallback = blocks.b5_to_b6_shape(b5)
    via_enrich = blocks.enrich(b5)
    assert set(via_enrich.columns) == set(via_fallback.columns)
    assert set(via_fallback["segment"]) == {"unassigned"}
    agg = blocks.aggregate(via_fallback)
    assert agg["orders"].sum() == (b5["status"] == "completed").sum()


def test_demo_pipeline_end_to_end(tmp_path) -> None:
    with Guardian(load_spec(DEMO_SPEC), tmp_path) as g:
        report = Executor(g).run("r1")
        assert all(r.outcome is Outcome.PASS for r in report.blocks)
        assert sum(r.quarantined for r in report.blocks) > 0  # the data is messy
        # row accounting: every row entering a row-wise block is promoted or quarantined
        for name in ("b2_parse", "b3_standardize", "b4_clean", "b5_normalize", "b6_enrich"):
            r = report.get(name)
            assert r.rows_in == r.rows_out + r.quarantined
        assert len(g.read(g.snapshots.last_good("b8_aggregate"))) > 0


def test_demo_replay_after_fix(tmp_path) -> None:
    """Quarantined b3 rows (unknown currency) replay once the block learns the code."""
    with Guardian(load_spec(DEMO_SPEC), tmp_path) as g:
        Executor(g).run("r1")
        failing = g.quarantine.list(block="b3_standardize", status=QuarantineStatus.QUARANTINED)
        assert failing

        def fixed(df: pd.DataFrame) -> pd.DataFrame:
            out = blocks.standardize(df)
            out["currency_code"] = out["currency_code"].where(
                out["currency_code"].isin(["USD", "EUR", "GBP"]), "USD"
            )
            out["country_code"] = out["country_code"].fillna("US")
            return out

        g.registry["demo.blocks:standardize"] = fixed
        result = g.replay("b3_standardize")
        assert result.replayed == len(failing) and result.still_failing == 0
        decision = g.on_output("b3_standardize", "check", g.read(result.snapshot))
        assert decision.action is Action.PASS


def test_v2_matches_v1_and_v_bad_differs_on_messy_demo_data(tmp_path) -> None:
    """v2s are improvements that leave well-formed rows alone; v_bads are off-by-ones.

    Each version runs on the live inputs its block saw in a real (messy) demo run.
    """
    with Guardian(load_spec(DEMO_SPEC), tmp_path) as g:
        Executor(g).run("r1")
        checked = 0
        for block in g.spec.blocks:
            if not block.versions:
                continue
            prov = g.provenance.get(block.name, "r1").as_dict()
            inputs = [g.snapshots.read(i["block"], i["run_id"]) for i in prov["inputs"]]
            inputs = g.prepare_inputs(block.name, inputs)  # the loaded frame for a source
            v1, v2, bad = (g.version_fn(block.name, v)(*inputs) for v in ("v1", "v2", "v_bad"))
            pd.testing.assert_frame_equal(v2, v1, obj=f"{block.name} v2")
            assert not bad.equals(v1), f"{block.name} v_bad"
            checked += 1
    assert checked == 5
