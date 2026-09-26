from datetime import UTC, datetime

import pandas as pd
import pytest

from guardian.core.models import QuarantineStatus
from guardian.core.quarantine import (
    DuckDBQuarantineStore,
    QuarantineEntry,
    QuarantineStore,
    entries_from_frame,
    rows_to_payloads,
)


@pytest.fixture
def store(tmp_path) -> DuckDBQuarantineStore:
    return DuckDBQuarantineStore(tmp_path)


@pytest.fixture
def bad_rows() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "amount": [-5.0, float("nan")], "name": ["x", None]})


def test_satisfies_protocol(store) -> None:
    assert isinstance(store, QuarantineStore)


def test_rows_to_payloads_handles_nan_and_dates() -> None:
    df = pd.DataFrame({"a": [1.0, float("nan")], "d": pd.to_datetime(["2024-01-01", None])})
    payloads = rows_to_payloads(df)
    assert len(payloads) == 2
    assert '"a": null' in payloads[1]
    assert "2024-01-01T00:00:00" in payloads[0]
    assert rows_to_payloads(df.iloc[0:0]) == []


def test_add_and_list_carries_all_fields(store, bad_rows) -> None:
    ids = store.add(entries_from_frame("b1", "r1", bad_rows, "amount_positive", "amount < 0"))
    assert len(ids) == 2 and ids[0] < ids[1]
    records = store.list()
    assert [r.id for r in records] == ids
    rec = records[0]
    assert (rec.block, rec.run_id, rec.rule_name, rec.reason) == (
        "b1",
        "r1",
        "amount_positive",
        "amount < 0",
    )
    assert rec.status is QuarantineStatus.QUARANTINED
    assert rec.ts.tzinfo is not None
    assert rec.payload() == {"id": 1, "amount": -5.0, "name": "x"}
    assert records[1].payload() == {"id": 2, "amount": None, "name": None}
    assert rec.replayed_at is None and rec.replay_run_id is None


def test_explicit_ts_is_preserved_in_utc(store) -> None:
    ts = datetime(2024, 5, 1, 12, 30, tzinfo=UTC)
    store.add([QuarantineEntry("b1", "r1", "rule", "why", '{"x": 1}', ts=ts)])
    assert store.list()[0].ts == ts


def test_add_empty_is_noop(store) -> None:
    assert store.add([]) == []
    assert store.count() == 0


def test_add_rejects_malformed_payload(store) -> None:
    with pytest.raises(ValueError):
        store.add([QuarantineEntry("b1", "r1", "rule", "why", "not json")])
    assert store.count() == 0


def test_filters(store, bad_rows) -> None:
    store.add(entries_from_frame("b1", "r1", bad_rows, "rule", "why"))
    store.add(entries_from_frame("b1", "r2", bad_rows.head(1), "rule", "why"))
    store.add(entries_from_frame("b2", "r1", bad_rows, "rule", "why"))
    assert store.count() == 5
    assert store.count(block="b1") == 3
    assert len(store.list(block="b1", run_id="r2")) == 1
    assert store.count(status=QuarantineStatus.REPLAYED) == 0


def test_load_frame_reconstructs_rows(store, bad_rows) -> None:
    store.add(entries_from_frame("b1", "r1", bad_rows, "rule", "why"))
    ids, frame = store.load_frame("b1")
    assert len(ids) == 2
    assert list(frame.columns) == sorted(bad_rows.columns)
    assert frame["id"].tolist() == [1, 2]
    empty_ids, empty = store.load_frame("nothing")
    assert empty_ids == [] and empty.empty


def test_mark_replayed_never_deletes(store, bad_rows) -> None:
    ids = store.add(entries_from_frame("b1", "r1", bad_rows, "rule", "why"))
    assert store.mark_replayed([ids[0]], replay_run_id="replay-1") == 1
    assert store.count() == 2  # nothing deleted
    replayed = store.list(status=QuarantineStatus.REPLAYED)
    assert [r.id for r in replayed] == [ids[0]]
    assert replayed[0].replay_run_id == "replay-1"
    assert replayed[0].replayed_at is not None
    remaining_ids, _ = store.load_frame("b1")
    assert remaining_ids == [ids[1]]
    # Already-replayed records are not transitioned again.
    assert store.mark_replayed(ids, replay_run_id="replay-2") == 1
    assert store.list(status=QuarantineStatus.REPLAYED)[0].replay_run_id == "replay-1"
    assert store.mark_replayed([], replay_run_id="replay-3") == 0


def test_persists_across_instances(tmp_path, bad_rows) -> None:
    DuckDBQuarantineStore(tmp_path).add(entries_from_frame("b1", "r1", bad_rows, "r", "w"))
    assert DuckDBQuarantineStore(tmp_path).count(block="b1") == 2
