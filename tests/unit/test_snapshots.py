import pandas as pd
import pytest

from guardian.core.models import DataRef
from guardian.core.snapshots import (
    LocalParquetSnapshotStore,
    SnapshotExistsError,
    SnapshotNotFoundError,
    SnapshotStore,
)


@pytest.fixture
def store(tmp_path) -> LocalParquetSnapshotStore:
    return LocalParquetSnapshotStore(tmp_path)


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", None, "c"],
            "amount": [1.5, float("nan"), 3.0],
            "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True),
        }
    )


def test_satisfies_protocol(store) -> None:
    assert isinstance(store, SnapshotStore)


def test_write_read_roundtrip(store, df) -> None:
    ref = store.write("b1", "r1", df)
    assert ref == DataRef("b1", "r1")
    assert store.exists("b1", "r1")
    pd.testing.assert_frame_equal(store.read("b1", "r1"), df)


def test_roundtrip_preserves_non_default_index(store, df) -> None:
    filtered = df.iloc[[0, 2]]
    store.write("b1", "r1", filtered)
    pd.testing.assert_frame_equal(store.read("b1", "r1"), filtered)


def test_snapshots_are_immutable(store, df) -> None:
    store.write("b1", "r1", df)
    with pytest.raises(SnapshotExistsError):
        store.write("b1", "r1", df.head(1))
    pd.testing.assert_frame_equal(store.read("b1", "r1"), df)


def test_empty_frame_roundtrip(store, df) -> None:
    store.write("b1", "r1", df.iloc[0:0])
    assert store.read("b1", "r1").empty


def test_read_missing_raises(store) -> None:
    with pytest.raises(SnapshotNotFoundError):
        store.read("b1", "nope")
    assert not store.exists("b1", "nope")


def test_list_runs(store, df) -> None:
    assert store.list_runs("b1") == []
    store.write("b1", "r1", df)
    store.write("b1", "r2", df)
    store.write("b2", "r1", df)
    assert store.list_runs("b1") == ["r1", "r2"]
    assert store.list_runs("b2") == ["r1"]


def test_no_temp_files_left_behind(store, df, tmp_path) -> None:
    store.write("b1", "r1", df)
    leftovers = [p for p in (tmp_path / "snapshots" / "b1").iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def test_last_good_tracking_per_block(store, df) -> None:
    assert store.last_good("b1") is None
    store.write("b1", "r1", df)
    store.write("b1", "r2", df)
    store.write("b2", "r1", df)
    assert store.mark_last_good("b1", "r1") == DataRef("b1", "r1")
    assert store.last_good("b1") == DataRef("b1", "r1")
    assert store.last_good("b2") is None
    # A newer write does not move last-good until it is promoted.
    store.mark_last_good("b1", "r2")
    assert store.last_good("b1") == DataRef("b1", "r2")


def test_mark_last_good_requires_snapshot(store) -> None:
    with pytest.raises(SnapshotNotFoundError):
        store.mark_last_good("b1", "r1")
    assert store.last_good("b1") is None


def test_last_good_persists_across_instances(tmp_path, df) -> None:
    LocalParquetSnapshotStore(tmp_path).write("b1", "r1", df)
    LocalParquetSnapshotStore(tmp_path).mark_last_good("b1", "r1")
    assert LocalParquetSnapshotStore(tmp_path).last_good("b1") == DataRef("b1", "r1")


@pytest.mark.parametrize("block,run_id", [("../evil", "r1"), ("b1", "..\\evil"), ("b1", "a:b")])
def test_rejects_unsafe_keys(store, df, block, run_id) -> None:
    with pytest.raises(ValueError):
        store.write(block, run_id, df)


def test_provenance_sidecar_is_immutable_and_not_a_run(store, df) -> None:
    assert store.read_provenance("b1", "r1") is None
    store.write("b1", "r1", df)
    store.write_provenance("b1", "r1", {"quality": "FRESH", "inputs": []})
    assert store.read_provenance("b1", "r1") == {"quality": "FRESH", "inputs": []}
    with pytest.raises(SnapshotExistsError):
        store.write_provenance("b1", "r1", {"quality": "STALE"})
    # provenance can exist without a snapshot (a rolled-back run) and is never a run
    store.write_provenance("b1", "r2", {"quality": "STALE"})
    assert store.list_runs("b1") == ["r1"]
