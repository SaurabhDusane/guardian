import json
from pathlib import Path

import numpy as np
import pytest

from guardian.core.events import ALWAYS_LOGGED, EventKind, EventLogger


def test_emit_writes_jsonl_and_duckdb(tmp_path) -> None:
    log = EventLogger(tmp_path)
    event = log.emit(EventKind.SNAPSHOT, block="b1", run_id="r1", rows=np.int64(3), path=Path("x"))
    assert event is not None
    lines = log.read_jsonl()
    assert len(lines) == 1
    assert lines[0]["event_id"] == event.event_id
    assert lines[0]["kind"] == "SNAPSHOT"
    assert lines[0]["data"] == {"rows": 3, "path": "x"}
    # every line is standalone JSON
    raw = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["block"] for line in raw] == ["b1"]

    stored = log.query()
    assert len(stored) == 1
    assert stored[0].event_id == event.event_id
    assert stored[0].data == {"rows": 3, "path": "x"}
    assert stored[0].ts == event.ts


def test_query_filters(tmp_path) -> None:
    log = EventLogger(tmp_path)
    log.emit(EventKind.RESOLVE, block="b2", run_id="r1", source="b1")
    log.emit(EventKind.REROUTE, block="b8", run_id="r1", source="b5")
    log.emit(EventKind.RESOLVE, block="b8", run_id="r2", source="b6")
    assert len(log.query(kind=EventKind.RESOLVE)) == 2
    assert len(log.query(kind="REROUTE")) == 1
    assert len(log.query(block="b8")) == 2
    assert len(log.query(block="b8", run_id="r2")) == 1


def test_sample_rate_zero_drops_routine_but_keeps_critical(tmp_path) -> None:
    log = EventLogger(tmp_path, sample_rate=0.0)
    for kind in EventKind:
        for _ in range(5):
            log.emit(kind, block="b")
    kept = {e.kind for e in log.query()}
    assert kept == set(ALWAYS_LOGGED)
    assert len(log.query()) == 5 * len(ALWAYS_LOGGED)
    assert log.sampled_out == 5 * (len(EventKind) - len(ALWAYS_LOGGED))
    assert len(log.read_jsonl()) == len(log.query())


def test_always_logged_set_matches_contract() -> None:
    # CLAUDE.md: ERROR/ROLLBACK/REROUTE/QUARANTINE are never sampled out. WARN joins them
    # so warnings such as "replay merge skipped" cannot be sampled away.
    # PROMOTION joins them: every change of a block's active version must be auditable.
    # DIAGNOSIS and PROPOSAL join them: every (advisory) agent action must be auditable.
    assert {k.value for k in ALWAYS_LOGGED} == {
        "DIAGNOSIS",
        "PROPOSAL",
        "PROMOTION",
        "WARN",
        "ERROR",
        "ROLLBACK",
        "REROUTE",
        "QUARANTINE",
    }


def test_partial_sampling_is_deterministic_with_seed(tmp_path) -> None:
    def run(root: Path) -> int:
        log = EventLogger(root, sample_rate=0.3, seed=42)
        for _ in range(200):
            log.emit(EventKind.RESOLVE)
        return len(log.query())

    a, b = run(tmp_path / "a"), run(tmp_path / "b")
    assert a == b
    assert 30 < a < 90  # roughly 30% of 200


def test_invalid_sample_rate(tmp_path) -> None:
    with pytest.raises(ValueError):
        EventLogger(tmp_path, sample_rate=1.5)


def test_unknown_kind_rejected(tmp_path) -> None:
    with pytest.raises(ValueError):
        EventLogger(tmp_path).emit("NOT_A_KIND")


def test_appends_across_instances(tmp_path) -> None:
    with EventLogger(tmp_path) as first:
        first.emit(EventKind.ERROR, message="boom")
    log = EventLogger(tmp_path)
    log.emit(EventKind.ROLLBACK)
    assert [e["kind"] for e in log.read_jsonl()] == ["ERROR", "ROLLBACK"]
    assert len(log.query()) == 2


def test_close_releases_and_reopens_lazily(tmp_path) -> None:
    log = EventLogger(tmp_path)
    log.emit(EventKind.ERROR)
    log.close()
    log.close()  # idempotent
    log.emit(EventKind.ROLLBACK)  # reopens on demand
    assert len(log.query()) == 2
