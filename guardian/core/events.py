"""Structured JSON event logging ("short logging").

Each kept event is appended to ``events.jsonl`` and inserted into a DuckDB table.
Routine events are sampled at ``sample_rate``; ERROR, ROLLBACK, REROUTE and
QUARANTINE events are always kept.
"""

from __future__ import annotations

import json
import random
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

import duckdb


class EventKind(StrEnum):
    RUN_STARTED = "RUN_STARTED"
    RUN_FINISHED = "RUN_FINISHED"
    BLOCK_STARTED = "BLOCK_STARTED"
    BLOCK_FINISHED = "BLOCK_FINISHED"
    VALIDATION = "VALIDATION"
    SNAPSHOT = "SNAPSHOT"
    RESOLVE = "RESOLVE"
    STATUS_CHANGE = "STATUS_CHANGE"
    REPLAY = "REPLAY"
    # Never sampled out:
    ERROR = "ERROR"
    ROLLBACK = "ROLLBACK"
    REROUTE = "REROUTE"
    QUARANTINE = "QUARANTINE"


ALWAYS_LOGGED: frozenset[EventKind] = frozenset(
    {EventKind.ERROR, EventKind.ROLLBACK, EventKind.REROUTE, EventKind.QUARANTINE}
)


@dataclass(frozen=True)
class Event:
    kind: EventKind
    block: str | None = None
    run_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "ts": self.ts.isoformat(),
            "kind": self.kind.value,
            "block": self.block,
            "run_id": self.run_id,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=_json_default, sort_keys=True)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime | date):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "item"):  # numpy scalars
        return obj.item()
    if isinstance(obj, set | frozenset | tuple):
        return list(obj)
    return str(obj)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id  VARCHAR PRIMARY KEY,
    ts        TIMESTAMP NOT NULL,  -- UTC
    kind      VARCHAR NOT NULL,
    block     VARCHAR,
    run_id    VARCHAR,
    data_json VARCHAR NOT NULL
);
"""


class EventLogger:
    def __init__(
        self,
        root: Path | str,
        sample_rate: float = 1.0,
        *,
        seed: int | None = None,
        jsonl_name: str = "events.jsonl",
        db_name: str = "events.duckdb",
    ) -> None:
        if not 0.0 <= sample_rate <= 1.0:
            raise ValueError(f"sample_rate must be in [0, 1], got {sample_rate}")
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.sample_rate = sample_rate
        self.jsonl_path = root / jsonl_name
        self.db_path = root / db_name
        self.sampled_out = 0
        self._rng = random.Random(seed)
        self._lock = threading.RLock()
        # One connection for the logger's lifetime: opening DuckDB per event is slow.
        # Call close() (or use the logger as a context manager) to release the file.
        self._con: duckdb.DuckDBPyConnection | None = None
        self._connection().execute(_SCHEMA)

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(str(self.db_path))
        return self._con

    def close(self) -> None:
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None

    def __enter__(self) -> EventLogger:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def should_log(self, kind: EventKind) -> bool:
        if kind in ALWAYS_LOGGED or self.sample_rate >= 1.0:
            return True
        return self._rng.random() < self.sample_rate

    def emit(
        self,
        kind: EventKind | str,
        *,
        block: str | None = None,
        run_id: str | None = None,
        **data: Any,
    ) -> Event | None:
        """Record an event. Returns the Event, or None if it was sampled out."""
        kind = EventKind(kind)
        with self._lock:
            if not self.should_log(kind):
                self.sampled_out += 1
                return None
            event = Event(kind=kind, block=block, run_id=run_id, data=data)
            line = event.to_json()
            with self.jsonl_path.open("a", encoding="utf-8", newline="\n") as fh:
                fh.write(line + "\n")
            self._connection().execute(
                "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
                [
                    event.event_id,
                    event.ts.astimezone(UTC).replace(tzinfo=None),
                    kind.value,
                    block,
                    run_id,
                    json.dumps(event.data, default=_json_default, sort_keys=True),
                ],
            )
            return event

    def query(
        self,
        kind: EventKind | str | None = None,
        block: str | None = None,
        run_id: str | None = None,
    ) -> list[Event]:
        clauses, params = [], []
        for column, value in (("kind", kind), ("block", block), ("run_id", run_id)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value.value if isinstance(value, Enum) else value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            rows = (
                self._connection()
                .execute(
                    "SELECT event_id, ts, kind, block, run_id, data_json FROM events"
                    f"{where} ORDER BY ts, event_id",
                    params,
                )
                .fetchall()
            )
        return [
            Event(
                kind=EventKind(r[2]),
                block=r[3],
                run_id=r[4],
                data=json.loads(r[5]),
                ts=r[1].replace(tzinfo=UTC),
                event_id=r[0],
            )
            for r in rows
        ]

    def read_jsonl(self) -> list[dict[str, Any]]:
        if not self.jsonl_path.exists():
            return []
        with self.jsonl_path.open(encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
