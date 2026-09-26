"""Quarantine for rows that failed validation. Records are never deleted."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import duckdb
import pandas as pd

from guardian.core.models import QuarantineRecord, QuarantineStatus, validate_name


@dataclass(frozen=True)
class QuarantineEntry:
    """A row to be quarantined (a QuarantineRecord before it has an id)."""

    block: str
    run_id: str
    rule_name: str
    reason: str
    payload_json: str
    ts: datetime | None = None


def rows_to_payloads(df: pd.DataFrame) -> list[str]:
    """Serialize each row of ``df`` as a JSON object string (NaN -> null, ISO dates)."""
    if df.empty:
        return []
    records = json.loads(df.to_json(orient="records", date_format="iso", date_unit="us"))
    return [json.dumps(r, sort_keys=True) for r in records]


def restore_dtypes(df: pd.DataFrame, dtypes: Mapping[str, Any]) -> pd.DataFrame:
    """Best-effort inverse of ``rows_to_payloads``: cast columns back to ``dtypes``.

    JSON payloads lose types (datetimes become ISO strings, ints may become floats).
    Columns that cannot be cast cleanly (e.g. they hold the bad values that got the
    rows quarantined) are left as they are for validation to judge.
    """
    out = df.copy()
    for column, dtype in dtypes.items():
        if column not in out.columns or out[column].dtype == dtype:
            continue
        try:
            if isinstance(dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_dtype(dtype):
                utc = isinstance(dtype, pd.DatetimeTZDtype)
                converted = pd.to_datetime(out[column], utc=utc, format="ISO8601")
                out[column] = converted.astype(dtype)
            else:
                out[column] = out[column].astype(dtype)
        except (TypeError, ValueError, OverflowError):
            continue
    return out


def entries_from_frame(
    block: str, run_id: str, df: pd.DataFrame, rule_name: str, reason: str
) -> list[QuarantineEntry]:
    return [
        QuarantineEntry(block, run_id, rule_name, reason, payload)
        for payload in rows_to_payloads(df)
    ]


@runtime_checkable
class QuarantineStore(Protocol):
    def add(self, entries: Sequence[QuarantineEntry]) -> list[int]:
        """Insert entries with status QUARANTINED; return their ids."""
        ...

    def list(
        self,
        block: str | None = None,
        status: QuarantineStatus | None = None,
        run_id: str | None = None,
    ) -> list[QuarantineRecord]: ...

    def count(self, block: str | None = None, status: QuarantineStatus | None = None) -> int: ...

    def load_frame(
        self, block: str, status: QuarantineStatus = QuarantineStatus.QUARANTINED
    ) -> tuple[list[int], pd.DataFrame]:
        """Return (ids, DataFrame of original rows) for ``block`` in ``status``."""
        ...

    def mark_replayed(self, ids: Iterable[int], replay_run_id: str) -> int:
        """Move QUARANTINED records to REPLAYED; return how many changed."""
        ...


_SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS quarantine_id_seq START 1;
CREATE TABLE IF NOT EXISTS quarantine (
    id            BIGINT PRIMARY KEY DEFAULT nextval('quarantine_id_seq'),
    block         VARCHAR NOT NULL,
    run_id        VARCHAR NOT NULL,
    rule_name     VARCHAR NOT NULL,
    reason        VARCHAR NOT NULL,
    ts            TIMESTAMP NOT NULL,   -- UTC
    status        VARCHAR NOT NULL CHECK (status IN ('QUARANTINED', 'REPLAYED')),
    payload_json  VARCHAR NOT NULL,
    replayed_at   TIMESTAMP,            -- UTC
    replay_run_id VARCHAR
);
"""

_COLUMNS = (
    "id, block, run_id, rule_name, reason, ts, status, payload_json, replayed_at, replay_run_id"
)


def _to_utc_naive(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(UTC).replace(tzinfo=None)


def _from_utc_naive(ts: datetime | None) -> datetime | None:
    return None if ts is None else ts.replace(tzinfo=UTC)


class DuckDBQuarantineStore:
    """Quarantine backed by a DuckDB file (default ``<root>/quarantine.duckdb``).

    A connection is opened per operation so the file is not held locked between
    calls (other processes, e.g. ``guardian status``, can read it).
    """

    def __init__(self, root: Path | str, filename: str = "quarantine.duckdb") -> None:
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / filename
        with self._connect() as con:
            con.execute(_SCHEMA)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.path))

    def add(self, entries: Sequence[QuarantineEntry]) -> list[int]:
        if not entries:
            return []
        now = datetime.now(UTC)
        rows = []
        for e in entries:
            validate_name(e.block, "block name")
            validate_name(e.run_id, "run_id")
            json.loads(e.payload_json)  # reject malformed payloads up front
            rows.append(
                (
                    e.block,
                    e.run_id,
                    e.rule_name,
                    e.reason,
                    _to_utc_naive(e.ts or now),
                    QuarantineStatus.QUARANTINED.value,
                    e.payload_json,
                )
            )
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            ids = []
            for row in rows:
                (new_id,) = con.execute(
                    "INSERT INTO quarantine "
                    "(block, run_id, rule_name, reason, ts, status, payload_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                    row,
                ).fetchone()
                ids.append(int(new_id))
            con.execute("COMMIT")
        return ids

    def list(
        self,
        block: str | None = None,
        status: QuarantineStatus | None = None,
        run_id: str | None = None,
    ) -> list[QuarantineRecord]:
        where, params = self._filters(block=block, status=status, run_id=run_id)
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_COLUMNS} FROM quarantine{where} ORDER BY id", params
            ).fetchall()
        return [
            QuarantineRecord(
                id=int(r[0]),
                block=r[1],
                run_id=r[2],
                rule_name=r[3],
                reason=r[4],
                ts=_from_utc_naive(r[5]),  # type: ignore[arg-type]
                status=QuarantineStatus(r[6]),
                payload_json=r[7],
                replayed_at=_from_utc_naive(r[8]),
                replay_run_id=r[9],
            )
            for r in rows
        ]

    def count(self, block: str | None = None, status: QuarantineStatus | None = None) -> int:
        where, params = self._filters(block=block, status=status)
        with self._connect() as con:
            (n,) = con.execute(f"SELECT count(*) FROM quarantine{where}", params).fetchone()
        return int(n)

    def load_frame(
        self, block: str, status: QuarantineStatus = QuarantineStatus.QUARANTINED
    ) -> tuple[list[int], pd.DataFrame]:
        records = self.list(block=block, status=status)
        ids = [r.id for r in records]
        frame = pd.DataFrame([r.payload() for r in records])
        return ids, frame

    def mark_replayed(self, ids: Iterable[int], replay_run_id: str) -> int:
        id_list = [int(i) for i in ids]
        if not id_list:
            return 0
        validate_name(replay_run_id, "run_id")
        placeholders = ", ".join("?" for _ in id_list)
        with self._connect() as con:
            changed = con.execute(
                "UPDATE quarantine SET status = ?, replayed_at = ?, replay_run_id = ? "
                f"WHERE status = ? AND id IN ({placeholders}) RETURNING id",
                [
                    QuarantineStatus.REPLAYED.value,
                    _to_utc_naive(datetime.now(UTC)),
                    replay_run_id,
                    QuarantineStatus.QUARANTINED.value,
                    *id_list,
                ],
            ).fetchall()
        return len(changed)

    @staticmethod
    def _filters(**filters: object) -> tuple[str, list[object]]:
        clauses, params = [], []
        for column, value in filters.items():
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value.value if isinstance(value, QuarantineStatus) else value)
        return (" WHERE " + " AND ".join(clauses) if clauses else ""), params
