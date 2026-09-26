"""Version registry: which implementation of each block is active, and how it got there.

The registry overrides the spec's ``active``. A promotion is recorded as PROMOTING
before anything changes and is only marked COMPLETED (and the new version made active)
at the very end, so an interrupted promotion is visible and can be resumed.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import duckdb

from guardian.core.dbfile import DuckDBFile
from guardian.core.models import GuardianError, validate_name


class VersionError(GuardianError):
    pass


class VersionState(StrEnum):
    ACTIVE = "ACTIVE"
    PROMOTING = "PROMOTING"


class PromotionKind(StrEnum):
    PROMOTE = "PROMOTE"
    ROLLBACK = "ROLLBACK"


class PromotionState(StrEnum):
    PROMOTING = "PROMOTING"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True)
class ActiveVersion:
    block: str
    version: str
    previous_version: str | None
    state: VersionState
    pending_version: str | None
    updated_at: datetime


@dataclass(frozen=True)
class Promotion:
    id: int
    block: str
    kind: PromotionKind
    from_version: str | None
    to_version: str
    reason: str
    approved: bool
    state: PromotionState
    started_at: datetime
    completed_at: datetime | None
    replay_run_id: str | None
    last_good_before: str | None


_SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS promotion_id_seq START 1;
CREATE TABLE IF NOT EXISTS active_versions (
    block            VARCHAR PRIMARY KEY,
    version          VARCHAR NOT NULL,
    previous_version VARCHAR,
    state            VARCHAR NOT NULL CHECK (state IN ('ACTIVE', 'PROMOTING')),
    pending_version  VARCHAR,
    updated_at       TIMESTAMP NOT NULL   -- UTC
);
CREATE TABLE IF NOT EXISTS promotions (
    id               BIGINT PRIMARY KEY DEFAULT nextval('promotion_id_seq'),
    block            VARCHAR NOT NULL,
    kind             VARCHAR NOT NULL CHECK (kind IN ('PROMOTE', 'ROLLBACK')),
    from_version     VARCHAR,
    to_version       VARCHAR NOT NULL,
    reason           VARCHAR NOT NULL,
    approved         BOOLEAN NOT NULL,
    state            VARCHAR NOT NULL CHECK (state IN ('PROMOTING', 'COMPLETED')),
    started_at       TIMESTAMP NOT NULL,  -- UTC
    completed_at     TIMESTAMP,           -- UTC
    replay_run_id    VARCHAR,
    last_good_before VARCHAR
);
"""

_ACTIVE_COLS = "block, version, previous_version, state, pending_version, updated_at"
_PROMO_COLS = (
    "id, block, kind, from_version, to_version, reason, approved, state, started_at, "
    "completed_at, replay_run_id, last_good_before"
)


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _utc(ts: datetime | None) -> datetime | None:
    return None if ts is None else ts.replace(tzinfo=UTC)


class VersionRegistry:
    """DuckDB-backed (``<root>/versions.duckdb``); a connection per operation."""

    def __init__(self, root: Path | str, filename: str = "versions.duckdb") -> None:
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / filename
        self._db = DuckDBFile(self.path)
        with self._connect() as con:
            con.execute(_SCHEMA)

    def _connect(self) -> AbstractContextManager[duckdb.DuckDBPyConnection]:
        return self._db.connect()

    def session(self) -> AbstractContextManager[None]:
        """Share one connection for a unit of work (see core/dbfile.py)."""
        return self._db.session()

    @contextmanager
    def _transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                yield con
            except BaseException:
                con.execute("ROLLBACK")
                raise
            con.execute("COMMIT")

    # ---------------------------------------------------------------- reads

    def get(self, block: str) -> ActiveVersion | None:
        with self._connect() as con:
            row = con.execute(
                f"SELECT {_ACTIVE_COLS} FROM active_versions WHERE block = ?", [block]
            ).fetchone()
        return None if row is None else _active(row)

    def all(self) -> dict[str, ActiveVersion]:
        with self._connect() as con:
            rows = con.execute(f"SELECT {_ACTIVE_COLS} FROM active_versions").fetchall()
        return {r[0]: _active(r) for r in rows}

    def promotion(self, promotion_id: int) -> Promotion:
        with self._connect() as con:
            row = con.execute(
                f"SELECT {_PROMO_COLS} FROM promotions WHERE id = ?", [promotion_id]
            ).fetchone()
        if row is None:
            raise VersionError(f"no promotion {promotion_id}")
        return _promotion(row)

    def pending(self, block: str) -> Promotion | None:
        """The block's unfinished (PROMOTING) promotion, if any."""
        with self._connect() as con:
            row = con.execute(
                f"SELECT {_PROMO_COLS} FROM promotions WHERE block = ? AND state = ? "
                "ORDER BY id DESC LIMIT 1",
                [block, PromotionState.PROMOTING.value],
            ).fetchone()
        return None if row is None else _promotion(row)

    def history(self, block: str | None = None) -> list[Promotion]:
        where, params = (" WHERE block = ?", [block]) if block else ("", [])
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_PROMO_COLS} FROM promotions{where} ORDER BY id", params
            ).fetchall()
        return [_promotion(r) for r in rows]

    # ---------------------------------------------------------------- writes

    def begin_promotion(
        self,
        block: str,
        from_version: str | None,
        to_version: str,
        *,
        reason: str,
        approved: bool,
        last_good_before: str | None,
    ) -> Promotion:
        """Record intent. The live version is unchanged until ``complete_promotion``."""
        validate_name(block, "block name")
        if self.pending(block) is not None:
            raise VersionError(f"block {block!r} already has a promotion in progress")
        now = _now()
        with self._transaction() as con:
            (pid,) = con.execute(
                "INSERT INTO promotions (block, kind, from_version, to_version, reason, "
                "approved, state, started_at, last_good_before) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                [
                    block,
                    PromotionKind.PROMOTE.value,
                    from_version,
                    to_version,
                    reason,
                    approved,
                    PromotionState.PROMOTING.value,
                    now,
                    last_good_before,
                ],
            ).fetchone()
            self._upsert_active(
                con,
                block,
                version=from_version or to_version,
                previous=self._previous(con, block),
                state=VersionState.PROMOTING,
                pending=to_version,
                now=now,
            )
        return self.promotion(int(pid))

    def set_replay_snapshot(self, promotion_id: int, run_id: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE promotions SET replay_run_id = ? WHERE id = ?", [run_id, promotion_id]
            )

    def complete_promotion(self, promotion_id: int) -> ActiveVersion:
        """Atomically make the target version active and mark the promotion COMPLETED."""
        promo = self.promotion(promotion_id)
        if promo.state is PromotionState.COMPLETED:
            raise VersionError(f"promotion {promotion_id} is already completed")
        now = _now()
        with self._transaction() as con:
            self._upsert_active(
                con,
                promo.block,
                version=promo.to_version,
                previous=promo.from_version,
                state=VersionState.ACTIVE,
                pending=None,
                now=now,
            )
            con.execute(
                "UPDATE promotions SET state = ?, completed_at = ? WHERE id = ?",
                [PromotionState.COMPLETED.value, now, promotion_id],
            )
        active = self.get(promo.block)
        assert active is not None
        return active

    def rollback(self, block: str) -> Promotion:
        """Make the previous version active again (recorded as a ROLLBACK)."""
        current = self.get(block)
        if current is None or current.previous_version is None:
            raise VersionError(f"block {block!r} has no previous version to roll back to")
        if current.state is VersionState.PROMOTING:
            raise VersionError(
                f"block {block!r} has a promotion in progress; resume it with "
                "`guardian shadow promote` before rolling back"
            )
        now = _now()
        with self._transaction() as con:
            (pid,) = con.execute(
                "INSERT INTO promotions (block, kind, from_version, to_version, reason, "
                "approved, state, started_at, completed_at) "
                "VALUES (?, ?, ?, ?, 'rollback', TRUE, ?, ?, ?) RETURNING id",
                [
                    block,
                    PromotionKind.ROLLBACK.value,
                    current.version,
                    current.previous_version,
                    PromotionState.COMPLETED.value,
                    now,
                    now,
                ],
            ).fetchone()
            self._upsert_active(
                con,
                block,
                version=current.previous_version,
                previous=current.version,
                state=VersionState.ACTIVE,
                pending=None,
                now=now,
            )
        return self.promotion(int(pid))

    @staticmethod
    def _previous(con: duckdb.DuckDBPyConnection, block: str) -> str | None:
        row = con.execute(
            "SELECT previous_version FROM active_versions WHERE block = ?", [block]
        ).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _upsert_active(
        con: duckdb.DuckDBPyConnection,
        block: str,
        *,
        version: str,
        previous: str | None,
        state: VersionState,
        pending: str | None,
        now: datetime,
    ) -> None:
        con.execute("DELETE FROM active_versions WHERE block = ?", [block])
        con.execute(
            f"INSERT INTO active_versions ({_ACTIVE_COLS}) VALUES (?, ?, ?, ?, ?, ?)",
            [block, version, previous, state.value, pending, now],
        )


def _active(row: tuple) -> ActiveVersion:
    return ActiveVersion(row[0], row[1], row[2], VersionState(row[3]), row[4], _utc(row[5]))


def _promotion(row: tuple) -> Promotion:
    return Promotion(
        id=int(row[0]),
        block=row[1],
        kind=PromotionKind(row[2]),
        from_version=row[3],
        to_version=row[4],
        reason=row[5],
        approved=bool(row[6]),
        state=PromotionState(row[7]),
        started_at=_utc(row[8]),  # type: ignore[arg-type]
        completed_at=_utc(row[9]),
        replay_run_id=row[10],
        last_good_before=row[11],
    )
