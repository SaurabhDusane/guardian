"""Shadow runs: compare a candidate version against the live one, and decide promotion.

A candidate runs on the same resolved live inputs as the active version. Its output
goes to a separate candidate snapshot store that ``resolve_input`` never reads.

- PARITY mode (the active version passed on this run): row-level diff against the
  active version's promoted output by ``merge_key`` (added / removed / changed rows and
  changed columns) plus per-column stats.
- ABSOLUTE mode (the active version is DEGRADED/OUT, so there is no trustworthy
  baseline): the candidate's validation pass rate plus stats only.

Auto-promotion needs ``required_runs`` consecutive PARITY runs within tolerance.
Absolute mode, or a shadow started with ``expect_diff``, needs an explicit approval.
"""

from __future__ import annotations

import json
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from guardian.core.dbfile import DuckDBFile
from guardian.core.models import GuardianError, ShadowPolicy, validate_name
from guardian.core.provenance import QUALITY_COL


class ShadowError(GuardianError):
    pass


class ShadowMode(StrEnum):
    PARITY = "PARITY"
    ABSOLUTE = "ABSOLUTE"


class ShadowStatus(StrEnum):
    ACTIVE = "ACTIVE"
    PROMOTED = "PROMOTED"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class Shadow:
    id: int
    block: str
    version: str
    expect_diff: bool
    status: ShadowStatus
    started_at: datetime


@dataclass(frozen=True)
class Comparison:
    rows_active: int
    rows_candidate: int
    added: int
    removed: int
    changed: int
    changed_columns: tuple[str, ...]

    @property
    def changed_fraction(self) -> float:
        return (self.added + self.removed + self.changed) / max(self.rows_active, 1)


@dataclass(frozen=True)
class ShadowRun:
    shadow_id: int
    block: str
    version: str
    run_id: str
    mode: ShadowMode
    rows_in: int  # rows the candidate produced (handed to validation)
    rows_passed: int
    pass_rate: float
    comparison: Comparison | None  # PARITY only
    stats: dict[str, Any]  # {"candidate": {col: {...}}, "active": {...}}
    within_tolerance: bool
    notes: tuple[str, ...] = ()
    ts: datetime | None = None

    @property
    def changed_fraction(self) -> float | None:
        return None if self.comparison is None else self.comparison.changed_fraction


# ---------------------------------------------------------------- comparison


def compare_rows(active: pd.DataFrame, candidate: pd.DataFrame, key: list[str]) -> Comparison:
    """Row-level diff of two outputs keyed by ``key`` (which must be unique in both)."""
    # Guardian's own annotation is not block output: never a difference.
    active = active.drop(columns=[QUALITY_COL], errors="ignore")
    candidate = candidate.drop(columns=[QUALITY_COL], errors="ignore")
    missing = [c for c in key if c not in active.columns or c not in candidate.columns]
    if missing:
        raise ShadowError(f"merge_key column(s) {missing} missing from an output")
    a = active.set_index(key)
    c = candidate.set_index(key)
    if not a.index.is_unique or not c.index.is_unique:
        raise ShadowError(f"merge_key {key} is not unique in an output; cannot diff rows")
    added = c.index.difference(a.index)
    removed = a.index.difference(c.index)
    common = a.index.intersection(c.index)
    common_cols = [col for col in a.columns if col in c.columns]
    schema_cols = sorted(set(a.columns) ^ set(c.columns))
    aa = a.loc[common, common_cols].astype(object)
    cc = c.loc[common, common_cols].astype(object)
    equal = (aa == cc) | (aa.isna() & cc.isna())
    row_changed = ~equal.all(axis=1)
    changed = len(common) if schema_cols else int(row_changed.sum())
    changed_columns = [col for col in common_cols if not bool(equal[col].all())] + schema_cols
    return Comparison(
        rows_active=len(a),
        rows_candidate=len(c),
        added=len(added),
        removed=len(removed),
        changed=changed,
        changed_columns=tuple(changed_columns),
    )


def column_stats(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Per-column null rate, mean (numeric columns) and distinct count."""
    stats: dict[str, dict[str, Any]] = {}
    for col in df.columns:
        series = df[col]
        numeric = pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)
        mean = series.mean() if numeric and len(series) else None
        stats[str(col)] = {
            "null_rate": float(series.isna().mean()) if len(series) else 0.0,
            "mean": None if mean is None or pd.isna(mean) else float(mean),
            "distinct": int(series.nunique(dropna=True)),
        }
    return stats


def evaluate(
    policy: ShadowPolicy, mode: ShadowMode, pass_rate: float, comparison: Comparison | None
) -> tuple[bool, list[str]]:
    """Is a shadow run within tolerance? Returns (ok, reasons it is not)."""
    reasons = []
    assert policy.min_pass_rate is not None
    if pass_rate < policy.min_pass_rate:
        reasons.append(f"pass rate {pass_rate:.4f} < min_pass_rate {policy.min_pass_rate}")
    if mode is ShadowMode.PARITY:
        assert comparison is not None
        if comparison.changed_fraction > policy.max_changed_fraction:
            reasons.append(
                f"changed fraction {comparison.changed_fraction:.4f} > max_changed_fraction "
                f"{policy.max_changed_fraction} (columns: {', '.join(comparison.changed_columns)})"
            )
    return not reasons, reasons


def auto_promotable(runs: list[ShadowRun], policy: ShadowPolicy) -> bool:
    """The last ``required_runs`` runs are all PARITY and within tolerance."""
    recent = runs[-policy.required_runs :]
    return len(recent) == policy.required_runs and all(
        r.mode is ShadowMode.PARITY and r.within_tolerance for r in recent
    )


# ---------------------------------------------------------------- storage


_SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS shadow_id_seq START 1;
CREATE TABLE IF NOT EXISTS shadows (
    id          BIGINT PRIMARY KEY DEFAULT nextval('shadow_id_seq'),
    block       VARCHAR NOT NULL,
    version     VARCHAR NOT NULL,
    expect_diff BOOLEAN NOT NULL,
    status      VARCHAR NOT NULL CHECK (status IN ('ACTIVE', 'PROMOTED', 'STOPPED')),
    started_at  TIMESTAMP NOT NULL,   -- UTC
    ended_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS shadow_runs (
    shadow_id        BIGINT NOT NULL,
    block            VARCHAR NOT NULL,
    version          VARCHAR NOT NULL,
    run_id           VARCHAR NOT NULL,
    mode             VARCHAR NOT NULL,
    rows_in          BIGINT NOT NULL,
    rows_passed      BIGINT NOT NULL,
    pass_rate        DOUBLE NOT NULL,
    comparison_json  VARCHAR,
    stats_json       VARCHAR NOT NULL,
    within_tolerance BOOLEAN NOT NULL,
    notes_json       VARCHAR NOT NULL,
    ts               TIMESTAMP NOT NULL,
    PRIMARY KEY (shadow_id, run_id)
);
"""

_SHADOW_COLS = "id, block, version, expect_diff, status, started_at"
_RUN_COLS = (
    "shadow_id, block, version, run_id, mode, rows_in, rows_passed, pass_rate, "
    "comparison_json, stats_json, within_tolerance, notes_json, ts"
)


class ShadowStore:
    """DuckDB-backed (``<root>/shadow.duckdb``); a connection per operation."""

    def __init__(self, root: Path | str, filename: str = "shadow.duckdb") -> None:
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

    def start(self, block: str, version: str, *, expect_diff: bool) -> Shadow:
        validate_name(block, "block name")
        validate_name(version, "version name")
        current = self.get(block)
        if current is not None:
            raise ShadowError(
                f"block {block!r} is already shadowing {current.version!r}; "
                "promote or stop it first"
            )
        with self._connect() as con:
            (sid,) = con.execute(
                "INSERT INTO shadows (block, version, expect_diff, status, started_at) "
                "VALUES (?, ?, ?, ?, ?) RETURNING id",
                [block, version, expect_diff, ShadowStatus.ACTIVE.value, _now()],
            ).fetchone()
        shadow = self.by_id(int(sid))
        return shadow

    def by_id(self, shadow_id: int) -> Shadow:
        with self._connect() as con:
            row = con.execute(
                f"SELECT {_SHADOW_COLS} FROM shadows WHERE id = ?", [shadow_id]
            ).fetchone()
        if row is None:
            raise ShadowError(f"no shadow {shadow_id}")
        return _shadow(row)

    def get(self, block: str) -> Shadow | None:
        """The block's ACTIVE shadow, if any."""
        with self._connect() as con:
            row = con.execute(
                f"SELECT {_SHADOW_COLS} FROM shadows WHERE block = ? AND status = ?",
                [block, ShadowStatus.ACTIVE.value],
            ).fetchone()
        return None if row is None else _shadow(row)

    def active(self) -> list[Shadow]:
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_SHADOW_COLS} FROM shadows WHERE status = ? ORDER BY id",
                [ShadowStatus.ACTIVE.value],
            ).fetchall()
        return [_shadow(r) for r in rows]

    def end(self, shadow_id: int, status: ShadowStatus) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE shadows SET status = ?, ended_at = ? WHERE id = ? AND status = ?",
                [status.value, _now(), shadow_id, ShadowStatus.ACTIVE.value],
            )

    def record(self, run: ShadowRun) -> None:
        comparison = None
        if run.comparison is not None:
            comparison = json.dumps(
                {**run.comparison.__dict__, "changed_columns": list(run.comparison.changed_columns)}
            )
        with self._connect() as con:
            con.execute(
                f"INSERT INTO shadow_runs ({_RUN_COLS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run.shadow_id,
                    run.block,
                    run.version,
                    run.run_id,
                    run.mode.value,
                    run.rows_in,
                    run.rows_passed,
                    run.pass_rate,
                    comparison,
                    json.dumps(run.stats, default=str),
                    run.within_tolerance,
                    json.dumps(list(run.notes)),
                    _now(),
                ],
            )

    def runs(self, shadow_id: int) -> list[ShadowRun]:
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_RUN_COLS} FROM shadow_runs WHERE shadow_id = ? ORDER BY ts, run_id",
                [shadow_id],
            ).fetchall()
        return [_run(r) for r in rows]


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _shadow(row: tuple) -> Shadow:
    return Shadow(
        int(row[0]), row[1], row[2], bool(row[3]), ShadowStatus(row[4]), row[5].replace(tzinfo=UTC)
    )


def _run(row: tuple) -> ShadowRun:
    comparison = None
    if row[8] is not None:
        data = json.loads(row[8])
        data["changed_columns"] = tuple(data["changed_columns"])
        comparison = Comparison(**data)
    return ShadowRun(
        shadow_id=int(row[0]),
        block=row[1],
        version=row[2],
        run_id=row[3],
        mode=ShadowMode(row[4]),
        rows_in=int(row[5]),
        rows_passed=int(row[6]),
        pass_rate=float(row[7]),
        comparison=comparison,
        stats=json.loads(row[9]),
        within_tolerance=bool(row[10]),
        notes=tuple(json.loads(row[11])),
        ts=row[12].replace(tzinfo=UTC),
    )
