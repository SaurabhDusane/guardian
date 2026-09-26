"""Provenance: where every snapshot's inputs came from, and how good they were.

Every output Guardian writes (a promoted snapshot, a rolled-back output, a replay
snapshot, a shadow candidate) gets a record: the block version that produced it and,
for each input, the snapshot actually read (``block@run_id``): the upstream's own
snapshot, a fallback source's through its adapter, or a stale last-good one. Each input
carries a quality flag, and a snapshot's quality is the worst of its inputs'
(FALLBACK > STALE > FRESH), which already include the quality of what they read, so
quality propagates downstream.

``impact`` and ``lineage`` answer "what did a degraded block touch?" and "where did
this snapshot come from?" for any block, from these records alone.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import duckdb

from guardian.core.models import Quality, validate_name

QUALITY_COL = "_guardian_quality"

LIVE = "live"
CANDIDATE = "candidate"


def snapshot_id(block: str, run_id: str) -> str:
    return f"{block}@{run_id}"


@dataclass(frozen=True)
class InputProvenance:
    """One input of a snapshot: what was asked for and what was actually read."""

    upstream: str  # the input the block declares
    source_block: str  # the block whose snapshot was read (differs on a fallback)
    source_run_id: str
    adapter: str | None
    read: Quality  # this read alone: FRESH, STALE (old last-good) or FALLBACK
    quality: Quality  # worst of the read and the source snapshot's own quality
    upstream_status: str | None = None  # the upstream's status when it was resolved

    @property
    def source_id(self) -> str:
        return snapshot_id(self.source_block, self.source_run_id)

    @property
    def how(self) -> str:
        if self.read is Quality.FALLBACK:
            return f"fallback to {self.source_id} via {self.adapter}"
        if self.read is Quality.STALE:
            return f"stale {self.source_id}"
        return self.source_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "store": LIVE,  # inputs are always read from live snapshots
            "block": self.source_block,
            "run_id": self.source_run_id,
            "adapter": self.adapter,
            "read": self.read.value,
            "quality": self.quality.value,
            "upstream_status": self.upstream_status,
        }


@dataclass(frozen=True)
class Provenance:
    block: str
    run_id: str
    kind: str  # "run" | "replay" | "candidate"
    version: str | None
    quality: Quality
    inputs: tuple[InputProvenance, ...] = ()
    store: str = LIVE  # LIVE, or CANDIDATE for shadow outputs consumers never read
    has_snapshot: bool = True  # False for a rolled-back output (quarantined, not written)
    base_run_id: str | None = None  # replay: the last-good snapshot it upserted into
    replayed_from: tuple[str, ...] = ()  # replay: runs whose quarantined rows it recovered
    ts: datetime | None = None

    @property
    def snapshot_id(self) -> str:
        return snapshot_id(self.block, self.run_id)

    def as_dict(self) -> dict[str, Any]:
        return {
            "block": self.block,
            "run_id": self.run_id,
            "kind": self.kind,
            "store": self.store,
            "version": self.version,
            "quality": self.quality.value,
            "has_snapshot": self.has_snapshot,
            "inputs": [i.as_dict() for i in self.inputs],
            "base": self.base_run_id,
            "replayed_from_runs": list(self.replayed_from),
        }


@dataclass(frozen=True)
class BlockRun:
    """What a block did on a run: PASS, ROLLBACK, SKIPPED (OUT) or BLOCKED."""

    block: str
    run_id: str
    outcome: str
    status: str
    version: str | None
    reason: str | None = None
    ts: datetime | None = None


@runtime_checkable
class ProvenanceStore(Protocol):
    def record(self, provenance: Provenance) -> None:
        """Store a snapshot's provenance (first write wins: records are immutable)."""
        ...

    def get(self, block: str, run_id: str, store: str = LIVE) -> Provenance | None: ...

    def records(self, store: str = LIVE) -> list[Provenance]: ...

    def record_run(self, run: BlockRun) -> None: ...

    def runs(self, block: str | None = None) -> list[BlockRun]: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS provenance (
    block          VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    store          VARCHAR NOT NULL,
    kind           VARCHAR NOT NULL,
    version        VARCHAR,
    quality        VARCHAR NOT NULL,
    has_snapshot   BOOLEAN NOT NULL,
    base_run_id    VARCHAR,
    replayed_from  VARCHAR NOT NULL,   -- JSON list of run ids
    ts             TIMESTAMP NOT NULL, -- UTC
    PRIMARY KEY (block, run_id, store)
);
CREATE TABLE IF NOT EXISTS provenance_inputs (
    block           VARCHAR NOT NULL,
    run_id          VARCHAR NOT NULL,
    store           VARCHAR NOT NULL,
    position        INTEGER NOT NULL,
    upstream        VARCHAR NOT NULL,
    source_block    VARCHAR NOT NULL,
    source_run_id   VARCHAR NOT NULL,
    adapter         VARCHAR,
    read            VARCHAR NOT NULL,
    quality         VARCHAR NOT NULL,
    upstream_status VARCHAR,
    PRIMARY KEY (block, run_id, store, position)
);
CREATE TABLE IF NOT EXISTS block_runs (
    block    VARCHAR NOT NULL,
    run_id   VARCHAR NOT NULL,
    outcome  VARCHAR NOT NULL,
    status   VARCHAR NOT NULL,
    version  VARCHAR,
    reason   VARCHAR,
    ts       TIMESTAMP NOT NULL,       -- UTC
    PRIMARY KEY (block, run_id)
);
"""

_PROV_COLS = (
    "block, run_id, store, kind, version, quality, has_snapshot, base_run_id, replayed_from, ts"
)
_INPUT_COLS = (
    "block, run_id, store, position, upstream, source_block, source_run_id, adapter, read, "
    "quality, upstream_status"
)
_RUN_COLS = "block, run_id, outcome, status, version, reason, ts"


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class DuckDBProvenanceStore:
    """Provenance in ``<root>/provenance.duckdb``; a connection per operation."""

    def __init__(self, root: Path | str, filename: str = "provenance.duckdb") -> None:
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / filename
        with self._connect() as con:
            con.execute(_SCHEMA)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.path))

    def record(self, provenance: Provenance) -> None:
        p = provenance
        validate_name(p.block, "block name")
        validate_name(p.run_id, "run_id")
        with self._connect() as con:
            exists = con.execute(
                "SELECT 1 FROM provenance WHERE block = ? AND run_id = ? AND store = ?",
                [p.block, p.run_id, p.store],
            ).fetchone()
            if exists:
                return  # immutable, like the snapshots it describes
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"INSERT INTO provenance ({_PROV_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    p.block,
                    p.run_id,
                    p.store,
                    p.kind,
                    p.version,
                    p.quality.value,
                    p.has_snapshot,
                    p.base_run_id,
                    json.dumps(list(p.replayed_from)),
                    _now(),
                ],
            )
            for position, i in enumerate(p.inputs):
                con.execute(
                    f"INSERT INTO provenance_inputs ({_INPUT_COLS}) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        p.block,
                        p.run_id,
                        p.store,
                        position,
                        i.upstream,
                        i.source_block,
                        i.source_run_id,
                        i.adapter,
                        i.read.value,
                        i.quality.value,
                        i.upstream_status,
                    ],
                )
            con.execute("COMMIT")

    def get(self, block: str, run_id: str, store: str = LIVE) -> Provenance | None:
        found = self._load("WHERE block = ? AND run_id = ? AND store = ?", [block, run_id, store])
        return found[0] if found else None

    def records(self, store: str = LIVE) -> list[Provenance]:
        return self._load("WHERE store = ?", [store])

    def _load(self, where: str, params: list[Any]) -> list[Provenance]:
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_PROV_COLS} FROM provenance {where} ORDER BY ts, block", params
            ).fetchall()
            inputs = con.execute(
                f"SELECT {_INPUT_COLS} FROM provenance_inputs {where} ORDER BY position", params
            ).fetchall()
        by_key: dict[tuple[str, str, str], list[InputProvenance]] = {}
        for r in inputs:
            by_key.setdefault((r[0], r[1], r[2]), []).append(
                InputProvenance(
                    upstream=r[4],
                    source_block=r[5],
                    source_run_id=r[6],
                    adapter=r[7],
                    read=Quality(r[8]),
                    quality=Quality(r[9]),
                    upstream_status=r[10],
                )
            )
        return [
            Provenance(
                block=r[0],
                run_id=r[1],
                store=r[2],
                kind=r[3],
                version=r[4],
                quality=Quality(r[5]),
                has_snapshot=bool(r[6]),
                base_run_id=r[7],
                replayed_from=tuple(json.loads(r[8])),
                ts=r[9].replace(tzinfo=UTC),
                inputs=tuple(by_key.get((r[0], r[1], r[2]), [])),
            )
            for r in rows
        ]

    def record_run(self, run: BlockRun) -> None:
        with self._connect() as con:
            con.execute(
                "DELETE FROM block_runs WHERE block = ? AND run_id = ?", [run.block, run.run_id]
            )
            con.execute(
                f"INSERT INTO block_runs ({_RUN_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [run.block, run.run_id, run.outcome, run.status, run.version, run.reason, _now()],
            )

    def runs(self, block: str | None = None) -> list[BlockRun]:
        where, params = ("WHERE block = ?", [block]) if block else ("", [])
        with self._connect() as con:
            rows = con.execute(
                f"SELECT {_RUN_COLS} FROM block_runs {where} ORDER BY ts, block", params
            ).fetchall()
        return [
            BlockRun(r[0], r[1], r[2], r[3], r[4], r[5], r[6].replace(tzinfo=UTC)) for r in rows
        ]


# ---------------------------------------------------------------- impact


@dataclass(frozen=True)
class ImpactEntry:
    block: str
    run_id: str
    relation: str  # "self": the block's own degraded run; "downstream": a touched snapshot
    quality: str  # the snapshot's quality, or the block's outcome for a "self" entry
    reason: str
    ts: datetime | None = None

    @property
    def snapshot_id(self) -> str:
        return snapshot_id(self.block, self.run_id)


DEGRADED_OUTCOMES = frozenset({"ROLLBACK", "SKIPPED", "BLOCKED"})


def impact(store: ProvenanceStore, block: str, since: str | None = None) -> list[ImpactEntry]:
    """Everything a degraded state of ``block`` touched.

    - "self" entries: the block's own runs that did not promote output (rolled back,
      taken OUT, blocked).
    - "downstream" entries: every live snapshot that read ``block`` while it was not
      healthy (a stale read of it, or a fallback in its place), and, transitively,
      every live snapshot that read one of those.

    ``since`` limits the result to runs at or after that run id (by record time).
    """
    records = [p for p in store.records(LIVE) if p.has_snapshot]
    runs = store.runs(block)
    cutoff = _since_ts(store, records, since)

    entries = [
        ImpactEntry(r.block, r.run_id, "self", r.outcome, r.reason or r.outcome.lower(), r.ts)
        for r in runs
        if r.outcome in DEGRADED_OUTCOMES and (cutoff is None or r.ts >= cutoff)
    ]

    touched: dict[str, tuple[Provenance, str]] = {}
    for p in records:
        for i in p.inputs:
            if i.upstream == block and i.read is not Quality.FRESH:
                touched[p.snapshot_id] = (p, f"read {block}: {i.how}")
                break
    frontier = list(touched)
    while frontier:
        new = []
        for p in records:
            if p.snapshot_id in touched:
                continue
            for i in p.inputs:
                if i.source_id in touched:
                    touched[p.snapshot_id] = (p, f"read {i.source_id}")
                    new.append(p.snapshot_id)
                    break
        frontier = new

    for p, reason in sorted(touched.values(), key=lambda t: (t[0].ts, t[0].block)):
        if cutoff is None or (p.ts is not None and p.ts >= cutoff):
            entries.append(
                ImpactEntry(p.block, p.run_id, "downstream", p.quality.value, reason, p.ts)
            )
    return sorted(entries, key=lambda e: (e.ts or datetime.min.replace(tzinfo=UTC), e.block))


def _since_ts(store: ProvenanceStore, records: list[Provenance], since: str | None):
    if since is None:
        return None
    times = [p.ts for p in records if p.run_id == since] + [
        r.ts for r in store.runs() if r.run_id == since
    ]
    if not times:
        raise KeyError(f"no run {since!r} in the provenance records")
    return min(t for t in times if t is not None)


# ---------------------------------------------------------------- lineage


@dataclass
class LineageNode:
    """A snapshot and, for each of its inputs, the lineage of the snapshot it read."""

    provenance: Provenance | None
    block: str
    run_id: str
    via: InputProvenance | None = None  # how the parent read this snapshot
    parents: list[LineageNode] = field(default_factory=list)

    @property
    def snapshot_id(self) -> str:
        return snapshot_id(self.block, self.run_id)


def lineage(store: ProvenanceStore, block: str, run_id: str, *, max_depth: int = 64) -> LineageNode:
    """The upstream provenance tree of the live snapshot (block, run_id)."""
    root = store.get(block, run_id)
    if root is None:
        raise KeyError(f"no provenance for snapshot {snapshot_id(block, run_id)}")
    return _node(store, root, block, run_id, None, max_depth)


def _node(store, prov, block, run_id, via, depth) -> LineageNode:
    node = LineageNode(prov, block, run_id, via)
    if prov is None or depth <= 0:
        return node
    sources: Iterable[tuple[InputProvenance | None, str, str]] = [
        (i, i.source_block, i.source_run_id) for i in prov.inputs
    ]
    if prov.kind == "replay" and prov.base_run_id:
        sources = [*sources, (None, block, prov.base_run_id)]
    for inp, src_block, src_run in sources:
        parent = store.get(src_block, src_run)
        node.parents.append(_node(store, parent, src_block, src_run, inp, depth - 1))
    return node
