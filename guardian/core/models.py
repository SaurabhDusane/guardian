"""Core data models shared by every Guardian component.

Nothing in here touches storage or third-party orchestration libraries; these are
plain, immutable value objects.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

DEFAULT_STORAGE_ROOT = Path(".guardian")

# Block names and run ids end up as file and directory names, so they are restricted
# to characters that are safe on every platform (notably no ':' or path separators).
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]*$")


def validate_name(value: str, what: str = "name") -> str:
    """Return ``value`` if it is safe to use as a path component, else raise ValueError."""
    if not isinstance(value, str) or not _SAFE_NAME.fullmatch(value) or ".." in value:
        raise ValueError(
            f"invalid {what} {value!r}: use letters, digits, '_', '-', '.' "
            "(must not start with '.' or '-', and must not contain '..')"
        )
    return value


class GuardianError(Exception):
    """Base class for Guardian errors."""


class NoSafeInputError(GuardianError):
    """Raised when no last-good snapshot exists for any candidate input source."""


class BlockStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    OUT = "OUT"


class Action(StrEnum):
    PASS = "PASS"
    ROLLBACK = "ROLLBACK"


class QuarantineStatus(StrEnum):
    QUARANTINED = "QUARANTINED"
    REPLAYED = "REPLAYED"


class Quality(StrEnum):
    """How trustworthy a snapshot's inputs were, from best to worst.

    FRESH: every input was read from this run's (or a healthy) upstream snapshot.
    STALE: some input was an unhealthy upstream's older last-good snapshot.
    FALLBACK: some input came through a fallback edge's adapter (approximated data).
    A snapshot's quality is the worst of its inputs' qualities, which already include
    the quality of the snapshots they read, so it propagates downstream.
    """

    FRESH = "FRESH"
    STALE = "STALE"
    FALLBACK = "FALLBACK"

    @property
    def rank(self) -> int:
        return _QUALITY_ORDER.index(self)

    @classmethod
    def worst(cls, qualities: Any) -> Quality:
        return max((cls(q) for q in qualities), key=lambda q: q.rank, default=cls.FRESH)


_QUALITY_ORDER = (Quality.FRESH, Quality.STALE, Quality.FALLBACK)


@dataclass(frozen=True)
class ShadowPolicy:
    """When a shadowed candidate version may be promoted.

    ``min_pass_rate`` defaults to ``1 - quarantine_threshold`` of the block
    (resolved by ``BlockSpec.shadow_policy``).
    """

    required_runs: int = 3
    max_changed_fraction: float = 0.01
    min_pass_rate: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.required_runs, bool) or int(self.required_runs) < 1:
            raise ValueError(f"shadow.required_runs must be >= 1, got {self.required_runs}")
        object.__setattr__(self, "required_runs", int(self.required_runs))
        if not 0.0 <= self.max_changed_fraction <= 1.0:
            raise ValueError("shadow.max_changed_fraction must be in [0, 1]")
        if self.min_pass_rate is not None and not 0.0 <= self.min_pass_rate <= 1.0:
            raise ValueError("shadow.min_pass_rate must be in [0, 1]")


@dataclass(frozen=True)
class DriftThresholds:
    """Drift at or above either threshold reaches this level (None: not checked)."""

    psi: float | None = None
    z: float | None = None

    def __post_init__(self) -> None:
        for name in ("psi", "z"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or float(value) <= 0):
                raise ValueError(f"drift threshold {name} must be a positive number")
            if value is not None:
                object.__setattr__(self, name, float(value))


@dataclass(frozen=True)
class DriftPolicy:
    """Statistical drift detection for a block's promoted output.

    The profile is learned from the block's last ``window`` promoted snapshots (at least
    ``min_history`` are needed). Categorical columns are scored by PSI, numeric ones by
    PSI and a z-score of the mean shift. ``columns`` limits the check (default: every
    numeric column and every categorical one with at most ``max_categories`` values,
    except the merge key); ``exclude`` removes columns.
    """

    window: int = 5
    min_history: int = 2
    warn: DriftThresholds = field(default_factory=lambda: DriftThresholds(psi=0.1, z=3.0))
    fail: DriftThresholds = field(default_factory=lambda: DriftThresholds(psi=0.25, z=6.0))
    columns: tuple[str, ...] | None = None
    exclude: tuple[str, ...] = ()
    max_categories: int = 50
    bins: int = 10

    def __post_init__(self) -> None:
        for name in ("window", "min_history", "max_categories", "bins"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"drift.{name} must be a positive integer")
        if self.min_history > self.window:
            raise ValueError("drift.min_history cannot exceed drift.window")
        for metric in ("psi", "z"):
            warn, fail = getattr(self.warn, metric), getattr(self.fail, metric)
            if warn is not None and fail is not None and warn > fail:
                raise ValueError(f"drift warn.{metric} must not exceed fail.{metric}")
        if self.columns is not None:
            object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "exclude", tuple(self.exclude))


@dataclass(frozen=True)
class FallbackEdge:
    """When ``replaces`` is unavailable, read ``source`` instead and apply ``adapter``."""

    replaces: str
    source: str
    adapter: str | None = None

    def __post_init__(self) -> None:
        validate_name(self.replaces, "fallback 'replaces'")
        validate_name(self.source, "fallback 'source'")
        if self.replaces == self.source:
            raise ValueError(f"fallback edge cannot replace {self.replaces!r} with itself")


@dataclass(frozen=True)
class BlockSpec:
    name: str
    fn: str
    inputs: tuple[str, ...] = ()
    schema: str | None = None
    quarantine_threshold: float = 0.0
    fallbacks: tuple[FallbackEdge, ...] = ()
    # Extra keyword arguments passed to ``fn`` (e.g. loader settings for a source block).
    params: Mapping[str, Any] = field(default_factory=dict, hash=False)
    # Columns identifying a row across runs. Replay upserts recovered rows into the
    # last-good snapshot by this key; without it, replay never merges.
    merge_key: tuple[str, ...] | None = None
    # Named implementations ("module:fn"); ``fn`` is the one named by ``active``.
    # Without ``versions`` a block has a single implicit version.
    versions: Mapping[str, str] = field(default_factory=dict, hash=False)
    active: str | None = None
    # Source blocks only: loads the raw data once per run; every version of the block
    # then receives it as its single input (so candidates see the same loaded data).
    load: str | None = None
    shadow: ShadowPolicy = field(default_factory=ShadowPolicy)
    # Opt-in: append a ``_guardian_quality`` column (FRESH/STALE/FALLBACK) to this
    # block's promoted output. Off by default: user frames are never changed silently.
    annotate_quality: bool = False
    # Opt-in: run the diagnosis agent after a ROLLBACK (advisory; never fails a run).
    auto_diagnose: bool = False
    # Columns whose values are masked in anything sent to an LLM.
    redact_columns: tuple[str, ...] = ()
    # Opt-in statistical drift detection (soft failures that pass schema validation).
    drift: DriftPolicy | None = None
    # The block's unit tests (pytest node ids or paths, relative to the repository root).
    # A proposed fix must pass them before it can become a pull request.
    tests: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_name(self.name, "block name")
        redact = tuple(self.redact_columns)
        if not all(isinstance(c, str) and c for c in redact):
            raise ValueError(f"block {self.name!r}: redact_columns must be column names")
        object.__setattr__(self, "redact_columns", redact)
        tests = tuple(self.tests)
        if not all(isinstance(t, str) and t for t in tests):
            raise ValueError(f"block {self.name!r}: tests must be pytest node ids or paths")
        object.__setattr__(self, "tests", tests)
        object.__setattr__(self, "versions", MappingProxyType(dict(self.versions)))
        if self.versions:
            for version in self.versions:
                validate_name(version, "version name")
            if self.active is None:
                raise ValueError(f"block {self.name!r}: 'versions' requires 'active'")
            if self.active not in self.versions:
                raise ValueError(
                    f"block {self.name!r}: active version {self.active!r} is not one of "
                    f"{sorted(self.versions)}"
                )
            if self.fn != self.versions[self.active]:
                raise ValueError(
                    f"block {self.name!r}: fn {self.fn!r} is not the active version's "
                    f"function {self.versions[self.active]!r}"
                )
        elif self.active is not None:
            raise ValueError(f"block {self.name!r}: 'active' requires 'versions'")
        if self.load is not None and self.inputs:
            raise ValueError(f"block {self.name!r}: only source blocks (no inputs) may 'load'")
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))
        if self.merge_key is not None:
            key = tuple(self.merge_key)
            if not key or not all(isinstance(c, str) and c for c in key):
                raise ValueError(
                    f"block {self.name!r}: merge_key must be a non-empty list of column names"
                )
            if len(set(key)) != len(key):
                raise ValueError(f"block {self.name!r}: duplicate columns in merge_key {list(key)}")
            object.__setattr__(self, "merge_key", key)
        # Allow lists from callers/YAML while keeping the dataclass hashable.
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "fallbacks", tuple(self.fallbacks))
        if not 0.0 <= self.quarantine_threshold <= 1.0:
            raise ValueError(
                f"block {self.name!r}: quarantine_threshold must be in [0, 1], "
                f"got {self.quarantine_threshold}"
            )
        if len(set(self.inputs)) != len(self.inputs):
            raise ValueError(f"block {self.name!r}: duplicate inputs {list(self.inputs)}")
        for edge in self.fallbacks:
            if edge.replaces not in self.inputs:
                raise ValueError(
                    f"block {self.name!r}: fallback replaces {edge.replaces!r}, "
                    f"which is not one of its inputs {list(self.inputs)}"
                )
        replaced = [edge.replaces for edge in self.fallbacks]
        if len(set(replaced)) != len(replaced):
            raise ValueError(f"block {self.name!r}: more than one fallback per input")

    def fallback_for(self, upstream: str) -> FallbackEdge | None:
        return next((e for e in self.fallbacks if e.replaces == upstream), None)

    def version_ref(self, version: str | None) -> str:
        """The function reference of ``version`` (None: the spec's active one)."""
        if version is None or (not self.versions and version == self.active):
            return self.fn
        if version not in self.versions:
            known = sorted(self.versions) or ["(none: block has no versions)"]
            raise KeyError(f"block {self.name!r} has no version {version!r}; known: {known}")
        return self.versions[version]

    def shadow_policy(self) -> ShadowPolicy:
        """The shadow policy with ``min_pass_rate`` resolved against the threshold."""
        if self.shadow.min_pass_rate is not None:
            return self.shadow
        return ShadowPolicy(
            required_runs=self.shadow.required_runs,
            max_changed_fraction=self.shadow.max_changed_fraction,
            min_pass_rate=1.0 - self.quarantine_threshold,
        )


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    blocks: tuple[BlockSpec, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "blocks", tuple(self.blocks))
        names = [b.name for b in self.blocks]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"pipeline {self.name!r}: duplicate block names {dupes}")

    @property
    def block_names(self) -> tuple[str, ...]:
        return tuple(b.name for b in self.blocks)

    def block(self, name: str) -> BlockSpec:
        for b in self.blocks:
            if b.name == name:
                return b
        raise KeyError(
            f"pipeline {self.name!r} has no block {name!r}; "
            f"its blocks: {', '.join(self.block_names)}"
        )

    def dependents(self, name: str) -> tuple[BlockSpec, ...]:
        return tuple(b for b in self.blocks if name in b.inputs)


@dataclass(frozen=True)
class DataRef:
    """A pointer to the snapshot a consumer should read.

    ``block``/``run_id`` identify the snapshot actually read. ``requested`` is the
    upstream the consumer asked for; it differs from ``block`` when rerouted through a
    fallback edge, in which case ``adapter`` names the transform to apply.
    """

    block: str
    run_id: str
    requested: str | None = None
    adapter: str | None = None
    stale: bool = False

    @property
    def rerouted(self) -> bool:
        return self.requested is not None and self.requested != self.block

    @property
    def read_quality(self) -> Quality:
        """Quality of this read alone (the source snapshot's own quality not included)."""
        if self.rerouted:
            return Quality.FALLBACK
        return Quality.STALE if self.stale else Quality.FRESH


@dataclass(frozen=True)
class Decision:
    block: str
    run_id: str
    action: Action
    reason: str | None = None
    total_rows: int = 0
    good_rows: int = 0
    bad_rows: int = 0
    snapshot: DataRef | None = None

    @property
    def bad_fraction(self) -> float:
        return self.bad_rows / self.total_rows if self.total_rows else 0.0


@dataclass(frozen=True)
class RunContext:
    run_id: str
    storage_root: Path = DEFAULT_STORAGE_ROOT
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        validate_name(self.run_id, "run_id")
        object.__setattr__(self, "storage_root", Path(self.storage_root))


@dataclass(frozen=True)
class QuarantineRecord:
    id: int
    block: str
    run_id: str
    rule_name: str
    reason: str
    ts: datetime
    status: QuarantineStatus
    payload_json: str
    replayed_at: datetime | None = None
    replay_run_id: str | None = None

    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)


@dataclass(frozen=True)
class ReplayResult:
    """Outcome of a replay.

    ``snapshot`` is the snapshot the recovered rows were written to. ``merged`` is True
    when they were upserted into a new last-good snapshot (the block has a merge_key),
    False when they were written to a separate, unpromoted snapshot.
    """

    block: str
    replayed: int
    still_failing: int
    snapshot: DataRef | None = None
    merged: bool = False


@dataclass(frozen=True)
class BlockCrash:
    """The block function raised; carried as a value so execution can continue."""

    error: BaseException


@dataclass(frozen=True)
class BlockSkipped:
    """The block did not run: taken OUT, or ``blocked`` because an input had no safe source."""

    reason: str
    blocked: bool = False
