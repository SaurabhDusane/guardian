"""Core data models shared by every Guardian component.

Nothing in here touches storage or third-party orchestration libraries; these are
plain, immutable value objects.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
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

    def __post_init__(self) -> None:
        validate_name(self.name, "block name")
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
        raise KeyError(f"pipeline {self.name!r} has no block {name!r}")

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
    block: str
    replayed: int
    still_failing: int
    snapshot: DataRef | None = None
