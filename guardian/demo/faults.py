"""Fault injection for demos and the scenario suite.

A fault transforms a block's output (or raises). ``inject(fn, *faults)`` wraps a
block function so its output passes through the faults in order. Row selection is
seeded and takes exactly ``round(fraction * n)`` rows, so tests can assert counts.
"""

from __future__ import annotations

import dataclasses
import functools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from guardian.core.models import PipelineSpec
from guardian.core.refs import load_ref

CORRUPT_TEXT = "__corrupt__"
CORRUPT_NUMBER = -999_999


class FaultInjected(RuntimeError):
    """Raised by the ``crash`` fault."""


@dataclass(frozen=True)
class Fault:
    name: str
    apply: Callable[[pd.DataFrame], pd.DataFrame] = field(repr=False)

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)


def pick_positions(n: int, fraction: float, seed: int) -> np.ndarray:
    """Exactly ``round(fraction * n)`` distinct row positions, deterministic per seed."""
    if not 0.0 <= fraction <= 1.0:
        raise ValueError(f"fraction must be in [0, 1], got {fraction}")
    k = round(fraction * n)
    return np.sort(np.random.default_rng(seed).choice(n, size=k, replace=False))


def _corrupt_value(series: pd.Series) -> object:
    if pd.api.types.is_bool_dtype(series):
        return None
    if pd.api.types.is_numeric_dtype(series):
        return CORRUPT_NUMBER
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.NaT
    return CORRUPT_TEXT


def corrupt_rows(fraction: float, columns: Sequence[str] | None = None, seed: int = 0) -> Fault:
    """Overwrite ``columns`` (default: all) in a fraction of rows with invalid values.

    Numbers become -999999, text becomes ``"__corrupt__"``, datetimes become NaT;
    column dtypes are preserved so the damage is row-level, not schema-level.
    """

    def apply(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        rows = pick_positions(len(out), fraction, seed)
        for column in columns or list(out.columns):
            value = _corrupt_value(out[column])
            if value is not None and len(rows):
                out.iloc[rows, out.columns.get_loc(column)] = value
        return out

    return Fault(f"corrupt_rows({fraction})", apply)


def schema_drift(rename: Mapping[str, str] | None = None, drop: Sequence[str] = ()) -> Fault:
    """Rename and/or drop columns, as an upstream schema change would."""
    if not rename and not drop:
        raise ValueError("schema_drift needs 'rename' and/or 'drop'")

    def apply(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=dict(rename or {})).drop(columns=list(drop))

    return Fault(f"schema_drift(rename={dict(rename or {})}, drop={list(drop)})", apply)


def crash(message: str = "injected crash") -> Fault:
    def apply(df: pd.DataFrame) -> pd.DataFrame:
        raise FaultInjected(message)

    return Fault("crash", apply)


def null_burst(column: str, fraction: float, seed: int = 0) -> Fault:
    """Set ``column`` to null in a fraction of rows."""

    def apply(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        rows = pick_positions(len(out), fraction, seed)
        if len(rows):
            if not (
                pd.api.types.is_float_dtype(out[column])
                or pd.api.types.is_object_dtype(out[column])
                or pd.api.types.is_datetime64_any_dtype(out[column])
            ):
                out[column] = out[column].astype(object)
            out.iloc[rows, out.columns.get_loc(column)] = None
        return out

    return Fault(f"null_burst({column}, {fraction})", apply)


def inject(fn: Callable[..., pd.DataFrame], *faults: Fault) -> Callable[..., pd.DataFrame]:
    """Wrap ``fn`` so its output goes through ``faults`` in order."""

    @functools.wraps(fn)
    def faulty(*args: object, **kwargs: object) -> pd.DataFrame:
        out = fn(*args, **kwargs)
        for fault in faults:
            out = fault(out)
        return out

    faulty.faults = faults  # type: ignore[attr-defined]
    return faulty


def apply_faults(
    spec: PipelineSpec, faults: Mapping[str, Sequence[Fault]]
) -> tuple[PipelineSpec, dict[str, Any]]:
    """Return a spec whose faulted blocks call wrapped functions, plus their registry.

    Pass both to ``Guardian(spec, root, registry=registry)``. Unfaulted blocks are
    unchanged, and snapshots/quarantine keep the real block names.
    """
    registry: dict[str, Any] = {}
    blocks = []
    for block in spec.blocks:
        if faults.get(block.name):
            key = f"__faulted__:{block.name}"
            registry[key] = inject(load_ref(block.fn), *faults[block.name])
            block = dataclasses.replace(block, fn=key)
        blocks.append(block)
    unknown = set(faults) - set(spec.block_names)
    if unknown:
        raise KeyError(f"cannot inject faults into unknown block(s) {sorted(unknown)}")
    return dataclasses.replace(spec, blocks=tuple(blocks)), registry


FAULT_SYNTAX = (
    "BLOCK:corrupt:FRACTION[:COL,COL]  |  BLOCK:null:COLUMN:FRACTION  |  "
    "BLOCK:drop:COL[,COL]  |  BLOCK:rename:OLD=NEW  |  BLOCK:crash"
)


def parse_fault(text: str, seed: int = 0) -> tuple[str, Fault]:
    """Parse a command-line fault such as ``b6_enrich:corrupt:0.5:region,segment``."""
    parts = text.split(":")
    if len(parts) < 2:
        raise ValueError(f"bad fault {text!r}; expected {FAULT_SYNTAX}")
    block, kind, args = parts[0], parts[1], parts[2:]
    try:
        if kind == "corrupt" and len(args) in (1, 2):
            columns = args[1].split(",") if len(args) == 2 else None
            return block, corrupt_rows(float(args[0]), columns=columns, seed=seed)
        if kind == "null" and len(args) == 2:
            return block, null_burst(args[0], float(args[1]), seed=seed)
        if kind == "drop" and len(args) == 1:
            return block, schema_drift(drop=args[0].split(","))
        if kind == "rename" and len(args) == 1 and "=" in args[0]:
            old, new = args[0].split("=", 1)
            return block, schema_drift(rename={old: new})
        if kind == "crash" and not args:
            return block, crash(f"injected crash in {block}")
    except ValueError as exc:
        raise ValueError(f"bad fault {text!r}: {exc}") from exc
    raise ValueError(f"bad fault {text!r}; expected {FAULT_SYNTAX}")
