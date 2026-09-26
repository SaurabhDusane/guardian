"""Fault injection for demos and the scenario suite.

A fault transforms a block's output (or raises). ``inject(fn, *faults)`` wraps a
block function so its output passes through the faults in order. Row selection is
seeded and takes exactly ``round(fraction * n)`` rows, so tests can assert counts.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

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
