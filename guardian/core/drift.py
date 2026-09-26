"""Statistical drift detection: soft failures that pass schema validation.

A block with a ``drift`` policy gets a profile learned from its last N promoted
snapshots (the runs it PASSed): per column, the null rate, and either the mean, std and
quantile bins (numeric) or the category frequencies (categorical). Each new output is
scored against it:

- PSI (population stability index) for every tracked column. Categorical columns
  compare category frequencies; numeric columns compare the share of rows in each of
  the profile's quantile bins. Nulls are a bin of their own, so a burst of nulls
  moves the PSI too.
- A z-score of the mean shift for numeric columns: ``|mean_now - mean_ref| /
  (std_ref / sqrt(n_now))``.

Reaching a ``warn`` threshold logs a DRIFT event; reaching a ``fail`` threshold makes
the output a ROLLBACK, like a validation failure. Every check is written to
``<root>/drift/<block>/<run_id>.json``, and the agent's evidence bundle includes it.
"""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from guardian.core.models import BlockSpec, DriftPolicy, DriftThresholds, validate_name
from guardian.core.provenance import QUALITY_COL

DRIFT_DIR = "drift"
DRIFT_RULE = "drift"
NULL = "__null__"
OTHER = "__other__"
EPSILON = 1e-4  # floor for bin shares, so PSI stays finite for empty bins
Z_CAP = 1e6  # a shift with zero reference spread; kept finite for JSON


class DriftLevel(StrEnum):
    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"
    NO_BASELINE = "NO_BASELINE"  # fewer than min_history promoted snapshots

    @property
    def rank(self) -> int:
        return {"NO_BASELINE": 0, "OK": 0, "WARN": 1, "FAIL": 2}[self.value]


# ------------------------------------------------------------------ profile


@dataclass(frozen=True)
class ColumnProfile:
    column: str
    kind: str  # "numeric" | "categorical"
    rows: int
    null_rate: float
    mean: float | None = None
    std: float | None = None
    edges: tuple[float, ...] = ()  # numeric: inner bin edges (reference quantiles)
    shares: dict[str, float] = field(default_factory=dict)  # bin or category -> share


def _is_numeric(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


def _categories(series: pd.Series) -> pd.Series:
    return series.map(
        lambda v: NULL if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v)
    )


def _numeric_bins(series: pd.Series, edges: Sequence[float]) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    labels = np.searchsorted(np.asarray(edges, dtype=float), values.to_numpy(dtype=float), "right")
    out = pd.Series([f"bin{i}" for i in labels], index=series.index, dtype=object)
    out[values.isna()] = NULL
    return out


def _shares(labels: pd.Series) -> dict[str, float]:
    if not len(labels):
        return {}
    counts = labels.value_counts(normalize=True)
    return {str(k): float(v) for k, v in counts.items()}


def psi(reference: dict[str, float], current: dict[str, float]) -> float:
    """Population stability index between two share tables (same bin names)."""
    total = 0.0
    for key in set(reference) | set(current):
        r = max(reference.get(key, 0.0), EPSILON)
        c = max(current.get(key, 0.0), EPSILON)
        total += (c - r) * math.log(c / r)
    return total


@dataclass(frozen=True)
class DriftProfile:
    """What a block's output normally looks like, learned from promoted snapshots."""

    reference_runs: tuple[str, ...]
    columns: tuple[ColumnProfile, ...]
    skipped: dict[str, str] = field(default_factory=dict)  # column -> why it is not tracked

    @classmethod
    def learn(
        cls,
        frames: Sequence[tuple[str, pd.DataFrame]],
        policy: DriftPolicy,
        merge_key: Sequence[str] = (),
    ) -> DriftProfile:
        runs = tuple(run for run, _ in frames)
        data = pd.concat(
            [df.drop(columns=[QUALITY_COL], errors="ignore") for _, df in frames],
            ignore_index=True,
        )
        profiles, skipped = [], {}
        for column in data.columns:
            series = data[column]
            if policy.columns is not None and column not in policy.columns:
                continue
            if column in policy.exclude:
                skipped[column] = "excluded"
                continue
            if policy.columns is None and column in merge_key:
                skipped[column] = "merge key (row identity, not a distribution)"
                continue
            n = len(series)
            null_rate = float(series.isna().mean()) if n else 0.0
            if pd.api.types.is_datetime64_any_dtype(series):
                skipped[column] = "datetime"
                continue
            if _is_numeric(series):
                values = pd.to_numeric(series, errors="coerce").dropna().astype(float)
                if values.empty:
                    skipped[column] = "no values"
                    continue
                quantiles = np.quantile(values, np.linspace(0, 1, policy.bins + 1)[1:-1])
                edges = tuple(float(q) for q in np.unique(quantiles))
                profiles.append(
                    ColumnProfile(
                        column,
                        "numeric",
                        n,
                        null_rate,
                        mean=float(values.mean()),
                        std=float(values.std(ddof=0)),
                        edges=edges,
                        shares=_shares(_numeric_bins(series, edges)),
                    )
                )
                continue
            labels = _categories(series)
            distinct = labels[labels != NULL].nunique()
            if distinct > policy.max_categories and policy.columns is None:
                skipped[column] = f"{distinct} distinct values (> max_categories)"
                continue
            shares = _shares(labels)
            profiles.append(ColumnProfile(column, "categorical", n, null_rate, shares=shares))
        return cls(runs, tuple(profiles), skipped)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_runs": list(self.reference_runs),
            "columns": [asdict(c) | {"edges": list(c.edges)} for c in self.columns],
            "skipped": self.skipped,
        }


# ------------------------------------------------------------------ scoring


@dataclass(frozen=True)
class ColumnDrift:
    column: str
    kind: str
    level: DriftLevel
    psi: float
    z: float | None
    null_rate_ref: float
    null_rate_now: float
    mean_ref: float | None = None
    mean_now: float | None = None
    std_ref: float | None = None
    top_ref: tuple[tuple[str, float], ...] = ()
    top_now: tuple[tuple[str, float], ...] = ()
    why: str = ""

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["level"] = self.level.value
        out["top_ref"] = [list(t) for t in self.top_ref]
        out["top_now"] = [list(t) for t in self.top_now]
        return out


def _reached(t: DriftThresholds, psi_value: float, z: float | None) -> list[str]:
    hits = []
    if t.psi is not None and psi_value >= t.psi:
        hits.append(f"PSI {psi_value:.3f} >= {t.psi}")
    if t.z is not None and z is not None and z >= t.z:
        hits.append(f"z {z:.2f} >= {t.z}")
    return hits


def _top(shares: dict[str, float], k: int = 5) -> tuple[tuple[str, float], ...]:
    ranked = sorted(shares.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
    return tuple((name, round(share, 4)) for name, share in ranked)


def score_column(profile: ColumnProfile, series: pd.Series, policy: DriftPolicy) -> ColumnDrift:
    n = len(series)
    null_now = float(series.isna().mean()) if n else 0.0
    if profile.kind == "numeric":
        shares = _shares(_numeric_bins(series, profile.edges))
        values = pd.to_numeric(series, errors="coerce").dropna().astype(float)
        mean_now = float(values.mean()) if len(values) else None
        z: float | None = None
        if mean_now is not None and profile.mean is not None:
            diff = abs(mean_now - profile.mean)
            se = (profile.std or 0.0) / math.sqrt(len(values))
            z = min(diff / se, Z_CAP) if se > 0 else (0.0 if diff < 1e-12 else Z_CAP)
    else:
        labels = _categories(series)
        known = set(profile.shares)
        labels = labels.where(labels.isin(known) | (labels == NULL), OTHER)
        shares = _shares(labels)
        mean_now, z = None, None
    value = psi(profile.shares, shares)
    fail = _reached(policy.fail, value, z)
    warn = _reached(policy.warn, value, z)
    level = DriftLevel.FAIL if fail else DriftLevel.WARN if warn else DriftLevel.OK
    return ColumnDrift(
        column=profile.column,
        kind=profile.kind,
        level=level,
        psi=round(value, 6),
        z=None if z is None else round(z, 4),
        null_rate_ref=round(profile.null_rate, 4),
        null_rate_now=round(null_now, 4),
        mean_ref=None if profile.mean is None else round(profile.mean, 6),
        mean_now=None if mean_now is None else round(mean_now, 6),
        std_ref=None if profile.std is None else round(profile.std, 6),
        top_ref=_top(profile.shares),
        top_now=_top(shares),
        why="; ".join(fail or warn),
    )


@dataclass(frozen=True)
class DriftReport:
    block: str
    run_id: str
    level: DriftLevel
    rows: int
    reference_runs: tuple[str, ...]
    columns: tuple[ColumnDrift, ...] = ()
    skipped: dict[str, str] = field(default_factory=dict)
    policy: dict[str, Any] = field(default_factory=dict)

    @property
    def drifted(self) -> list[ColumnDrift]:
        """Columns at WARN or FAIL, worst first."""
        hits = [c for c in self.columns if c.level in (DriftLevel.WARN, DriftLevel.FAIL)]
        return sorted(hits, key=lambda c: (-c.level.rank, -c.psi, c.column))

    @property
    def reason(self) -> str:
        if self.level is DriftLevel.NO_BASELINE:
            return f"no drift baseline yet ({len(self.reference_runs)} promoted snapshot(s))"
        if not self.drifted:
            return "no drift"
        parts = [f"{c.column} ({c.why})" for c in self.drifted[:3]]
        more = len(self.drifted) - 3
        return f"distribution drift: {', '.join(parts)}" + (f" and {more} more" if more > 0 else "")

    def to_dict(self) -> dict[str, Any]:
        return {
            "block": self.block,
            "run_id": self.run_id,
            "level": self.level.value,
            "reason": self.reason,
            "rows": self.rows,
            "reference_runs": list(self.reference_runs),
            "columns": [c.to_dict() for c in self.columns],
            "skipped": self.skipped,
            "policy": self.policy,
        }


def check(
    spec: BlockSpec,
    run_id: str,
    output: pd.DataFrame,
    reference: Sequence[tuple[str, pd.DataFrame]],
) -> DriftReport:
    """Score ``output`` of ``spec``'s block against its reference snapshots."""
    policy = spec.drift
    assert policy is not None
    policy_dict = _policy_dict(policy)
    reference = list(reference)[-policy.window :]
    runs = tuple(run for run, _ in reference)
    if len(reference) < policy.min_history:
        return DriftReport(
            spec.name, run_id, DriftLevel.NO_BASELINE, len(output), runs, policy=policy_dict
        )
    profile = DriftProfile.learn(reference, policy, spec.merge_key or ())
    output = output.drop(columns=[QUALITY_COL], errors="ignore")
    columns = []
    skipped = dict(profile.skipped)
    for column in profile.columns:
        if column.column not in output.columns:
            skipped[column.column] = "missing from this output"
            continue
        columns.append(score_column(column, output[column.column], policy))
    level = max((c.level for c in columns), key=lambda lv: lv.rank, default=DriftLevel.OK)
    return DriftReport(
        spec.name, run_id, level, len(output), runs, tuple(columns), skipped, policy_dict
    )


def _policy_dict(policy: DriftPolicy) -> dict[str, Any]:
    out = asdict(policy)
    out["columns"] = list(policy.columns) if policy.columns is not None else None
    out["exclude"] = list(policy.exclude)
    return out


# ------------------------------------------------------------------ storage


class DriftStore:
    """Drift reports as ``<root>/drift/<block>/<run_id>.json``."""

    def __init__(self, root: Path | str) -> None:
        self.dir = Path(root) / DRIFT_DIR

    def path(self, block: str, run_id: str) -> Path:
        return (
            self.dir
            / validate_name(block, "block name")
            / f"{validate_name(run_id, 'run_id')}.json"
        )

    def write(self, report: DriftReport) -> Path:
        path = self.path(report.block, report.run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(report.to_dict(), indent=2, sort_keys=True)
        path.write_text(text + "\n", encoding="utf-8", newline="\n")
        return path

    def get(self, block: str, run_id: str) -> dict[str, Any] | None:
        path = self.path(block, run_id)
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
