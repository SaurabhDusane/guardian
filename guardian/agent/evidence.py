"""Evidence bundle: everything the diagnosis agent may cite, with stable IDs.

``build_evidence(guardian, block, run_id)`` gathers, for one block run:

- the run's outcome;
- each failed validation rule with its row count;
- a capped sample of the quarantined rows;
- the output schema compared with the last good run's, and with the declared schema;
- the statistical drift check of the run (PSI / z-score per column), if the block has
  a drift policy;
- per-column stats for the run's good rows, its bad rows and the last good snapshot;
- whether the block's code changed since the last good run (from recorded code
  fingerprints), and the git history of its source file since its last promotion;
- where each input came from on this run (provenance) and its quality;
- the block's place in the DAG;
- recent events for the block and its upstreams.

Items are numbered E1, E2, ... in that order. The bundle is deterministic: the same
stored state always yields the same JSON. It depends only on stored records up to
the diagnosed run, never on the wall clock. It is also minimized before it can reach
an LLM: samples are capped, long text is cut, and values of the block's
``redact_columns`` are masked everywhere, including inside messages.
"""

from __future__ import annotations

import difflib
import json
import math
import re
import subprocess
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from guardian.core.code import CodeFingerprint
from guardian.core.dag import ancestors, dependents, roles_of
from guardian.core.events import ALWAYS_LOGGED, Event, EventKind
from guardian.core.guardian import ROLLBACK_RULE, Guardian
from guardian.core.models import GuardianError, QuarantineRecord, validate_name
from guardian.core.provenance import QUALITY_COL, BlockRun, snapshot_id
from guardian.core.quarantine import restore_dtypes
from guardian.core.validation import PanderaValidator

DIAGNOSES_DIR = "diagnoses"
EVIDENCE_FILE = "evidence.json"
DEFAULT_SAMPLE_SIZE = 20
MAX_EVENTS = 20
MAX_TOP_VALUES = 5
MAX_EXAMPLES = 3
MAX_TEXT = 300
MAX_TRACEBACK = 1500
MAX_DIFF_LINES = 80
MAX_COMMITS = 10
REDACTED = "<redacted>"
# Shortest redacted value that is also scrubbed from free text (shorter ones would
# mask unrelated words).
MIN_SCRUB_LENGTH = 3

# Bookkeeping events that say nothing about a failure, and diagnoses themselves (a
# bundle must not depend on earlier diagnoses of the same run).
_SKIPPED_EVENTS = frozenset(
    {
        EventKind.RUN_STARTED,
        EventKind.RUN_FINISHED,
        EventKind.BLOCK_STARTED,
        EventKind.BLOCK_FINISHED,
        EventKind.BLOCK_OUTCOME,
        EventKind.SHADOW,
        EventKind.DIAGNOSIS,
        EventKind.PROPOSAL,
    }
)
_NOTABLE_EVENTS = ALWAYS_LOGGED | {EventKind.STATUS_CHANGE, EventKind.REPLAY}


class EvidenceError(GuardianError):
    """The requested run cannot be diagnosed (e.g. the block never ran on it)."""


@dataclass(frozen=True)
class EvidenceItem:
    id: str
    kind: str
    title: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "kind": self.kind, "title": self.title, "data": self.data}


@dataclass(frozen=True)
class EvidenceBundle:
    pipeline: str
    block: str
    run_id: str
    items: tuple[EvidenceItem, ...]
    redact_columns: tuple[str, ...] = ()
    sample_size: int = DEFAULT_SAMPLE_SIZE
    _index: dict[str, EvidenceItem] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._index.update({item.id: item for item in self.items})

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(item.id for item in self.items)

    def item(self, item_id: str) -> EvidenceItem:
        return self._index[item_id]

    def of_kind(self, kind: str) -> list[EvidenceItem]:
        return [item for item in self.items if item.kind == kind]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "block": self.block,
            "run_id": self.run_id,
            "redact_columns": list(self.redact_columns),
            "sample_size": self.sample_size,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> EvidenceBundle:
        return cls(
            pipeline=raw["pipeline"],
            block=raw["block"],
            run_id=raw["run_id"],
            items=tuple(
                EvidenceItem(i["id"], i["kind"], i["title"], i["data"]) for i in raw["items"]
            ),
            redact_columns=tuple(raw.get("redact_columns", ())),
            sample_size=raw.get("sample_size", DEFAULT_SAMPLE_SIZE),
        )

    @classmethod
    def load(cls, path: Path | str) -> EvidenceBundle:
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def write(self, root: Path | str) -> Path:
        """Write ``<root>/diagnoses/<block>/<run_id>/evidence.json``; return its path."""
        path = diagnosis_dir(root, self.block, self.run_id) / EVIDENCE_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json() + "\n", encoding="utf-8", newline="\n")
        return path


def diagnosis_dir(root: Path | str, block: str, run_id: str) -> Path:
    return (
        Path(root)
        / DIAGNOSES_DIR
        / validate_name(block, "block name")
        / validate_name(run_id, "run_id")
    )


# ------------------------------------------------------------------ run selection


def block_run(g: Guardian, block: str, run_id: str) -> BlockRun:
    g.spec.block(block)  # KeyError for an unknown block
    runs = g.provenance.runs(block)
    for run in runs:
        if run.run_id == run_id:
            return run
    known = ", ".join(r.run_id for r in runs[-10:]) or "none"
    raise EvidenceError(f"block {block!r} has no recorded run {run_id!r} (recent runs: {known})")


def default_run(g: Guardian, block: str) -> str:
    """The run to diagnose when none is given: the block's latest ROLLBACK, else its
    latest run."""
    g.spec.block(block)
    runs = g.provenance.runs(block)
    if not runs:
        raise EvidenceError(f"block {block!r} has no recorded runs yet")
    rollbacks = [r for r in runs if r.outcome == "ROLLBACK"]
    return (rollbacks or runs)[-1].run_id


def baseline_run(g: Guardian, block: str, run_id: str) -> BlockRun | None:
    """The last PASS run of ``block`` before ``run_id`` whose snapshot exists."""
    runs = g.provenance.runs(block)
    before = []
    for run in runs:
        if run.run_id == run_id:
            break
        before.append(run)
    for run in reversed(before):
        if run.outcome == "PASS" and g.snapshots.exists(block, run.run_id):
            return run
    return None


# ------------------------------------------------------------------ redaction


class Redactor:
    """Masks the values of ``columns`` in rows, stats and free text."""

    def __init__(self, columns: Iterable[str], frames: Iterable[pd.DataFrame | None]) -> None:
        self.columns = tuple(columns)
        values: set[str] = set()
        for frame in frames:
            if frame is None:
                continue
            for column in self.columns:
                if column in frame.columns:
                    for v in frame[column].dropna().unique():
                        text = str(_json_value(v))
                        if len(text) >= MIN_SCRUB_LENGTH:
                            values.add(text)
        # Longest first, so a value containing another is masked whole.
        self._values = sorted(values, key=lambda v: (-len(v), v))
        self._reason_patterns = [
            re.compile(rf"(column '{re.escape(c)}' failed [^;]*?)\(value=.*?\)(?=;|$)")
            for c in self.columns
        ]

    def is_redacted(self, column: str) -> bool:
        return column in self.columns

    def text(self, text: str) -> str:
        for pattern in self._reason_patterns:
            text = pattern.sub(rf"\g<1>(value={REDACTED})", text)
        for value in self._values:
            if value in text:
                text = text.replace(value, REDACTED)
        return text

    def scrub(self, obj: Any) -> Any:
        """Mask redacted values anywhere in a JSON-like structure."""
        if not self.columns:
            return obj
        if isinstance(obj, str):
            return self.text(obj)
        if isinstance(obj, list):
            return [self.scrub(v) for v in obj]
        if isinstance(obj, dict):  # keys too: event data can key counts by rule text
            return {self.scrub(k): self.scrub(v) for k, v in obj.items()}
        return obj


# ------------------------------------------------------------------ helpers


def _json_value(value: Any) -> Any:
    """A JSON-safe, deterministic version of a scalar."""
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else round(value, 6)
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, datetime | date):
        return value.isoformat()
    if value is pd.NaT or (not isinstance(value, str | bool | int) and pd.isna(value)):
        return None
    if isinstance(value, bool | int | str):
        return value
    return str(value)


def _cut(text: str | None, limit: int = MAX_TEXT) -> str | None:
    if text is None:
        return None
    text = str(text)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _jsonable(obj: Any, limit: int = MAX_TEXT) -> Any:
    if isinstance(obj, Mapping):
        return {str(k): _jsonable(v, limit) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_jsonable(v, limit) for v in obj]
    value = _json_value(obj)
    return _cut(value, limit) if isinstance(value, str) else value


def _frame_from_payloads(
    records: Sequence[QuarantineRecord], dtypes: Mapping[str, Any] | None
) -> pd.DataFrame:
    rows = [r.payload() for r in records]
    columns = list(dict.fromkeys(c for row in rows for c in row))
    df = pd.DataFrame(rows, columns=columns)
    return restore_dtypes(df, dtypes) if dtypes else df


def _kind(dtype: Any) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "bool"
    if pd.api.types.is_numeric_dtype(dtype):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
        return "text"
    return str(dtype)


def column_profile(series: pd.Series | None, redacted: bool) -> dict[str, Any] | None:
    """Count, nulls, distinct values, plus min/max/mean (numbers) or top values (text)."""
    if series is None:
        return None
    n = len(series)
    nulls = int(series.isna().sum())
    non_null = series.dropna()
    try:
        distinct = int(non_null.nunique())
    except TypeError:  # unhashable values
        distinct = int(non_null.astype(str).nunique())
    out: dict[str, Any] = {
        "kind": _kind(series.dtype),
        "rows": n,
        "nulls": nulls,
        "null_rate": round(nulls / n, 4) if n else 0.0,
        "distinct": distinct,
    }
    if redacted:
        out["values"] = REDACTED
        return out
    if out["kind"] == "numeric" and len(non_null):
        out["min"] = _json_value(non_null.min())
        out["max"] = _json_value(non_null.max())
        out["mean"] = _json_value(float(non_null.mean()))
        out["negative"] = int((non_null < 0).sum())
    elif len(non_null):
        counts = Counter(str(_json_value(v)) for v in non_null)
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:MAX_TOP_VALUES]
        out["top_values"] = [[_cut(v, 60), c] for v, c in top]
    return out


def _frame_columns(*frames: pd.DataFrame | None) -> list[str]:
    columns: list[str] = []
    for frame in frames:
        if frame is not None:
            columns.extend(c for c in frame.columns if c not in columns)
    return columns


def _drop_internal(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None:
        return None
    return df.drop(columns=[c for c in (QUALITY_COL,) if c in df.columns])


def _code_summary(fp: CodeFingerprint | None) -> dict[str, Any] | None:
    if fp is None:
        return None
    return {"fingerprint": fp.sha, "function": fp.qualname, "file": _display_path(fp.file)}


def _display_path(file: str | None) -> str | None:
    if file is None:
        return None
    path = Path(file)
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.name


# ------------------------------------------------------------------ git


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )
    if done.returncode != 0:
        raise RuntimeError(done.stderr.strip() or f"git {args[0]} failed")
    return done.stdout


def git_history(file: str | None, since: datetime | None) -> dict[str, Any]:
    """Commits touching ``file`` since ``since`` and its diff since then (including
    uncommitted changes), capped. Never raises: failures are reported in the result."""
    if file is None:
        return {"available": False, "reason": "the block's source file is unknown"}
    path = Path(file)
    try:
        top = Path(_git(path.parent, "rev-parse", "--show-toplevel").strip())
        rel = path.resolve().relative_to(top.resolve()).as_posix()
        log_args = ["log", f"-n{MAX_COMMITS}", "--format=%h %ad %s", "--date=short"]
        if since is not None:
            log_args.append(f"--since={since.isoformat()}")
        commits = _git(top, *log_args, "--", rel).splitlines()
        base = "HEAD"
        if since is not None:
            before = _git(top, "rev-list", "-1", f"--before={since.isoformat()}", "HEAD").strip()
            base = before or "HEAD"
        diff = _git(top, "diff", base, "--", rel).splitlines()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        return {"available": False, "reason": _cut(f"git unavailable: {exc}")}
    return {
        "available": True,
        "file": rel,
        "since": since.isoformat() if since else None,
        "commits": commits,
        "diff": [_cut(line, 200) for line in diff[:MAX_DIFF_LINES]],
        "diff_truncated": len(diff) > MAX_DIFF_LINES,
    }


def _last_promotion(g: Guardian, block: str, baseline: BlockRun | None) -> tuple[Any, str]:
    completed = [p.completed_at for p in g.versions.history(block) if p.completed_at]
    if completed:
        return max(completed), "last promotion"
    if baseline is not None:
        return baseline.ts, "last good run"
    return None, "no promotion or good run recorded"


# ------------------------------------------------------------------ builder


class _Items:
    def __init__(self) -> None:
        self.items: list[EvidenceItem] = []

    def add(self, kind: str, title: str, data: dict[str, Any]) -> None:
        self.items.append(EvidenceItem(f"E{len(self.items) + 1}", kind, title, data))


def build_evidence(
    g: Guardian,
    block: str,
    run_id: str | None = None,
    *,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    git: bool = True,
    max_events: int = MAX_EVENTS,
) -> EvidenceBundle:
    """Gather the evidence for one run of ``block`` (default: its latest ROLLBACK)."""
    if sample_size < 0:
        raise ValueError("sample_size must be >= 0")
    spec = g.spec.block(block)
    run_id = run_id or default_run(g, block)
    run = block_run(g, block, run_id)
    baseline = baseline_run(g, block, run_id)

    last_good = _drop_internal(g.snapshots.read(block, baseline.run_id)) if baseline else None
    dtypes = dict(last_good.dtypes) if last_good is not None else None
    records = sorted(g.quarantine.list(block=block, run_id=run_id), key=lambda r: r.id)
    bad_records = [r for r in records if r.rule_name != ROLLBACK_RULE]
    held_good = [r for r in records if r.rule_name == ROLLBACK_RULE]
    if run.outcome == "PASS" and g.snapshots.exists(block, run_id):
        good = _drop_internal(g.snapshots.read(block, run_id))
    else:
        good = _frame_from_payloads(held_good, dtypes)
    bad = _frame_from_payloads(bad_records, dtypes)
    has_output = bool(records) or run.outcome == "PASS"
    redactor = Redactor(spec.redact_columns, [good, bad, last_good])
    items = _Items()

    # Run outcome.
    total = len(good) + len(bad)
    items.add(
        "run_outcome",
        f"{block} on run {run_id}: {run.outcome}",
        {
            "block": block,
            "run_id": run_id,
            "outcome": run.outcome,
            "reason": _cut(redactor.text(run.reason)) if run.reason else None,
            "status_after_run": run.status,
            "version": run.version,
            "output_rows": total if has_output else None,
            "good_rows": len(good) if has_output else None,
            "bad_rows": len(bad) if has_output else None,
            "bad_fraction": round(len(bad) / total, 4) if total else None,
            "quarantine_threshold": spec.quarantine_threshold,
            "schema_level_failure": any(r.rule_name == "schema" for r in bad_records),
            "last_good_run": baseline.run_id if baseline else None,
        },
    )

    # Failed validation rules, most frequent first.
    per_rule: dict[str, list[QuarantineRecord]] = {}
    for record in bad_records:
        for rule in dict.fromkeys(record.rule_name.split(";")):
            per_rule.setdefault(rule, []).append(record)
    for rule, recs in sorted(per_rule.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        examples = list(dict.fromkeys(redactor.text(r.reason) for r in recs))[:MAX_EXAMPLES]
        items.add(
            "validation_rule",
            f"rule {rule} failed on {len(recs)} row(s)",
            {
                "rule": rule,
                "rows": len(recs),
                "fraction_of_output": round(len(recs) / total, 4) if total else None,
                "example_reasons": [_cut(e) for e in examples],
            },
        )

    # Quarantine sample.
    sample = []
    for record in bad_records[:sample_size]:
        values = {
            k: REDACTED if redactor.is_redacted(k) and v is not None else _jsonable(v, 60)
            for k, v in record.payload().items()
        }
        sample.append({"rule": record.rule_name, "reason": _cut(record.reason), "row": values})
    items.add(
        "quarantine_sample",
        f"sample of {len(sample)} of {len(bad_records)} quarantined bad row(s)",
        {
            "bad_rows": len(bad_records),
            "held_good_rows": len(held_good),
            "sample_size": sample_size,
            "rows": sample,
        },
    )

    # Schema diff.
    output_columns = _frame_columns(good, bad) if has_output else []
    if last_good is not None:  # payload keys are sorted: list known columns in their order
        known = [c for c in last_good.columns if c in output_columns]
        output_columns = known + [c for c in output_columns if c not in known]
    schema: dict[str, Any] = {
        "last_good_run": baseline.run_id if baseline else None,
        "output_columns": output_columns if has_output else None,
        "last_good_columns": list(last_good.columns) if last_good is not None else None,
    }
    if last_good is not None and has_output:
        schema["missing_vs_last_good"] = [c for c in last_good.columns if c not in output_columns]
        schema["added_vs_last_good"] = [c for c in output_columns if c not in last_good.columns]
        observed = good if len(good) else bad
        schema["kind_changes"] = [
            {
                "column": c,
                "last_good": _kind(last_good[c].dtype),
                "now": _kind(observed[c].dtype),
            }
            for c in output_columns
            if c in last_good.columns
            and c in observed.columns
            and observed[c].notna().any()
            and _kind(observed[c].dtype) != _kind(last_good[c].dtype)
        ]
    validator = g.validator_for(block)
    if isinstance(validator, PanderaValidator):
        declared = list(validator.schema.columns)
        required = [c for c, col in validator.schema.columns.items() if col.required]
        schema["declared_columns"] = declared
        if has_output:
            schema["declared_required_missing"] = [c for c in required if c not in output_columns]
    if not has_output:
        schema["note"] = "the block produced no output on this run"
    items.add("schema_diff", "output schema vs last good run and declared schema", schema)

    # Statistical drift, when the block has a drift policy and this run was checked.
    drift = g.drift.get(block, run_id)
    if drift is not None:
        columns = sorted(
            drift["columns"],
            key=lambda c: ({"FAIL": 0, "WARN": 1}.get(c["level"], 2), -c["psi"], c["column"]),
        )
        for c in columns:
            if redactor.is_redacted(c["column"]):
                for key in ("top_ref", "top_now", "mean_ref", "mean_now", "std_ref"):
                    c[key] = REDACTED if c.get(key) not in (None, []) else c.get(key)
        items.add(
            "drift",
            f"distribution drift vs {len(drift['reference_runs'])} promoted snapshot(s): "
            f"{drift['level']}",
            {
                "level": drift["level"],
                "reason": drift["reason"],
                "reference_runs": drift["reference_runs"],
                "rows": drift["rows"],
                "thresholds": {"warn": drift["policy"]["warn"], "fail": drift["policy"]["fail"]},
                "columns": columns,
                "not_tracked": drift["skipped"],
            },
        )

    # Per-column stats.
    for column in _frame_columns(last_good, good, bad):
        redacted = redactor.is_redacted(column)

        def profile(df: pd.DataFrame | None, column: str = column, redacted: bool = redacted):
            if df is None:
                return None
            if column not in df.columns:
                return {"present": False}
            return column_profile(df[column], redacted)

        items.add(
            "column_stats",
            f"column {column}: good rows vs bad rows vs last good snapshot",
            {
                "column": column,
                "redacted": redacted,
                "good_rows": profile(good) if has_output else None,
                "bad_rows": profile(bad) if has_output else None,
                "last_good": profile(last_good),
            },
        )

    # Code change since the last good run.
    now_code = g.code.get(run.code) if run.code else None
    base_code = g.code.get(baseline.code) if baseline and baseline.code else None
    code: dict[str, Any] = {
        "last_good_run": baseline.run_id if baseline else None,
        "this_run": _code_summary(now_code),
        "last_good": _code_summary(base_code),
        "changed": (run.code != baseline.code) if baseline and run.code and baseline.code else None,
    }
    if code["changed"] and now_code and base_code:
        diff = list(
            difflib.unified_diff(
                (base_code.source or "").splitlines(),
                (now_code.source or "").splitlines(),
                fromfile=f"last good ({base_code.qualname})",
                tofile=f"this run ({now_code.qualname})",
                lineterm="",
            )
        )
        code["diff"] = [_cut(line, 200) for line in diff[:MAX_DIFF_LINES]]
        code["diff_truncated"] = len(diff) > MAX_DIFF_LINES
    items.add("code_change", "the block's code on this run vs its last good run", code)

    # Git history of the source file.
    if git:
        since, since_what = _last_promotion(g, block, baseline)
        history = git_history(now_code.file if now_code else None, since)
        history["since_what"] = since_what
    else:
        history = {"available": False, "reason": "git lookup disabled"}
    items.add("git_history", "git history of the block's source since its last promotion", history)

    # Input provenance.
    runs_on = {r.block: r for r in g.provenance.runs() if r.run_id == run_id}
    for entry in _inputs(g, block, run_id):
        upstream_run = runs_on.get(entry["upstream"])
        entry["upstream_outcome_on_run"] = upstream_run.outcome if upstream_run else None
        items.add("input", f"input {entry['upstream']}: {entry['quality']}", entry)

    # DAG context.
    protecting = [
        {"dependent": d, "fallback_source": e.source, "adapter": e.adapter}
        for d in dependents(g.spec, block)
        if (e := g.spec.block(d).fallback_for(block)) is not None
    ]
    items.add(
        "dag",
        f"{block}'s place in the DAG",
        {
            "roles": roles_of(g.spec, block),
            "inputs": list(spec.inputs),
            "dependents": dependents(g.spec, block),
            "upstream_blocks": ancestors(g.spec, block),
            "fallbacks_replacing_this_block": protecting,
            "own_fallback_edges": [
                {"replaces": e.replaces, "source": e.source, "adapter": e.adapter}
                for e in spec.fallbacks
            ],
        },
    )

    # Recent events.
    for event in _recent_events(g, block, run_id, max_events):
        data = dict(event.data)
        if "traceback" in data:
            data["traceback"] = "..." + str(data["traceback"])[-MAX_TRACEBACK:]
        items.add(
            "event",
            f"{event.kind.value} event for {event.block} on {event.run_id}",
            {
                "event": event.kind.value,
                "block": event.block,
                "run_id": event.run_id,
                "ts": event.ts.isoformat(),
                "data": _jsonable(data, MAX_TRACEBACK + 3),
            },
        )

    scrubbed = tuple(
        EvidenceItem(i.id, i.kind, redactor.text(i.title), _scrub_item(redactor, i))
        for i in items.items
    )
    return EvidenceBundle(
        pipeline=g.spec.name,
        block=block,
        run_id=run_id,
        items=scrubbed,
        redact_columns=spec.redact_columns,
        sample_size=sample_size,
    )


def _scrub_item(redactor: Redactor, item: EvidenceItem) -> dict[str, Any]:
    """Mask redacted values in an item's free text.

    Structured values (column stats, sample rows) were already masked column by column,
    so a non-redacted column keeps its own values even if one also occurs in a redacted
    column; only text that may embed values (messages, rule names, events) is scrubbed.
    """
    if item.kind == "column_stats":
        return item.data
    if item.kind == "quarantine_sample":
        rows = [
            {**r, "rule": redactor.text(r["rule"]), "reason": redactor.scrub(r["reason"])}
            for r in item.data["rows"]
        ]
        return {**item.data, "rows": rows}
    return redactor.scrub(item.data)


def _inputs(g: Guardian, block: str, run_id: str) -> list[dict[str, Any]]:
    """Where each input came from on ``run_id``: provenance, else resolution events."""
    record = g.provenance.get(block, run_id)
    if record is not None and record.inputs:
        return [
            {
                "upstream": i.upstream,
                "read": i.source_id,
                "how": i.how,
                "quality": i.quality.value,
                "upstream_status": i.upstream_status,
            }
            for i in record.inputs
        ]
    found: dict[str, dict[str, Any]] = {}
    for event in g.events.query(block=block, run_id=run_id):
        if event.kind in (EventKind.RESOLVE, EventKind.REROUTE):
            d = event.data
            source = snapshot_id(d.get("source", "?"), d.get("source_run_id", "?"))
            how = source
            if event.kind is EventKind.REROUTE:
                how = f"fallback to {source} via {d.get('adapter')}"
            elif d.get("stale"):
                how = f"stale {source}"
            found[d.get("upstream", "?")] = {
                "upstream": d.get("upstream"),
                "read": source,
                "how": how,
                "quality": d.get("quality"),
                "upstream_status": d.get("upstream_status"),
            }
    spec = g.spec.block(block)
    return [
        found.get(u, {"upstream": u, "read": None, "how": "not recorded", "quality": None})
        for u in spec.inputs
    ]


def _recent_events(g: Guardian, block: str, run_id: str, limit: int) -> list[Event]:
    """Up to ``limit`` events for ``block`` and its upstreams, up to the diagnosed run.

    The block's own events on the run come first, then notable events (errors,
    rollbacks, quarantines, reroutes, status changes, replays), then the rest, each
    group most recent first; the chosen events are returned in time order.
    """
    scope = {block, *ancestors(g.spec, block)}
    events = [
        e
        for e in g.events.query()
        if e.block in scope and e.kind not in _SKIPPED_EVENTS and not e.data.get("agent")
    ]
    own = [e for e in events if e.block == block and e.run_id == run_id]
    if own:
        cutoff = max((e.ts, e.event_id) for e in own)
        events = [e for e in events if (e.ts, e.event_id) <= cutoff]
    rest = [e for e in events if not (e.block == block and e.run_id == run_id)]
    notable = [e for e in rest if e.kind in _NOTABLE_EVENTS]
    other = [e for e in rest if e.kind not in _NOTABLE_EVENTS]

    def recent(group: list[Event]) -> list[Event]:
        return sorted(group, key=lambda e: (e.ts, e.event_id), reverse=True)

    chosen: list[Event] = []
    for group in (recent(own), recent(notable), recent(other)):
        chosen.extend(group[: max(0, limit - len(chosen))])
    return sorted(chosen, key=lambda e: (e.ts, e.event_id))
