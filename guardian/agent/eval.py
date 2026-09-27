"""Evaluate the diagnosis agent on labeled faults, for every block of a spec, and its
fixes on injected code bugs.

``plan_cases`` picks the (block, fault type) pairs to evaluate, filtered by block, DAG
role and fault type and, with ``max_cases``, sampled with a seeded RNG, so the same
arguments always give the same cases. ``generate_cases`` runs the pipeline once
cleanly, then, for every planned pair, copies that state, injects the fault into the
block, re-runs the block and builds its evidence bundle. Each case is labeled with the
root cause its fault simulates:

    schema_drift -> schema_change
    corrupt_rows -> upstream_data_drift
    null_burst   -> upstream_data_drift
    code_bug     -> code_bug

Which column a fault touches comes from the block's own clean output and declared
schema, never from block names, so any spec works. Bundles leave out git history and
event timestamps: the same case always gives the same prompt, which is what lets
``MeteredClient`` serve re-runs from its cache.

``run_eval`` diagnoses every case (``repeats`` times) and reports accuracy (overall,
by DAG role, by fault type), a confusion matrix, calibration, the rate of answers
rejected for citing nonexistent evidence, latency, tokens and cost, and, with
repeats, the agreement between them.

``run_fix_eval`` measures fixes: for every code_bug case, the agent diagnoses and
proposes a fix (dry run, in a git worktree; GitHub is never touched). A proposal
succeeds when its new version then passes shadow promotion in that case's state: it
is shadowed on a run where the live version is still buggy and promoted with an
approval, as a human reviewer would do after merging. That approval is simulated by
the eval harness, never by the agent. Every repeat works on its own copy of the case.

Every number is from synthetic faults injected into the demo data; see SYNTHETIC_NOTE.
"""

from __future__ import annotations

import dataclasses
import json
import random
import shutil
import statistics
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from guardian.agent.diagnose import (
    ACCEPTED,
    BAD_CITATION,
    ROOT_CAUSES,
    Claim,
    Diagnosis,
    LLMClient,
    LLMError,
    LLMRequest,
    diagnose_bundle,
)
from guardian.agent.evidence import DEFAULT_SAMPLE_SIZE, EvidenceBundle, build_evidence
from guardian.agent.metering import (
    CallRecord,
    MeteredClient,
    Prices,
    ResponseCache,
    request_tokens,
)
from guardian.core.dag import ROLES, roles_of
from guardian.core.guardian import Guardian
from guardian.core.models import PipelineSpec
from guardian.core.provenance import QUALITY_COL
from guardian.core.snapshots import LocalParquetSnapshotStore
from guardian.core.validation import PanderaValidator
from guardian.demo.faults import (
    Fault,
    apply_faults,
    code_bug,
    corrupt_rows,
    null_burst,
    schema_drift,
)
from guardian.runner.executor import Executor
from guardian.runner.spec_loader import topological_order

FAULT_TYPES = ("schema_drift", "corrupt_rows", "null_burst", "code_bug")
EXPECTED: dict[str, str] = {
    "schema_drift": "schema_change",
    "corrupt_rows": "upstream_data_drift",
    "null_burst": "upstream_data_drift",
    "code_bug": "code_bug",
}
FAULT_FRACTION = 0.5
BASELINE_RUN = "eval-baseline"
FAULT_RUN = "eval-fault"
# Upper edges of the calibration buckets (the last one includes 1.0).
CONFIDENCE_BUCKETS = (0.5, 0.7, 0.9, 1.0)
SYNTHETIC_NOTE = (
    "All numbers come from synthetic faults injected into the demo pipeline's generated "
    "data (one fault type per case, labeled by construction), not from real incidents."
)


@dataclass(frozen=True)
class EvalCase:
    case_id: str  # "<block>:<fault type>"; also the LLM request key
    block: str
    fault: str
    label: str
    roles: tuple[str, ...]
    fault_detail: str
    outcome: str  # the block's outcome on the faulted run
    bundle: EvidenceBundle = field(repr=False)
    root: Path | None = field(default=None, repr=False, compare=False)  # the case's state
    injected: Fault | None = field(default=None, repr=False, compare=False)


def _clean_output(root: Path, block: str) -> pd.DataFrame:
    df = LocalParquetSnapshotStore(root).read(block, BASELINE_RUN)
    return df.drop(columns=[c for c in (QUALITY_COL,) if c in df.columns])


def _declared(g: Guardian, block: str) -> Mapping[str, Any]:
    validator = g.validator_for(block)
    return validator.schema.columns if isinstance(validator, PanderaValidator) else {}


def make_fault(
    fault_type: str, clean: pd.DataFrame, declared: Mapping[str, Any], seed: int
) -> Fault:
    """The fault of ``fault_type`` for a block with this clean output and declared schema.

    schema_drift renames the first required declared column (else the first column);
    null_burst nulls the first non-nullable declared column (else the first column).
    """
    columns = list(clean.columns)
    if not columns:
        raise ValueError("the block's clean output has no columns")
    required = [c for c, col in declared.items() if col.required and c in columns]
    not_null = [c for c, col in declared.items() if not col.nullable and c in columns]
    if fault_type == "schema_drift":
        column = (required or columns)[0]
        return schema_drift(rename={column: f"{column}_v2"})
    if fault_type == "corrupt_rows":
        return corrupt_rows(FAULT_FRACTION, seed=seed, unique=True)
    if fault_type == "null_burst":
        return null_burst((not_null or columns)[0], FAULT_FRACTION, seed=seed)
    if fault_type == "code_bug":
        return code_bug(FAULT_FRACTION, seed=seed)
    raise ValueError(f"unknown fault type {fault_type!r}; expected one of {list(FAULT_TYPES)}")


def plan_cases(
    spec: PipelineSpec,
    *,
    blocks: Sequence[str] | None = None,
    roles: Sequence[str] | None = None,
    faults: Sequence[str] = FAULT_TYPES,
    max_cases: int | None = None,
    seed: int = 0,
) -> list[tuple[str, str]]:
    """The (block, fault type) pairs to evaluate, in topological x FAULT_TYPES order.

    ``blocks`` and ``roles`` narrow the blocks (a block qualifies with any of the
    roles); ``max_cases`` keeps a sample of that many pairs chosen with ``seed``.
    """
    unknown = sorted(set(faults) - set(FAULT_TYPES))
    if unknown:
        raise ValueError(f"unknown fault type(s) {unknown}; expected {list(FAULT_TYPES)}")
    bad_roles = sorted(set(roles or ()) - set(ROLES))
    if bad_roles:
        raise ValueError(f"unknown role(s) {bad_roles}; expected {list(ROLES)}")
    for block in blocks or ():
        spec.block(block)  # KeyError for an unknown block
    if max_cases is not None and max_cases < 1:
        raise ValueError("max_cases must be at least 1")
    selected = [
        b
        for b in topological_order(spec)
        if (not blocks or b in blocks) and (not roles or set(roles) & set(roles_of(spec, b)))
    ]
    pairs = [(b, f) for b in selected for f in FAULT_TYPES if f in faults]
    if max_cases is not None and max_cases < len(pairs):
        keep = set(random.Random(seed).sample(range(len(pairs)), max_cases))
        pairs = [p for i, p in enumerate(pairs) if i in keep]
    return pairs


def generate_cases(
    spec: PipelineSpec,
    workdir: Path | str,
    *,
    blocks: Sequence[str] | None = None,
    faults: Sequence[str] = FAULT_TYPES,
    pairs: Sequence[tuple[str, str]] | None = None,
    registry: Mapping[str, Any] | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    seed: int = 0,
) -> list[EvalCase]:
    """One labeled case per planned (block, fault type) pair, each in its own copy of
    the state. Without ``pairs``: every block (or ``blocks``) x every fault type."""
    if pairs is None:
        pairs = plan_cases(spec, blocks=blocks, faults=faults)
    else:
        for block, fault in pairs:
            spec.block(block)
            if fault not in FAULT_TYPES:
                raise ValueError(f"unknown fault type {fault!r}; expected {list(FAULT_TYPES)}")
    spec = eval_spec(spec)
    workdir = Path(workdir)
    base_root = workdir / "baseline"
    if base_root.exists():
        shutil.rmtree(base_root)
    with Guardian(spec, base_root, registry=registry) as g:
        Executor(g).run(BASELINE_RUN)
        declared = {b: _declared(g, b) for b in spec.block_names}

    cases = []
    for block, fault_type in pairs:
        clean = _clean_output(base_root, block)
        case_root = workdir / "cases" / f"{block}__{fault_type}"
        if case_root.exists():
            shutil.rmtree(case_root)
        shutil.copytree(base_root, case_root)
        fault = make_fault(fault_type, clean, declared[block], seed)
        faulted, fault_registry = apply_faults(spec, {block: [fault]})
        with Guardian(faulted, case_root, registry={**(registry or {}), **fault_registry}) as g:
            report = Executor(g).run(FAULT_RUN, only=[block])
            bundle = build_evidence(
                g, block, FAULT_RUN, sample_size=sample_size, git=False, timestamps=False
            )
        bundle.write(case_root)
        cases.append(
            EvalCase(
                case_id=f"{block}:{fault_type}",
                block=block,
                fault=fault_type,
                label=EXPECTED[fault_type],
                roles=tuple(roles_of(spec, block)),
                fault_detail=fault.name,
                outcome=report.get(block).outcome.value,
                bundle=bundle,
                root=case_root,
                injected=fault,
            )
        )
    return cases


def eval_spec(spec: PipelineSpec) -> PipelineSpec:
    """The spec cases run with: the eval diagnoses cases itself, so no auto-diagnosis."""
    return dataclasses.replace(
        spec, blocks=tuple(dataclasses.replace(b, auto_diagnose=False) for b in spec.blocks)
    )


# ------------------------------------------------------------------ shared figures


def _calls_since(client: LLMClient, start: int) -> tuple[CallRecord, ...]:
    calls = getattr(client, "calls", None)
    if not isinstance(calls, list) or not all(isinstance(c, CallRecord) for c in calls[start:]):
        return ()
    return tuple(calls[start:])


def _n_calls(client: LLMClient) -> int:
    calls = getattr(client, "calls", None)
    return len(calls) if isinstance(calls, list) else 0


def _set_sample(client: LLMClient, sample: int) -> None:
    if isinstance(client, MeteredClient):
        client.sample = sample


def usage_summary(calls: Sequence[CallRecord], prices: Prices | None) -> dict[str, Any]:
    """Tokens and cost of ``calls``: all of them, and only those paid for in this run."""
    prices = prices or Prices()
    billed = [c for c in calls if not c.cached]
    tokens_in = sum(c.input_tokens for c in calls)
    tokens_out = sum(c.output_tokens for c in calls)
    billed_in = sum(c.input_tokens for c in billed)
    billed_out = sum(c.output_tokens for c in billed)
    return {
        "calls": len(calls),
        "cached_calls": len(calls) - len(billed),
        "input_tokens": tokens_in,
        "output_tokens": tokens_out,
        "billed_input_tokens": billed_in,
        "billed_output_tokens": billed_out,
        "tokens_estimated": any(c.estimated for c in calls),
        "prices": prices.to_dict(),
        "cost_usd": _round(prices.cost(tokens_in, tokens_out)),
        "billed_cost_usd": _round(prices.cost(billed_in, billed_out)),
    }


def _round(x: float | None, digits: int = 4) -> float | None:
    return None if x is None else round(x, digits)


def _median(values: Sequence[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def _agreement(outcomes: Mapping[str, Sequence[Any]]) -> dict[str, Any] | None:
    """How often repeats of the same case agree (None with a single repeat)."""
    if not outcomes or max(len(v) for v in outcomes.values()) < 2:
        return None
    unanimous = sum(len(set(v)) == 1 for v in outcomes.values())
    pairs = agree = 0
    for values in outcomes.values():
        for i in range(len(values)):
            for j in range(i + 1, len(values)):
                pairs += 1
                agree += values[i] == values[j]
    return {
        "cases": len(outcomes),
        "unanimous": unanimous,
        "unanimous_rate": round(unanimous / len(outcomes), 4),
        "pairwise_agreement": round(agree / pairs, 4) if pairs else None,
    }


def git_commit(where: Path | str | None = None) -> tuple[str | None, bool | None]:
    """HEAD of the repository at ``where`` (default: the working directory), and whether
    tracked files have uncommitted changes."""
    cwd = str(where) if where else None

    def git(*args: str) -> str | None:
        try:
            done = subprocess.run(
                ["git", *args], cwd=cwd, capture_output=True, text=True, check=True, timeout=30
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return done.stdout.strip()

    commit = git("rev-parse", "HEAD")
    status = git("status", "--porcelain", "--untracked-files=no")
    return commit, (bool(status) if status is not None else None)


def run_meta(
    *,
    model: str,
    real: bool,
    cases: int,
    repeats: int,
    seed: int,
    filters: Mapping[str, Any],
    repo: Path | str | None = None,
) -> dict[str, Any]:
    commit, dirty = git_commit(repo)
    return {
        "model": model,
        "real_model": real,
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_commit": commit,
        "git_dirty": dirty,
        "cases": cases,
        "repeats": repeats,
        "seed": seed,
        "filters": dict(filters),
        "note": SYNTHETIC_NOTE,
    }


# ------------------------------------------------------------------ diagnosis


@dataclass(frozen=True)
class CaseResult:
    case: EvalCase
    diagnosis: Diagnosis
    repeat: int = 0
    calls: tuple[CallRecord, ...] = ()

    @property
    def predicted(self) -> str:
        return self.diagnosis.root_cause

    @property
    def correct(self) -> bool:
        return self.predicted == self.case.label

    @property
    def citations_valid(self) -> bool | None:
        """True: every citation was checked and exists. False: rejected for citing
        evidence that is not in the bundle. None: no answer got that far."""
        if self.diagnosis.status == ACCEPTED:
            return True
        if self.diagnosis.rejection == BAD_CITATION:
            return False
        return None

    @property
    def latency_s(self) -> float | None:
        return round(sum(c.latency_s for c in self.calls), 4) if self.calls else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "case": self.case.case_id,
            "block": self.case.block,
            "fault": self.case.fault,
            "roles": list(self.case.roles),
            "repeat": self.repeat,
            "label": self.case.label,
            "predicted": self.predicted,
            "correct": self.correct,
            "confidence": self.diagnosis.confidence,
            "status": self.diagnosis.status,
            "rejection": self.diagnosis.rejection,
            "citations_valid": self.citations_valid,
            "attempts": self.diagnosis.attempts,
            "outcome": self.case.outcome,
            "latency_s": self.latency_s,
            "input_tokens": sum(c.input_tokens for c in self.calls),
            "output_tokens": sum(c.output_tokens for c in self.calls),
            "cached": bool(self.calls) and all(c.cached for c in self.calls),
        }


def _rate(results: Sequence[CaseResult]) -> tuple[int, int]:
    return sum(r.correct for r in results), len(results)


@dataclass
class EvalReport:
    model: str
    results: list[CaseResult]
    repeats: int = 1
    prices: Prices | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def cases(self) -> int:
        return len({r.case.case_id for r in self.results})

    @property
    def accuracy(self) -> float:
        correct, total = _rate(self.results)
        return correct / total if total else 0.0

    def by_fault(self) -> dict[str, tuple[int, int]]:
        faults = dict.fromkeys(r.case.fault for r in self.results)
        return {f: _rate([r for r in self.results if r.case.fault == f]) for f in faults}

    def by_role(self) -> dict[str, tuple[int, int]]:
        """Per DAG role (a case counts under every role of its block)."""
        return {
            role: _rate(members)
            for role in ROLES
            if (members := [r for r in self.results if role in r.case.roles])
        }

    def confusion(self) -> dict[str, dict[str, int]]:
        """``confusion()[label][predicted]`` = number of answers."""
        matrix = {label: dict.fromkeys(ROOT_CAUSES, 0) for label in ROOT_CAUSES}
        for r in self.results:
            matrix[r.case.label][r.predicted] += 1
        return matrix

    def calibration(self) -> list[dict[str, Any]]:
        """Accuracy by stated confidence, over accepted answers (a rejected answer's
        confidence is not the model's: it is set to 0)."""
        accepted = [r for r in self.results if r.diagnosis.status == ACCEPTED]
        rows, low = [], 0.0
        for high in CONFIDENCE_BUCKETS:
            last = high == CONFIDENCE_BUCKETS[-1]
            members = [
                r
                for r in accepted
                if low <= r.diagnosis.confidence < high or (last and r.diagnosis.confidence == high)
            ]
            correct = sum(r.correct for r in members)
            rows.append(
                {
                    "bucket": f"[{low:.1f}, {high:.1f}{']' if last else ')'}",
                    "answers": len(members),
                    "correct": correct,
                    "accuracy": round(correct / len(members), 4) if members else None,
                    "mean_confidence": (
                        round(statistics.fmean(r.diagnosis.confidence for r in members), 4)
                        if members
                        else None
                    ),
                }
            )
            low = high
        return rows

    def expected_calibration_error(self) -> float | None:
        rows = [r for r in self.calibration() if r["answers"]]
        total = sum(r["answers"] for r in rows)
        if not total:
            return None
        return round(
            sum(r["answers"] / total * abs(r["accuracy"] - r["mean_confidence"]) for r in rows), 4
        )

    @property
    def rejected(self) -> int:
        """Answers rejected for citing evidence that is not in the bundle."""
        return sum(r.diagnosis.rejection == BAD_CITATION for r in self.results)

    @property
    def rejected_rate(self) -> float:
        return self.rejected / len(self.results) if self.results else 0.0

    def rejections(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in self.results:
            if r.diagnosis.rejection:
                counts[r.diagnosis.rejection] = counts.get(r.diagnosis.rejection, 0) + 1
        return dict(sorted(counts.items()))

    def calls(self) -> list[CallRecord]:
        return [c for r in self.results for c in r.calls]

    def median_latency(self) -> float | None:
        return _median([r.latency_s for r in self.results if r.latency_s is not None])

    def accuracy_by_repeat(self) -> list[float]:
        out = []
        for k in range(self.repeats):
            correct, total = _rate([r for r in self.results if r.repeat == k])
            out.append(round(correct / total, 4) if total else 0.0)
        return out

    def agreement(self) -> dict[str, Any] | None:
        outcomes: dict[str, list[str]] = {}
        for r in sorted(self.results, key=lambda r: r.repeat):
            outcomes.setdefault(r.case.case_id, []).append(r.predicted)
        summary = _agreement(outcomes)
        if summary is not None:
            majority_correct = 0
            labels = {r.case.case_id: r.case.label for r in self.results}
            for case, predictions in outcomes.items():
                top, n = Counter(predictions).most_common(1)[0]
                majority_correct += top == labels[case] and n > len(predictions) / 2
            summary["majority_vote_accuracy"] = round(majority_correct / len(outcomes), 4)
            summary["accuracy_by_repeat"] = self.accuracy_by_repeat()
        return summary

    def to_dict(self) -> dict[str, Any]:
        correct, total = _rate(self.results)
        return {
            "meta": self.meta,
            "model": self.model,
            "cases": self.cases,
            "repeats": self.repeats,
            "answers": total,
            "correct": correct,
            "accuracy": round(self.accuracy, 4),
            "by_fault": {k: list(v) for k, v in self.by_fault().items()},
            "by_role": {k: list(v) for k, v in self.by_role().items()},
            "confusion": self.confusion(),
            "calibration": self.calibration(),
            "expected_calibration_error": self.expected_calibration_error(),
            "rejected_bad_citation": self.rejected,
            "rejected_rate": round(self.rejected_rate, 4),
            "rejections": self.rejections(),
            "median_latency_s": self.median_latency(),
            "usage": usage_summary(self.calls(), self.prices),
            "agreement": self.agreement(),
            "results": [r.to_dict() for r in self.results],
        }

    def to_markdown(self, title: str = "Diagnosis agent eval") -> str:
        return f"# {title}\n\n" + "\n".join(render_diagnose(self.to_dict())) + "\n"


def run_eval(
    cases: Sequence[EvalCase],
    client: LLMClient,
    *,
    repeats: int = 1,
    prices: Prices | None = None,
    meta: Mapping[str, Any] | None = None,
) -> EvalReport:
    """Diagnose every case ``repeats`` times with ``client`` (request key: the case id)
    and score it. With a MeteredClient, repeat k is cache sample k."""
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    results = []
    for repeat in range(repeats):
        _set_sample(client, repeat)
        for case in cases:
            start = _n_calls(client)
            diagnosis = diagnose_bundle(case.bundle, client, key=case.case_id)
            results.append(CaseResult(case, diagnosis, repeat, _calls_since(client, start)))
    return EvalReport(
        model=getattr(client, "model", "?"),
        results=results,
        repeats=repeats,
        prices=prices,
        meta=dict(meta or {}),
    )


# ------------------------------------------------------------------ fixes


SHADOW_RUN = "eval-shadow"

# Why a fix did not succeed (``FixResult.failure``).
MISDIAGNOSED = "misdiagnosed"
DIAGNOSIS_UNTRUSTED = "diagnosis not trusted"
NO_PLAN = "no usable fix plan"
NOTE_ONLY = "a note instead of a fix"
TESTS_FAILED = "unit tests failed"
SHADOW_FAILED = "shadow check failed"
NOT_APPLIED = "fix could not be applied"
PROMOTION_REFUSED = "promotion refused"
NOT_ACTIVE = "promoted version not active"


@dataclass(frozen=True)
class FixResult:
    case: EvalCase
    diagnosed: str
    proposal: str  # the proposal's status
    promoted: bool  # the proposed version passed shadow promotion
    reason: str
    repeat: int = 0
    calls: tuple[CallRecord, ...] = ()
    failure: str | None = None  # one of the failure kinds above; None when promoted

    @property
    def latency_s(self) -> float | None:
        return round(sum(c.latency_s for c in self.calls), 4) if self.calls else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "case": self.case.case_id,
            "block": self.case.block,
            "roles": list(self.case.roles),
            "repeat": self.repeat,
            "diagnosed": self.diagnosed,
            "proposal": self.proposal,
            "promoted": self.promoted,
            "failure": self.failure,
            "reason": self.reason,
            "latency_s": self.latency_s,
            "input_tokens": sum(c.input_tokens for c in self.calls),
            "output_tokens": sum(c.output_tokens for c in self.calls),
            "cached": bool(self.calls) and all(c.cached for c in self.calls),
        }


@dataclass
class FixEvalReport:
    model: str
    results: list[FixResult]
    repeats: int = 1
    prices: Prices | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        return sum(r.promoted for r in self.results) / len(self.results) if self.results else 0.0

    def by_role(self) -> dict[str, tuple[int, int]]:
        return {
            role: (sum(r.promoted for r in members), len(members))
            for role in ROLES
            if (members := [r for r in self.results if role in r.case.roles])
        }

    def failures(self) -> dict[str, int]:
        return dict(Counter(r.failure for r in self.results if r.failure).most_common())

    def agreement(self) -> dict[str, Any] | None:
        outcomes: dict[str, list[bool]] = {}
        for r in sorted(self.results, key=lambda r: r.repeat):
            outcomes.setdefault(r.case.case_id, []).append(r.promoted)
        summary = _agreement(outcomes)
        if summary is not None:
            summary["success_by_repeat"] = [
                round(sum(r.promoted for r in rs) / len(rs), 4) if rs else 0.0
                for k in range(self.repeats)
                for rs in [[r for r in self.results if r.repeat == k]]
            ]
        return summary

    def to_dict(self) -> dict[str, Any]:
        calls = [c for r in self.results for c in r.calls]
        return {
            "meta": self.meta,
            "model": self.model,
            "cases": len({r.case.case_id for r in self.results}),
            "repeats": self.repeats,
            "attempts": len(self.results),
            "promoted": sum(r.promoted for r in self.results),
            "fix_success_rate": round(self.success_rate, 4),
            "by_role": {k: list(v) for k, v in self.by_role().items()},
            "failures": self.failures(),
            "median_latency_s": _median([r.latency_s for r in self.results if r.latency_s]),
            "usage": usage_summary(calls, self.prices),
            "agreement": self.agreement(),
            "results": [r.to_dict() for r in self.results],
        }

    def to_markdown(self, title: str = "Fix agent eval") -> str:
        return f"# {title}\n\n" + "\n".join(render_propose(self.to_dict())) + "\n"


def run_fix_eval(
    cases: Sequence[EvalCase],
    client: LLMClient,
    *,
    spec: PipelineSpec,
    spec_path: Path | str,
    repo: Path | str | None = None,
    repeats: int = 1,
    workdir: Path | str | None = None,
    prices: Prices | None = None,
    meta: Mapping[str, Any] | None = None,
) -> FixEvalReport:
    """Diagnose, propose (always a dry run) and try to promote a fix for every code_bug
    case, ``repeats`` times, each time on a fresh copy of the case's state.

    ``spec`` is the spec the cases were generated from; ``spec_path`` the spec file in
    the git repository the fixes are proposed against.
    """
    if repeats < 1:
        raise ValueError("repeats must be at least 1")
    spec = eval_spec(spec)
    results = []
    for repeat in range(repeats):
        _set_sample(client, repeat)
        for case in cases:
            if case.fault != "code_bug":
                continue
            assert case.root is not None and case.injected is not None
            base = Path(workdir) if workdir else case.root.parent.parent / "fix-runs"
            root = base / f"{case.block}__{case.fault}__r{repeat}"
            if root.exists():
                shutil.rmtree(root)
            shutil.copytree(case.root, root)
            start = _n_calls(client)
            result = _fix_case(case, root, client, spec, spec_path, repo)
            results.append(
                dataclasses.replace(result, repeat=repeat, calls=_calls_since(client, start))
            )
    return FixEvalReport(
        model=getattr(client, "model", "?"),
        results=results,
        repeats=repeats,
        prices=prices,
        meta=dict(meta or {}),
    )


def _fix_case(
    case: EvalCase,
    root: Path,
    client: LLMClient,
    spec: PipelineSpec,
    spec_path: Path | str,
    repo: Path | str | None,
) -> FixResult:
    from guardian.agent.propose import FAILED, NOTE, propose

    assert case.injected is not None
    # The case's own (deterministic) bundle is already in ``root``; with the diagnosis
    # next to it, propose uses both instead of rebuilding them.
    diagnosis = diagnose_bundle(case.bundle, client, key=case.case_id)
    diagnosis.write(root)
    faulted, registry = apply_faults(spec, {case.block: [case.injected]})
    with Guardian(faulted, root, registry=registry) as g:
        proposal = propose(
            g,
            case.block,
            FAULT_RUN,
            client=client,
            spec_path=spec_path,
            repo=repo,
            key=case.case_id,
            git_history=False,
            open_pr=False,  # never GitHub: the eval only ever makes dry runs
        )
    reason = "; ".join(proposal.reasons)[:500]
    failure = None
    promoted = False
    if not diagnosis.accepted:
        failure = f"{DIAGNOSIS_UNTRUSTED} ({diagnosis.rejection})"
    elif diagnosis.root_cause != "code_bug":
        failure = f"{MISDIAGNOSED} as {diagnosis.root_cause}"
    elif proposal.status == NOTE:
        failure = NOTE_ONLY
    elif proposal.status == FAILED:
        failure = (
            TESTS_FAILED
            if "unit tests failed" in reason
            else SHADOW_FAILED
            if "shadow run" in reason
            else NOT_APPLIED
        )
    elif not proposal.ok:
        failure = NO_PLAN
    else:
        promoted, reason = _simulate_reviewer(spec, case, root, proposal)
        if not promoted:
            failure = PROMOTION_REFUSED if reason != NOT_ACTIVE else NOT_ACTIVE
    return FixResult(case, diagnosis.root_cause, proposal.status, promoted, reason, failure=failure)


def _simulate_reviewer(
    spec: PipelineSpec, case: EvalCase, root: Path, proposal: Any
) -> tuple[bool, str]:
    """What a reviewer does after merging: shadow the new version while the buggy one
    is still live, then promote it with approval. Runs only on the case's copy."""
    from guardian.agent.propose import with_proposed_version
    from guardian.core.guardian import PromotionError

    assert case.injected is not None
    merged, registry = with_proposed_version(spec, proposal)
    faulted, fault_registry = apply_faults(merged, {case.block: [case.injected]})
    with Guardian(faulted, root, registry={**registry, **fault_registry}) as g:
        g.shadow_start(case.block, proposal.version)
        Executor(g).run(SHADOW_RUN, only=[case.block])
        try:
            g.promote(case.block, approve=True, reason="eval: simulated reviewer approval")
        except PromotionError as exc:
            return False, str(exc)
        if g.active_version(case.block) != proposal.version:
            return False, NOT_ACTIVE
        return True, "promoted"


# ------------------------------------------------------------------ dry runs


class NotCached(LLMError):
    """A dry run reached a call that is not in the cache (so it would be paid for)."""


class _CacheOnly:
    """The inner client of a dry run: it never calls a model."""

    def __init__(self, model: str) -> None:
        self.model = model
        self.misses: list[LLMRequest] = []

    def complete(self, request: LLMRequest) -> str:
        self.misses.append(request)
        raise NotCached(request.key)


@dataclass
class CostEstimate:
    """What an eval would cost: calls it would make, beyond those already cached.

    Expected: one attempt per call, ``expected_output`` output tokens each. Upper
    bound: every call retried once, with ``max_tokens`` output tokens per attempt.
    Input tokens are estimated from the exact prompts (CHARS_PER_TOKEN per token),
    except fix prompts that depend on a diagnosis not in the cache yet (estimated from
    a diagnosis of typical length).
    """

    command: str
    model: str | None
    cases: int
    repeats: int
    calls: int = 0
    cached_calls: int = 0
    input_tokens: int = 0
    expected_output: int = 0
    max_tokens: int = 0
    prices: Prices = field(default_factory=Prices)

    def add(self, request: LLMRequest) -> None:
        self.calls += 1
        self.input_tokens += request_tokens(request)

    @property
    def output_tokens(self) -> int:
        return self.calls * self.expected_output

    @property
    def input_upper(self) -> int:
        return 2 * self.input_tokens

    @property
    def output_upper(self) -> int:
        return 2 * self.calls * self.max_tokens

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "model": self.model,
            "cases": self.cases,
            "repeats": self.repeats,
            "calls": self.calls,
            "cached_calls": self.cached_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "input_tokens_upper": self.input_upper,
            "output_tokens_upper": self.output_upper,
            "prices": self.prices.to_dict(),
            "cost_usd": _round(self.prices.cost(self.input_tokens, self.output_tokens)),
            "cost_usd_upper": _round(self.prices.cost(self.input_upper, self.output_upper)),
        }

    def lines(self) -> list[str]:
        cost = self.prices.cost(self.input_tokens, self.output_tokens)
        upper = self.prices.cost(self.input_upper, self.output_upper)
        money = (
            f"${cost:,.2f} expected, at most ${upper:,.2f}"
            if cost is not None and upper is not None
            else "unknown (no prices configured)"
        )
        return [
            f"Dry run of `guardian eval {self.command}` (nothing was sent to any model).",
            f"Model: {self.model or '(not set)'}",
            f"Cases: {self.cases} x {self.repeats} repeat(s)",
            f"Calls to make: {self.calls} (already cached, free: {self.cached_calls})",
            f"Input tokens (estimated): {self.input_tokens:,} expected, "
            f"at most {self.input_upper:,} with every call retried",
            f"Output tokens (estimated): {self.output_tokens:,} expected "
            f"({self.expected_output:,} per call), at most {self.output_upper:,} "
            f"({self.max_tokens:,} max_tokens per attempt, every call retried)",
            f"Prices: {self.prices.describe()}",
            f"Estimated cost: {money}",
        ]


def _probe(model: str | None, cache: ResponseCache | None, read_cache: bool) -> MeteredClient:
    return MeteredClient(_CacheOnly(model or "?"), cache=cache if model else None,
                         read_cache=read_cache)  # fmt: skip


def estimate_diagnose(
    cases: Sequence[EvalCase],
    *,
    model: str | None,
    repeats: int,
    cache: ResponseCache | None,
    read_cache: bool,
    expected_output: int,
    max_tokens: int,
    prices: Prices,
) -> CostEstimate:
    """The calls ``run_eval`` would pay for. Cached answers are replayed (without any
    model) to see which calls, retries included, are already paid for."""
    estimate = CostEstimate("diagnose", model, len(cases), repeats,
                            expected_output=expected_output, max_tokens=max_tokens,
                            prices=prices)  # fmt: skip
    probe = _probe(model, cache, read_cache)
    for repeat in range(repeats):
        probe.sample = repeat
        for case in cases:
            _replay_diagnosis(case, probe, estimate)
    return estimate


def _replay_diagnosis(case: EvalCase, probe: MeteredClient, estimate: CostEstimate) -> Diagnosis:
    inner = probe.inner
    assert isinstance(inner, _CacheOnly)
    start_calls, start_misses = len(probe.calls), len(inner.misses)
    diagnosis = diagnose_bundle(case.bundle, probe, key=case.case_id)
    estimate.cached_calls += len(probe.calls) - start_calls
    for request in inner.misses[start_misses:]:
        estimate.add(request)
    return diagnosis


def estimate_propose(
    cases: Sequence[EvalCase],
    *,
    spec_path: Path | str,
    repo: Path | str | None,
    model: str | None,
    repeats: int,
    cache: ResponseCache | None,
    read_cache: bool,
    expected_output: int,
    max_tokens: int,
    prices: Prices,
) -> CostEstimate:
    """The calls ``run_fix_eval`` would pay for: per code_bug case and repeat, the
    diagnosis and the fix request."""
    from guardian.agent.propose import FIX_SCHEMA, FIX_SYSTEM_PROMPT, fix_prompt

    code_bugs = [c for c in cases if c.fault == "code_bug"]
    estimate = CostEstimate("propose", model, len(code_bugs), repeats,
                            expected_output=expected_output, max_tokens=max_tokens,
                            prices=prices)  # fmt: skip
    probe = _probe(model, cache, read_cache)
    contexts: dict[str, dict[str, Any]] = {}
    for repeat in range(repeats):
        probe.sample = repeat
        for case in code_bugs:
            misses_before = estimate.calls
            diagnosis = _replay_diagnosis(case, probe, estimate)
            if case.block not in contexts:
                contexts[case.block] = _fix_context(case.block, spec_path, repo)
            context = contexts[case.block]
            if estimate.calls > misses_before:
                # The diagnosis is not cached yet: estimate its fix request from a
                # diagnosis of typical length (propose asks for one only for a
                # trusted diagnosis; the estimate assumes it gets one).
                typical = _typical_diagnosis(case, model)
                estimate.add(
                    LLMRequest(
                        key=f"{case.case_id}:fix",
                        system=FIX_SYSTEM_PROMPT,
                        prompt=fix_prompt(case.bundle, typical, context),
                        schema=FIX_SCHEMA,
                    )
                )
                continue
            if not diagnosis.accepted or diagnosis.root_cause == "unknown":
                continue  # propose stops before asking for a fix
            request = LLMRequest(
                key=f"{case.case_id}:fix",
                system=FIX_SYSTEM_PROMPT,
                prompt=fix_prompt(case.bundle, diagnosis, context),
                schema=FIX_SCHEMA,
            )
            if probe.cached(request):
                estimate.cached_calls += 1
            else:
                estimate.add(request)
    return estimate


def _typical_diagnosis(case: EvalCase, model: str | None) -> Diagnosis:
    ids = case.bundle.ids
    return Diagnosis(
        pipeline=case.bundle.pipeline,
        block=case.block,
        run_id=case.bundle.run_id,
        root_cause="code_bug",
        confidence=0.8,
        summary="x" * 400,
        claims=tuple(Claim("x" * 150, tuple(ids[:2])) for _ in range(3)),
        status=ACCEPTED,
        model=model or "?",
        attempts=1,
    )


def _fix_context(block: str, spec_path: Path | str, repo: Path | str | None) -> dict[str, Any]:
    from guardian.agent.propose import fix_context
    from guardian.agent.safety import SafeGit
    from guardian.runner.spec_loader import load_spec

    spec_path = Path(spec_path).resolve()
    git = SafeGit(repo or spec_path.parent)
    repo_root = Path(git.run("rev-parse", "--show-toplevel").strip())
    file_spec = load_spec(spec_path)
    file_block = file_spec.block(block)
    live_ref = file_block.version_ref(file_block.active)
    return fix_context(file_block, file_spec, live_ref, repo_root)


# ------------------------------------------------------------------ the report file


def write_report(md_path: Path | str, section: str, data: Mapping[str, Any]) -> dict[str, Any]:
    """Store ``data`` as the ``section`` ("diagnose" or "propose") of the JSON report
    next to ``md_path`` (keeping the other section), and re-render the Markdown from
    the whole JSON. Returns the whole report."""
    if section not in ("diagnose", "propose"):
        raise ValueError(f"unknown report section {section!r}")
    md_path = Path(md_path)
    json_path = md_path.with_suffix(".json")
    report: dict[str, Any] = {}
    if json_path.exists():
        try:
            report = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            report = {}
    report["format"] = 1
    report["note"] = SYNTHETIC_NOTE
    report[section] = dict(data)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_report(report), encoding="utf-8", newline="\n")
    return report


def render_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Agent eval",
        "",
        f"> {SYNTHETIC_NOTE}",
        "",
        "Written by `guardian eval diagnose` and `guardian eval propose` "
        "(each command replaces its own section of the JSON next to this file).",
    ]
    if "diagnose" in report:
        lines += ["", "# Diagnosis", "", *render_diagnose(report["diagnose"], level=2)]
    if "propose" in report:
        lines += ["", "# Fix proposals", "", *render_propose(report["propose"], level=2)]
    return "\n".join(lines) + "\n"


def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "n/a"


def _meta_lines(data: Mapping[str, Any]) -> list[str]:
    meta = data.get("meta") or {}
    model = f"Model: `{data['model']}`" + (
        " (real model)" if meta.get("real_model") else " (recorded / fake answers)"
        if "real_model" in meta else ""
    )  # fmt: skip
    lines = [model + "."]
    if meta:
        commit = (meta.get("git_commit") or "unknown")[:12]
        if meta.get("git_dirty"):
            commit += " (uncommitted changes)"
        filters = {k: v for k, v in (meta.get("filters") or {}).items() if v}
        lines += [
            "",
            f"Date: {meta.get('date')}. Git commit: `{commit}`. Cases: {data['cases']} "
            f"x {data.get('repeats', 1)} repeat(s), seed {meta.get('seed')}"
            + (f", filters {json.dumps(filters, sort_keys=True)}" if filters else "")
            + ".",
        ]
    return lines


def _usage_lines(data: Mapping[str, Any]) -> list[str]:
    usage = data.get("usage") or {}
    if not usage.get("calls"):
        return []
    est = " (estimated offline: the client reports no usage)" if usage["tokens_estimated"] else ""
    cost = usage.get("cost_usd")
    billed = usage.get("billed_cost_usd")
    return [
        f"| median latency per case | {data.get('median_latency_s')} s |",
        f"| LLM calls | {usage['calls']} ({usage['cached_calls']} served from the cache) |",
        f"| tokens in / out | {usage['input_tokens']:,} / {usage['output_tokens']:,}{est} |",
        f"| tokens paid in this run | {usage['billed_input_tokens']:,} / "
        f"{usage['billed_output_tokens']:,} |",
        "| cost | "
        + (
            f"${cost:,.4f} (this run: ${billed:,.4f})"
            if cost is not None and billed is not None
            else "unknown (no prices configured)"
        )
        + " |",
    ]


def render_diagnose(data: Mapping[str, Any], level: int = 2) -> list[str]:
    h = "#" * level
    total, correct = data.get("answers", len(data["results"])), data.get("correct")
    if correct is None:
        correct = sum(r["predicted"] == r["label"] for r in data["results"])
    rejected = data["rejected_bad_citation"]
    lines = [
        *_meta_lines(data),
        "",
        "| metric | value |",
        "|---|---|",
        f"| accuracy | {_pct(correct, total)} ({correct}/{total}) |",
        f"| rejected for bad citations | {_pct(rejected, total)} ({rejected}/{total}) |",
    ]
    for rejection, n in data.get("rejections", {}).items():
        if rejection != BAD_CITATION:
            lines.append(f"| downgraded: {rejection} | {n}/{total} |")
    if data.get("expected_calibration_error") is not None:
        lines.append(f"| expected calibration error | {data['expected_calibration_error']} |")
    lines += _usage_lines(data)
    lines += ["", f"{h} By fault type", "", "| fault | expected | accuracy |", "|---|---|---|"]
    for fault, (c, n) in data["by_fault"].items():
        lines.append(f"| {fault} | {EXPECTED[fault]} | {_pct(c, n)} ({c}/{n}) |")
    lines += [
        "",
        f"{h} By DAG role",
        "",
        "A case counts under every role of its block.",
        "",
        "| role | accuracy |",
        "|---|---|",
    ]
    for role, (c, n) in data["by_role"].items():
        lines.append(f"| {role} | {_pct(c, n)} ({c}/{n}) |")
    labels = [c for c in ROOT_CAUSES if any(r["label"] == c for r in data["results"])]
    matrix = data["confusion"]
    lines += [
        "",
        f"{h} Confusion matrix",
        "",
        "Rows: expected root cause. Columns: diagnosed root cause.",
        "",
        "| expected \\ diagnosed | " + " | ".join(ROOT_CAUSES) + " |",
        "|---|" + "---|" * len(ROOT_CAUSES),
    ]
    for label in labels:
        lines.append(
            f"| {label} | " + " | ".join(str(matrix[label][p]) for p in ROOT_CAUSES) + " |"
        )
    if data.get("calibration"):
        lines += [
            "",
            f"{h} Calibration",
            "",
            "Accuracy by the model's stated confidence, over accepted answers.",
            "",
            "| confidence | answers | accuracy | mean confidence |",
            "|---|---|---|---|",
        ]
        for row in data["calibration"]:
            acc = "n/a" if row["accuracy"] is None else f"{100 * row['accuracy']:.1f}%"
            mean = "n/a" if row["mean_confidence"] is None else f"{row['mean_confidence']:.2f}"
            lines.append(f"| {row['bucket']} | {row['answers']} | {acc} | {mean} |")
    agreement = data.get("agreement")
    if agreement:
        lines += [
            "",
            f"{h} Agreement across repeats",
            "",
            f"- Cases with the same diagnosis in every repeat: {agreement['unanimous']}/"
            f"{agreement['cases']} ({100 * agreement['unanimous_rate']:.1f}%)",
            f"- Pairwise agreement: {100 * agreement['pairwise_agreement']:.1f}%",
            f"- Majority-vote accuracy: {100 * agreement['majority_vote_accuracy']:.1f}%",
            "- Accuracy by repeat: "
            + ", ".join(f"{100 * a:.1f}%" for a in agreement["accuracy_by_repeat"]),
        ]
    lines += [
        "",
        f"{h} Cases",
        "",
        "| case | repeat | block outcome | expected | diagnosed | confidence | status "
        "| citations | latency (s) | tokens in / out |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in data["results"]:
        status = r["status"] + (f" ({r['rejection']})" if r.get("rejection") else "")
        citations = {True: "valid", False: "rejected", None: "-"}[r.get("citations_valid")]
        latency = "-" if r.get("latency_s") is None else f"{r['latency_s']:.2f}"
        tokens = f"{r.get('input_tokens', 0):,} / {r.get('output_tokens', 0):,}"
        if r.get("cached"):
            tokens += " (cached)"
        lines.append(
            f"| {r['case']} | {r.get('repeat', 0)} | {r['outcome']} | {r['label']} | "
            f"{r['predicted']} | {r['confidence']:.2f} | {status} | {citations} | "
            f"{latency} | {tokens} |"
        )
    return lines


def render_propose(data: Mapping[str, Any], level: int = 2) -> list[str]:
    h = "#" * level
    ok = data.get("promoted", sum(r["promoted"] for r in data["results"]))
    n = data.get("attempts", len(data["results"]))
    lines = [
        *_meta_lines(data),
        "",
        "Code_bug cases only, always in dry-run mode (GitHub is never touched). A fix",
        "succeeds when the agent's proposed version passes its unit tests and a shadow",
        "run, and then passes shadow promotion (with a simulated reviewer's approval)",
        "while the buggy version is still live.",
        "",
        "| metric | value |",
        "|---|---|",
        f"| fix success | {_pct(ok, n)} ({ok}/{n}) |",
    ]
    lines += _usage_lines(data)
    lines += ["", f"{h} By DAG role", "", "| role | fix success |", "|---|---|"]
    for role, (c, m) in data["by_role"].items():
        lines.append(f"| {role} | {_pct(c, m)} ({c}/{m}) |")
    if data.get("failures"):
        lines += ["", f"{h} Failure reasons", "", "| reason | cases |", "|---|---|"]
        for reason, count in data["failures"].items():
            lines.append(f"| {reason} | {count} |")
    agreement = data.get("agreement")
    if agreement:
        lines += [
            "",
            f"{h} Agreement across repeats",
            "",
            f"- Cases with the same outcome in every repeat: {agreement['unanimous']}/"
            f"{agreement['cases']} ({100 * agreement['unanimous_rate']:.1f}%)",
            f"- Pairwise agreement: {100 * agreement['pairwise_agreement']:.1f}%",
            "- Fix success by repeat: "
            + ", ".join(f"{100 * a:.1f}%" for a in agreement["success_by_repeat"]),
        ]
    lines += [
        "",
        f"{h} Cases",
        "",
        "| case | repeat | diagnosed | proposal | promoted | failure | reason |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in data["results"]:
        reason = r["reason"].replace("|", "\\|").replace("\n", " ")[:200]
        lines.append(
            f"| {r['case']} | {r.get('repeat', 0)} | {r['diagnosed']} | {r['proposal']} | "
            f"{'yes' if r['promoted'] else 'no'} | {r.get('failure') or '-'} | {reason} |"
        )
    return lines
