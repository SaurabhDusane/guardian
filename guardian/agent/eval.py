"""Evaluate the diagnosis agent on labeled faults, for every block of a spec, and its
fixes on injected code bugs.

``generate_cases`` runs the pipeline once cleanly, then, for every block and every
fault type, copies that state, injects the fault into the block, re-runs the block
and builds its evidence bundle. Each case is labeled with the root cause its fault
simulates:

    schema_drift -> schema_change
    corrupt_rows -> upstream_data_drift
    null_burst   -> upstream_data_drift
    code_bug     -> code_bug

Which column a fault touches comes from the block's own clean output and declared
schema, never from block names, so any spec works. ``run_eval`` diagnoses every case
and reports accuracy (overall, by DAG role, by fault type), a confusion matrix and the
rate of answers rejected for citing nonexistent evidence.

Git history is left out of eval bundles: faults are injected at run time, not
committed, so the source file's history would only reflect the state of the
checkout the eval runs in.

``run_fix_eval`` measures fixes: for every block's code_bug case, the agent diagnoses
and proposes a fix (dry run, in a git worktree). A proposal succeeds when its new
version then passes shadow promotion in that case's state: it is shadowed on a run
where the live version is still buggy and promoted with an approval, as a human
reviewer would do after merging. That approval is simulated by the eval harness,
never by the agent.
"""

from __future__ import annotations

import dataclasses
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from guardian.agent.diagnose import (
    BAD_CITATION,
    ROOT_CAUSES,
    Diagnosis,
    LLMClient,
    diagnose_bundle,
)
from guardian.agent.evidence import DEFAULT_SAMPLE_SIZE, EvidenceBundle, build_evidence
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


def generate_cases(
    spec: PipelineSpec,
    workdir: Path | str,
    *,
    blocks: Sequence[str] | None = None,
    faults: Sequence[str] = FAULT_TYPES,
    registry: Mapping[str, Any] | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    seed: int = 0,
) -> list[EvalCase]:
    """One labeled case per (block, fault type), each in its own copy of the state."""
    unknown = sorted(set(faults) - set(FAULT_TYPES))
    if unknown:
        raise ValueError(f"unknown fault type(s) {unknown}; expected {list(FAULT_TYPES)}")
    for block in blocks or ():
        spec.block(block)  # KeyError for an unknown block
    spec = eval_spec(spec)
    workdir = Path(workdir)
    base_root = workdir / "baseline"
    if base_root.exists():
        shutil.rmtree(base_root)
    with Guardian(spec, base_root, registry=registry) as g:
        Executor(g).run(BASELINE_RUN)
        declared = {b: _declared(g, b) for b in spec.block_names}

    cases = []
    selected = [b for b in topological_order(spec) if blocks is None or b in blocks]
    for block in selected:
        clean = _clean_output(base_root, block)
        for fault_type in faults:
            case_root = workdir / "cases" / f"{block}__{fault_type}"
            if case_root.exists():
                shutil.rmtree(case_root)
            shutil.copytree(base_root, case_root)
            fault = make_fault(fault_type, clean, declared[block], seed)
            faulted, fault_registry = apply_faults(spec, {block: [fault]})
            with Guardian(faulted, case_root, registry={**(registry or {}), **fault_registry}) as g:
                report = Executor(g).run(FAULT_RUN, only=[block])
                bundle = build_evidence(g, block, FAULT_RUN, sample_size=sample_size, git=False)
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


# ------------------------------------------------------------------ scoring


@dataclass(frozen=True)
class CaseResult:
    case: EvalCase
    diagnosis: Diagnosis

    @property
    def predicted(self) -> str:
        return self.diagnosis.root_cause

    @property
    def correct(self) -> bool:
        return self.predicted == self.case.label


def _rate(results: Sequence[CaseResult]) -> tuple[int, int]:
    return sum(r.correct for r in results), len(results)


@dataclass
class EvalReport:
    model: str
    results: list[CaseResult]

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
        """``confusion()[label][predicted]`` = number of cases."""
        matrix = {label: dict.fromkeys(ROOT_CAUSES, 0) for label in ROOT_CAUSES}
        for r in self.results:
            matrix[r.case.label][r.predicted] += 1
        return matrix

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

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "cases": len(self.results),
            "accuracy": round(self.accuracy, 4),
            "by_fault": {k: list(v) for k, v in self.by_fault().items()},
            "by_role": {k: list(v) for k, v in self.by_role().items()},
            "confusion": self.confusion(),
            "rejected_bad_citation": self.rejected,
            "rejected_rate": round(self.rejected_rate, 4),
            "rejections": self.rejections(),
            "results": [
                {
                    "case": r.case.case_id,
                    "label": r.case.label,
                    "predicted": r.predicted,
                    "confidence": r.diagnosis.confidence,
                    "status": r.diagnosis.status,
                    "rejection": r.diagnosis.rejection,
                    "outcome": r.case.outcome,
                }
                for r in self.results
            ],
        }

    def to_markdown(self, title: str = "Diagnosis agent eval") -> str:
        correct, total = _rate(self.results)
        lines = [
            f"# {title}",
            "",
            f"Model: `{self.model}`. Cases: {total} (every block x every fault type).",
            "",
            "| metric | value |",
            "|---|---|",
            f"| accuracy | {_pct(correct, total)} ({correct}/{total}) |",
            f"| rejected for bad citations | {_pct(self.rejected, total)} "
            f"({self.rejected}/{total}) |",
        ]
        for rejection, n in self.rejections().items():
            if rejection != BAD_CITATION:
                lines.append(f"| downgraded: {rejection} | {n}/{total} |")
        lines += ["", "## By fault type", "", "| fault | expected | accuracy |", "|---|---|---|"]
        for fault, (c, n) in self.by_fault().items():
            lines.append(f"| {fault} | {EXPECTED[fault]} | {_pct(c, n)} ({c}/{n}) |")
        lines += ["", "## By DAG role", "", "| role | accuracy |", "|---|---|"]
        for role, (c, n) in self.by_role().items():
            lines.append(f"| {role} | {_pct(c, n)} ({c}/{n}) |")
        labels = [c for c in ROOT_CAUSES if any(r.case.label == c for r in self.results)]
        lines += [
            "",
            "## Confusion matrix",
            "",
            "Rows: expected root cause. Columns: diagnosed root cause.",
            "",
            "| expected \\ diagnosed | " + " | ".join(ROOT_CAUSES) + " |",
            "|---|" + "---|" * len(ROOT_CAUSES),
        ]
        matrix = self.confusion()
        for label in labels:
            lines.append(
                f"| {label} | " + " | ".join(str(matrix[label][p]) for p in ROOT_CAUSES) + " |"
            )
        lines += [
            "",
            "## Cases",
            "",
            "| case | block outcome | expected | diagnosed | confidence | status |",
            "|---|---|---|---|---|---|",
        ]
        for r in self.results:
            status = r.diagnosis.status
            if r.diagnosis.rejection:
                status += f" ({r.diagnosis.rejection})"
            lines.append(
                f"| {r.case.case_id} | {r.case.outcome} | {r.case.label} | {r.predicted} | "
                f"{r.diagnosis.confidence:.2f} | {status} |"
            )
        return "\n".join(lines) + "\n"


def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "n/a"


def run_eval(cases: Sequence[EvalCase], client: LLMClient) -> EvalReport:
    """Diagnose every case with ``client`` (request key: the case id) and score it."""
    results = [
        CaseResult(case, diagnose_bundle(case.bundle, client, key=case.case_id)) for case in cases
    ]
    return EvalReport(model=getattr(client, "model", "?"), results=results)


# ------------------------------------------------------------------ fixes


SHADOW_RUN = "eval-shadow"


@dataclass(frozen=True)
class FixResult:
    case: EvalCase
    diagnosed: str
    proposal: str  # the proposal's status
    promoted: bool  # the proposed version passed shadow promotion
    reason: str


@dataclass
class FixEvalReport:
    model: str
    results: list[FixResult]

    @property
    def success_rate(self) -> float:
        return sum(r.promoted for r in self.results) / len(self.results) if self.results else 0.0

    def by_role(self) -> dict[str, tuple[int, int]]:
        return {
            role: (sum(r.promoted for r in members), len(members))
            for role in ROLES
            if (members := [r for r in self.results if role in r.case.roles])
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "cases": len(self.results),
            "fix_success_rate": round(self.success_rate, 4),
            "by_role": {k: list(v) for k, v in self.by_role().items()},
            "results": [
                {
                    "case": r.case.case_id,
                    "diagnosed": r.diagnosed,
                    "proposal": r.proposal,
                    "promoted": r.promoted,
                    "reason": r.reason,
                }
                for r in self.results
            ],
        }

    def to_markdown(self, title: str = "Fix agent eval") -> str:
        ok = sum(r.promoted for r in self.results)
        n = len(self.results)
        lines = [
            f"# {title}",
            "",
            f"Model: `{self.model}`. Cases: {n} (an injected code_bug in every block).",
            "",
            "A fix succeeds when the agent's proposed version passes its unit tests and a",
            "shadow run, and then passes shadow promotion (with approval) while the buggy",
            "version is still live.",
            "",
            "| metric | value |",
            "|---|---|",
            f"| fix success | {_pct(ok, n)} ({ok}/{n}) |",
            "",
            "## By DAG role",
            "",
            "| role | fix success |",
            "|---|---|",
        ]
        for role, (c, m) in self.by_role().items():
            lines.append(f"| {role} | {_pct(c, m)} ({c}/{m}) |")
        lines += [
            "",
            "## Cases",
            "",
            "| case | diagnosed | proposal | promoted | reason |",
            "|---|---|---|---|---|",
        ]
        for r in self.results:
            reason = r.reason.replace("|", "\\|").replace("\n", " ")[:200]
            lines.append(
                f"| {r.case.case_id} | {r.diagnosed} | {r.proposal} | "
                f"{'yes' if r.promoted else 'no'} | {reason} |"
            )
        return "\n".join(lines) + "\n"


def run_fix_eval(
    cases: Sequence[EvalCase],
    client: LLMClient,
    *,
    spec: PipelineSpec,
    spec_path: Path | str,
    repo: Path | str | None = None,
) -> FixEvalReport:
    """Diagnose, propose (dry run) and try to promote a fix for every code_bug case.

    ``spec`` is the spec the cases were generated from; ``spec_path`` the spec file in
    the git repository the fixes are proposed against.
    """
    from guardian.agent.diagnose import diagnose
    from guardian.agent.propose import propose

    spec = eval_spec(spec)
    results = []
    for case in cases:
        if case.fault != "code_bug":
            continue
        assert case.root is not None and case.injected is not None
        faulted, registry = apply_faults(spec, {case.block: [case.injected]})
        with Guardian(faulted, case.root, registry=registry) as g:
            diagnosis = diagnose(
                g, case.block, FAULT_RUN, client=client, key=case.case_id, git=False
            ).diagnosis
            proposal = propose(
                g,
                case.block,
                FAULT_RUN,
                client=client,
                spec_path=spec_path,
                repo=repo,
                key=case.case_id,
                git_history=False,
            )
        if proposal.ok:
            promoted, reason = _simulate_reviewer(spec, case, proposal)
        else:
            promoted, reason = False, "; ".join(proposal.reasons)[:500]
        results.append(FixResult(case, diagnosis.root_cause, proposal.status, promoted, reason))
    return FixEvalReport(model=getattr(client, "model", "?"), results=results)


def _simulate_reviewer(spec: PipelineSpec, case: EvalCase, proposal: Any) -> tuple[bool, str]:
    """What a reviewer does after merging: shadow the new version while the buggy one
    is still live, then promote it with approval. Runs only on the case's own copy."""
    from guardian.agent.propose import with_proposed_version
    from guardian.core.guardian import PromotionError

    assert case.root is not None and case.injected is not None
    merged, registry = with_proposed_version(spec, proposal)
    faulted, fault_registry = apply_faults(merged, {case.block: [case.injected]})
    with Guardian(faulted, case.root, registry={**registry, **fault_registry}) as g:
        g.shadow_start(case.block, proposal.version)
        Executor(g).run(SHADOW_RUN, only=[case.block])
        try:
            g.promote(case.block, approve=True, reason="eval: simulated reviewer approval")
        except PromotionError as exc:
            return False, str(exc)
        return g.active_version(case.block) == proposal.version, "promoted"
