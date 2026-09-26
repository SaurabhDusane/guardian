"""Shared setup for agent tests: run the (scenario) demo spec cleanly, then with a fault."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

from guardian.agent.eval import make_fault
from guardian.core.guardian import Guardian
from guardian.core.models import PipelineSpec
from guardian.core.snapshots import LocalParquetSnapshotStore
from guardian.demo.faults import Fault, apply_faults
from guardian.runner.executor import Executor, RunReport

from ..helpers.roles import present_roles
from ..scenarios.runners import scenario_spec

SPEC = scenario_spec(rows=120)
ROLES = present_roles(SPEC)
FAULT_TYPES = ("schema_drift", "corrupt_rows", "null_burst", "code_bug")


def with_block(spec: PipelineSpec, block: str, **changes: object) -> PipelineSpec:
    blocks = tuple(dataclasses.replace(b, **changes) if b.name == block else b for b in spec.blocks)
    return dataclasses.replace(spec, blocks=blocks)


def run(root: Path, spec: PipelineSpec, run_id: str, faults: dict[str, list[Fault]] | None = None):
    faulted, registry = apply_faults(spec, faults or {})
    with Guardian(faulted, root, registry=registry) as g:
        return Executor(g).run(run_id)


def fault_for(root: Path, spec: PipelineSpec, block: str, fault_type: str) -> Fault:
    """The eval's fault of ``fault_type`` for ``block``, from its clean r1 output."""
    clean = LocalParquetSnapshotStore(root).read(block, "r1")
    with Guardian(spec, root) as g:
        declared = g.validator_for(block).schema.columns
    return make_fault(
        fault_type, clean.drop(columns=["_guardian_quality"], errors="ignore"), declared, seed=0
    )


def clean_then_fault(root: Path, spec: PipelineSpec, block: str, fault_type: str) -> RunReport:
    """r1 clean, then r2 with ``fault_type`` injected into ``block``."""
    run(root, spec, "r1")
    return run(root, spec, "r2", {block: [fault_for(root, spec, block, fault_type)]})


def answer(root_cause: str = "code_bug", evidence: tuple[str, ...] = ("E1",), **extra) -> str:
    payload = {
        "root_cause": root_cause,
        "confidence": 0.8,
        "summary": f"looks like {root_cause}",
        "claims": [{"statement": "the run rolled back", "evidence": list(evidence)}],
        **extra,
    }
    return json.dumps(payload)


def live_function(spec: PipelineSpec, block: str) -> str:
    """Name of the function behind the block's live version."""
    b = spec.block(block)
    return b.version_ref(b.active if b.versions else None).split(":")[1]


def fix_answer(
    action: str = "new_version",
    name: str = "",
    code: str = "",
    evidence: tuple[str, ...] = ("E1",),
    note: str = "",
    dependent: str = "",
) -> str:
    return json.dumps(
        {
            "action": action,
            "rationale": f"proposed {action}",
            "evidence": list(evidence),
            "name": name,
            "code": code,
            "note": note,
            "dependent": dependent,
        }
    )


def revert_fix(spec: PipelineSpec, block: str, evidence: tuple[str, ...] = ("E1",)) -> str:
    """A new version that restores the last good logic (the repo's live function): the
    right fix for an injected bad deploy."""
    fn = live_function(spec, block)
    code = f"def {fn}_fixed(*args, **kwargs):\n    return {fn}(*args, **kwargs)\n"
    return fix_answer("new_version", f"{fn}_fixed", code, evidence)


def broken_fix(spec: PipelineSpec, block: str) -> str:
    """A new version that makes things worse: it drops the first output column."""
    fn = live_function(spec, block)
    code = (
        f"def {fn}_broken(*args, **kwargs):\n"
        f"    out = {fn}(*args, **kwargs)\n"
        "    return out.drop(columns=list(out.columns)[:1])\n"
    )
    return fix_answer("new_version", f"{fn}_broken", code)
