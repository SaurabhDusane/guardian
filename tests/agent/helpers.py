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
