"""Scenario 9: automated diagnosis after a ROLLBACK, for every DAG role, under every runner.

`auto_diagnose: true` on a block runs the (advisory) diagnosis agent after the block
rolls back. Whatever the agent does, including failing, the run proceeds exactly as it
would without it. The LLM is a FakeClient with a recorded answer: no network.
"""

import dataclasses
import json

import pytest

from guardian.agent.diagnose import FakeClient, auto_diagnoser
from guardian.core.events import EventKind
from guardian.demo.faults import code_bug, crash

from ..helpers.roles import representative
from .test_scenarios import ROLES, SPEC, assert_dependents_follow_rules, check_invariants

by_role = pytest.mark.parametrize("role", ROLES)

RECORDED = json.dumps(
    {
        "root_cause": "code_bug",
        "confidence": 0.9,
        "summary": "The block's code changed since its last good run.",
        "claims": [{"statement": "the run rolled back", "evidence": ["E1"]}],
    }
)


def auto(runner, block: str) -> None:
    runner.spec = dataclasses.replace(
        runner.spec,
        blocks=tuple(
            dataclasses.replace(b, auto_diagnose=True) if b.name == block else b
            for b in runner.spec.blocks
        ),
    )


@by_role
def test_9a_auto_diagnosis_after_rollback(runner, role: str) -> None:
    block = representative(SPEC, role)
    auto(runner, block)
    client = FakeClient({f"{block}/r2": RECORDED}, model="recorded")
    runner.diagnoser = auto_diagnoser(client)

    runner.run("r1")
    assert client.calls == []  # nothing rolled back: no diagnosis
    runner.inject_fault(block, code_bug())
    result = runner.run("r2")

    assert result[block].outcome == "ROLLBACK"
    assert_dependents_follow_rules(runner, result, "r2", block, "r1")
    check_invariants(runner, "r1", "r2")
    (event,) = runner.events(EventKind.DIAGNOSIS)
    assert (event.block, event.run_id, event.data["root_cause"]) == (block, "r2", "code_bug")
    assert event.data["status"] == "accepted"
    folder = runner.root / "diagnoses" / block / "r2"
    diagnosis = json.loads((folder / "diagnosis.json").read_text(encoding="utf-8"))
    assert diagnosis["root_cause"] == "code_bug" and diagnosis["model"] == "recorded"
    evidence = json.loads((folder / "evidence.json").read_text(encoding="utf-8"))
    code = next(i for i in evidence["items"] if i["kind"] == "code_change")
    assert code["data"]["changed"] is True


@by_role
def test_9b_diagnosis_failure_never_fails_the_pipeline(runner, role: str) -> None:
    block = representative(SPEC, role)
    auto(runner, block)

    def broken(g, block_name, run_id):
        raise RuntimeError("diagnosis service down")

    runner.diagnoser = broken
    runner.run("r1")
    runner.inject_fault(block, crash())
    result = runner.run("r2")

    assert result[block].outcome == "ROLLBACK"
    assert_dependents_follow_rules(runner, result, "r2", block, "r1")
    check_invariants(runner, "r1", "r2")
    warnings = [e for e in runner.events(EventKind.WARN, block=block) if e.data.get("agent")]
    assert [w.data["error"] for w in warnings] == ["RuntimeError: diagnosis service down"]
    assert runner.events(EventKind.DIAGNOSIS) == []


def test_9c_auto_diagnosis_is_opt_in(runner) -> None:
    block = representative(SPEC, "source")
    calls = []
    runner.diagnoser = lambda g, b, r: calls.append((b, r))
    runner.run("r1")
    runner.inject_fault(block, crash())
    assert runner.run("r2")[block].outcome == "ROLLBACK"
    assert calls == []
