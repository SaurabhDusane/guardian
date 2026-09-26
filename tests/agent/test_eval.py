"""The diagnosis eval: labeled cases for every block x fault type, scored against a
FakeClient with recorded (scripted) answers. No network."""

from __future__ import annotations

import json
from collections import Counter

import pytest
from typer.testing import CliRunner

from guardian.agent.diagnose import BAD_CITATION, INVALID_RESPONSE, ROOT_CAUSES, FakeClient
from guardian.agent.eval import EXPECTED, FAULT_TYPES, generate_cases, run_eval
from guardian.runner.cli import app

from ..helpers.roles import representative, roles_of
from .helpers import ROLES, SPEC, answer


@pytest.fixture(scope="module")
def cases(tmp_path_factory):
    return generate_cases(SPEC, tmp_path_factory.mktemp("eval"))


def test_a_case_for_every_block_and_fault_type(cases) -> None:
    assert [(c.block, c.fault) for c in cases] == [
        (b, f) for b in SPEC.block_names for f in FAULT_TYPES
    ]
    assert len({c.case_id for c in cases}) == len(cases)
    for c in cases:
        assert c.label == EXPECTED[c.fault]
        assert set(c.roles) == roles_of(SPEC, c.block)
        assert c.outcome == "ROLLBACK", c.case_id  # every fault makes its block fail
        assert c.bundle.block == c.block
    assert {r for c in cases for r in c.roles} == set(ROLES)
    assert EXPECTED == {
        "schema_drift": "schema_change",
        "corrupt_rows": "upstream_data_drift",
        "null_burst": "upstream_data_drift",
        "code_bug": "code_bug",
    }


def test_every_case_carries_the_evidence_that_separates_its_label(cases) -> None:
    """A correct diagnosis is possible from the bundle alone: schema changes show in the
    schema diff, code bugs in the code fingerprint, data drift in neither."""
    for c in cases:
        schema = c.bundle.of_kind("schema_diff")[0].data
        code = c.bundle.of_kind("code_change")[0].data
        git = c.bundle.of_kind("git_history")[0].data
        assert bool(schema["missing_vs_last_good"]) is (c.label == "schema_change"), c.case_id
        assert code["changed"] is (c.label == "code_bug"), c.case_id
        assert git == {"available": False, "reason": "git lookup disabled"}
        assert c.fault not in c.bundle.to_json()  # the label is never spelled out


def _script(cases) -> dict[str, list[str]]:
    """Per case: a bad citation, a wrong cause, invalid JSON (twice), or the right cause."""
    responses = {}
    for i, c in enumerate(cases):
        wrong = ROOT_CAUSES[(ROOT_CAUSES.index(c.label) + 1) % 3]
        responses[c.case_id] = [
            [answer(c.label, ("E1", "E999"))],
            [answer(wrong, ("E1",))],
            ["{not json", "still not json"],
            [answer(c.label, ("E1", c.bundle.ids[-1]))],
            [answer(c.label, ("E2",))],
        ][i % 5]
    return responses


def test_report_metrics_match_the_scripted_answers(cases) -> None:
    client = FakeClient(_script(cases), model="scripted")
    report = run_eval(cases, client)
    assert [call.key for call in client.calls if call.key] == [
        k for c in cases for k in [c.case_id] * (2 if cases.index(c) % 5 == 2 else 1)
    ]

    # Independent expectation from the script.
    predicted = {}
    for i, c in enumerate(cases):
        kind = i % 5
        predicted[c.case_id] = (
            c.label
            if kind >= 3
            else "unknown"
            if kind in (0, 2)
            else ROOT_CAUSES[(ROOT_CAUSES.index(c.label) + 1) % 3]
        )
    correct = [c for c in cases if predicted[c.case_id] == c.label]
    assert [r.predicted for r in report.results] == [predicted[c.case_id] for c in cases]
    assert report.accuracy == pytest.approx(len(correct) / len(cases))
    assert report.rejected == sum(1 for i in range(len(cases)) if i % 5 == 0)
    assert report.rejected_rate == pytest.approx(report.rejected / len(cases))
    assert report.rejections() == {
        BAD_CITATION: report.rejected,
        INVALID_RESPONSE: sum(1 for i in range(len(cases)) if i % 5 == 2),
    }
    for fault, (c_ok, n) in report.by_fault().items():
        members = [c for c in cases if c.fault == fault]
        assert (c_ok, n) == (sum(c in correct for c in members), len(members))
    for role, (c_ok, n) in report.by_role().items():
        members = [c for c in cases if role in c.roles]
        assert (c_ok, n) == (sum(c in correct for c in members), len(members))
    assert set(report.by_role()) == set(ROLES)

    matrix = report.confusion()
    expected = Counter((c.label, predicted[c.case_id]) for c in cases)
    for label in ROOT_CAUSES:
        for pred in ROOT_CAUSES:
            assert matrix[label][pred] == expected[(label, pred)]
    assert sum(sum(row.values()) for row in matrix.values()) == len(cases)

    md = report.to_markdown()
    for heading in ("## By fault type", "## By DAG role", "## Confusion matrix", "## Cases"):
        assert heading in md
    assert f"({len(correct)}/{len(cases)})" in md
    data = report.to_dict()
    assert data["cases"] == len(cases) and len(data["results"]) == len(cases)
    json.dumps(data)


def test_a_perfect_client_scores_one(cases) -> None:
    client = FakeClient({c.case_id: answer(c.label, ("E1",)) for c in cases})
    report = run_eval(cases, client)
    assert report.accuracy == 1.0 and report.rejected == 0
    assert all(c == n for c, n in report.by_role().values())
    matrix = report.confusion()
    assert all(matrix[a][b] == 0 for a in ROOT_CAUSES for b in ROOT_CAUSES if a != b)


def test_eval_cli_with_recorded_answers(tmp_path) -> None:
    block = representative(SPEC, "leaf")
    recordings = tmp_path / "recorded.json"
    recordings.write_text(
        json.dumps(
            {
                "model": "recorded-model",
                "responses": {f"{block}:{f}": answer(EXPECTED[f]) for f in FAULT_TYPES},
            }
        ),
        encoding="utf-8",
    )
    out = tmp_path / "bench" / "agent_eval.md"
    rerecorded = tmp_path / "again.json"
    result = CliRunner().invoke(
        app,
        [
            "eval",
            "diagnose",
            "demo/pipeline.yaml",
            "--provider",
            "fake",
            "--fake-responses",
            str(recordings),
            "--block",
            block,
            "--out",
            str(out),
            "--record",
            str(rerecorded),
        ],
        env={"COLUMNS": "200"},
    )
    assert result.exit_code == 0, result.output
    assert "accuracy 4/4 (100.0%)" in result.output
    assert "Model: `recorded-model`" in out.read_text(encoding="utf-8")
    assert json.loads(out.with_suffix(".json").read_text(encoding="utf-8"))["accuracy"] == 1.0
    assert set(json.loads(rerecorded.read_text(encoding="utf-8"))["responses"]) == {
        f"{block}:{f}" for f in FAULT_TYPES
    }

    bad = CliRunner().invoke(
        app,
        [
            "eval",
            "diagnose",
            "--provider",
            "fake",
            "--fake-responses",
            str(recordings),
            "--block",
            "no_such_block",
            "--out",
            str(out),
        ],
    )
    assert bad.exit_code == 2 and "no_such_block" in bad.output
    unconfigured = CliRunner().invoke(
        app, ["eval", "diagnose", "--out", str(out)], env={"GUARDIAN_LLM_PROVIDER": ""}
    )
    assert unconfigured.exit_code == 1 and "GUARDIAN_LLM_PROVIDER" in unconfigured.output
