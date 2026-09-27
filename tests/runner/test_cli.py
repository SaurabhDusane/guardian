import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from guardian.runner.cli import app, find_spec
from guardian.runner.spec_loader import load_spec

from ..helpers.roles import block_params

runner = CliRunner()
DEMO = load_spec(find_spec(Path("demo/pipeline.yaml")))


def invoke(*args: str):
    return runner.invoke(app, list(args), env={"COLUMNS": "200"})


def test_run_status_set_status_replay(tmp_path) -> None:
    root = str(tmp_path / "store")
    result = invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    assert result.exit_code == 0, result.output
    for block in ("b1_ingest", "b6_enrich", "b8_aggregate"):
        assert block in result.output
    assert "PASS" in result.output and "rows quarantined" in result.output

    result = invoke("set-status", "b6_enrich", "out", "--root", root)
    assert result.exit_code == 0 and "OUT" in result.output

    result = invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2")
    assert result.exit_code == 0, result.output
    assert "SKIPPED" in result.output
    assert "fallback for b6_enrich" in result.output

    result = invoke("status", "--root", root)
    assert result.exit_code == 0
    assert "OUT" in result.output and "b2_parse" in result.output

    result = invoke("replay", "b2_parse", "--root", root)
    assert result.exit_code == 0
    assert "replayed" in result.output


def test_missing_spec(tmp_path) -> None:
    result = invoke("run", "nope.yaml", "--root", str(tmp_path))
    assert result.exit_code != 0


def test_status_without_prior_run(tmp_path) -> None:
    result = invoke("status", "--root", str(tmp_path))
    assert result.exit_code != 0


def test_set_status_rejects_degraded(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root)
    result = invoke("set-status", "b6_enrich", "DEGRADED", "--root", root)
    assert result.exit_code == 2


def test_run_with_fault_reroutes(tmp_path) -> None:
    root = str(tmp_path)
    result = invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--fault", "b6_enrich:corrupt:0.5:region"
    )
    assert result.exit_code == 0, result.output
    assert "ROLLBACK" in result.output and "fallback for b6_enrich" in result.output


def test_run_with_bad_fault(tmp_path) -> None:
    result = invoke("run", "demo/pipeline.yaml", "--root", str(tmp_path), "--fault", "nope:crash")
    assert result.exit_code != 0


def test_replay_twice_and_refresh_only(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    invoke(
        "run",
        "demo/pipeline.yaml",
        "--root",
        root,
        "--run-id",
        "r2",
        "--fault",
        "b6_enrich:corrupt:0.5:region,segment",
    )
    first = invoke("replay", "b6_enrich", "--root", root)
    assert "upserted into new last-good snapshot" in first.output
    second = invoke("replay", "b6_enrich", "--root", root)
    assert "nothing to replay" in second.output
    result = invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r3", "--only", "b8_aggregate"
    )
    assert result.exit_code == 0, result.output
    assert "b8_aggregate" in result.output and "b1_ingest" not in result.output


def test_only_rejects_unknown_block(tmp_path) -> None:
    result = invoke("run", "demo/pipeline.yaml", "--root", str(tmp_path), "--only", "nope")
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "args",
    [
        ["replay", "no_such_block"],
        ["set-status", "no_such_block", "OUT"],
        ["run", "demo/pipeline.yaml", "--only", "no_such_block"],
        ["run", "demo/pipeline.yaml", "--fault", "no_such_block:crash"],
    ],
    ids=["replay", "set-status", "run-only", "run-fault"],
)
def test_block_commands_reject_unknown_block_clearly(tmp_path, args) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    result = invoke(*args, "--root", root)
    assert result.exit_code != 0
    assert "no_such_block" in result.output


@pytest.mark.parametrize("block", block_params(DEMO))
def test_block_commands_work_for_every_block(tmp_path, block) -> None:
    root = str(tmp_path)
    assert invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1").exit_code == 0
    assert invoke("set-status", block, "OUT", "--root", root).exit_code == 0
    assert invoke("set-status", block, "HEALTHY", "--root", root).exit_code == 0
    result = invoke("replay", block, "--root", root)
    assert result.exit_code == 0, result.output
    result = invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2",
        "--fault", f"{block}:crash", "--only", block,
    )  # fmt: skip
    assert result.exit_code == 0 and "ROLLBACK" in result.output, result.output


def test_stale_reads_are_visible_in_the_summary(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    result = invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2", "--fault", "b2_parse:crash"
    )
    assert "b2_parse@r1 (stale)" in result.output


def test_shadow_commands_end_to_end(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r0")
    result = invoke("shadow", "start", "b6_enrich", "v2", "--root", root)
    assert result.exit_code == 0 and "shadowing v2 next to live v1" in result.output
    result = invoke("shadow", "status", "--root", root)
    assert "b6_enrich" in result.output and "v2" in result.output
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    result = invoke("shadow", "status", "b6_enrich", "--root", root)
    assert result.exit_code == 0 and "PARITY" in result.output and "r1" in result.output
    result = invoke("shadow", "promote", "b6_enrich", "--root", root)
    assert result.exit_code == 1 and "needs approval" in result.output
    result = invoke("shadow", "promote", "b6_enrich", "--approve", "--root", root)
    assert result.exit_code == 0 and "promoted v1 -> v2" in result.output
    result = invoke("status", "--root", root)
    assert "v2 (spec: v1)" in result.output  # the registry overrides the spec's active
    result = invoke("shadow", "rollback", "b6_enrich", "--root", root)
    assert result.exit_code == 0 and "rolled back v2 -> v1" in result.output


def test_shadow_commands_reject_unknown_block_and_version(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r0")
    for args in (
        ["shadow", "start", "no_such_block", "v2"],
        ["shadow", "status", "no_such_block"],
        ["shadow", "promote", "no_such_block"],
        ["shadow", "rollback", "no_such_block"],
        ["shadow", "stop", "no_such_block"],
    ):
        result = invoke(*args, "--root", root)
        assert result.exit_code == 2 and "no_such_block" in result.output, args
    result = invoke("shadow", "start", "b6_enrich", "v9", "--root", root)
    assert result.exit_code == 2 and "no version 'v9'" in result.output
    result = invoke("shadow", "start", "b3_standardize", "v2", "--root", root)
    assert result.exit_code == 2 and "no version" in result.output  # a block without versions


@pytest.mark.parametrize(
    "block",
    [b.name for b in load_spec(find_spec(Path("demo/pipeline.yaml"))).blocks if b.versions],
)
def test_shadow_start_works_for_every_versioned_block(tmp_path, block) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r0")
    assert invoke("shadow", "start", block, "v_bad", "--root", root).exit_code == 0
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    result = invoke("shadow", "status", block, "--root", root)
    assert "PARITY" in result.output and " no " in result.output  # v_bad is caught
    assert invoke("shadow", "stop", block, "--root", root).exit_code == 0


@pytest.mark.parametrize("block", block_params(DEMO))
def test_impact_and_lineage_work_for_every_block(tmp_path, block) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2", "--fault", f"{block}:crash"
    )
    result = invoke("impact", block, "--root", root)
    assert result.exit_code == 0, result.output
    assert f"{block}: 1 degraded run(s)" in result.output
    result = invoke("impact", block, "--since", "r2", "--root", root)
    assert result.exit_code == 0
    assert "│ r1 " not in result.output and "│ r2 " in result.output  # the run column
    result = invoke("lineage", block, "r1", "--root", root)
    assert result.exit_code == 0 and f"{block}@r1" in result.output and "FRESH" in result.output


def test_impact_lists_the_fallback_reader(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2", "--fault", "b6_enrich:crash"
    )
    result = invoke("impact", "b6_enrich", "--root", root)
    assert "b8_aggregate" in result.output and "FALLBACK" in result.output
    result = invoke("lineage", "b8_aggregate", "r2", "--root", root)
    assert "as b6_enrich, via adapter demo.blocks:b5_to_b6_shape" in result.output


def test_impact_and_lineage_reject_unknown_names(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    for args in (["impact", "no_such_block"], ["lineage", "no_such_block", "r1"]):
        result = invoke(*args, "--root", root)
        assert result.exit_code == 2 and "no_such_block" in result.output
    result = invoke("impact", "b1_ingest", "--since", "zz", "--root", root)
    assert result.exit_code == 2 and "no run 'zz'" in result.output
    result = invoke("lineage", "b1_ingest", "zz", "--root", root)
    assert result.exit_code == 2 and "no provenance" in result.output


def test_diagnose_command(tmp_path) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    invoke(
        "run",
        "demo/pipeline.yaml",
        "--root",
        root,
        "--run-id",
        "r2",
        "--fault",
        "b4_clean:code_bug",
    )
    result = invoke("diagnose", "b4_clean", "--root", root, "--evidence-only")
    assert result.exit_code == 0, result.output
    assert "E1" in result.output and "code_change" in result.output
    assert (tmp_path / "diagnoses" / "b4_clean" / "r2" / "evidence.json").exists()

    result = runner.invoke(
        app, ["diagnose", "b4_clean", "--root", root], env={"GUARDIAN_LLM_PROVIDER": ""}
    )
    assert result.exit_code == 1 and "GUARDIAN_LLM_PROVIDER" in result.output

    recorded = tmp_path / "recorded.json"
    answer = {
        "root_cause": "code_bug",
        "confidence": 0.9,
        "summary": "Code changed since r1.",
        "claims": [{"statement": "fingerprint changed", "evidence": ["E1"]}],
    }
    recorded.write_text(
        json.dumps({"model": "rec", "responses": {"b4_clean/r2": json.dumps(answer)}}),
        encoding="utf-8",
    )
    result = invoke(
        "diagnose",
        "b4_clean",
        "--run",
        "r2",
        "--root",
        root,
        "--provider",
        "fake",
        "--fake-responses",
        str(recorded),
    )
    assert result.exit_code == 0, result.output
    assert "root cause: code_bug" in result.output and "accepted" in result.output
    saved = json.loads(
        (tmp_path / "diagnoses" / "b4_clean" / "r2" / "diagnosis.json").read_text(encoding="utf-8")
    )
    assert saved["root_cause"] == "code_bug"

    for args, code, text in (
        (["diagnose", "no_such_block"], 2, "no_such_block"),
        (["diagnose", "b4_clean", "--run", "zz", "--evidence-only"], 1, "no recorded run 'zz'"),
    ):
        result = invoke(*args, "--root", root)
        assert result.exit_code == code and text in result.output, result.output


@pytest.mark.parametrize("block", block_params(DEMO))
def test_diagnose_evidence_works_for_every_block(tmp_path, block) -> None:
    root = str(tmp_path)
    invoke("run", "demo/pipeline.yaml", "--root", root, "--run-id", "r1")
    invoke(
        "run", "demo/pipeline.yaml", "--root", root, "--run-id", "r2", "--fault", f"{block}:crash"
    )
    result = invoke("diagnose", block, "--root", root, "--evidence-only")
    assert result.exit_code == 0, result.output
    assert f"{block} on run r2: ROLLBACK" in result.output
    evidence = json.loads(
        (tmp_path / "diagnoses" / block / "r2" / "evidence.json").read_text(encoding="utf-8")
    )
    assert evidence["block"] == block and evidence["items"][0]["id"] == "E1"


def test_propose_command(tmp_path, project_repo) -> None:
    from ..agent.helpers import answer, revert_fix
    from ..helpers.project_repo import SPEC_REL
    from ..helpers.roles import representative

    spec_file = project_repo / SPEC_REL
    pipeline = load_spec(spec_file)
    block = representative(pipeline, "unprotected")
    root = str(tmp_path / "store")
    invoke("run", str(spec_file), "--root", root, "--run-id", "r1")
    invoke("run", str(spec_file), "--root", root, "--run-id", "r2", "--fault", f"{block}:code_bug")
    recorded = tmp_path / "recorded.json"
    responses = {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": revert_fix(pipeline, block)}
    recorded.write_text(json.dumps({"model": "rec", "responses": responses}), encoding="utf-8")

    fake = ["--provider", "fake", "--fake-responses", str(recorded)]
    result = invoke("propose", block, "--root", root, *fake)
    assert result.exit_code == 0, result.output
    assert "ready" in result.output and "new version: fix1" in result.output
    assert "Dry run: GitHub was not touched" in result.output
    assert (tmp_path / "store" / "proposals" / block / "r2" / "pr_body.md").exists()

    result = invoke("propose", "no_such_block", "--root", root, *fake)
    assert result.exit_code == 2 and "no_such_block" in result.output
    result = runner.invoke(
        app, ["propose", block, "--root", root], env={"GUARDIAN_LLM_PROVIDER": ""}
    )
    assert result.exit_code == 1 and "GUARDIAN_LLM_PROVIDER" in result.output


def test_eval_fix_command(tmp_path, project_repo) -> None:
    from ..agent.helpers import answer, revert_fix
    from ..helpers.project_repo import SPEC_REL
    from ..helpers.roles import representative

    spec_file = project_repo / SPEC_REL
    pipeline = load_spec(spec_file)
    block = representative(pipeline, "leaf")
    recorded = tmp_path / "recorded.json"
    case = f"{block}:code_bug"
    responses = {case: answer("code_bug"), f"{case}:fix": revert_fix(pipeline, block)}
    recorded.write_text(json.dumps({"model": "rec", "responses": responses}), encoding="utf-8")
    out = tmp_path / "bench" / "agent_fix_eval.md"
    result = invoke(
        "eval",
        "fix",
        str(spec_file),
        "--block",
        block,
        "--out",
        str(out),
        "--provider",
        "fake",
        "--fake-responses",
        str(recorded),
    )
    assert result.exit_code == 0, result.output
    assert "fix success 1/1 (100.0%)" in result.output
    assert "| fix success | 100.0% (1/1) |" in out.read_text(encoding="utf-8")
