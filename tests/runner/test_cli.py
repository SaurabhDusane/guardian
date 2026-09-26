from pathlib import Path

import pytest
from typer.testing import CliRunner

from guardian.runner.cli import app, find_spec
from guardian.runner.spec_loader import load_spec

runner = CliRunner()


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


@pytest.mark.parametrize("block", load_spec(find_spec(Path("demo/pipeline.yaml"))).block_names)
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
