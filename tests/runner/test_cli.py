from typer.testing import CliRunner

from guardian.runner.cli import app

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
