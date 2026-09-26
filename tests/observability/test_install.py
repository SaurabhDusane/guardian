"""Exporters are off by default, configured by env, and can never fail a run."""

from __future__ import annotations

import json

import pytest

from guardian.core.events import EventKind
from guardian.core.guardian import Guardian
from guardian.core.models import GuardianError
from guardian.observability import install
from guardian.observability.openlineage import OpenLineageEmitter
from guardian.runner.executor import Executor

from .helpers import SPEC


def test_everything_is_off_by_default(tmp_path) -> None:
    with Guardian(SPEC, tmp_path / "g") as g:
        assert install(g, env={}) == [] and g.events.listeners == []


def test_openlineage_to_a_file(tmp_path) -> None:
    path = tmp_path / "lineage.jsonl"
    env = {"GUARDIAN_OPENLINEAGE_FILE": str(path), "GUARDIAN_OPENLINEAGE_NAMESPACE": "prod"}
    with Guardian(SPEC, tmp_path / "g") as g:
        (listener,) = install(g, env=env)
        assert isinstance(listener, OpenLineageEmitter)
        Executor(g).run("r1")
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert events and {e["job"]["namespace"] for e in events} == {"prod"}
    assert sum(e["eventType"] == "COMPLETE" for e in events) == len(SPEC.blocks) + 1


def test_otel_from_env(tmp_path) -> None:
    pytest.importorskip("opentelemetry.sdk")
    from guardian.observability.otel import SpanEmitter

    with Guardian(SPEC, tmp_path / "g") as g:
        (listener,) = install(g, env={"GUARDIAN_OTEL": "console"})
        assert isinstance(listener, SpanEmitter)
        with pytest.raises(GuardianError, match="otlp"):
            install(g, env={"GUARDIAN_OTEL": "carrier-pigeon"})


def test_listeners_see_sampled_events_and_never_fail_a_run(tmp_path) -> None:
    seen = []

    def broken(event):
        raise RuntimeError("exporter down")

    with Guardian(SPEC, tmp_path / "g", sample_rate=0.0) as g:
        g.events.add_listener(broken)
        g.events.add_listener(seen.append)
        report = Executor(g).run("r1")
        assert report.ok  # the broken exporter changed nothing
        assert g.events.listener_errors > 0
        stored = g.events.query(kind=EventKind.BLOCK_OUTCOME)
    outcomes = [e for e in seen if e.kind is EventKind.BLOCK_OUTCOME]
    assert len(outcomes) == len(SPEC.blocks) and stored == []  # sampled out of storage only


def test_cli_run_with_openlineage_env(tmp_path, monkeypatch) -> None:
    from typer.testing import CliRunner

    from guardian.runner.cli import app

    path = tmp_path / "ol.jsonl"
    monkeypatch.setenv("GUARDIAN_OPENLINEAGE_FILE", str(path))
    result = CliRunner().invoke(
        app, ["run", "demo/pipeline.yaml", "--root", str(tmp_path / "g"), "--run-id", "r1"]
    )
    assert result.exit_code == 0, result.output
    assert path.exists() and '"eventType": "COMPLETE"' in path.read_text(encoding="utf-8")
    monkeypatch.delenv("GUARDIAN_OPENLINEAGE_FILE")
    monkeypatch.setenv("GUARDIAN_OTEL", "nonsense")
    result = CliRunner().invoke(app, ["run", "demo/pipeline.yaml", "--root", str(tmp_path / "g")])
    assert result.exit_code == 2 and "GUARDIAN_OTEL" in result.output
