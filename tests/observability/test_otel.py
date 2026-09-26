"""OpenTelemetry spans per block run, under both runners (in-memory span exporter)."""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from guardian.demo.faults import corrupt_rows, crash
from guardian.observability.otel import SpanEmitter

from ..helpers.roles import dependents, representative
from .helpers import RUNNERS, SPEC, execute

by_runner = pytest.mark.parametrize("runner", RUNNERS)


def tracing() -> tuple[SpanEmitter, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return SpanEmitter(provider.get_tracer("test"), SPEC.name), exporter


def block_spans(exporter: InMemorySpanExporter) -> dict[str, object]:
    return {
        s.attributes["guardian.block"]: s
        for s in exporter.get_finished_spans()
        if s.name.startswith("guardian.block ")
    }


@by_runner
def test_one_span_per_block_run_with_decision_quality_and_rows(tmp_path, runner: str) -> None:
    root = tmp_path / "g"
    execute(runner, root, "r1", [])
    emitter, exporter = tracing()
    failed = representative(SPEC, "unprotected")
    execute(runner, root, "r2", [emitter], {failed: [corrupt_rows(0.6, seed=1, unique=True)]})

    spans = block_spans(exporter)
    assert set(spans) == set(SPEC.block_names)
    for block, span in spans.items():
        a = span.attributes
        assert (a["guardian.pipeline"], a["guardian.run_id"]) == (SPEC.name, "r2")
        assert span.start_time <= span.end_time
        assert a["guardian.rows.out"] >= a["guardian.rows.promoted"] >= 0
        if block == failed:
            assert a["guardian.decision"] == "ROLLBACK"
            assert span.status.status_code is StatusCode.ERROR
            assert a["guardian.rows.promoted"] == 0
            assert a["guardian.rows.quarantined"] == a["guardian.rows.out"] > 0
            assert "bad-row fraction" in a["guardian.reason"]
        else:
            assert a["guardian.decision"] == "PASS" and a["guardian.status"] == "HEALTHY"
            assert span.status.status_code is not StatusCode.ERROR
        if block in dependents(SPEC, failed):
            assert a["guardian.quality"] == "STALE" and a["guardian.inputs.stale"] == 1
            assert any(f"<- {failed}@r1 (STALE)" in i for i in a["guardian.inputs"])

    runs = [s for s in exporter.get_finished_spans() if s.name.startswith("guardian.run ")]
    if runner == "standalone":  # block spans are children of the pipeline run's span
        (run,) = runs
        assert all(s.parent.span_id == run.context.span_id for s in spans.values())
        assert run.attributes["guardian.blocks.rollback"] == 1
    else:
        assert runs == []


def test_fallback_reads_are_counted(tmp_path) -> None:
    protected = representative(SPEC, "fallback_protected")
    reader = next(d for d in dependents(SPEC, protected) if SPEC.block(d).fallback_for(protected))
    root = tmp_path / "g"
    execute("standalone", root, "r1", [])
    emitter, exporter = tracing()
    execute("standalone", root, "r2", [emitter], {protected: [crash()]})
    a = block_spans(exporter)[reader].attributes
    assert (a["guardian.quality"], a["guardian.inputs.fallback"]) == ("FALLBACK", 1)
