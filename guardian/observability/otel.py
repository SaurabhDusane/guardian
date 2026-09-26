"""Guardian block runs as OpenTelemetry spans (needs the ``observability`` extra).

One span per block run, ``guardian.block <block>``, from Guardian's first report on
the block in that run to its BLOCK_OUTCOME, with the decision, status, quality,
version and row counts as attributes; a ROLLBACK or BLOCKED run has status ERROR.
When the standalone runner reports the pipeline run (RUN_STARTED / RUN_FINISHED),
block spans are children of a ``guardian.run <pipeline>`` span.
"""

from __future__ import annotations

import atexit
import os
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
)
from opentelemetry.trace import Status, StatusCode

from guardian.core.events import Event, EventKind
from guardian.observability.openlineage import RUN_EVENTS

FAILED = frozenset({"ROLLBACK", "BLOCKED"})


def _ns(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000_000)


class SpanEmitter:
    """An EventLogger listener that records one span per block run."""

    def __init__(self, tracer: trace.Tracer, pipeline: str) -> None:
        self.tracer = tracer
        self.pipeline = pipeline
        self._starts: dict[tuple[str, str], int] = {}
        self._runs: dict[str, Any] = {}  # run_id -> pipeline span

    def __call__(self, event: Event) -> None:
        if event.kind is EventKind.RUN_STARTED and event.run_id:
            self._runs[event.run_id] = self.tracer.start_span(
                f"guardian.run {self.pipeline}",
                start_time=_ns(event.ts),
                attributes={"guardian.pipeline": self.pipeline, "guardian.run_id": event.run_id},
            )
        elif event.kind is EventKind.RUN_FINISHED and event.run_id in self._runs:
            span = self._runs.pop(event.run_id)
            outcomes = event.data.get("outcomes", {})
            for outcome in ("PASS", "ROLLBACK", "SKIPPED", "BLOCKED"):
                span.set_attribute(
                    f"guardian.blocks.{outcome.lower()}",
                    sum(1 for v in outcomes.values() if v == outcome),
                )
            span.end(end_time=_ns(event.ts))
        elif event.block and event.run_id and event.kind in RUN_EVENTS:
            key = (event.block, event.run_id)
            self._starts.setdefault(key, _ns(event.ts))
            if event.kind is EventKind.BLOCK_OUTCOME:
                self._block_span(event, self._starts.pop(key))

    def _block_span(self, event: Event, start: int) -> None:
        d = event.data
        inputs = d.get("inputs", [])
        attributes = {
            "guardian.pipeline": self.pipeline,
            "guardian.block": event.block,
            "guardian.run_id": event.run_id,
            "guardian.decision": d.get("outcome"),
            "guardian.status": d.get("status"),
            "guardian.quality": d.get("quality"),
            "guardian.version": d.get("version") or "",
            "guardian.reason": d.get("reason") or "",
            "guardian.rows.out": int(d.get("rows_out") or 0),
            "guardian.rows.promoted": int(d.get("rows_promoted") or 0),
            "guardian.rows.quarantined": int(d.get("rows_quarantined") or 0),
            "guardian.inputs": [
                f"{i['upstream']} <- {i['block']}@{i['run_id']} ({i['read']})" for i in inputs
            ],
            "guardian.inputs.stale": sum(i["read"] == "STALE" for i in inputs),
            "guardian.inputs.fallback": sum(i["read"] == "FALLBACK" for i in inputs),
        }
        parent = self._runs.get(event.run_id)
        context = trace.set_span_in_context(parent) if parent is not None else None
        span = self.tracer.start_span(
            f"guardian.block {event.block}",
            context=context,
            start_time=start,
            attributes=attributes,
        )
        if d.get("outcome") in FAILED:
            span.set_status(Status(StatusCode.ERROR, d.get("reason") or d.get("outcome")))
        span.end(end_time=max(_ns(event.ts), start))


def provider_from_env(env: Mapping[str, str] | None = None) -> TracerProvider:
    """A tracer provider for ``GUARDIAN_OTEL`` = ``otlp`` (OTLP over HTTP; endpoint from
    the standard OTEL_EXPORTER_OTLP_* variables) or ``console``."""
    env = os.environ if env is None else env
    mode = (env.get("GUARDIAN_OTEL") or "").strip().lower()
    exporter: SpanExporter
    if mode == "otlp":
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter()
    elif mode == "console":
        exporter = ConsoleSpanExporter()
    else:
        raise ValueError(f"GUARDIAN_OTEL must be 'otlp' or 'console', got {mode!r}")
    resource = Resource.create({"service.name": env.get("OTEL_SERVICE_NAME") or "guardian"})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    atexit.register(provider.shutdown)  # flush spans when a short-lived CLI exits
    return provider
