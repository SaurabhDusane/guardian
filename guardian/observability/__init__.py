"""Optional exporters for Guardian events: OpenLineage run events and OpenTelemetry spans.

Both are off by default and switched on by environment variables; ``install`` attaches
the configured ones to a Guardian's event logger (the CLI and the Dagster definitions
call it). Core never imports this package, and OpenTelemetry is imported only when it
is enabled (it needs the ``observability`` extra).

OpenLineage (standard library only):
    GUARDIAN_OPENLINEAGE_URL        POST events to <url>/api/v1/lineage (e.g. Marquez)
    GUARDIAN_OPENLINEAGE_FILE       or append them to a JSON-lines file
    GUARDIAN_OPENLINEAGE_CONSOLE    or print them (set to 1)
    GUARDIAN_OPENLINEAGE_NAMESPACE  job and dataset namespace (default "guardian")
    GUARDIAN_OPENLINEAGE_API_KEY    optional bearer token

OpenTelemetry:
    GUARDIAN_OTEL                   "otlp" or "console"
    OTEL_EXPORTER_OTLP_ENDPOINT     standard OTLP settings (default http://localhost:4318)
    OTEL_SERVICE_NAME               service name (default "guardian")
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from guardian.core.events import Event
from guardian.core.models import GuardianError
from guardian.observability.openlineage import (
    ConsoleTransport,
    FileTransport,
    HttpTransport,
    OpenLineageEmitter,
)

Listener = Callable[[Event], None]


def openlineage_from_env(pipeline: str, env: Mapping[str, str]) -> OpenLineageEmitter | None:
    transport: Callable[[dict[str, Any]], None]
    if env.get("GUARDIAN_OPENLINEAGE_URL"):
        transport = HttpTransport(
            env["GUARDIAN_OPENLINEAGE_URL"], api_key=env.get("GUARDIAN_OPENLINEAGE_API_KEY")
        )
    elif env.get("GUARDIAN_OPENLINEAGE_FILE"):
        transport = FileTransport(env["GUARDIAN_OPENLINEAGE_FILE"])
    elif (env.get("GUARDIAN_OPENLINEAGE_CONSOLE") or "").lower() in ("1", "true", "yes"):
        transport = ConsoleTransport()
    else:
        return None
    namespace = env.get("GUARDIAN_OPENLINEAGE_NAMESPACE") or "guardian"
    return OpenLineageEmitter(pipeline, transport, namespace=namespace)


def otel_from_env(pipeline: str, env: Mapping[str, str]) -> Listener | None:
    if not (env.get("GUARDIAN_OTEL") or "").strip():
        return None
    try:
        from guardian.observability.otel import SpanEmitter, provider_from_env
    except ImportError as exc:
        raise GuardianError(
            "GUARDIAN_OTEL is set but OpenTelemetry is not installed: uv sync --extra observability"
        ) from exc
    try:
        provider = provider_from_env(env)
    except ValueError as exc:
        raise GuardianError(str(exc)) from exc
    return SpanEmitter(provider.get_tracer("guardian"), pipeline)


def install(guardian: Any, env: Mapping[str, str] | None = None) -> list[Listener]:
    """Attach the exporters enabled in ``env`` (default: the environment) to
    ``guardian``'s events; returns them (empty when everything is off)."""
    env = os.environ if env is None else env
    pipeline = guardian.spec.name
    listeners = [
        listener
        for listener in (openlineage_from_env(pipeline, env), otel_from_env(pipeline, env))
        if listener is not None
    ]
    for listener in listeners:
        guardian.events.add_listener(listener)
    return listeners
