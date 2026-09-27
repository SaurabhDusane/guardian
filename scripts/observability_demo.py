"""Bring up the local observability stack, run the demo against it, print what to open.

    uv sync --extra observability
    uv run python scripts/observability_demo.py            # up + demo, leaves it running
    uv run python scripts/observability_demo.py --down     # tear it down (and its data)

The demo is two runs of demo/pipeline.yaml exporting OpenLineage to Marquez and spans
to Tempo: r1 is clean, and in r2 the demo's fallback-protected block crashes, so its
fallback consumer reads the fallback source instead. The script prints the Marquez and
Grafana URLs for the README screenshots.

The same functions drive tests/docker/test_observability_stack.py.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "observability" / "docker-compose.yml"
PROJECT = "guardian-observability"
SPEC = "demo/pipeline.yaml"

MARQUEZ_API = "http://localhost:5000"
MARQUEZ_WEB = "http://localhost:3000"
TEMPO_API = "http://localhost:3200"
TEMPO_OTLP = "http://localhost:4318"
GRAFANA = "http://localhost:3001"
HEALTH = {
    "marquez-api": f"{MARQUEZ_API}/api/v1/namespaces",
    "marquez-web": MARQUEZ_WEB,
    "tempo": f"{TEMPO_API}/ready",
    "grafana": f"{GRAFANA}/api/health",
}

# Local services must never go through an outbound proxy.
_LOCAL = urllib.request.build_opener(urllib.request.ProxyHandler({}))


# ------------------------------------------------------------------ docker compose


def compose(*args: str, project: str = PROJECT, timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(COMPOSE), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def docker_available() -> bool:
    try:
        done = subprocess.run(["docker", "info"], capture_output=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return done.returncode == 0


def up(project: str = PROJECT) -> None:
    done = compose("up", "-d", project=project)
    if done.returncode != 0:
        raise RuntimeError(f"docker compose up failed:\n{done.stderr[-3000:]}")


def down(project: str = PROJECT) -> None:
    compose("down", "-v", "--remove-orphans", project=project, timeout=300)


def logs(project: str = PROJECT, tail: int = 80) -> str:
    return compose("logs", "--no-color", f"--tail={tail}", project=project, timeout=120).stdout


# ------------------------------------------------------------------ HTTP


def get(url: str, *, params: dict[str, str] | None = None) -> Any:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with _LOCAL.open(request, timeout=15) as response:
        body = response.read()
    return json.loads(body) if body else {}


def wait_healthy(timeout: float = 300.0) -> dict[str, float]:
    """Poll every service's health URL until all answer 200; seconds each took."""
    start = time.monotonic()
    ready: dict[str, float] = {}
    while len(ready) < len(HEALTH):
        for name, url in HEALTH.items():
            if name in ready:
                continue
            try:
                with _LOCAL.open(url, timeout=5) as response:
                    if response.status == 200:
                        ready[name] = round(time.monotonic() - start, 1)
            except (OSError, urllib.error.URLError):
                pass
        if len(ready) < len(HEALTH):
            if time.monotonic() - start > timeout:
                missing = sorted(set(HEALTH) - set(ready))
                raise TimeoutError(f"not healthy after {timeout:.0f}s: {missing}")
            time.sleep(2)
    return ready


def poll(fn: Any, timeout: float = 60.0, interval: float = 2.0) -> Any:
    """Call ``fn`` until it returns something truthy (backends index asynchronously)."""
    deadline = time.monotonic() + timeout
    while True:
        result = fn()
        if result or time.monotonic() > deadline:
            return result
        time.sleep(interval)


# ------------------------------------------------------------------ the demo


@dataclass(frozen=True)
class Demo:
    namespace: str  # OpenLineage namespace in Marquez
    service: str  # OpenTelemetry service name in Tempo
    pipeline: str
    failed: str  # the fallback-protected block that crashes in r2
    consumer: str  # its dependent that reads the fallback source instead
    fallback_source: str
    adapter: str | None
    blocks: tuple[str, ...]


def demo_blocks() -> tuple[str, str, str, str | None, tuple[str, ...], str]:
    """The demo's fallback-protected block, its fallback consumer and source, chosen by
    DAG role (no block names hardcoded)."""
    sys.path.insert(0, str(ROOT))
    from guardian.core.dag import blocks_by_role
    from guardian.runner.cli import find_spec
    from guardian.runner.spec_loader import load_spec

    spec = load_spec(find_spec(Path(SPEC)))
    failed = blocks_by_role(spec)["fallback_protected"][0]
    consumer = next(b for b in spec.dependents(failed) if b.fallback_for(failed))
    edge = consumer.fallback_for(failed)
    assert edge is not None
    return failed, consumer.name, edge.source, edge.adapter, spec.block_names, spec.name


def run_demo(root: Path, *, namespace: str | None = None, service: str | None = None) -> Demo:
    """r1 clean, r2 with the fallback-protected block crashing; both runs export."""
    suffix = uuid.uuid4().hex[:8]
    namespace = namespace or f"guardian-demo-{suffix}"
    service = service or f"guardian-demo-{suffix}"
    failed, consumer, source, adapter, blocks, pipeline = demo_blocks()
    env = {
        **os.environ,
        "GUARDIAN_OPENLINEAGE_URL": MARQUEZ_API,
        "GUARDIAN_OPENLINEAGE_NAMESPACE": namespace,
        "GUARDIAN_OTEL": "otlp",
        "OTEL_EXPORTER_OTLP_ENDPOINT": TEMPO_OTLP,
        "OTEL_SERVICE_NAME": service,
    }
    env["NO_PROXY"] = env["no_proxy"] = ",".join(
        filter(None, [env.get("NO_PROXY") or env.get("no_proxy"), "localhost", "127.0.0.1"])
    )
    for run_id, extra in (("r1", []), ("r2", ["--fault", f"{failed}:crash"])):
        done = subprocess.run(
            [sys.executable, "-m", "guardian.runner.cli", "run", SPEC, "--root", str(root),
             "--run-id", run_id, *extra],
            cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=600, check=False,
        )  # fmt: skip
        if done.returncode != 0:
            raise RuntimeError(f"demo run {run_id} failed:\n{done.stdout}\n{done.stderr}")
    return Demo(namespace, service, pipeline, failed, consumer, source, adapter, blocks)


# ------------------------------------------------------------------ backend queries


def marquez_jobs(namespace: str) -> set[str]:
    data = get(f"{MARQUEZ_API}/api/v1/namespaces/{urllib.parse.quote(namespace)}/jobs")
    return {job["name"] for job in data.get("jobs", [])}


def marquez_run(namespace: str, job: str, run_id: str) -> dict[str, Any] | None:
    """The Marquez run of ``job`` for Guardian run ``run_id``."""
    path = f"namespaces/{urllib.parse.quote(namespace)}/jobs/{urllib.parse.quote(job)}/runs"
    for run in get(f"{MARQUEZ_API}/api/v1/{path}").get("runs", []):
        if run.get("facets", {}).get("guardian_decision", {}).get("run_id") == run_id:
            return run
    return None


def marquez_events(namespace: str, job: str) -> list[dict[str, Any]]:
    """The OpenLineage events Marquez stored for ``job`` (newest first)."""
    events = get(f"{MARQUEZ_API}/api/v1/events/lineage", params={"limit": "500"})
    return [
        e
        for e in events.get("events", [])
        if e["job"]["namespace"] == namespace and e["job"]["name"] == job
    ]


def tempo_trace_id(service: str, run_id: str) -> str | None:
    query = f'{{resource.service.name="{service}" && span.guardian.run_id="{run_id}"}}'
    traces = get(f"{TEMPO_API}/api/search", params={"q": query}).get("traces", [])
    return traces[0]["traceID"] if traces else None


def tempo_spans(trace_id: str) -> list[dict[str, Any]]:
    """Spans of a trace as {name, status, attributes}."""
    trace = get(f"{TEMPO_API}/api/traces/{trace_id}")
    spans = []
    for batch in trace.get("batches", trace.get("resourceSpans", [])):
        for scope in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
            for span in scope.get("spans", []):
                attributes = {
                    a["key"]: next(iter(a["value"].values()), None)
                    for a in span.get("attributes", [])
                }
                spans.append(
                    {
                        "name": span["name"],
                        "status": span.get("status", {}),
                        "attributes": attributes,
                    }
                )
    return spans


# ------------------------------------------------------------------ CLI


def urls(demo: Demo, trace_id: str | None) -> list[tuple[str, str]]:
    ns = urllib.parse.quote(demo.namespace)
    job = urllib.parse.quote(f"{demo.pipeline}.{demo.consumer}")
    out = [
        ("Marquez: lineage of the fallback consumer", f"{MARQUEZ_WEB}/lineage/job/{ns}/{job}"),
        ("Marquez: all jobs of the demo", f"{MARQUEZ_WEB}/?namespace={ns}"),
    ]
    if trace_id:
        panes = {
            "a": {
                "datasource": "tempo",
                "queries": [
                    {
                        "refId": "A",
                        "datasource": {"type": "tempo", "uid": "tempo"},
                        "queryType": "traceql",
                        "query": trace_id,
                    }
                ],
                "range": {"from": "now-1h", "to": "now"},
            }
        }
        query = urllib.parse.urlencode({"schemaVersion": 1, "panes": json.dumps(panes)})
        out.append(("Grafana: trace of run r2 (Tempo)", f"{GRAFANA}/explore?{query}"))
    out.append(("Grafana: search traces", f"{GRAFANA}/explore"))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--down", action="store_true", help="tear the stack down and exit")
    parser.add_argument("--timeout", type=float, default=300, help="health wait (seconds)")
    args = parser.parse_args(argv)
    if args.down:
        down()
        print("Stack stopped and its volumes removed.")
        return 0
    if not docker_available():
        print("Docker is not available (is the daemon running?)", file=sys.stderr)
        return 1
    print("Starting the stack (the first run pulls the images)...")
    up()
    print("Healthy after (s):", wait_healthy(args.timeout))
    root = ROOT / ".guardian" / "observability-demo"  # under the gitignored .guardian/
    demo = run_demo(root)
    print(
        f"Ran r1 (clean) and r2 ({demo.failed} crashes, {demo.consumer} reads "
        f"{demo.fallback_source} through the fallback) with namespace {demo.namespace}."
    )
    trace_id = poll(lambda: tempo_trace_id(demo.service, "r2"), timeout=60)
    print()
    for label, url in urls(demo, trace_id):
        print(f"{label}:\n  {url}")
    print(
        "\nIn Grafana's Explore, pick the Tempo data source and search for service "
        f"{demo.service!r} if the direct link does not open the trace."
    )
    print("Stop it with: uv run python scripts/observability_demo.py --down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
