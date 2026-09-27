"""The local observability stack, end to end (marker ``docker``; needs Docker).

    uv sync --extra observability
    uv run pytest -m docker

Brings up observability/docker-compose.yml (Marquez, Postgres, Tempo, Grafana) under a
separate compose project, waits for every service to be healthy, and runs the demo
pipeline twice with both exporters on: r1 clean, r2 with the fallback-protected block
crashing. It then checks:

- Marquez: the pipeline job and every block job exist, the fallback consumer's r2 run
  read the fallback source, and its stored OpenLineage event carries the FALLBACK input
  facet;
- Tempo: the r2 trace has one span per block (plus the pipeline span), ERROR on the
  crashed block only.

The stack is always torn down, including its volumes, even when a check fails. Ports
5000, 5001, 3000, 3001, 3200 and 4318 must be free.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from scripts import observability_demo as stack

PROJECT = "guardian-observability-test"

pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed"),
]


def test_compose_file_is_valid() -> None:
    done = stack.compose("config", "--quiet", project=PROJECT, timeout=60)
    assert done.returncode == 0, done.stderr


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    """The stack, up and healthy, after the two demo runs; always torn down."""
    if not stack.docker_available():  # checked here, not at import: collection stays fast
        pytest.skip("no Docker daemon")
    try:
        stack.up(PROJECT)
        try:
            stack.wait_healthy(timeout=360)
        except TimeoutError as exc:
            pytest.fail(f"{exc}\n\n{stack.logs(PROJECT)}")
        yield stack.run_demo(tmp_path_factory.mktemp("guardian"))
    finally:
        # CI sets GUARDIAN_DOCKER_LOGS to keep the services' logs, which teardown erases.
        if os.environ.get("GUARDIAN_DOCKER_LOGS"):
            path = Path(os.environ["GUARDIAN_DOCKER_LOGS"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(stack.logs(PROJECT, tail=400), encoding="utf-8")
        stack.down(PROJECT)


def test_marquez_has_the_jobs_and_the_fallback_facet(demo) -> None:
    expected = {demo.pipeline, *(f"{demo.pipeline}.{b}" for b in demo.blocks)}
    jobs = stack.poll(lambda: expected <= stack.marquez_jobs(demo.namespace), timeout=60)
    assert jobs, (expected, stack.marquez_jobs(demo.namespace))

    consumer = f"{demo.pipeline}.{demo.consumer}"
    run = stack.poll(lambda: stack.marquez_run(demo.namespace, consumer, "r2"), timeout=60)
    assert run is not None and run["state"] == "COMPLETED"
    decision = run["facets"]["guardian_decision"]
    assert (decision["outcome"], decision["quality"]) == ("PASS", "FALLBACK")
    inputs = [i["datasetVersionId"]["name"] for i in run["inputDatasetVersions"]]
    assert inputs == [f"{demo.pipeline}.{demo.fallback_source}"]

    events = stack.marquez_events(demo.namespace, consumer)
    (complete,) = [
        e
        for e in events
        if e["eventType"] == "COMPLETE"
        and e["run"]["facets"]["guardian_decision"]["run_id"] == "r2"
    ]
    (read,) = complete["inputs"]
    facet = read["facets"]["guardian_input"]
    assert read["name"] == f"{demo.pipeline}.{demo.fallback_source}"
    assert (facet["read"], facet["fallback"], facet["upstream"]) == ("FALLBACK", True, demo.failed)
    assert facet["adapter"] == demo.adapter

    failed = stack.marquez_run(demo.namespace, f"{demo.pipeline}.{demo.failed}", "r2")
    assert failed is not None and failed["state"] == "FAILED"


def test_tempo_has_one_span_per_block_and_the_failure(demo) -> None:
    trace_id = stack.poll(lambda: stack.tempo_trace_id(demo.service, "r2"), timeout=90)
    assert trace_id, f"no r2 trace for service {demo.service!r} in Tempo"
    spans = stack.poll(
        lambda: (s := stack.tempo_spans(trace_id)) and len(s) >= len(demo.blocks) + 1 and s,
        timeout=60,
    )
    blocks = {
        s["attributes"]["guardian.block"]: s for s in spans if "guardian.block" in s["attributes"]
    }
    assert sorted(blocks) == sorted(demo.blocks)  # exactly one span per block
    assert len([s for s in spans if s["name"] == f"guardian.run {demo.pipeline}"]) == 1
    for block, span in blocks.items():
        code = span["status"].get("code", "STATUS_CODE_UNSET")
        if block == demo.failed:
            assert code == "STATUS_CODE_ERROR" and span["status"].get("message") == "crash"
            assert span["attributes"]["guardian.decision"] == "ROLLBACK"
        else:
            assert code != "STATUS_CODE_ERROR", block
            assert span["attributes"]["guardian.decision"] == "PASS", block
    consumer = blocks[demo.consumer]["attributes"]
    assert consumer["guardian.quality"] == "FALLBACK"
