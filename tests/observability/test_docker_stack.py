"""Opt-in: the local observability stack (marker ``docker``; needs Docker).

    uv run pytest -m docker

The first test only needs the docker CLI. The second starts Marquez and Tempo from
observability/docker-compose.yml (ports 5000 and 4318/3200 must be free), runs the demo
pipeline with both exporters on, checks that Marquez stored the block jobs and that
Tempo received the spans, then tears the stack down.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
COMPOSE = ROOT / "observability" / "docker-compose.yml"
HAS_DOCKER = shutil.which("docker") is not None


def daemon_up() -> bool:
    if not HAS_DOCKER:
        return False
    done = subprocess.run(["docker", "info"], capture_output=True, timeout=30, check=False)
    return done.returncode == 0


pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not HAS_DOCKER, reason="docker CLI not installed"),
]


def compose(*args: str, timeout: int = 600) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def test_compose_file_is_valid() -> None:
    done = compose("config", "--quiet", timeout=60)
    assert done.returncode == 0, done.stderr


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read() or b"{}")


def _wait(url: str, seconds: int = 180) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(url, timeout=5).close()
            return
        except OSError:
            time.sleep(3)
    raise AssertionError(f"{url} did not come up within {seconds}s")


def test_stack_receives_lineage_and_traces(tmp_path) -> None:
    if not daemon_up():  # checked here, not at import: collection must stay fast
        pytest.skip("no Docker daemon")
    up = compose("up", "-d", "marquez-db", "marquez-api", "tempo")
    assert up.returncode == 0, up.stderr
    try:
        _wait("http://localhost:5000/api/v1/namespaces")
        _wait("http://localhost:3200/ready")
        env = {
            **os.environ,
            "GUARDIAN_OPENLINEAGE_URL": "http://localhost:5000",
            "GUARDIAN_OPENLINEAGE_NAMESPACE": "guardian-docker-test",
            "GUARDIAN_OTEL": "otlp",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_SERVICE_NAME": "guardian-docker-test",
        }
        run = subprocess.run(
            [
                sys.executable,
                "-m",
                "guardian.runner.cli",
                "run",
                "demo/pipeline.yaml",
                "--root",
                str(tmp_path / "g"),
                "--run-id",
                "r1",
            ],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        assert run.returncode == 0, run.stderr
        jobs = _get("http://localhost:5000/api/v1/namespaces/guardian-docker-test/jobs")
        names = {job["name"] for job in jobs.get("jobs", [])}
        assert "demo.b1_ingest" in names and "demo" in names, names
        deadline = time.monotonic() + 60  # Tempo indexes traces asynchronously
        found = []
        while time.monotonic() < deadline and not found:
            search = _get(
                "http://localhost:3200/api/search?tags=service.name%3Dguardian-docker-test"
            )
            found = search.get("traces", [])
            time.sleep(2)
        assert found, "no Guardian traces in Tempo"
    finally:
        compose("down", "-v")
