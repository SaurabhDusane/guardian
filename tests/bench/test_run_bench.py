"""Smoke test: the benchmark script runs end to end at a tiny scale."""

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]


@pytest.mark.slow
def test_run_bench_smoke(tmp_path) -> None:
    out = tmp_path / "results.md"
    proc = subprocess.run(
        [sys.executable, "bench/run_bench.py", "--rows", "300", "--reps", "1", "--out", str(out)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    text = out.read_text(encoding="utf-8")
    for heading in (
        "## Machine",
        "Throughput during an outage",
        "Recovery time",
        "Logging overhead",
    ):
        assert heading in text
