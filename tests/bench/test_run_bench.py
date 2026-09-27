"""Smoke test: the benchmark runs end to end at a tiny scale, and its results fill in a
README's Results table."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]


@pytest.mark.slow
def test_run_bench_smoke(tmp_path) -> None:
    proc = subprocess.run(
        [sys.executable, "bench/run_bench.py", "--scales", "300", "--repeats", "1",
         "--out-dir", str(tmp_path)],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
    )  # fmt: skip
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    results = json.loads((tmp_path / "results.json").read_text(encoding="utf-8"))
    assert results["meta"]["complete"] and not results["meta"]["quick"]
    measured = {(s["measurement"], s["config"]) for s in results["series"] if "median" in s}
    for measurement, configs in {
        "overhead": ("bare", "core", "provenance"),
        "throughput": ("clean", "protected_crash", "unprotected_crash"),
        "recovery": ("1%", "10%", "50%", "cli_floor"),
        "shadow": ("none", "one"),
    }.items():
        for config in configs:
            assert (measurement, config) in measured
    text = (tmp_path / "results.md").read_text(encoding="utf-8")
    for heading in ("## Summary", "## Methodology", "## Machine", "## Blocks"):
        assert heading in text

    readme = tmp_path / "README.md"
    shutil.copy(ROOT / "README.md", readme)
    update = [sys.executable, "bench/update_readme.py", "--results",
              str(tmp_path / "results.json"), "--readme", str(readme)]  # fmt: skip
    assert subprocess.run(update, cwd=ROOT, capture_output=True).returncode == 0
    assert "| Throughput, clean run |" in readme.read_text(encoding="utf-8")
    assert subprocess.run([*update, "--check"], cwd=ROOT, capture_output=True).returncode == 0
