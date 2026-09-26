"""Core must never import dagster (adapters depend on core, never the reverse)."""

import subprocess
import sys
from pathlib import Path


def test_core_and_runner_never_import_dagster() -> None:
    code = (
        "import sys, guardian.core.guardian, guardian.runner.cli, guardian.demo.blocks; "
        "assert not any(m == 'dagster' or m.startswith('dagster.') for m in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True, cwd=Path(__file__).parents[2])
