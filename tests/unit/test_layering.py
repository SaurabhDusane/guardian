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


def test_core_never_imports_the_agent_and_the_agent_never_imports_dagster() -> None:
    code = (
        "import sys, guardian.core.guardian; "
        "assert not any(m.startswith('guardian.agent') for m in sys.modules); "
        "import guardian.agent.diagnose, guardian.agent.eval; "
        "assert not any(m == 'dagster' or m.startswith('dagster.') for m in sys.modules); "
        "assert 'anthropic' not in sys.modules  # the SDK loads only when a client is built"
    )
    subprocess.run([sys.executable, "-c", code], check=True, cwd=Path(__file__).parents[2])


def test_no_block_names_in_core_runner_adapters_or_agent() -> None:
    from guardian.runner.spec_loader import load_spec

    root = Path(__file__).parents[2]
    names = load_spec(root / "guardian" / "demo" / "pipeline.yaml").block_names
    for package in ("core", "runner", "adapters", "agent"):
        for path in (root / "guardian" / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            found = [n for n in names if n in text]
            assert not found, (path, found)
