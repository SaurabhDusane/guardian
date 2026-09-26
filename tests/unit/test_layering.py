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
    for package in ("core", "runner", "adapters", "agent", "observability"):
        for path in (root / "guardian" / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            found = [n for n in names if n in text]
            assert not found, (path, found)


def test_agent_code_never_promotes_merges_or_changes_state() -> None:
    """Belt and braces for the runtime guards (agent_view, SafeGit, GitHubClient): no
    agent module calls a state-changing Guardian method or a merge endpoint. The eval
    harness is the one exception: it plays the human reviewer on its own scratch copies."""
    import re

    root = Path(__file__).parents[2] / "guardian" / "agent"
    calls = re.compile(
        r"\.(promote|shadow_start|shadow_stop|rollback_version|set_block_status|replay|"
        r"begin_promotion|complete_promotion)\("
    )
    merges = re.compile(r"/merge\b|merge_pull|\"merge\"|'merge'")
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        found_calls = calls.findall(text)
        if path.name == "eval.py":
            assert found_calls == ["shadow_start", "promote"], found_calls
        else:
            assert not found_calls, (path.name, found_calls)
        assert path.name == "safety.py" or not merges.findall(text), path.name


def test_observability_is_optional_and_outside_core() -> None:
    """Core never imports the exporters; the CLI imports them without OpenTelemetry
    unless it is enabled."""
    code = (
        "import sys, guardian.core.guardian; "
        "assert not any(m.startswith('guardian.observability') for m in sys.modules); "
        "import guardian.runner.cli; "
        "assert not any(m.startswith('opentelemetry') for m in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True, cwd=Path(__file__).parents[2])
