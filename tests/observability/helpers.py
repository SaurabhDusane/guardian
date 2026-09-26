"""Run the (scenario) demo pipeline under either runner with observability listeners."""

from __future__ import annotations

import importlib.util
from collections.abc import Callable, Sequence
from pathlib import Path

import pytest

from guardian.core.events import Event
from guardian.core.guardian import Guardian
from guardian.core.models import BlockStatus
from guardian.demo.faults import Fault, apply_faults
from guardian.runner.executor import Executor

from ..helpers.roles import present_roles
from ..scenarios.runners import scenario_spec

SPEC = scenario_spec(rows=120)
ROLES = present_roles(SPEC)
HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
RUNNERS = [
    "standalone",
    pytest.param(
        "dagster",
        marks=[
            pytest.mark.dagster,
            pytest.mark.skipif(not HAS_DAGSTER, reason="dagster extra not installed"),
        ],
    ),
]

Listener = Callable[[Event], None]


def execute(
    runner: str,
    root: Path,
    run_id: str,
    listeners: Sequence[Listener],
    faults: dict[str, list[Fault]] | None = None,
    out: Sequence[str] = (),
) -> None:
    """One pipeline run under ``runner`` with ``listeners`` attached to Guardian's events."""
    faulted, registry = apply_faults(SPEC, faults or {})
    with Guardian(faulted, root, registry=registry) as g:
        for block in out:
            g.set_block_status(block, BlockStatus.OUT)
        for listener in listeners:
            g.events.add_listener(listener)
        if runner == "standalone":
            Executor(g).run(run_id)
            return
        from guardian.adapters.dagster import PIPELINE_JOB, build_definitions
        from guardian.adapters.dagster.io_manager import RUN_ID_TAG

        result = (
            build_definitions(g)
            .resolve_job_def(PIPELINE_JOB)
            .execute_in_process(raise_on_error=False, tags={RUN_ID_TAG: run_id})
        )
        assert result.success, [e.message for e in result.all_events if e.is_failure]
