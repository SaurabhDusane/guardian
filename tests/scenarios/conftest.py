import importlib.util
from collections.abc import Iterator

import pytest

from .runners import DagsterRunner, ScenarioRunner, StandaloneRunner, scenario_spec

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None

# Every scenario runs once per runner. Dagster is an optional extra: without it the
# dagster variants are skipped, not failed.
RUNNERS = [
    pytest.param(StandaloneRunner, id="standalone"),
    pytest.param(
        DagsterRunner,
        id="dagster",
        marks=[
            pytest.mark.dagster,
            pytest.mark.slow,
            pytest.mark.skipif(not HAS_DAGSTER, reason="dagster extra not installed"),
        ],
    ),
]


@pytest.fixture(params=RUNNERS)
def runner(request, tmp_path) -> Iterator[ScenarioRunner]:
    instance = request.param(scenario_spec(), tmp_path / "guardian")
    yield instance
    instance.close()
