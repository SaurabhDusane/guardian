from collections.abc import Iterator

import pytest

from .runners import ScenarioRunner, StandaloneRunner, scenario_spec

# Add further runner classes here (e.g. a Dagster runner) to run the whole suite on them.
RUNNERS: list[type[ScenarioRunner]] = [StandaloneRunner]


@pytest.fixture(params=RUNNERS, ids=lambda cls: cls.name)
def runner(request, tmp_path) -> Iterator[ScenarioRunner]:
    instance = request.param(scenario_spec(), tmp_path / "guardian")
    yield instance
    instance.close()
