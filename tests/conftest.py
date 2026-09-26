import pytest

from .helpers.project_repo import make_project_repo


@pytest.fixture(scope="session")
def project_repo(tmp_path_factory):
    """A git copy of this project for the agent to propose fixes against. Proposals
    leave it unchanged (tests check that), so one copy serves the whole session."""
    return make_project_repo(tmp_path_factory.mktemp("project") / "repo")
