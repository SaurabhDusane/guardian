import pytest

from .helpers.project_repo import make_project_repo


@pytest.fixture(scope="session")
def project_repo(tmp_path_factory):
    """A git copy of this project for the agent to propose fixes against. Proposals
    leave it unchanged (tests check that), so one copy serves the whole session (one
    per xdist worker)."""
    return make_project_repo(tmp_path_factory.mktemp("project") / "repo")


@pytest.fixture(autouse=True, scope="session")
def wide_cli_console():
    """CLI tests assert on rendered tables, so the CLI's console gets a fixed width.

    Rich reads COLUMNS once, when the console is created at import; pytest-xdist
    workers have no terminal and no COLUMNS, so their tables would be 80 columns wide
    and truncate cells. Pinning the width makes the output identical in every mode.
    """
    from guardian.runner import cli

    width = cli.console.width
    cli.console.width = 200
    yield
    cli.console.width = width
