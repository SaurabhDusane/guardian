"""The agent's hard limits: no promoting, no merging, no pushing to the default branch."""

from __future__ import annotations

import json

import pytest

from guardian.agent.safety import (
    BRANCH_PREFIX,
    AgentPolicyError,
    GitHubClient,
    SafeGit,
    agent_view,
    github_repo,
)
from guardian.core.guardian import Guardian
from guardian.core.models import BlockStatus

from ..helpers.project_repo import git
from .helpers import SPEC, run

FORBIDDEN = [
    "promote",
    "rollback_version",
    "shadow_start",
    "shadow_stop",
    "run_shadow",
    "set_block_status",
    "replay",
    "run_block",
    "begin_block",
    "complete_block",
    "handle_result",
    "on_output",
    "on_crash",
]


@pytest.fixture
def guardian(tmp_path):
    run(tmp_path / "g", SPEC, "r1")
    with Guardian(SPEC, tmp_path / "g") as g:
        yield g


@pytest.mark.parametrize("method", FORBIDDEN)
def test_agent_view_forbids_every_state_change(guardian, method: str) -> None:
    view = agent_view(guardian)
    with pytest.raises(AgentPolicyError, match=method):
        getattr(view, method)


@pytest.mark.parametrize(
    ("store", "method"),
    [
        ("versions", "begin_promotion"),
        ("versions", "complete_promotion"),
        ("versions", "rollback"),
        ("shadows", "start"),
        ("shadows", "end"),
        ("statuses", "set"),
        ("snapshots", "write"),
        ("snapshots", "mark_last_good"),
        ("quarantine", "add"),
        ("quarantine", "mark_replayed"),
        ("drift", "write"),
    ],
)
def test_agent_view_forbids_store_writes(guardian, store: str, method: str) -> None:
    with pytest.raises(AgentPolicyError):
        getattr(getattr(agent_view(guardian), store), method)


def test_agent_view_still_reads_and_logs(guardian) -> None:
    view = agent_view(guardian)
    block = SPEC.block_names[0]
    assert view.snapshots.last_good(block).run_id == "r1"
    assert view.provenance.runs(block)
    assert view.versions.history(block) == []
    assert view.status(block) is BlockStatus.HEALTHY
    view.events.emit("PROPOSAL", block=block, status="test")
    with pytest.raises(AgentPolicyError):
        view.spec = SPEC
    assert agent_view(view) is view


# ---------------------------------------------------------------- git


@pytest.fixture
def repo(tmp_path):
    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "main")
    (work / "f.txt").write_text("x\n", encoding="utf-8")
    git(work, "add", ".")
    git(work, "commit", "-qm", "init")
    git(tmp_path, "clone", "-q", "--bare", str(work), str(origin))
    git(work, "remote", "add", "origin", str(origin))
    git(work, "fetch", "-q", "origin")
    return work


@pytest.mark.parametrize(
    "args",
    [
        ("merge", "anything"),
        ("rebase", "main"),
        ("pull", "origin", "main"),
        ("reset", "--hard", "HEAD~1"),
        ("cherry-pick", "abc"),
        ("checkout", "main"),
        ("push", "origin", "main:main"),
        ("push", "origin", "HEAD:main"),
        ("push", "origin", "guardian/fix/x:main"),
        ("push", "origin", "guardian/fix/x"),
        ("push", "--force", "origin", "guardian/fix/x:guardian/fix/x"),
        ("push", "origin", "+guardian/fix/x:guardian/fix/x"),
        ("push", "origin", "feature:feature"),
        ("worktree", "add", "/tmp/somewhere", "main"),
        ("worktree", "add", "-b", "main", "/tmp/somewhere", "HEAD"),
        ("branch", "-D", "main"),
        ("branch", "-f", "main", "HEAD"),
        ("config", "user.name", "x"),
        ("remote", "set-url", "origin", "x"),
        ("commit", "-m", "on main"),  # HEAD is main
    ],
)
def test_safe_git_refuses(repo, args) -> None:
    with pytest.raises(AgentPolicyError):
        SafeGit(repo).run(*args)
    assert git(repo, "rev-parse", "main") == git(repo, "rev-parse", "origin/main")


def test_safe_git_allows_an_agent_branch_and_protects_the_default(repo, tmp_path) -> None:
    safe = SafeGit(repo)
    branch = f"{BRANCH_PREFIX}blk/r1-fix1"
    wt = tmp_path / "wt"
    safe.run("worktree", "add", "-b", branch, str(wt), "HEAD")
    (wt / "g.txt").write_text("y\n", encoding="utf-8")
    safe.run("add", "g.txt", cwd=wt)
    safe.run("commit", "-m", "agent change", cwd=wt)
    assert git(wt, "log", "-1", "--format=%an") == "Guardian agent"
    safe.run("push", "origin", f"{branch}:{branch}", cwd=wt)
    origin = tmp_path / "origin.git"
    assert git(origin, "rev-parse", branch) == git(wt, "rev-parse", "HEAD")
    assert git(origin, "rev-parse", "main") == git(repo, "rev-parse", "main")
    safe.run("worktree", "remove", "--force", str(wt))
    safe.run("branch", "-D", branch)

    # Whatever the default branch is called, it is protected.
    git(repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/trunk")
    assert "trunk" in safe.protected() and "main" in safe.protected()
    with pytest.raises(AgentPolicyError):
        safe.check_branch("trunk")


# ---------------------------------------------------------------- GitHub


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, json.loads(body) if body else {}))
        assert headers["Authorization"] == "Bearer tok"
        return 201, {"number": 7, "html_url": "https://github.example/pr/7"}


def test_github_client_only_creates_draft_pull_requests() -> None:
    transport = FakeTransport()
    client = GitHubClient("tok", api="https://api.example", transport=transport)
    pr = client.create_pull_request(
        "o/r", head=f"{BRANCH_PREFIX}b/r1-fix1", base="main", title="t", body="b"
    )
    assert (pr.number, pr.url, pr.draft) == (7, "https://github.example/pr/7", True)
    ((method, url, payload),) = transport.calls
    assert (method, url) == ("POST", "https://api.example/repos/o/r/pulls")
    assert payload["draft"] is True and payload["base"] == "main"

    assert not [name for name in dir(client) if "merge" in name.lower()]
    for method, path in (
        ("PUT", "/repos/o/r/pulls/7/merge"),
        ("POST", "/repos/o/r/merges"),
        ("PATCH", "/repos/o/r/git/refs/heads/main"),
        ("DELETE", "/repos/o/r/git/refs/heads/main"),
        ("POST", "/repos/o/r/pulls/7/reviews"),
    ):
        with pytest.raises(AgentPolicyError):
            client._request(method, path, {})
    with pytest.raises(AgentPolicyError):
        client.create_pull_request("o/r", head="main", base="main", title="t", body="b")
    with pytest.raises(AgentPolicyError):
        client.create_pull_request("o/r", head="feature", base="main", title="t", body="b")
    assert len(transport.calls) == 1


def test_github_settings_come_from_the_environment(repo) -> None:
    with pytest.raises(Exception, match="GITHUB_TOKEN"):
        GitHubClient.from_env(env={})
    client = GitHubClient.from_env(env={"GUARDIAN_GITHUB_TOKEN_ENV": "MY_TOKEN", "MY_TOKEN": "t"})
    assert isinstance(client, GitHubClient)
    safe = SafeGit(repo)
    assert github_repo(safe, env={"GUARDIAN_GITHUB_REPO": "own/rep"}) == "own/rep"
    for url, slug in (
        ("https://github.com/acme/pipes.git", "acme/pipes"),
        ("git@github.com:acme/pipes.git", "acme/pipes"),
        ("http://proxy@127.0.0.1:9/git/acme/pipes", "acme/pipes"),
    ):
        git(repo, "remote", "set-url", "origin", url)
        assert github_repo(safe, env={}) == slug
