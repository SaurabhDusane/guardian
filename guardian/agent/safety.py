"""What the agent may and may not do, enforced in code.

The agent proposes; people decide. Three guards make that hold regardless of what an
LLM answers:

- ``agent_view(guardian)``: a read-only view of Guardian. Promoting, shadowing, rolling
  back, changing a block's status, replaying or running blocks raises AgentPolicyError.
- ``SafeGit``: git with an allowlist of subcommands. No merge, rebase, pull, reset or
  cherry-pick; commits only on the agent's own ``guardian/fix/...`` branches; pushes only
  of such a branch to the same name, never forced, never to a protected branch (the
  default branch, ``main``, ``master``).
- ``GitHubClient``: can create a (draft) pull request and nothing else. Its transport
  refuses any other API call, so merging is impossible through it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from guardian.core.models import GuardianError

BRANCH_PREFIX = "guardian/fix/"
ALWAYS_PROTECTED = frozenset({"main", "master"})

# Commits the agent makes are attributed to it, not to whoever runs it.
AGENT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Guardian agent",
    "GIT_AUTHOR_EMAIL": "guardian-agent@users.noreply.github.com",
    "GIT_COMMITTER_NAME": "Guardian agent",
    "GIT_COMMITTER_EMAIL": "guardian-agent@users.noreply.github.com",
}

ENV_GITHUB_TOKEN_VAR = "GUARDIAN_GITHUB_TOKEN_ENV"  # names the variable holding the token
DEFAULT_GITHUB_TOKEN_VAR = "GITHUB_TOKEN"
ENV_GITHUB_REPO = "GUARDIAN_GITHUB_REPO"  # "owner/repo"; default: parsed from the remote
ENV_GITHUB_API = "GUARDIAN_GITHUB_API"
DEFAULT_GITHUB_API = "https://api.github.com"


class AgentPolicyError(GuardianError):
    """The agent attempted something it is never allowed to do."""


# ------------------------------------------------------------------ Guardian


_FORBIDDEN_GUARDIAN = frozenset(
    {
        "shadow_promote",
        "shadow_rollback",
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
    }
)
_FORBIDDEN_STORES = {
    "versions": frozenset(
        {"begin_promotion", "complete_promotion", "rollback", "set_replay_snapshot"}
    ),
    "shadows": frozenset({"start", "end", "record"}),
    "statuses": frozenset({"set"}),
    "snapshots": frozenset({"write", "mark_last_good"}),
    "quarantine": frozenset({"add", "mark_replayed"}),
    "drift": frozenset({"write"}),
}


class _Guarded:
    def __init__(self, target: Any, forbidden: frozenset[str], what: str) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_forbidden", forbidden)
        object.__setattr__(self, "_what", what)

    def __getattr__(self, name: str) -> Any:
        if name in self._forbidden:
            raise AgentPolicyError(f"the agent may not call {self._what}.{name}()")
        value = getattr(self._target, name)
        if self._what == "guardian" and name in _FORBIDDEN_STORES:
            return _Guarded(value, _FORBIDDEN_STORES[name], f"guardian.{name}")
        return value

    def __setattr__(self, name: str, value: Any) -> None:
        raise AgentPolicyError(f"the agent may not modify {self._what}.{name}")


def agent_view(guardian: Any) -> Any:
    """A read-only view of ``guardian`` for the agent (events may still be logged)."""
    if isinstance(guardian, _Guarded):
        return guardian
    return _Guarded(guardian, _FORBIDDEN_GUARDIAN, "guardian")


# ------------------------------------------------------------------ git


_ALLOWED_GIT = frozenset(
    {
        "add",
        "branch",
        "commit",
        "config",
        "diff",
        "log",
        "push",
        "remote",
        "rev-list",
        "rev-parse",
        "show-ref",
        "status",
        "symbolic-ref",
        "worktree",
    }
)


_WRITE_WORDS = frozenset(
    {"--add", "--unset", "--replace-all", "set-url", "add", "remove", "rename"}
)


class SafeGit:
    """Runs git in ``repo`` (or a worktree of it) under the agent's policy."""

    def __init__(self, repo: Path | str) -> None:
        self.repo = Path(repo)

    def _raw(self, *args: str, cwd: Path | None = None, env: Mapping[str, str] = {}) -> str:
        done = subprocess.run(
            ["git", *args],
            cwd=cwd or self.repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
            check=False,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0", **env},
        )
        if done.returncode != 0:
            raise GuardianError(f"git {' '.join(args)} failed: {done.stderr.strip()}")
        return done.stdout

    # -------------------------------------------------------------- policy

    def default_branch(self) -> str | None:
        try:
            ref = self._raw("symbolic-ref", "--quiet", "refs/remotes/origin/HEAD").strip()
            return ref.rsplit("/", 1)[-1] or None
        except GuardianError:
            return None

    def protected(self) -> frozenset[str]:
        default = self.default_branch()
        return ALWAYS_PROTECTED | ({default} if default else set())

    def check_branch(self, branch: str) -> str:
        """``branch`` if the agent may write to it, else AgentPolicyError."""
        if branch in self.protected() or not branch.startswith(BRANCH_PREFIX):
            raise AgentPolicyError(
                f"the agent may only write to its own {BRANCH_PREFIX}* branches, not {branch!r}"
            )
        if ".." in branch or branch.endswith("/") or any(c.isspace() for c in branch):
            raise AgentPolicyError(f"invalid branch name {branch!r}")
        return branch

    def current_branch(self, cwd: Path) -> str:
        return self._raw("rev-parse", "--abbrev-ref", "HEAD", cwd=cwd).strip()

    def run(self, *args: str, cwd: Path | None = None) -> str:
        """Run an allowed git command; refuse anything the policy forbids."""
        if not args or args[0] not in _ALLOWED_GIT:
            raise AgentPolicyError(f"the agent may not run `git {' '.join(args)}`")
        command, rest = args[0], list(args[1:])
        if command == "push":
            self._check_push(rest)
        elif command == "commit":
            self.check_branch(self.current_branch(cwd or self.repo))
        elif command == "worktree":
            if rest[:1] == ["add"]:
                if "-b" not in rest:
                    raise AgentPolicyError("worktrees must be created on a new agent branch (-b)")
                self.check_branch(rest[rest.index("-b") + 1])
            elif rest[:1] not in (["remove"], ["prune"], ["list"]):
                raise AgentPolicyError(f"the agent may not run `git worktree {' '.join(rest)}`")
        elif command == "branch":
            # Listing branches, or deleting one of its own; nothing else.
            deleting = len(rest) == 2 and rest[0] in ("-D", "--delete")
            if rest and not deleting and not rest[0].startswith("--list"):
                raise AgentPolicyError(f"the agent may not run `git branch {' '.join(rest)}`")
            if deleting:
                self.check_branch(rest[1])
        elif command in ("config", "remote", "symbolic-ref") and (
            any(a in _WRITE_WORDS for a in rest)
            or (command == "config" and len(rest) > 1 and not rest[0].startswith("--get"))
        ):
            raise AgentPolicyError(f"the agent may only read git {command}")
        return self._raw(*args, cwd=cwd, env=AGENT_IDENTITY if command == "commit" else {})

    def _check_push(self, rest: list[str]) -> None:
        flags = [a for a in rest if a.startswith("-")]
        if flags:
            raise AgentPolicyError(f"the agent may not push with {flags} (no force, no options)")
        if len(rest) != 2:
            raise AgentPolicyError("push must be exactly: git push <remote> <branch>:<branch>")
        src, _, dst = rest[1].partition(":")
        if src.startswith("+") or dst.startswith("+"):
            raise AgentPolicyError("the agent may not force-push")
        if not dst or src != dst:
            raise AgentPolicyError("the agent pushes a branch only to the same branch name")
        self.check_branch(dst)


# ------------------------------------------------------------------ GitHub


Transport = Callable[[str, str, Mapping[str, str], bytes | None], tuple[int, dict[str, Any]]]


def urllib_transport(
    method: str, url: str, headers: Mapping[str, str], body: bytes | None
) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(url, data=body, method=method, headers=dict(headers))
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read() or b"{}")
        except ValueError:
            payload = {}
        return exc.code, payload


@dataclass(frozen=True)
class PullRequest:
    number: int
    url: str
    draft: bool


class GitHubClient:
    """Creates draft pull requests. It has no other capability, and its transport
    refuses every request that is not ``POST /repos/<owner>/<repo>/pulls``."""

    _ALLOWED = re.compile(r"^/repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pulls$")

    def __init__(
        self, token: str, *, api: str = DEFAULT_GITHUB_API, transport: Transport | None = None
    ) -> None:
        if not token:
            raise GuardianError("GitHubClient needs a non-empty token")
        self._token = token
        self._api = api.rstrip("/")
        self._transport = transport or urllib_transport

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, transport: Transport | None = None
    ) -> GitHubClient:
        env = os.environ if env is None else env
        var = env.get(ENV_GITHUB_TOKEN_VAR) or DEFAULT_GITHUB_TOKEN_VAR
        token = env.get(var)
        if not token:
            raise GuardianError(f"no GitHub token: set {var} (or {ENV_GITHUB_TOKEN_VAR})")
        return cls(token, api=env.get(ENV_GITHUB_API) or DEFAULT_GITHUB_API, transport=transport)

    def _request(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method != "POST" or not self._ALLOWED.match(path):
            raise AgentPolicyError(f"the agent may not call {method} {path} on GitHub")
        status, body = self._transport(
            method,
            f"{self._api}{path}",
            {
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json.dumps(payload).encode("utf-8"),
        )
        if status >= 300:
            raise GuardianError(f"GitHub {method} {path} failed ({status}): {body.get('message')}")
        return body

    def create_pull_request(
        self, repo: str, *, head: str, base: str, title: str, body: str
    ) -> PullRequest:
        if head == base or not head.startswith(BRANCH_PREFIX):
            raise AgentPolicyError(f"pull requests must come from a {BRANCH_PREFIX}* branch")
        data = self._request(
            "POST",
            f"/repos/{repo}/pulls",
            {"title": title, "head": head, "base": base, "body": body, "draft": True},
        )
        return PullRequest(int(data.get("number", 0)), str(data.get("html_url", "")), True)


def github_repo(git: SafeGit, env: Mapping[str, str] | None = None) -> str:
    """``owner/repo`` from GUARDIAN_GITHUB_REPO, else from the ``origin`` remote URL."""
    env = os.environ if env is None else env
    if env.get(ENV_GITHUB_REPO):
        return env[ENV_GITHUB_REPO]
    url = git.run("remote", "get-url", "origin").strip()
    match = re.search(r"[:/]([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?/?$", url)
    if not match:
        raise GuardianError(
            f"cannot tell the GitHub repository from {url!r}; set {ENV_GITHUB_REPO}"
        )
    return f"{match.group(1)}/{match.group(2)}"
