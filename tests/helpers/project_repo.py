"""A throwaway git repository holding a copy of this project, for agent tests that
create worktrees, branches and commits. Tests never touch the real checkout."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
SPEC_REL = Path("guardian") / "demo" / "pipeline.yaml"
IDENTITY = {
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
}


def git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, **IDENTITY},
    )
    return done.stdout.strip()


def make_project_repo(dest: Path) -> Path:
    """Copy guardian/, tests/ and pyproject.toml into ``dest`` and commit them on main."""
    dest.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".guardian", ".pytest_cache")
    for name in ("guardian", "tests"):
        shutil.copytree(PROJECT / name, dest / name, ignore=ignore)
    shutil.copy(PROJECT / "pyproject.toml", dest / "pyproject.toml")
    git(dest, "init", "-q", "-b", "main")
    git(dest, "add", ".")
    git(dest, "commit", "-qm", "project snapshot")
    return dest


def repo_state(repo: Path) -> dict[str, str]:
    """What the agent must leave unchanged in the repository it patches."""
    return {
        "main": git(repo, "rev-parse", "main"),
        "head": git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
        "status": git(repo, "status", "--porcelain"),
        "worktrees": git(repo, "worktree", "list", "--porcelain").count("worktree "),
        "fix_branches": git(repo, "branch", "--list", "guardian/fix/*"),
    }
