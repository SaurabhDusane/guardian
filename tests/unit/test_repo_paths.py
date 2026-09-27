"""Every tracked path must be checkable-out on Windows (the dev machine and a CI target)."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
RESERVED = re.compile(r"^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$", re.IGNORECASE)
INVALID_CHARS = re.compile(r'[<>:"|?*\\\x00-\x1f]')


def windows_problem(path: str) -> str | None:
    for part in path.split("/"):
        if INVALID_CHARS.search(part):
            return f"{part!r} has a character Windows forbids"
        if part.endswith((".", " ")):
            return f"{part!r} ends with a dot or space"
        if RESERVED.match(part):
            return f"{part!r} is a reserved device name"
    return None


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        ("...", True),
        ("a/b.", True),
        ("aux.py", True),
        ("x:y", True),
        ("guardian/core/models.py", False),
    ],
)
def test_windows_problem_detects_invalid_names(path: str, bad: bool) -> None:
    assert (windows_problem(path) is not None) is bad


@pytest.mark.skipif(
    shutil.which("git") is None or not (ROOT / ".git").exists(), reason="not a git checkout"
)
def test_tracked_paths_are_valid_on_windows() -> None:
    files = (
        subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True)
        .stdout.decode("utf-8")
        .split("\0")
    )
    problems = {f: p for f in files if f and (p := windows_problem(f))}
    assert not problems, problems
