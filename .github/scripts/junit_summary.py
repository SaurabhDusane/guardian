"""Write the failing tests of a JUnit XML report to the GitHub job summary.

Usage: python .github/scripts/junit_summary.py REPORT.xml

Appends a Markdown table (test, failure message) to $GITHUB_STEP_SUMMARY, or prints
it when that variable is not set. Standard library only.
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

MAX_ROWS = 200


def failures(report: Path) -> list[tuple[str, str, str]]:
    """(test id, kind, first line of the message) for every failed or errored test."""
    root = ET.parse(report).getroot()
    found = []
    for case in root.iter("testcase"):
        for kind in ("failure", "error"):
            node = case.find(kind)
            if node is None:
                continue
            classname = case.get("classname", "")
            module = classname.replace(".", "/") + ".py"
            test_id = f"{module}::{case.get('name', '?')}"
            message = (node.get("message") or node.text or "").strip().splitlines()
            found.append((test_id, kind, message[0][:200] if message else ""))
    return found


def markdown(report: Path) -> str:
    if not report.exists():
        return f"### Failing tests\n\nNo JUnit report at `{report}`: tests did not run.\n"
    rows = failures(report)
    title = os.environ.get("RUNNER_OS", "")
    lines = [f"### Failing tests{f' ({title})' if title else ''}: {len(rows)}", ""]
    if not rows:
        lines.append("No test failed; the job failed in another step (see its log).")
        return "\n".join(lines) + "\n"
    lines += ["| test | kind | message |", "|---|---|---|"]
    for test_id, kind, message in rows[:MAX_ROWS]:
        cell = message.replace("|", "\\|")
        lines.append(f"| `{test_id}` | {kind} | {cell} |")
    if len(rows) > MAX_ROWS:
        lines.append(f"\n...and {len(rows) - MAX_ROWS} more (see the JUnit artifact).")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    text = markdown(Path(argv[1]))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
