"""Propose a fix for a diagnosed block failure, verified, as a pull request at most.

Given a diagnosis whose root cause is not ``unknown``, the LLM proposes a fix plan:

- ``code_bug``: a new implementation of the block, added as a NEW version in the block's
  ``versions`` (``fix1``, ``fix2``, ...). The live version is not touched, so the fix goes
  through shadow promotion like any other version;
- ``schema_change``: an updated schema (a new schema object the block now points to), or
  a new fallback adapter on an edge that stands in for the block;
- ``upstream_data_drift``: an updated schema, or a written note for the upstream owner
  when the data itself is wrong (no code change).

New code is always *appended* under a new name; existing definitions are never
edited. The plan is applied in a temporary git worktree on a new ``guardian/fix/...``
branch, never on the default branch. Then the block's unit tests (its ``tests`` in the
spec) run there, and so does a shadow run on the failing run's inputs. Only if both
pass does ``--open-pr`` push the branch and open a draft pull request. By default
(dry run) the patch and the PR body are only written to
``<root>/proposals/<block>/<run_id>/``. Every attempt, failed or not, is recorded
with its reasons.

The agent never merges, never promotes and never pushes to the default branch; see
safety.py for how that is enforced.
"""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import difflib
import functools
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from guardian.agent.diagnose import (
    UNKNOWN,
    Diagnosis,
    LLMClient,
    LLMError,
    LLMRequest,
    ResponseError,
    diagnose,
)
from guardian.agent.evidence import (
    EVIDENCE_FILE,
    EvidenceBundle,
    baseline_run,
    block_run,
    default_run,
    diagnosis_dir,
)
from guardian.agent.safety import (
    BRANCH_PREFIX,
    AgentPolicyError,
    GitHubClient,
    SafeGit,
    agent_view,
    github_repo,
)
from guardian.core.events import EventKind
from guardian.core.guardian import ROLLBACK_RULE
from guardian.core.models import (
    BlockCrash,
    BlockSpec,
    DataRef,
    GuardianError,
    PipelineSpec,
    validate_name,
)
from guardian.core.provenance import QUALITY_COL
from guardian.core.quarantine import restore_dtypes
from guardian.core.shadow import ShadowError, compare_rows
from guardian.core.validation import PanderaValidator, Validator
from guardian.runner.spec_loader import SpecError, parse_spec

PROPOSALS_DIR = "proposals"
PROPOSAL_FILE = "proposal.json"
PATCH_FILE = "patch.diff"
PR_BODY_FILE = "pr_body.md"
NOTE_FILE = "note.md"
ATTEMPTS_FILE = "attempts.jsonl"
FILES_DIR = "files"  # full copies of the patched files, to load the fix as if merged

ACTIONS = ("new_version", "schema_update", "adapter_update", "upstream_note")
ALLOWED_ACTIONS: dict[str, tuple[str, ...]] = {
    "code_bug": ("new_version",),
    "schema_change": ("schema_update", "adapter_update"),
    "upstream_data_drift": ("schema_update", "upstream_note"),
}

# Proposal status.
READY = "ready"  # verified; dry run: patch and PR body written, GitHub untouched
OPENED = "opened"  # verified and opened as a draft pull request
NOTE = "note"  # a note for the upstream owner; no code change
FAILED = "failed"  # the fix did not pass its checks (or could not be applied)
REJECTED = "rejected"  # no usable plan: diagnosis unusable, invalid answer, bad citations

MAX_SOURCE = 6000
TEST_TIMEOUT = 900

FIX_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": list(ACTIONS)},
        "rationale": {"type": "string"},
        "evidence": {"type": "array", "items": {"type": "string"}},
        "name": {"type": "string"},
        "code": {"type": "string"},
        "note": {"type": "string"},
        "dependent": {"type": "string"},
    },
    "required": ["action", "rationale", "evidence", "name", "code", "note", "dependent"],
    "additionalProperties": False,
}

FIX_SYSTEM_PROMPT = """\
You propose a fix for one block (one step) of a data pipeline, given a diagnosis of why \
it failed and the evidence behind it. A human reviews every proposal; you never merge, \
promote or deploy anything.

Choose exactly one action, allowed for the diagnosed root cause:
- new_version (code_bug): write a NEW top-level Python function, named `name`, that \
implements the block correctly. It is appended to the module of the block's live \
function and added to the block's versions, so it must take the same arguments and \
return a pandas DataFrame. Do not redefine or edit existing names; you may call them.
- schema_update (schema_change, upstream_data_drift): write a NEW schema object named \
`name` (a pandera DataFrameSchema assignment), appended to the module of the block's \
current schema, that the block will validate against instead.
- adapter_update (schema_change): write a NEW adapter function named `name` for the \
fallback edge of block `dependent` that stands in for this block.
- upstream_note (upstream_data_drift): no code; write `note` for the owner of the \
upstream data when the data itself is wrong.

Rules:
- Put only the new definition(s) in `code`, with any imports they need that the module \
does not already bind. Never rebind an existing top-level name.
- `evidence` must cite the IDs of evidence items (E1, E2, ...) that justify the fix. \
Citing an ID that does not exist gets the proposal rejected.
- Leave unused fields as empty strings.
- Reply with only a JSON object with the fields action, rationale, evidence, name, \
code, note, dependent.
"""


class ProposalError(GuardianError):
    """The fix could not be applied (bad target, unexpected spec layout, git failure)."""


# ------------------------------------------------------------------ fix plan


@dataclass(frozen=True)
class FixPlan:
    action: str
    rationale: str
    evidence: tuple[str, ...]
    name: str = ""
    code: str = ""
    note: str = ""
    dependent: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self) | {"evidence": list(self.evidence)}


def parse_fix(text: str, root_cause: str) -> FixPlan:
    """Parse and shape-check a fix plan; raise ResponseError with the problem."""
    text = (text or "").strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResponseError(f"invalid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ResponseError("the answer must be a JSON object")
    missing = [k for k in ("action", "rationale", "evidence") if k not in raw]
    if missing:
        raise ResponseError(f"missing field(s) {missing}")
    fields = {k: raw.get(k, "") for k in ("name", "code", "note", "dependent")}
    if not all(isinstance(v, str) for v in (raw["action"], raw["rationale"], *fields.values())):
        raise ResponseError("action, rationale, name, code, note and dependent must be strings")
    if not isinstance(raw["evidence"], list) or not all(
        isinstance(e, str) for e in raw["evidence"]
    ):
        raise ResponseError("evidence must be a list of IDs")
    allowed = ALLOWED_ACTIONS.get(root_cause, ())
    if raw["action"] not in allowed:
        raise ResponseError(
            f"action {raw['action']!r} is not allowed for root cause {root_cause!r}; "
            f"use one of {list(allowed)}"
        )
    plan = FixPlan(
        action=raw["action"],
        rationale=raw["rationale"],
        evidence=tuple(e.strip() for e in raw["evidence"]),
        **fields,
    )
    if plan.action == "upstream_note":
        if not plan.note.strip():
            raise ResponseError("upstream_note needs a note")
        if plan.code.strip():
            raise ResponseError("upstream_note must not change code")
    else:
        if not plan.name.isidentifier() or not plan.code.strip():
            raise ResponseError(f"{plan.action} needs a valid `name` and `code`")
    return plan


def fix_prompt(bundle: EvidenceBundle, diagnosis: Diagnosis, context: Mapping[str, Any]) -> str:
    """The first request for a fix plan (a retry appends why the answer was rejected)."""
    return (
        f"Propose a fix for block {bundle.block!r} (run {bundle.run_id!r}).\n\n"
        f"<diagnosis>\n{json.dumps(diagnosis.to_dict(), sort_keys=True)}\n</diagnosis>\n\n"
        f"<context>\n{json.dumps(context, sort_keys=True)}\n</context>\n\n"
        f"<evidence>\n{bundle.to_json(indent=None)}\n</evidence>"
    )


def request_fix(
    bundle: EvidenceBundle,
    diagnosis: Diagnosis,
    context: Mapping[str, Any],
    client: LLMClient,
    *,
    key: str,
    check: Callable[[FixPlan], None],
) -> tuple[FixPlan | None, str | None, int]:
    """Ask for a fix plan; returns (plan, rejection reason, attempts).

    Invalid answers (bad JSON, wrong shape, disallowed action, code that fails
    ``check``) are retried once; citations of evidence that does not exist are rejected
    without retry.
    """
    prompt = fix_prompt(bundle, diagnosis, context)
    error = ""
    for attempt in (1, 2):
        text_prompt = prompt
        if attempt == 2:
            text_prompt += (
                f"\n\nYour previous answer was rejected ({error}). "
                "Answer again with only the JSON object."
            )
        request = LLMRequest(
            key=key, system=FIX_SYSTEM_PROMPT, prompt=text_prompt, schema=FIX_SCHEMA
        )
        try:
            text = client.complete(request)
        except LLMError as exc:
            return None, f"LLM call failed: {exc}", attempt
        try:
            plan = parse_fix(text, diagnosis.root_cause)
        except ResponseError as exc:
            error = str(exc)
            continue
        known = set(bundle.ids)
        bad = [e for e in plan.evidence if e not in known]
        if not plan.evidence or bad:
            cited = ", ".join(bad) if bad else "nothing"
            return None, f"the fix cites evidence that is not in the bundle: {cited}", attempt
        try:
            check(plan)
        except ResponseError as exc:
            error = str(exc)
            continue
        return plan, None, attempt
    return None, f"invalid fix plan after one retry: {error}", 2


# ------------------------------------------------------------------ source edits


def _top_level_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                names.update(n.id for n in ast.walk(target) if isinstance(n, ast.Name))
        elif isinstance(node, ast.AnnAssign | ast.AugAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            names.update((a.asname or a.name).split(".")[0] for a in node.names)
    return names


def append_definition(source: str, code: str, name: str, *, kind: str) -> str:
    """``source`` with ``code`` appended; raise ResponseError unless ``code`` defines
    ``name`` (a function, or an assignment for kind "schema") and rebinds nothing that
    ``source`` already defines. The result only adds lines."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise ResponseError(f"the code does not parse: {exc}") from exc
    existing = _top_level_names(ast.parse(source))
    new = _top_level_names(tree)
    clashes = sorted(new & existing)
    if clashes:
        raise ResponseError(f"the code rebinds existing name(s) {clashes}; use new names")
    if kind == "schema":
        defined = any(
            isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)
            for n in tree.body
        )
    else:
        defined = any(isinstance(n, ast.FunctionDef) and n.name == name for n in tree.body)
    if not defined:
        what = "an assignment to" if kind == "schema" else "a top-level function"
        raise ResponseError(f"the code must define {what} {name!r}")
    patched = source.rstrip("\n") + "\n\n\n" + code.strip("\n") + "\n"
    removed = [
        line
        for line in difflib.ndiff(source.splitlines(), patched.splitlines())
        if line.startswith("- ")
    ]
    if removed:  # appending never removes lines; guard against it anyway
        raise ResponseError("the patch would change existing lines")
    return patched


_ITEM = re.compile(r"^(?P<indent>\s*)- name:\s*['\"]?(?P<name>[^'\"\s#]+)['\"]?\s*(#.*)?$")


def _block_section(lines: list[str], block: str) -> tuple[int, int, int]:
    """(first line, end line (exclusive), key indent) of ``block``'s list item."""
    for i, line in enumerate(lines):
        m = _ITEM.match(line)
        if m and m.group("name") == block:
            item_indent = len(m.group("indent"))
            end = i + 1
            while end < len(lines):
                text = lines[end]
                if text.strip() and not text.lstrip().startswith("#"):
                    indent = len(text) - len(text.lstrip())
                    if indent <= item_indent:
                        break
                end += 1
            return i, end, item_indent + 2
    raise ProposalError(f"block {block!r} not found in the spec file")


def _key_line(lines: list[str], start: int, end: int, indent: int, key: str) -> int | None:
    pattern = re.compile(rf"^ {{{indent}}}{re.escape(key)}:(\s|$)")
    for i in range(start, end):
        if pattern.match(lines[i]):
            return i
    return None


def add_version(text: str, block: str, version: str, ref: str) -> str:
    """Add ``version: ref`` to ``block``'s versions in YAML ``text`` (comments kept).

    A block with a single ``fn`` gets ``versions: {v1: <fn>, <version>: <ref>}`` and
    ``active: v1``: its live implementation is unchanged either way.
    """
    lines = text.split("\n")
    start, end, ki = _block_section(lines, block)
    at = _key_line(lines, start, end, ki, "versions")
    if at is not None:
        if lines[at].split(":", 1)[1].strip():
            raise ProposalError(f"block {block!r}: inline 'versions' mappings are not supported")
        last = at
        for i in range(at + 1, end):
            stripped = lines[i].strip()
            if not stripped or stripped.startswith("#"):
                continue
            if len(lines[i]) - len(lines[i].lstrip()) <= ki:
                break
            last = i
        lines.insert(last + 1, f"{' ' * (ki + 2)}{version}: {ref}")
    else:
        fn_at = _key_line(lines, start, end, ki, "fn")
        if fn_at is None:
            raise ProposalError(f"block {block!r} has neither 'versions' nor 'fn'")
        fn = lines[fn_at].split(":", 1)[1].split("#", 1)[0].strip()
        if _key_line(lines, start, end, ki, "active") is not None:
            raise ProposalError(f"block {block!r}: 'active' without 'versions'")
        pad = " " * ki
        lines[fn_at : fn_at + 1] = [
            f"{pad}versions:",
            f"{pad}  v1: {fn}",
            f"{pad}  {version}: {ref}",
            f"{pad}active: v1",
        ]
    return "\n".join(lines)


def set_block_key(text: str, block: str, key: str, value: str) -> str:
    """Set ``key: value`` on ``block`` (replacing the existing line, or adding one)."""
    lines = text.split("\n")
    start, end, ki = _block_section(lines, block)
    at = _key_line(lines, start, end, ki, key)
    line = f"{' ' * ki}{key}: {value}"
    if at is None:
        lines.insert(start + 1, line)
    else:
        lines[at] = line
    return "\n".join(lines)


def set_edge_adapter(text: str, dependent: str, replaces: str, adapter: str) -> str:
    """Point the fallback edge of ``dependent`` that replaces ``replaces`` at ``adapter``."""
    lines = text.split("\n")
    start, end, _ = _block_section(lines, dependent)
    edge = re.compile(rf"^(?P<indent>\s*)- replaces:\s*['\"]?{re.escape(replaces)}['\"]?\s*(#.*)?$")
    for i in range(start, end):
        m = edge.match(lines[i])
        if not m:
            continue
        item_indent = len(m.group("indent"))
        j = i + 1
        while j < end and (
            not lines[j].strip() or len(lines[j]) - len(lines[j].lstrip()) > item_indent
        ):
            if re.match(rf"^ {{{item_indent + 2}}}adapter:", lines[j]):
                lines[j] = f"{' ' * (item_indent + 2)}adapter: {adapter}"
                return "\n".join(lines)
            j += 1
        lines.insert(i + 1, f"{' ' * (item_indent + 2)}adapter: {adapter}")
        return "\n".join(lines)
    raise ProposalError(f"block {dependent!r} has no fallback edge replacing {replaces!r}")


def _parse(text: str, where: str) -> PipelineSpec:
    try:
        return parse_spec(yaml.safe_load(text), source=where)
    except (SpecError, yaml.YAMLError) as exc:
        raise ProposalError(f"the edited spec is invalid: {exc}") from exc


def _only_changed(before: PipelineSpec, after: PipelineSpec, block: str) -> None:
    """Every block but ``block`` is unchanged by the edit."""
    if before.block_names != after.block_names:
        raise ProposalError("the spec edit changed the list of blocks")
    for name in before.block_names:
        if name != block and before.block(name) != after.block(name):
            raise ProposalError(f"the spec edit changed block {name!r} unexpectedly")


# ------------------------------------------------------------------ locating code


def module_file(module: str, repo: Path) -> Path:
    """Path, relative to ``repo``, of the module a spec reference names.

    References resolve like the spec loader does: ``pkg.mod``, else ``guardian.pkg.mod``.
    """
    for name in (module, f"guardian.{module}"):
        for rel in (
            Path(*name.split(".")).with_suffix(".py"),
            Path(*name.split("."), "__init__.py"),
        ):
            if (repo / rel).is_file():
                return rel
    raise ProposalError(f"module {module!r} is not a file in {repo}")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _cut(text: str, limit: int = MAX_SOURCE) -> str:
    return text if len(text) <= limit else text[:limit] + "\n# ... (truncated)"


def _load_module(path: Path, label: str) -> Any:
    """Import a (patched) module file under a fresh private name."""
    safe = re.sub(r"\W", "_", label)
    name = f"_guardian_proposal_{safe}_{uuid.uuid4().hex[:8]}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ProposalError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ------------------------------------------------------------------ checks


@dataclass(frozen=True)
class UnitTestResult:
    passed: bool
    command: str
    returncode: int | None
    output: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def run_block_tests(worktree: Path, tests: tuple[str, ...]) -> UnitTestResult:
    """Run the block's unit tests in the worktree, against the worktree's code."""
    if not tests:
        return UnitTestResult(
            False, "", None, "the block declares no unit tests (`tests` in the spec)"
        )
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *tests]
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env["PYTHONPATH"] = str(worktree)
    try:
        done = subprocess.run(
            command,
            cwd=worktree,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TEST_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return UnitTestResult(
            False, " ".join(command[1:]), None, f"timed out after {TEST_TIMEOUT}s"
        )
    tail = "\n".join((done.stdout + done.stderr).strip().splitlines()[-40:])
    return UnitTestResult(done.returncode == 0, " ".join(command[2:]), done.returncode, tail)


@dataclass(frozen=True)
class ShadowCheck:
    passed: bool
    columns: tuple[str, str, str]  # header: metric, live, candidate
    rows: tuple[tuple[str, str, str], ...]
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "columns": list(self.columns),
            "rows": [list(r) for r in self.rows],
            "notes": list(self.notes),
        }

    def markdown(self) -> str:
        head = "| " + " | ".join(self.columns) + " |"
        lines = [head, "|" + "---|" * len(self.columns)]
        lines += ["| " + " | ".join(row) + " |" for row in self.rows]
        return "\n".join(lines + [f"\n{n}" for n in self.notes])


def failing_inputs(g: Any, block: str, run_id: str) -> list[pd.DataFrame]:
    """The inputs ``block`` ran on in ``run_id``, re-read from the snapshots it read."""
    spec = g.spec.block(block)
    if not spec.inputs:
        prepared = g.prepare_inputs(block, [])
        if isinstance(prepared, BlockCrash):
            raise ProposalError(f"cannot reload {block!r}'s input: {prepared.error}")
        return list(prepared)
    refs: dict[str, DataRef] = {}
    record = g.provenance.get(block, run_id)
    if record is not None:
        for i in record.inputs:
            refs[i.upstream] = DataRef(i.source_block, i.source_run_id, i.upstream, i.adapter)
    if len(refs) < len(spec.inputs):  # e.g. a crash: no output provenance was recorded
        for event in g.events.query(block=block, run_id=run_id):
            if event.kind in (EventKind.RESOLVE, EventKind.REROUTE):
                d = event.data
                refs.setdefault(
                    d["upstream"],
                    DataRef(d["source"], d["source_run_id"], d["upstream"], d.get("adapter")),
                )
    missing = [u for u in spec.inputs if u not in refs]
    if missing:
        raise ProposalError(f"which snapshots {block!r} read on {run_id!r} is unknown: {missing}")
    return [g.read(refs[u]) for u in spec.inputs]


def failing_output(g: Any, block: str, run_id: str) -> pd.DataFrame | None:
    """The live output of ``block`` on ``run_id`` (promoted rows plus quarantined ones)."""
    run = block_run(g, block, run_id)
    records = g.quarantine.list(block=block, run_id=run_id)
    rows = pd.DataFrame([r.payload() for r in records])
    baseline = baseline_run(g, block, run_id)
    if baseline is not None and len(rows):
        rows = restore_dtypes(rows, dict(g.snapshots.read(block, baseline.run_id).dtypes))
    if run.outcome == "PASS" and g.snapshots.exists(block, run_id):
        promoted = g.snapshots.read(block, run_id).drop(columns=[QUALITY_COL], errors="ignore")
        return pd.concat([promoted, rows], ignore_index=True) if len(rows) else promoted
    return rows if len(records) else None


def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "n/a"


def _live_summary(g: Any, block: str, run_id: str) -> tuple[str, int, int]:
    run = block_run(g, block, run_id)
    records = g.quarantine.list(block=block, run_id=run_id)
    bad = sum(r.rule_name != ROLLBACK_RULE for r in records)
    promoted = len(g.snapshots.read(block, run_id)) if run.outcome == "PASS" else 0
    total = promoted + len(records)
    label = run.outcome + (f" ({run.reason})" if run.reason else "")
    return label, total, bad


def check_new_version(
    g: Any, block: str, run_id: str, version: str, fn: Callable[..., pd.DataFrame]
) -> ShadowCheck:
    """Run the candidate on the failing run's inputs, as a shadow run would."""
    spec = g.spec.block(block)
    policy = spec.shadow_policy()
    live_label, live_rows, live_bad = _live_summary(g, block, run_id)
    live_version = g.active_version(block) or "live"
    notes: list[str] = []
    columns = (
        "metric",
        f"live `{live_version}` on {run_id}",
        f"candidate `{version}` (same inputs)",
    )
    try:
        inputs = failing_inputs(g, block, run_id)
        call = functools.partial(fn, **spec.params) if spec.params and spec.load is None else fn
        out = call(*inputs)
        if not isinstance(out, pd.DataFrame):
            raise TypeError(f"returned {type(out).__name__}, not a DataFrame")
    except Exception as exc:
        return ShadowCheck(
            False,
            columns,
            (("outcome", live_label, f"crash: {type(exc).__name__}: {exc}"),),
            ("The candidate crashed on the failing run's inputs.",),
        )
    result = g.validator_for(block).validate(out)
    n, bad = len(out), len(result.bad)
    pass_rate = (n - bad) / n if n and result.schema_error is None else 0.0
    assert policy.min_pass_rate is not None
    passed = result.schema_error is None and n > 0 and pass_rate >= policy.min_pass_rate
    outcome = "PASS" if passed else f"fails ({result.schema_error or 'too many bad rows'})"
    rows = [
        ("outcome", live_label, outcome),
        ("rows out", str(live_rows), str(n)),
        ("bad rows", str(live_bad), str(bad)),
        ("pass rate", _pct(live_rows - live_bad, live_rows), _pct(n - bad, n) if n else "n/a"),
        ("min pass rate (policy)", "", f"{policy.min_pass_rate:.2%}"),
    ]
    baseline = baseline_run(g, block, run_id)
    if baseline is not None and spec.merge_key and len(result.good):
        try:
            cmp = compare_rows(
                g.snapshots.read(block, baseline.run_id), result.good, list(spec.merge_key)
            )
            rows.append(
                (
                    f"vs last good ({baseline.run_id})",
                    "",
                    f"{cmp.added} added, {cmp.removed} removed, {cmp.changed} changed "
                    f"({cmp.changed_fraction:.1%})",
                )
            )
        except ShadowError as exc:
            notes.append(f"No row diff against the last good snapshot: {exc}")
    if g.status(block).value != "HEALTHY":
        notes.append(
            f"`{block}` is {g.status(block).value}, so its shadow runs are in absolute mode: "
            "promotion needs an explicit approval (`guardian shadow promote --approve`)."
        )
    return ShadowCheck(passed, columns, tuple(rows), tuple(notes))


def _as_validator(obj: Any) -> Validator:
    # A pandera schema also has .validate(), so try it as a pandera schema first.
    try:
        return PanderaValidator(obj)
    except TypeError:
        if isinstance(obj, Validator):
            return obj
        raise


def check_schema(g: Any, block: str, run_id: str, name: str, schema: Any) -> ShadowCheck:
    """Validate the failing run's live output against the proposed schema."""
    spec = g.spec.block(block)
    policy = spec.shadow_policy()
    columns = ("metric", "current schema", f"proposed `{name}`")
    output = failing_output(g, block, run_id)
    if output is None:
        return ShadowCheck(False, columns, (), ("The failing run produced no output to check.",))
    old = g.validator_for(block).validate(output)
    new = _as_validator(schema).validate(output)
    n = len(output)

    def rate(r: Any) -> float:
        return 0.0 if r.schema_error else (n - len(r.bad)) / n

    assert policy.min_pass_rate is not None
    passed = new.schema_error is None and rate(new) >= policy.min_pass_rate
    rows = [
        ("rows checked (failing run output)", str(n), str(n)),
        ("schema error", old.schema_error or "none", new.schema_error or "none"),
        ("bad rows", str(len(old.bad)), str(len(new.bad))),
        ("pass rate", f"{rate(old):.1%}", f"{rate(new):.1%}"),
        ("min pass rate (policy)", "", f"{policy.min_pass_rate:.2%}"),
    ]
    return ShadowCheck(passed, columns, tuple(rows))


def check_adapter(
    g: Any, block: str, dependent: str, name: str, adapter: Callable[[pd.DataFrame], Any]
) -> ShadowCheck:
    """The proposed adapter on the fallback source's last good snapshot must produce
    data ``block``'s schema accepts."""
    edge = g.spec.block(dependent).fallback_for(block)
    assert edge is not None
    columns = ("metric", f"current adapter `{edge.adapter}`", f"proposed `{name}`")
    source = g.snapshots.last_good(edge.source)
    if source is None:
        return ShadowCheck(False, columns, (), (f"{edge.source!r} has no last good snapshot.",))
    df = g.snapshots.read(edge.source, source.run_id).drop(columns=[QUALITY_COL], errors="ignore")
    validator = g.validator_for(block)
    results = []
    for fn in (g.resolve(edge.adapter) if edge.adapter else (lambda x: x), adapter):
        try:
            out = fn(df)
            r = validator.validate(out)
            results.append((len(out), len(r.bad), r.schema_error))
        except Exception as exc:
            results.append((0, 0, f"crash: {type(exc).__name__}: {exc}"))
    (on, ob, oe), (nn, nb, ne) = results
    passed = ne is None and nn > 0 and nb == 0
    rows = (
        (f"rows from {edge.source}@{source.run_id}", str(on), str(nn)),
        ("schema error", oe or "none", ne or "none"),
        (f"bad rows against {block}'s schema", str(ob), str(nb)),
    )
    return ShadowCheck(passed, columns, rows)


# ------------------------------------------------------------------ proposal record


@dataclass
class Proposal:
    block: str
    run_id: str
    root_cause: str
    status: str
    reasons: list[str] = field(default_factory=list)
    action: str | None = None
    plan: FixPlan | None = None
    version: str | None = None  # new_version: the version added to the block
    ref: str | None = None  # the new definition, "module:name"
    target_file: str | None = None  # repo-relative file the definition was appended to
    branch: str | None = None
    base: str | None = None
    tests: UnitTestResult | None = None
    shadow: ShadowCheck | None = None
    pr_url: str | None = None
    model: str | None = None
    attempts: int = 0
    dir: Path | None = None
    ts: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def ok(self) -> bool:
        return self.status in (READY, OPENED)

    def to_dict(self) -> dict[str, Any]:
        return {
            "block": self.block,
            "run_id": self.run_id,
            "root_cause": self.root_cause,
            "status": self.status,
            "reasons": self.reasons,
            "action": self.action,
            "plan": self.plan.to_dict() if self.plan else None,
            "version": self.version,
            "ref": self.ref,
            "target_file": self.target_file,
            "branch": self.branch,
            "base": self.base,
            "tests": self.tests.to_dict() if self.tests else None,
            "shadow": self.shadow.to_dict() if self.shadow else None,
            "pr_url": self.pr_url,
            "model": self.model,
            "attempts": self.attempts,
            "ts": self.ts,
            "advisory": True,
        }

    @classmethod
    def load(cls, path: Path | str) -> Proposal:
        path = Path(path)
        folder = path.parent if path.name == PROPOSAL_FILE else path
        raw = json.loads((folder / PROPOSAL_FILE).read_text(encoding="utf-8"))
        plan = raw.get("plan")
        return cls(
            block=raw["block"],
            run_id=raw["run_id"],
            root_cause=raw["root_cause"],
            status=raw["status"],
            reasons=raw["reasons"],
            action=raw["action"],
            plan=FixPlan(**{**plan, "evidence": tuple(plan["evidence"])}) if plan else None,
            version=raw["version"],
            ref=raw["ref"],
            target_file=raw["target_file"],
            branch=raw["branch"],
            base=raw["base"],
            tests=UnitTestResult(**raw["tests"]) if raw.get("tests") else None,
            shadow=(
                ShadowCheck(
                    raw["shadow"]["passed"],
                    tuple(raw["shadow"]["columns"]),
                    tuple(tuple(r) for r in raw["shadow"]["rows"]),
                    tuple(raw["shadow"]["notes"]),
                )
                if raw.get("shadow")
                else None
            ),
            pr_url=raw["pr_url"],
            model=raw.get("model"),
            attempts=raw.get("attempts", 0),
            dir=folder,
            ts=raw.get("ts", ""),
        )


def proposal_dir(root: Path | str, block: str, run_id: str) -> Path:
    return (
        Path(root)
        / PROPOSALS_DIR
        / validate_name(block, "block name")
        / validate_name(run_id, "run_id")
    )


def _record(g: Any, proposal: Proposal) -> Proposal:
    folder = proposal_dir(g.root, proposal.block, proposal.run_id)
    folder.mkdir(parents=True, exist_ok=True)
    proposal.dir = folder
    text = json.dumps(proposal.to_dict(), indent=2, sort_keys=True, default=str)
    (folder / PROPOSAL_FILE).write_text(text + "\n", encoding="utf-8", newline="\n")
    with (folder.parent / ATTEMPTS_FILE).open("a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(proposal.to_dict(), sort_keys=True, default=str) + "\n")
    g.events.emit(
        EventKind.PROPOSAL,
        block=proposal.block,
        run_id=proposal.run_id,
        status=proposal.status,
        action=proposal.action,
        version=proposal.version,
        branch=proposal.branch,
        pr_url=proposal.pr_url,
        reasons=proposal.reasons,
    )
    return proposal


# ------------------------------------------------------------------ PR body


def render_pr_body(
    proposal: Proposal, diagnosis: Diagnosis, bundle: EvidenceBundle, patch: str
) -> str:
    plan = proposal.plan
    assert plan is not None
    cited = list(
        dict.fromkeys([e for c in diagnosis.claims for e in c.evidence] + list(plan.evidence))
    )
    lines = [
        f"## Guardian proposal: `{plan.action}` for `{proposal.block}`",
        "",
        "> Proposed by the Guardian diagnosis agent. It is advisory: the agent never merges, "
        "never promotes and never pushes to the default branch. A human reviews this PR; "
        "once merged, the change still has to pass shadow promotion before it serves data.",
        "",
        "### Diagnosis",
        "",
        f"Root cause **{diagnosis.root_cause}** (confidence {diagnosis.confidence:.2f}) for run "
        f"`{proposal.run_id}`, model `{diagnosis.model}`.",
        "",
        diagnosis.summary,
        "",
        "| claim | evidence |",
        "|---|---|",
        *[f"| {c.statement} | {', '.join(c.evidence)} |" for c in diagnosis.claims],
        "",
        "### Cited evidence",
        "",
        "| id | kind | item |",
        "|---|---|---|",
        *[
            f"| {i} | {bundle.item(i).kind} | {bundle.item(i).title} |"
            for i in cited
            if i in bundle.ids
        ],
        "",
        "### Proposed change",
        "",
        plan.rationale,
        "",
        f"Fix evidence: {', '.join(plan.evidence)}.",
        "",
    ]
    if proposal.version:
        lines.append(
            f"Adds version `{proposal.version}` (`{proposal.ref}`) to `{proposal.block}`'s "
            "`versions`. The live version is unchanged."
        )
    else:
        lines.append(f"Adds `{proposal.ref}` and points the spec at it.")
    lines += ["", "### Checks", ""]
    if proposal.tests:
        state = "passed" if proposal.tests.passed else "FAILED"
        lines.append(f"- Unit tests ({state}): `{proposal.tests.command}`")
    if proposal.shadow:
        state = "passed" if proposal.shadow.passed else "FAILED"
        lines += [
            f"- Shadow run on the failing run's inputs ({state}):",
            "",
            proposal.shadow.markdown(),
        ]
    if proposal.version:
        lines += [
            "",
            "### After merging (humans)",
            "",
            "```",
            f"guardian shadow start {proposal.block} {proposal.version}",
            "guardian run <spec>   # the candidate runs in shadow on live inputs",
            f"guardian shadow promote {proposal.block} --approve   # promotion replays quarantine",
            "```",
        ]
    lines += [
        "",
        "<details><summary>Patch</summary>",
        "",
        "```diff",
        patch.rstrip(),
        "```",
        "",
        "</details>",
        "",
    ]
    return "\n".join(lines)


# ------------------------------------------------------------------ entry point


def _diagnosis_and_bundle(
    g: Any, block: str, run_id: str, client: LLMClient, key: str, git_history: bool
) -> tuple[Diagnosis, EvidenceBundle]:
    diagnosis = Diagnosis.load(g.root, block, run_id)
    evidence = diagnosis_dir(g.root, block, run_id) / EVIDENCE_FILE
    if diagnosis is None or not evidence.exists():
        result = diagnose(g, block, run_id, client=client, key=key, git=git_history)
        return result.diagnosis, result.bundle
    return diagnosis, EvidenceBundle.load(evidence)


def fix_context(
    file_block: BlockSpec, file_spec: PipelineSpec, live_ref: str, repo: Path
) -> dict[str, Any]:
    """What the model needs to write a fix: the live function, the schema module and the
    adapters standing in for the block, plus the names each target module already binds."""
    context: dict[str, Any] = {"block": file_block.name, "live_function": live_ref}
    module, _, _ = live_ref.partition(":")
    source = _read(repo / module_file(module, repo))
    context["live_module_source"] = _cut(source)
    context["live_module_names"] = sorted(_top_level_names(ast.parse(source)))
    if file_block.schema:
        schema_module = file_block.schema.partition(":")[0]
        schema_source = _read(repo / module_file(schema_module, repo))
        context["schema"] = file_block.schema
        context["schema_module_source"] = _cut(schema_source)
        context["schema_module_names"] = sorted(_top_level_names(ast.parse(schema_source)))
    context["fallback_edges_standing_in"] = [
        {"dependent": d.name, "source": e.source, "adapter": e.adapter}
        for d in file_spec.dependents(file_block.name)
        if (e := d.fallback_for(file_block.name)) is not None
    ]
    return context


def propose(
    g: Any,
    block: str,
    run_id: str | None = None,
    *,
    client: LLMClient,
    spec_path: Path | str,
    repo: Path | str | None = None,
    open_pr: bool = False,
    key: str | None = None,
    github: GitHubClient | None = None,
    git_history: bool = True,
) -> Proposal:
    """Propose (and verify) a fix for ``block``'s failure on ``run_id``.

    ``g`` is the Guardian that ran the pipeline (only read, through ``agent_view``);
    ``spec_path`` is the pipeline spec file in the git repository ``repo`` (default: the
    repository containing it). Returns the recorded Proposal; never raises for a fix that
    does not work, only for misconfiguration.
    """
    g = agent_view(g)
    g.spec.block(block)
    run_id = run_id or default_run(g, block)
    key = key or f"{block}/{run_id}"
    spec_path = Path(spec_path).resolve()
    git = SafeGit(repo or spec_path.parent)
    repo_root = Path(git.run("rev-parse", "--show-toplevel").strip())
    git = SafeGit(repo_root)
    spec_rel = spec_path.relative_to(repo_root.resolve())

    diagnosis, bundle = _diagnosis_and_bundle(g, block, run_id, client, key, git_history)
    proposal = Proposal(block, run_id, diagnosis.root_cause, REJECTED, model=client.model)
    if not diagnosis.accepted or diagnosis.root_cause == UNKNOWN:
        proposal.reasons.append(
            f"no fix proposed: the diagnosis is {diagnosis.status} with root cause "
            f"{diagnosis.root_cause!r}" + (f" ({diagnosis.reason})" if diagnosis.reason else "")
        )
        return _record(g, proposal)

    file_text = _read(spec_path)
    file_spec = _parse(file_text, str(spec_path))
    file_block = file_spec.block(block)
    live = g.active_version(block)
    live_ref = file_block.version_ref(live if live in file_block.versions else None)
    context = fix_context(file_block, file_spec, live_ref, repo_root)
    n = 1
    while f"fix{n}" in file_block.versions:
        n += 1
    version = f"fix{n}"

    def check(plan: FixPlan) -> None:
        if plan.action == "upstream_note":
            return
        target = _target_module(plan, file_block, file_spec, live_ref)
        source = _read(repo_root / module_file(target, repo_root))
        kind = "schema" if plan.action == "schema_update" else "function"
        append_definition(source, plan.code, plan.name, kind=kind)
        if plan.action == "adapter_update":
            dependent = (
                file_spec.block(plan.dependent) if plan.dependent in file_spec.block_names else None
            )
            if dependent is None or dependent.fallback_for(block) is None:
                raise ResponseError(
                    f"{plan.dependent!r} has no fallback edge standing in for {block!r}"
                )

    plan, rejection, attempts = request_fix(
        bundle, diagnosis, context, client, key=f"{key}:fix", check=check
    )
    proposal.attempts = attempts
    if plan is None:
        proposal.reasons.append(rejection or "no plan")
        return _record(g, proposal)
    proposal.plan, proposal.action = plan, plan.action

    if plan.action == "upstream_note":
        proposal.status = NOTE
        _record(g, proposal)
        assert proposal.dir is not None
        note = "\n".join(
            [
                f"# Note for the owner of the data upstream of `{block}`",
                "",
                f"Run `{run_id}`: {diagnosis.summary}",
                "",
                plan.note,
                "",
                "Evidence: " + ", ".join(f"{i} ({bundle.item(i).title})" for i in plan.evidence),
                "",
            ]
        )
        (proposal.dir / NOTE_FILE).write_text(note, encoding="utf-8", newline="\n")
        return proposal

    return _apply_and_verify(
        g,
        git,
        repo_root,
        spec_rel,
        file_text,
        file_spec,
        proposal,
        plan,
        diagnosis,
        bundle,
        version,
        live_ref,
        open_pr=open_pr,
        github=github,
    )


def _target_module(
    plan: FixPlan, file_block: BlockSpec, file_spec: PipelineSpec, live_ref: str
) -> str:
    if plan.action == "new_version":
        return live_ref.partition(":")[0]
    if plan.action == "schema_update":
        if not file_block.schema:
            raise ResponseError(f"block {file_block.name!r} has no schema to update")
        return file_block.schema.partition(":")[0]
    if plan.dependent not in file_spec.block_names:
        raise ResponseError(f"unknown dependent {plan.dependent!r}")
    edge = file_spec.block(plan.dependent).fallback_for(file_block.name)
    if edge is None:
        raise ResponseError(f"{plan.dependent!r} has no fallback edge standing in for the block")
    return (edge.adapter or live_ref).partition(":")[0]


def _apply_and_verify(
    g: Any,
    git: SafeGit,
    repo_root: Path,
    spec_rel: Path,
    file_text: str,
    file_spec: PipelineSpec,
    proposal: Proposal,
    plan: FixPlan,
    diagnosis: Diagnosis,
    bundle: EvidenceBundle,
    version: str,
    live_ref: str,
    *,
    open_pr: bool,
    github: GitHubClient | None,
) -> Proposal:
    block = proposal.block
    file_block = file_spec.block(block)
    module = _target_module(plan, file_block, file_spec, live_ref)
    target_rel = module_file(module, repo_root)
    ref = f"{module}:{plan.name}"
    proposal.ref, proposal.target_file = ref, target_rel.as_posix()
    if plan.action == "new_version":
        proposal.version = version

    base = git.run("rev-parse", "HEAD").strip()
    label = version if plan.action == "new_version" else plan.name
    stem = f"{BRANCH_PREFIX}{block}/{proposal.run_id}-{label}"
    branch, n = git.check_branch(stem), 1
    while git.run("branch", "--list", branch).strip():  # an earlier attempt kept its branch
        n += 1
        branch = git.check_branch(f"{stem}-{n}")
    proposal.branch = branch
    proposal.base = git.default_branch() or "main"
    tmp = Path(tempfile.mkdtemp(prefix="guardian-fix-"))
    worktree = tmp / "worktree"
    pushed = False
    try:
        git.run("worktree", "add", "-b", branch, str(worktree), base)
        # Append the definition and point the spec at it; verify both edits.
        target = worktree / target_rel
        source = _read(target)
        kind = "schema" if plan.action == "schema_update" else "function"
        target.write_text(
            append_definition(source, plan.code, plan.name, kind=kind), encoding="utf-8"
        )
        spec_file = worktree / spec_rel
        if plan.action == "new_version":
            edited = add_version(file_text, block, version, ref)
            after = _parse(edited, str(spec_file))
            if after.block(block).versions.get(version) != ref:
                raise ProposalError("the spec edit did not add the new version")
            if after.block(block).fn != file_block.fn:
                raise ProposalError("the spec edit changed the block's live function")
        elif plan.action == "schema_update":
            edited = set_block_key(file_text, block, "schema", ref)
            after = _parse(edited, str(spec_file))
            if after.block(block).schema != ref:
                raise ProposalError("the spec edit did not set the schema")
        else:
            edited = set_edge_adapter(file_text, plan.dependent, block, ref)
            after = _parse(edited, str(spec_file))
            edge = after.block(plan.dependent).fallback_for(block)
            if edge is None or edge.adapter != ref:
                raise ProposalError("the spec edit did not set the adapter")
        changed = plan.dependent if plan.action == "adapter_update" else block
        _only_changed(file_spec, after, changed)
        spec_file.write_text(edited, encoding="utf-8")

        git.run("add", "--", target_rel.as_posix(), spec_rel.as_posix(), cwd=worktree)
        git.run(
            "commit",
            "-m",
            f"guardian: propose {plan.action} for {block} ({diagnosis.root_cause} on "
            f"{proposal.run_id})",
            cwd=worktree,
        )
        patch = git.run("diff", base, "HEAD", cwd=worktree)

        folder = proposal_dir(g.root, block, proposal.run_id)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / PATCH_FILE).write_text(patch, encoding="utf-8", newline="\n")
        saved = folder / FILES_DIR / target_rel
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(target, saved)

        # Checks: the block's unit tests (in the worktree), then a shadow run.
        tests = after.block(block).tests
        proposal.tests = run_block_tests(worktree, tests)
        try:
            loaded = _load_module(target, f"{block}_{plan.name}")
            obj = getattr(loaded, plan.name)
            if plan.action == "new_version":
                proposal.shadow = check_new_version(g, block, proposal.run_id, version, obj)
            elif plan.action == "schema_update":
                proposal.shadow = check_schema(g, block, proposal.run_id, plan.name, obj)
            else:
                proposal.shadow = check_adapter(g, block, plan.dependent, plan.name, obj)
        except Exception as exc:
            proposal.shadow = ShadowCheck(
                False, ("metric", "live", "candidate"), (), (f"could not run: {exc}",)
            )
        if not proposal.tests.passed:
            proposal.reasons.append("unit tests failed: " + proposal.tests.output[-500:])
        if not proposal.shadow.passed:
            proposal.reasons.append(
                "shadow run did not pass validation: "
                + "; ".join(f"{m}: {c}" for m, _, c in proposal.shadow.rows[:4])
                + " ".join(proposal.shadow.notes)
            )
        if proposal.reasons:
            proposal.status = FAILED
            return _record(g, proposal)

        body = render_pr_body(proposal, diagnosis, bundle, patch)
        (folder / PR_BODY_FILE).write_text(body, encoding="utf-8", newline="\n")
        proposal.status = READY
        if open_pr:
            client = github or GitHubClient.from_env()
            slug = github_repo(git)
            git.run("push", "origin", f"{branch}:{branch}", cwd=worktree)
            pushed = True
            pr = client.create_pull_request(
                slug,
                head=branch,
                base=proposal.base,
                title=f"Guardian: {plan.action} for {block} ({diagnosis.root_cause})",
                body=body,
            )
            proposal.pr_url = pr.url
            proposal.status = OPENED
        return _record(g, proposal)
    except AgentPolicyError:
        raise  # a policy violation is never just a failed attempt
    except (GuardianError, OSError) as exc:
        proposal.status = FAILED
        proposal.reasons.append(f"{type(exc).__name__}: {exc}")
        return _record(g, proposal)
    finally:
        with contextlib.suppress(GuardianError):
            git.run("worktree", "remove", "--force", str(worktree))
        if not pushed:  # a dry run (or a failed push) leaves no branch behind
            with contextlib.suppress(GuardianError):
                git.run("branch", "-D", branch)
        shutil.rmtree(tmp, ignore_errors=True)


# ------------------------------------------------------------------ as if merged


def with_proposed_version(
    spec: PipelineSpec, proposal: Proposal
) -> tuple[PipelineSpec, dict[str, Any]]:
    """``spec`` with a proposal's new version added, and the registry to run it: the
    state after a human merged the pull request. Used to shadow a dry-run proposal."""
    if proposal.version is None or proposal.plan is None or proposal.dir is None:
        raise ProposalError("the proposal does not add a version")
    assert proposal.target_file is not None
    module = _load_module(
        proposal.dir / FILES_DIR / proposal.target_file, f"{proposal.block}_{proposal.version}"
    )
    key = f"__proposal__:{proposal.block}:{proposal.version}"
    registry = {key: getattr(module, proposal.plan.name)}
    blocks = []
    for b in spec.blocks:
        if b.name == proposal.block:
            versions = dict(b.versions) or {"v1": b.fn}
            versions[proposal.version] = key
            b = dataclasses.replace(b, versions=versions, active=b.active or "v1")
        blocks.append(b)
    return dataclasses.replace(spec, blocks=tuple(blocks)), registry
