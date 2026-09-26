"""Fix proposals: new versions (never in-place edits), verified in a git worktree, as a
draft PR at most. The LLM is always a FakeClient; GitHub is a fake transport."""

from __future__ import annotations

import json
import shutil

import pytest
import yaml

from guardian.agent.diagnose import FakeClient, ResponseError
from guardian.agent.propose import (
    FAILED,
    NOTE,
    OPENED,
    READY,
    REJECTED,
    Proposal,
    ProposalError,
    add_version,
    append_definition,
    propose,
    set_edge_adapter,
    with_proposed_version,
)
from guardian.agent.safety import BRANCH_PREFIX, GitHubClient
from guardian.core.events import EventKind
from guardian.core.guardian import Guardian
from guardian.core.refs import load_ref
from guardian.demo.faults import apply_faults, code_bug, schema_drift
from guardian.runner.spec_loader import parse_spec

from ..helpers.project_repo import SPEC_REL, git, make_project_repo, repo_state
from ..helpers.roles import block_params, dependents, representative, role_params
from .helpers import (
    ROLES,
    SPEC,
    answer,
    broken_fix,
    fault_for,
    fix_answer,
    live_function,
    revert_fix,
    run,
    with_block,
)

by_role = pytest.mark.parametrize("role", role_params(ROLES))
UNKNOWN_ANSWER = json.dumps(
    {"root_cause": "unknown", "confidence": 0.3, "summary": "unclear", "claims": []}
)
YAML_TEXT = (SPEC_REL.parent / "pipeline.yaml").as_posix()


def faulted_run(root, block: str, *faults, spec=SPEC):
    """r1 clean, r2 with ``faults`` on ``block``; returns the faulted (spec, registry)."""
    run(root, spec, "r1")
    faulted, registry = apply_faults(spec, {block: list(faults)})
    run(root, spec, "r2", {block: list(faults)})
    return faulted, registry


def do_propose(root, block, client, repo, spec=SPEC, faults=(), **kw) -> Proposal:
    faulted, registry = apply_faults(spec, {block: list(faults)})
    with Guardian(faulted, root, registry=registry) as g:
        return propose(
            g, block, "r2", client=client, spec_path=repo / SPEC_REL, git_history=False, **kw
        )


# ---------------------------------------------------------------- source edits


def test_append_definition_only_adds_new_names() -> None:
    source = "import pandas as pd\n\n\ndef f(df):\n    return df\n"
    patched = append_definition(source, "def g(df):\n    return f(df)\n", "g", kind="function")
    assert patched.startswith(source.rstrip("\n")) and patched.endswith("return f(df)\n")
    for code, name, match in (
        ("def f(df):\n    return None\n", "f", "rebinds"),
        ("import pandas as pd\ndef g(df):\n    return df\n", "g", "rebinds"),
        ("def g(df):\n    return df\n", "h", "must define"),
        ("def g(df:\n", "g", "does not parse"),
        ("g = 1\n", "g", "must define"),
    ):
        with pytest.raises(ResponseError, match=match):
            append_definition(source, code, name, kind="function")
    schema = append_definition(source, "S2 = dict()\n", "S2", kind="schema")
    assert schema.endswith("S2 = dict()\n")


@pytest.mark.parametrize("block", block_params(SPEC))
def test_add_version_to_any_block_keeps_everything_else(project_repo, block: str) -> None:
    text = (project_repo / SPEC_REL).read_text(encoding="utf-8")
    before = parse_spec(yaml.safe_load(text))
    edited = add_version(text, block, "fix9", "demo.blocks:something_new")
    after = parse_spec(yaml.safe_load(edited))
    assert after.block(block).versions["fix9"] == "demo.blocks:something_new"
    assert after.block(block).fn == before.block(block).fn  # the live function is unchanged
    for other in before.block_names:
        if other != block:
            assert after.block(other) == before.block(other)
    removed = set(text.splitlines()) - set(edited.splitlines())
    assert all(line.strip().startswith("fn:") for line in removed)  # comments all kept


def test_set_edge_adapter_and_bad_targets(project_repo) -> None:
    text = (project_repo / SPEC_REL).read_text(encoding="utf-8")
    block = representative(SPEC, "fallback_protected")
    dependent = next(d for d in dependents(SPEC, block) if SPEC.block(d).fallback_for(block))
    edited = set_edge_adapter(text, dependent, block, "demo.blocks:new_adapter")
    assert parse_spec(yaml.safe_load(edited)).block(dependent).fallback_for(block).adapter == (
        "demo.blocks:new_adapter"
    )
    with pytest.raises(ProposalError):
        set_edge_adapter(text, block, dependent, "x:y")
    with pytest.raises(ProposalError):
        add_version(text, "no_such_block", "fix1", "x:y")


# ---------------------------------------------------------------- code_bug -> new version


@by_role
def test_code_bug_proposal_is_a_verified_new_version(tmp_path, project_repo, role: str) -> None:
    block = representative(SPEC, role)
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    before = repo_state(project_repo)
    client = FakeClient(
        {f"{block}/r2": answer("code_bug", ("E1",)), f"{block}/r2:fix": revert_fix(SPEC, block)}
    )
    p = do_propose(root, block, client, project_repo, faults=[code_bug()])

    assert p.status == READY, p.reasons
    assert p.version == "fix1" and p.ref.endswith(f":{live_function(SPEC, block)}_fixed")
    assert p.tests.passed and f"test_block_contract[{block}]" in p.tests.command
    assert p.shadow.passed
    assert p.branch.startswith(f"{BRANCH_PREFIX}{block}/r2-")
    # The repository is exactly as before: no commit on main, no worktree, no branch left.
    assert repo_state(project_repo) == before

    folder = root / "proposals" / block / "r2"
    patch = (folder / "patch.diff").read_text(encoding="utf-8")
    removed = [
        line for line in patch.splitlines() if line.startswith("-") and not line.startswith("---")
    ]
    assert all(line.strip().startswith("-    fn:") or line == "-" for line in removed), removed
    assert f"+def {live_function(SPEC, block)}_fixed" in patch and "fix1: demo.blocks:" in patch

    body = (folder / "pr_body.md").read_text(encoding="utf-8")
    for text in (
        "### Diagnosis",
        "code_bug",
        "### Cited evidence",
        "| E1 | run_outcome |",
        "proposed new_version",
        "never merges",
        "| metric | live `",
        "| pass rate |",
        f"guardian shadow start {block} fix1",
    ):
        assert text in body, text
    saved = json.loads((folder / "proposal.json").read_text(encoding="utf-8"))
    assert saved["status"] == READY and saved["advisory"] is True
    attempts = (folder.parent / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(a)["status"] for a in attempts] == [READY]

    with Guardian(SPEC, root) as g:
        (event,) = g.events.query(kind=EventKind.PROPOSAL, block=block)
        assert event.data["status"] == READY
        # The agent changed nothing in Guardian: no promotion, no shadow, status as is.
        assert g.versions.history(block) == [] and g.shadows.active() == []
        assert g.status(block).value == "DEGRADED"


def test_a_fix_that_breaks_the_block_is_recorded_as_failed(tmp_path, project_repo) -> None:
    block = representative(SPEC, "leaf")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    before = repo_state(project_repo)
    client = FakeClient(
        {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": broken_fix(SPEC, block)}
    )
    p = do_propose(root, block, client, project_repo, faults=[code_bug()])
    assert p.status == FAILED
    assert not p.tests.passed and not p.shadow.passed
    assert any(r.startswith("unit tests failed") for r in p.reasons)
    assert any(r.startswith("shadow run did not pass") for r in p.reasons)
    folder = root / "proposals" / block / "r2"
    assert (folder / "patch.diff").exists() and not (folder / "pr_body.md").exists()
    assert repo_state(project_repo) == before


def test_rejected_plans(tmp_path, project_repo) -> None:
    block = representative(SPEC, "source")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    good = revert_fix(SPEC, block)
    fn = live_function(SPEC, block)
    rebinding = fix_answer("new_version", fn, f"def {fn}(*a, **k):\n    return None\n")
    cases = [
        # diagnosis unknown: no fix is even requested
        (UNKNOWN_ANSWER, [good], REJECTED, "diagnosis is accepted with root cause 'unknown'", 0),
        # an action not allowed for code_bug, twice
        (
            answer("code_bug"),
            [fix_answer("upstream_note", note="x")] * 2,
            REJECTED,
            "not allowed for root cause 'code_bug'",
            2,
        ),
        # citing evidence that does not exist: rejected at once
        (answer("code_bug"), [revert_fix(SPEC, block, ("E999",)), good], REJECTED, "E999", 1),
        # rebinding the live function (an in-place edit) is refused, the retry is fine
        (answer("code_bug"), [rebinding, good], READY, None, 2),
    ]
    for diagnosis, fixes, status, reason, fix_calls in cases:
        for sub in ("diagnoses", "proposals"):
            shutil.rmtree(root / sub, ignore_errors=True)
        client = FakeClient({f"{block}/r2": diagnosis, f"{block}/r2:fix": fixes})
        p = do_propose(root, block, client, project_repo, faults=[code_bug()])
        assert p.status == status, (p.status, p.reasons)
        if reason:
            assert any(reason in r for r in p.reasons), p.reasons
        assert sum(c.key.endswith(":fix") for c in client.calls) == fix_calls


# ---------------------------------------------------------------- other root causes


def _non_strict_block() -> str:
    """The first non-source block whose schema accepts extra columns."""
    return next(
        b.name for b in SPEC.blocks if b.schema and b.inputs and not load_ref(b.schema).strict
    )


def test_schema_change_gets_a_new_schema(tmp_path, project_repo) -> None:
    block = _non_strict_block()
    root = tmp_path / "g"
    with Guardian(SPEC, root) as g:
        declared = g.validator_for(block).schema
        column = next(c for c, col in declared.columns.items() if col.required)
    schema_name = SPEC.block(block).schema.split(":")[1]
    drift = schema_drift(rename={column: f"{column}_v2"})
    faulted_run(root, block, drift)
    code = (
        f"{schema_name}_optional_{column} = {schema_name}.update_column(\n"
        f"    {column!r}, required=False\n)\n"
    )
    client = FakeClient(
        {
            f"{block}/r2": answer("schema_change", ("E1",)),
            f"{block}/r2:fix": fix_answer(
                "schema_update", f"{schema_name}_optional_{column}", code, ("E1",)
            ),
        }
    )
    p = do_propose(root, block, client, project_repo, faults=[drift])
    assert p.status == READY, p.reasons
    assert p.version is None and p.ref.endswith(f":{schema_name}_optional_{column}")
    assert p.shadow.passed and dict((r[0], r[2]) for r in p.shadow.rows)["schema error"] == "none"
    patch = (root / "proposals" / block / "r2" / "patch.diff").read_text(encoding="utf-8")
    assert f"+    schema: {p.ref}" in patch


def test_schema_change_can_get_a_new_fallback_adapter(tmp_path, project_repo) -> None:
    block = representative(SPEC, "fallback_protected")
    dependent = next(d for d in dependents(SPEC, block) if SPEC.block(d).fallback_for(block))
    old = SPEC.block(dependent).fallback_for(block).adapter.split(":")[1]
    root = tmp_path / "g"
    run(root, SPEC, "r1")
    drift = fault_for(root, SPEC, block, "schema_drift")
    run(root, SPEC, "r2", {block: [drift]})
    code = f"def {old}_v2(df):\n    return {old}(df)\n"
    client = FakeClient(
        {
            f"{block}/r2": answer("schema_change"),
            f"{block}/r2:fix": fix_answer(
                "adapter_update", f"{old}_v2", code, ("E1",), dependent=dependent
            ),
        }
    )
    p = do_propose(root, block, client, project_repo, faults=[drift])
    assert p.status == READY, p.reasons
    assert p.tests.passed and p.shadow.passed
    patch = (root / "proposals" / block / "r2" / "patch.diff").read_text(encoding="utf-8")
    assert f"adapter: demo.blocks:{old}_v2" in patch


def test_data_drift_can_get_a_note_instead_of_code(tmp_path, project_repo) -> None:
    block = representative(SPEC, "source")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    before = repo_state(project_repo)
    note = "Half of yesterday's orders arrived with sentinel values; please re-export."
    client = FakeClient(
        {
            f"{block}/r2": answer("upstream_data_drift", ("E1", "E2")),
            f"{block}/r2:fix": fix_answer("upstream_note", note=note, evidence=("E2",)),
        }
    )
    p = do_propose(root, block, client, project_repo, faults=[code_bug()])
    assert p.status == NOTE and p.branch is None and p.tests is None
    text = (root / "proposals" / block / "r2" / "note.md").read_text(encoding="utf-8")
    assert note in text and "E2" in text
    assert not (root / "proposals" / block / "r2" / "patch.diff").exists()
    assert repo_state(project_repo) == before


# ---------------------------------------------------------------- opening a PR


class Transport:
    def __init__(self) -> None:
        self.calls = []

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, json.loads(body)))
        return 201, {"number": 1, "html_url": "https://github.example/pull/1"}


def test_open_pr_pushes_only_an_agent_branch(tmp_path, monkeypatch) -> None:
    repo = make_project_repo(tmp_path / "repo")
    origin = tmp_path / "origin.git"
    git(tmp_path, "clone", "-q", "--bare", str(repo), str(origin))
    git(repo, "remote", "add", "origin", str(origin))
    git(repo, "fetch", "-q", "origin")
    monkeypatch.setenv("GUARDIAN_GITHUB_REPO", "example/guardian")
    origin_main = git(origin, "rev-parse", "main")

    block = representative(SPEC, "multi_dependent")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    client = FakeClient(
        {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": revert_fix(SPEC, block)}
    )
    transport = Transport()
    p = do_propose(
        root,
        block,
        client,
        repo,
        faults=[code_bug()],
        open_pr=True,
        github=GitHubClient("tok", transport=transport),
    )
    assert p.status == OPENED and p.pr_url == "https://github.example/pull/1"
    assert git(origin, "rev-parse", "main") == origin_main  # the default branch is untouched
    assert git(origin, "rev-parse", "--verify", p.branch)
    ((method, url, payload),) = transport.calls
    assert (method, url) == ("POST", "https://api.github.com/repos/example/guardian/pulls")
    assert payload["head"] == p.branch and payload["base"] == "main" and payload["draft"]
    body = (root / "proposals" / block / "r2" / "pr_body.md").read_text(encoding="utf-8")
    assert payload["body"] == body


def test_a_failed_fix_opens_no_pr(tmp_path, project_repo) -> None:
    block = representative(SPEC, "unprotected")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    transport = Transport()
    client = FakeClient(
        {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": broken_fix(SPEC, block)}
    )
    p = do_propose(
        root,
        block,
        client,
        project_repo,
        faults=[code_bug()],
        open_pr=True,
        github=GitHubClient("tok", transport=transport),
    )
    assert p.status == FAILED and transport.calls == []


# ---------------------------------------------------------------- never promotes


def test_propose_never_touches_promotion_or_shadow_state(tmp_path, project_repo, monkeypatch):
    block = representative(SPEC, "fallback_protected")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())

    def forbidden(*args, **kwargs):
        raise AssertionError("the agent called a state-changing Guardian method")

    for name in (
        "promote",
        "shadow_start",
        "shadow_stop",
        "rollback_version",
        "set_block_status",
        "replay",
    ):
        monkeypatch.setattr(Guardian, name, forbidden)
    client = FakeClient(
        {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": revert_fix(SPEC, block)}
    )
    assert do_propose(root, block, client, project_repo, faults=[code_bug()]).status == READY


def test_with_proposed_version_loads_the_fix_as_if_merged(tmp_path, project_repo) -> None:
    block = representative(SPEC, "leaf")
    root = tmp_path / "g"
    faulted_run(root, block, code_bug())
    client = FakeClient(
        {f"{block}/r2": answer("code_bug"), f"{block}/r2:fix": revert_fix(SPEC, block)}
    )
    p = do_propose(root, block, client, project_repo, faults=[code_bug()])
    spec, registry = with_proposed_version(SPEC, Proposal.load(p.dir))
    b = spec.block(block)
    assert p.version in b.versions and b.active in b.versions
    assert b.version_ref(b.active) == SPEC.block(block).fn  # live unchanged
    assert callable(registry[b.versions[p.version]])
    unversioned = with_block(SPEC, block, versions={}, active=None)
    spec2, _ = with_proposed_version(unversioned, Proposal.load(p.dir))
    assert set(spec2.block(block).versions) == {"v1", p.version}
