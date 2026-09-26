"""Evidence bundles: deterministic, complete, minimized. Blocks are picked by DAG role."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from guardian.agent.evidence import (
    REDACTED,
    EvidenceBundle,
    EvidenceError,
    build_evidence,
    default_run,
    git_history,
)
from guardian.core.dag import ROLES as CORE_ROLES
from guardian.core.dag import ancestors as core_ancestors
from guardian.core.dag import roles_of as core_roles_of
from guardian.core.events import EventKind
from guardian.core.guardian import ROLLBACK_RULE, Guardian
from guardian.core.provenance import QUALITY_COL
from guardian.demo.faults import CORRUPT_NUMBER, CORRUPT_TEXT, crash

from ..helpers.roles import ROLES as TEST_ROLES
from ..helpers.roles import ancestors, dependents, representative, roles_of
from .helpers import FAULT_TYPES, ROLES, SPEC, clean_then_fault, run, with_block

by_role = pytest.mark.parametrize("role", ROLES)


def bundle(root, block: str, run_id: str | None = "r2", spec=SPEC, **kw) -> EvidenceBundle:
    kw.setdefault("git", False)
    with Guardian(spec, root) as g:
        return build_evidence(g, block, run_id, **kw)


def only(b: EvidenceBundle, kind: str) -> dict:
    items = b.of_kind(kind)
    assert len(items) == 1, (kind, items)
    return items[0].data


def stats(b: EvidenceBundle, column: str) -> dict:
    return next(i.data for i in b.of_kind("column_stats") if i.data["column"] == column)


# ---------------------------------------------------------------- structure


@by_role
def test_bundle_is_deterministic_with_sequential_ids(tmp_path, role: str) -> None:
    block = representative(SPEC, role)
    root = tmp_path / "g"
    clean_then_fault(root, SPEC, block, "code_bug")
    first = bundle(root, block, git=True)
    again = bundle(root, block, git=True)
    assert first.to_json() == again.to_json()
    assert first.ids == tuple(f"E{i}" for i in range(1, len(first.items) + 1))

    path = first.write(root)
    assert path == root / "diagnoses" / block / "r2" / "evidence.json"
    assert EvidenceBundle.load(path).to_json() == first.to_json()

    # Later activity (another run, a replay) does not change the bundle of r2.
    run(root, SPEC, "r3")
    with Guardian(SPEC, root) as g:
        g.replay(block)
    assert bundle(root, block, git=True).to_json() == first.to_json()


@pytest.mark.parametrize("fault_type", FAULT_TYPES)
@by_role
def test_bundle_carries_the_signal_of_each_fault(tmp_path, role: str, fault_type: str) -> None:
    block = representative(SPEC, role)
    root = tmp_path / "g"
    report = clean_then_fault(root, SPEC, block, fault_type)
    assert report.get(block).outcome.value == "ROLLBACK"
    b = bundle(root, block)
    outcome = only(b, "run_outcome")
    schema = only(b, "schema_diff")
    code = only(b, "code_change")

    with Guardian(SPEC, root) as g:
        records = g.quarantine.list(block=block, run_id="r2")
    bad = [r for r in records if r.rule_name != ROLLBACK_RULE]
    assert outcome["outcome"] == "ROLLBACK" and outcome["last_good_run"] == "r1"
    assert outcome["bad_rows"] == len(bad)
    assert outcome["good_rows"] + outcome["bad_rows"] == len(records)
    rules = b.of_kind("validation_rule")
    assert rules and all(0 < r.data["rows"] <= len(bad) for r in rules)
    assert code["this_run"] and code["last_good"]

    if fault_type == "schema_drift":
        (missing,) = schema["missing_vs_last_good"]
        assert schema["added_vs_last_good"] == [f"{missing}_v2"]
        assert missing in schema["declared_required_missing"]
        assert outcome["schema_level_failure"] is True
    else:
        assert schema["missing_vs_last_good"] == [] and schema["added_vs_last_good"] == []
        assert outcome["schema_level_failure"] is False

    if fault_type == "code_bug":
        assert code["changed"] is True
        assert code["this_run"]["function"] != code["last_good"]["function"]
        assert any(line.startswith("+") for line in code["diff"])
    else:
        assert code["changed"] is False and "diff" not in code

    if fault_type == "corrupt_rows":
        bad_stats = [i.data["bad_rows"] for i in b.of_kind("column_stats")]
        assert any(
            (s.get("min") is not None and s["min"] <= CORRUPT_NUMBER + 1000)
            or any(str(v).startswith(CORRUPT_TEXT) for v, _ in s.get("top_values", []))
            for s in bad_stats
            if s and s.get("present", True)
        )
    if fault_type == "null_burst":
        column = next(
            i.data["column"]
            for i in b.of_kind("column_stats")
            if (i.data["bad_rows"] or {}).get("null_rate") == 1.0
        )
        assert stats(b, column)["last_good"]["null_rate"] == 0.0


def test_quarantine_sample_is_capped(tmp_path) -> None:
    block = representative(SPEC, "source")
    root = tmp_path / "g"
    clean_then_fault(root, SPEC, block, "corrupt_rows")
    for size in (20, 5, 0):
        sample = only(bundle(root, block, sample_size=size), "quarantine_sample")
        assert sample["bad_rows"] > 20
        assert len(sample["rows"]) == size == sample["sample_size"]


@by_role
def test_redacted_columns_never_appear(tmp_path, role: str) -> None:
    """Every column of the block is redacted: no text value of it may leave, even in
    validation messages; stats keep only counts and null rates."""
    block = representative(SPEC, role)
    root = tmp_path / "g"
    run(root, SPEC, "r0")
    with Guardian(SPEC, root) as g:
        # User columns only: _guardian_quality is Guardian's own annotation.
        columns = [c for c in g.snapshots.read(block, "r0").columns if c != QUALITY_COL]
    spec = with_block(SPEC, block, redact_columns=tuple(columns))
    clean_then_fault(root, spec, block, "code_bug")
    b = bundle(root, block, spec=spec)
    text = b.to_json()

    with Guardian(spec, root) as g:
        frames = [g.snapshots.read(block, "r1")]
        payloads = [r.payload() for r in g.quarantine.list(block=block, run_id="r2")]
    values = {str(v) for f in frames for c in columns if c in f for v in f[c].dropna()}
    values |= {
        str(v) for p in payloads for c in columns if isinstance(p.get(c), str) for v in [p[c]]
    }
    secrets = {v.strip() for v in values if len(v.strip()) >= 4 and not v.strip().isdigit()}
    assert secrets, "the block should have text values to redact"
    leaked = sorted(s for s in secrets if s in text)
    assert not leaked, leaked[:5]
    assert REDACTED in text
    for item in b.of_kind("column_stats"):
        for side in ("good_rows", "bad_rows", "last_good"):
            profile = item.data[side]
            if profile and profile.get("present", True):
                assert profile["values"] == REDACTED
                assert not {"top_values", "min", "max", "mean"} & set(profile)
    for row in only(b, "quarantine_sample")["rows"]:
        assert all(v in (REDACTED, None) for v in row["row"].values())


# ---------------------------------------------------------------- context


@by_role
def test_events_come_from_the_block_and_its_upstreams(tmp_path, role: str) -> None:
    block = representative(SPEC, role)
    root = tmp_path / "g"
    clean_then_fault(root, SPEC, block, "corrupt_rows")
    b = bundle(root, block)
    events = [i.data for i in b.of_kind("event")]
    scope = {block, *ancestors(SPEC, block)}
    assert events and {e["block"] for e in events} <= scope
    assert any(
        e["event"] == "ROLLBACK" and e["block"] == block and e["run_id"] == "r2" for e in events
    )
    assert len(events) <= 20
    assert [e["ts"] for e in events] == sorted(e["ts"] for e in events)

    # A diagnosis event (or any agent warning) does not feed back into the bundle.
    with Guardian(SPEC, root) as g:
        g.events.emit(EventKind.DIAGNOSIS, block=block, run_id="r2", root_cause="unknown")
        g.events.emit(EventKind.WARN, block=block, run_id="r2", warning="x", agent=True)
    assert bundle(root, block).to_json() == b.to_json()


def test_dag_item_matches_the_spec_for_every_block(tmp_path) -> None:
    root = tmp_path / "g"
    run(root, SPEC, "r1")
    assert CORE_ROLES == TEST_ROLES
    for block in SPEC.block_names:
        dag = only(bundle(root, block, "r1"), "dag")
        assert set(dag["roles"]) == roles_of(SPEC, block)
        assert dag["dependents"] == dependents(SPEC, block)
        assert set(dag["upstream_blocks"]) == set(ancestors(SPEC, block))
        assert set(core_ancestors(SPEC, block)) == set(ancestors(SPEC, block))
        assert set(core_roles_of(SPEC, block)) == roles_of(SPEC, block)
        assert dag["inputs"] == list(SPEC.block(block).inputs)


def test_inputs_record_where_each_input_came_from(tmp_path) -> None:
    """A crash of a fallback-protected block: its dependent's bundle shows the fallback
    read; the crashed block's bundle shows its (fresh) inputs, no output, the error."""
    block = representative(SPEC, "fallback_protected")
    reader = next(d for d in dependents(SPEC, block) if SPEC.block(d).fallback_for(block))
    root = tmp_path / "g"
    run(root, SPEC, "r1")
    run(root, SPEC, "r2", {block: [crash("boom in test")]})

    via = {i.data["upstream"]: i.data for i in bundle(root, reader).of_kind("input")}
    assert via[block]["quality"] == "FALLBACK"
    assert via[block]["read"].startswith(SPEC.block(reader).fallback_for(block).source)
    assert "fallback" in via[block]["how"]

    b = bundle(root, block)
    outcome = only(b, "run_outcome")
    assert (outcome["outcome"], outcome["reason"], outcome["output_rows"]) == (
        "ROLLBACK",
        "crash",
        None,
    )
    assert "no output" in only(b, "schema_diff")["note"]
    inputs = [i.data for i in b.of_kind("input")]
    assert [i["upstream"] for i in inputs] == list(SPEC.block(block).inputs)
    assert all(i["quality"] == "FRESH" and i["upstream_outcome_on_run"] == "PASS" for i in inputs)
    errors = [i.data for i in b.of_kind("event") if i.data["event"] == "ERROR"]
    assert errors and "boom in test" in errors[-1]["data"]["error"]
    assert "traceback" in errors[-1]["data"]


def test_run_selection_and_errors(tmp_path) -> None:
    block = representative(SPEC, "leaf")
    root = tmp_path / "g"
    clean_then_fault(root, SPEC, block, "corrupt_rows")
    run(root, SPEC, "r3")
    with Guardian(SPEC, root) as g:
        assert default_run(g, block) == "r2"  # the latest ROLLBACK, not the latest run
        assert build_evidence(g, block, git=False).run_id == "r2"
        with pytest.raises(EvidenceError, match="no recorded run 'nope'"):
            build_evidence(g, block, "nope")
        with pytest.raises(KeyError):
            build_evidence(g, "no_such_block", "r2")
        with pytest.raises(ValueError):
            build_evidence(g, block, "r2", sample_size=-1)


def test_bundle_json_is_plain_json(tmp_path) -> None:
    block = representative(SPEC, "multi_dependent")
    root = tmp_path / "g"
    clean_then_fault(root, SPEC, block, "null_burst")
    raw = json.loads(bundle(root, block).to_json())
    assert raw["block"] == block and raw["run_id"] == "r2"
    assert "NaN" not in json.dumps(raw)


# ---------------------------------------------------------------- git


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_git_history_of_a_source_file(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }

    def git(*args: str, when: str | None = None) -> None:
        extra = {"GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when} if when else {}
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, env={**env, **extra}
        )

    source = repo / "blocks.py"
    git("init", "-q")
    source.write_text("def f(df):\n    return df\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-qm", "old version", when="2020-01-01T00:00:00+00:00")
    source.write_text("def f(df):\n    return df.head()\n", encoding="utf-8")
    git("commit", "-qam", "truncate output", when="2024-06-01T00:00:00+00:00")
    source.write_text("def f(df):\n    return df.tail()\n", encoding="utf-8")  # uncommitted

    since = datetime(2023, 1, 1, tzinfo=UTC)
    history = git_history(str(source), since)
    assert history["available"] and history["file"] == "blocks.py"
    assert len(history["commits"]) == 1 and "truncate output" in history["commits"][0]
    diff = "\n".join(history["diff"])
    assert "-    return df" in diff and "+    return df.tail()" in diff  # since `since`

    recent = git_history(str(source), datetime.now(UTC) + timedelta(days=1))
    assert recent["commits"] == []
    assert "+    return df.tail()" in "\n".join(recent["diff"])  # uncommitted still shows

    outside = tmp_path / "loose.py"
    outside.write_text("x = 1\n", encoding="utf-8")
    assert git_history(str(outside), since)["available"] is False
    assert git_history(None, since)["available"] is False


@by_role
def test_drift_details_are_evidence_and_redacted(tmp_path, role: str) -> None:
    from guardian.core.models import DriftPolicy
    from guardian.demo.faults import drift

    block = representative(SPEC, role)
    root = tmp_path / "g"
    run(root, SPEC, "r0")
    with Guardian(SPEC, root) as g:
        columns = [c for c in g.snapshots.read(block, "r0").columns if c != QUALITY_COL]
    spec = with_block(SPEC, block, drift=DriftPolicy(min_history=1), redact_columns=tuple(columns))
    run(root, spec, "r1")
    report = run(root, spec, "r2", {block: [drift()]})
    assert report.get(block).outcome.value == "ROLLBACK"
    b = bundle(root, block, spec=spec)
    (item,) = b.of_kind("drift")
    assert item.data["level"] == "FAIL" and "r1" in item.data["reference_runs"]
    assert item.data["columns"] and item.data["columns"][0]["level"] == "FAIL"
    for column in item.data["columns"]:
        assert column["top_now"] in (REDACTED, []) and column["mean_now"] in (REDACTED, None)
    assert b.of_kind("validation_rule")[0].data["rule"] == "drift"
