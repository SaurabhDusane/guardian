"""Getting `guardian eval` ready for a real model: the --real gate, seeded case
planning, the response cache, metering, repeats, dry-run estimates, the report file
and redaction of everything that could be sent. FakeClient only: no network."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from typer.testing import CliRunner

from guardian.agent import eval as agent_eval
from guardian.agent.diagnose import (
    AnthropicClient,
    FakeClient,
    LLMConfig,
    LLMConfigError,
    LLMRequest,
    Usage,
    build_prompt,
    make_real_client,
)
from guardian.agent.metering import (
    ENV_PRICE_INPUT,
    ENV_PRICE_OUTPUT,
    MeteredClient,
    Prices,
    ResponseCache,
    prompt_hash,
)
from guardian.core.dag import blocks_by_role
from guardian.core.snapshots import LocalParquetSnapshotStore
from guardian.runner.cli import app

from ..helpers.project_repo import SPEC_REL, repo_state
from ..helpers.roles import representative
from .helpers import SPEC, answer, broken_fix, revert_fix

NO_LLM_ENV = {
    "ANTHROPIC_API_KEY": None,
    "GUARDIAN_LLM_API_KEY_ENV": None,
    "GUARDIAN_LLM_MODEL": None,
    "GUARDIAN_LLM_PROVIDER": None,
    "GUARDIAN_LLM_FAKE_RESPONSES": None,
    ENV_PRICE_INPUT: None,
    ENV_PRICE_OUTPUT: None,
    "COLUMNS": "200",
}


@pytest.fixture(scope="module")
def source_cases(tmp_path_factory):
    """Every fault type for the source block (a small, fast case set)."""
    pairs = agent_eval.plan_cases(SPEC, roles=["source"])
    return agent_eval.generate_cases(SPEC, tmp_path_factory.mktemp("eval"), pairs=pairs)


def right(case, confidence: float = 0.9) -> str:
    return answer(case.label, ("E1",), confidence=confidence)


def wrong(case, confidence: float = 0.6) -> str:
    other = next(c for c in ("schema_change", "upstream_data_drift", "code_bug") if c != case.label)
    return answer(other, ("E1",), confidence=confidence)


# ------------------------------------------------------------------ the --real gate


def test_real_mode_names_the_missing_environment_variables() -> None:
    with pytest.raises(LLMConfigError) as err:
        make_real_client(LLMConfig.from_env({}), env={})
    assert "GUARDIAN_LLM_MODEL" in str(err.value) and "ANTHROPIC_API_KEY" in str(err.value)

    env = {"GUARDIAN_LLM_MODEL": "some-model"}
    with pytest.raises(LLMConfigError) as err:
        make_real_client(LLMConfig.from_env(env), env=env)
    assert "ANTHROPIC_API_KEY" in str(err.value) and "GUARDIAN_LLM_MODEL" not in str(err.value)

    env = {"GUARDIAN_LLM_MODEL": "some-model", "GUARDIAN_LLM_API_KEY_ENV": "MY_KEY"}
    with pytest.raises(LLMConfigError, match="MY_KEY"):
        make_real_client(LLMConfig.from_env(env), env=env)

    env = {"GUARDIAN_LLM_PROVIDER": "fake", "GUARDIAN_LLM_MODEL": "m", "ANTHROPIC_API_KEY": "k"}
    with pytest.raises(LLMConfigError, match="real model"):
        make_real_client(LLMConfig.from_env(env), env=env)


def test_real_mode_builds_the_real_client_without_calling_it() -> None:
    pytest.importorskip("anthropic")
    env = {"GUARDIAN_LLM_MODEL": "some-model", "ANTHROPIC_API_KEY": "not-a-real-key"}
    client = make_real_client(LLMConfig.from_env(env), env=env)
    assert isinstance(client, AnthropicClient) and client.model == "some-model"


def test_cli_calls_a_real_model_only_with_real(tmp_path) -> None:
    out = tmp_path / "agent_eval.md"
    runner = CliRunner()
    missing = runner.invoke(app, ["eval", "diagnose", "--real", "--out", str(out)], env=NO_LLM_ENV)
    assert missing.exit_code == 1
    assert "GUARDIAN_LLM_MODEL" in missing.output and "ANTHROPIC_API_KEY" in missing.output

    configured = {**NO_LLM_ENV, "GUARDIAN_LLM_PROVIDER": "anthropic", "GUARDIAN_LLM_MODEL": "m"}
    for command in ("diagnose", "propose"):
        refused = runner.invoke(app, ["eval", command, "--out", str(out)], env=configured)
        assert refused.exit_code == 1 and "--real" in refused.output
    assert not out.exists()


# ------------------------------------------------------------------ case planning


def test_case_planning_is_seeded_and_filtered() -> None:
    every = agent_eval.plan_cases(SPEC)
    assert len(every) == len(SPEC.block_names) * len(agent_eval.FAULT_TYPES)
    sample = agent_eval.plan_cases(SPEC, max_cases=5, seed=1)
    assert sample == agent_eval.plan_cases(SPEC, max_cases=5, seed=1)  # deterministic
    assert len(sample) == 5 and sample == [p for p in every if p in sample]  # order kept
    assert sample != agent_eval.plan_cases(SPEC, max_cases=5, seed=2)

    roles = blocks_by_role(SPEC)
    leaves = agent_eval.plan_cases(SPEC, roles=["leaf"], faults=["code_bug", "null_burst"])
    assert {b for b, _ in leaves} == set(roles["leaf"])
    assert {f for _, f in leaves} == {"code_bug", "null_burst"}
    both = agent_eval.plan_cases(SPEC, roles=["leaf", "source"])
    assert {b for b, _ in both} == set(roles["leaf"]) | set(roles["source"])

    with pytest.raises(ValueError, match="role"):
        agent_eval.plan_cases(SPEC, roles=["no_such_role"])
    with pytest.raises(ValueError, match="max_cases"):
        agent_eval.plan_cases(SPEC, max_cases=0)


def test_generated_prompts_are_identical_across_generations(tmp_path, source_cases) -> None:
    pairs = [(c.block, c.fault) for c in source_cases]
    again = agent_eval.generate_cases(SPEC, tmp_path, pairs=pairs)
    for a, b in zip(source_cases, again, strict=True):
        assert build_prompt(a.bundle) == build_prompt(b.bundle), a.case_id
    assert all("ts" not in item.data for c in again for item in c.bundle.of_kind("event"))


# ------------------------------------------------------------------ cache and metering


def test_prices_come_from_the_environment_or_flags() -> None:
    assert not Prices.from_env({}).known and Prices().cost(10, 10) is None
    env = {ENV_PRICE_INPUT: "3", ENV_PRICE_OUTPUT: "15"}
    prices = Prices.from_env(env)
    assert prices.cost(1_000_000, 100_000) == pytest.approx(3 + 1.5)
    assert Prices.from_env(env, output_per_mtok=5).cost(0, 1_000_000) == pytest.approx(5)
    with pytest.raises(ValueError, match=ENV_PRICE_INPUT):
        Prices.from_env({ENV_PRICE_INPUT: "cheap"})


def test_the_cache_serves_reruns_and_keeps_repeats_apart(tmp_path, source_cases) -> None:
    cache = ResponseCache(tmp_path / "cache")
    first = FakeClient({c.case_id: right(c) for c in source_cases}, model="m")
    run1 = agent_eval.run_eval(source_cases, MeteredClient(first, cache=cache), repeats=2)
    n = len(source_cases)
    assert len(first.calls) == 2 * n
    assert len(cache.entries()) == 2 * n  # one entry per (prompt, repeat)
    assert not any(c.cached for c in run1.calls())

    # A re-run (or a resumed one) pays for nothing: every answer comes from the cache.
    second = FakeClient({c.case_id: wrong(c) for c in source_cases}, model="m")
    run2 = agent_eval.run_eval(source_cases, MeteredClient(second, cache=cache), repeats=2)
    assert second.calls == []
    assert [r.predicted for r in run2.results] == [r.predicted for r in run1.results]
    usage = run2.to_dict()["usage"]
    assert usage["cached_calls"] == 2 * n and usage["billed_input_tokens"] == 0
    assert [c.latency_s for c in run2.calls()] == [c.latency_s for c in run1.calls()]

    # --no-cache calls the model again (and stores the fresh answers).
    third = FakeClient({c.case_id: wrong(c) for c in source_cases}, model="m")
    agent_eval.run_eval(source_cases, MeteredClient(third, cache=cache, read_cache=False))
    assert len(third.calls) == n
    fourth = FakeClient({}, model="m")
    rerun = agent_eval.run_eval(source_cases, MeteredClient(fourth, cache=cache))
    assert fourth.calls == [] and rerun.accuracy == 0.0  # the fresh (wrong) answers

    # Another model never gets this model's answers.
    other = FakeClient({c.case_id: right(c) for c in source_cases}, model="other")
    agent_eval.run_eval(source_cases, MeteredClient(other, cache=cache))
    assert len(other.calls) == n


def test_errors_are_never_cached(tmp_path, source_cases) -> None:
    cache = ResponseCache(tmp_path / "cache")
    report = agent_eval.run_eval(source_cases, MeteredClient(FakeClient({}), cache=cache))
    assert all(r.diagnosis.status == "failed" for r in report.results)
    assert cache.entries() == []


def test_api_usage_is_used_when_the_client_reports_it(tmp_path) -> None:
    class Reporting:
        model = "reporting"
        last_usage: Usage | None = None

        def complete(self, request: LLMRequest) -> str:
            self.last_usage = Usage(1234, 567)
            return "answer"

    metered = MeteredClient(Reporting(), cache=ResponseCache(tmp_path))
    request = LLMRequest(key="k", system="s", prompt="p")
    metered.complete(request)
    metered.complete(request)
    fresh, cached = metered.calls
    assert (fresh.input_tokens, fresh.output_tokens, fresh.estimated) == (1234, 567, False)
    assert (cached.cached, cached.input_tokens) == (True, 1234)
    assert fresh.prompt_hash == prompt_hash(request)

    estimated = MeteredClient(FakeClient(default="x" * 70))
    estimated.complete(request)
    assert estimated.calls[0].estimated and estimated.calls[0].output_tokens == 20


# ------------------------------------------------------------------ report figures


def test_repeats_report_agreement_and_calibration(source_cases) -> None:
    client = FakeClient(
        {c.case_id: [right(c, 0.95), wrong(c, 0.6)] for c in source_cases}, model="m"
    )
    prices = Prices(3.0, 15.0)
    report = agent_eval.run_eval(source_cases, MeteredClient(client), repeats=2, prices=prices)
    n = len(source_cases)
    assert report.cases == n and len(report.results) == 2 * n
    assert report.accuracy_by_repeat() == [1.0, 0.0]
    agreement = report.agreement()
    assert agreement["unanimous"] == 0 and agreement["pairwise_agreement"] == 0.0
    assert agreement["majority_vote_accuracy"] == 0.0  # a 1-1 tie is not a majority

    buckets = {row["bucket"]: row for row in report.calibration()}
    assert (buckets["[0.9, 1.0]"]["answers"], buckets["[0.9, 1.0]"]["accuracy"]) == (n, 1.0)
    assert (buckets["[0.5, 0.7)"]["answers"], buckets["[0.5, 0.7)"]["accuracy"]) == (n, 0.0)
    assert report.expected_calibration_error() == pytest.approx(0.5 * 0.05 + 0.5 * 0.6)

    data = report.to_dict()
    assert data["median_latency_s"] is not None
    assert data["usage"]["tokens_estimated"] and data["usage"]["cost_usd"] > 0
    assert all(r["citations_valid"] is True for r in data["results"])
    md = report.to_markdown()
    for heading in ("## Calibration", "## Agreement across repeats", "| cost |"):
        assert heading in md
    json.dumps(data)


def test_citation_rejections_are_recorded_per_case(source_cases) -> None:
    client = FakeClient({c.case_id: answer(c.label, ("E999",)) for c in source_cases})
    report = agent_eval.run_eval(source_cases, MeteredClient(client))
    assert report.rejected_rate == 1.0
    assert all(r["citations_valid"] is False for r in report.to_dict()["results"])


# ------------------------------------------------------------------ dry runs


def test_dry_run_counts_only_calls_not_in_the_cache(tmp_path, source_cases) -> None:
    cache = ResponseCache(tmp_path / "cache")
    prices = Prices(3.0, 15.0)
    kwargs = dict(model="m", repeats=2, cache=cache, read_cache=True, expected_output=1000,
                  max_tokens=8000, prices=prices)  # fmt: skip
    before = agent_eval.estimate_diagnose(source_cases, **kwargs)
    n = len(source_cases)
    assert (before.calls, before.cached_calls) == (2 * n, 0)
    assert before.input_tokens > 0 and before.output_tokens == 2 * n * 1000
    assert before.output_upper == 2 * 2 * n * 8000
    assert prices.cost(before.input_tokens, before.output_tokens) == pytest.approx(
        before.to_dict()["cost_usd"], abs=1e-4
    )

    # Repeat 0 is paid for; a retry after an invalid first answer is cached too.
    scripted = {c.case_id: ["{not json", right(c)] for c in source_cases}
    agent_eval.run_eval(source_cases, MeteredClient(FakeClient(scripted, model="m"), cache=cache))
    after = agent_eval.estimate_diagnose(source_cases, **kwargs)
    assert (after.calls, after.cached_calls) == (n, 2 * n)  # repeat 1 is still to pay
    assert (
        agent_eval.estimate_diagnose(source_cases, **{**kwargs, "read_cache": False}).calls == 2 * n
    )


def test_cli_dry_run_estimates_and_calls_nothing(tmp_path, source_cases) -> None:
    block = source_cases[0].block
    recordings = tmp_path / "recorded.json"
    recordings.write_text(json.dumps({"model": "rec", "responses": {}}), encoding="utf-8")
    out = tmp_path / "agent_eval.md"
    env = {**NO_LLM_ENV, ENV_PRICE_INPUT: "3", ENV_PRICE_OUTPUT: "15"}
    args = ["--provider", "fake", "--fake-responses", str(recordings), "--block", block,
            "--out", str(out), "--dry-run", "--cache-dir", str(tmp_path / "cache")]  # fmt: skip
    result = CliRunner().invoke(app, ["eval", "diagnose", *args, "--repeats", "3"], env=env)
    assert result.exit_code == 0, result.output
    assert "Cases: 4 x 3 repeat(s)" in result.output
    assert "Calls to make: 12" in result.output and "Estimated cost: $" in result.output
    assert not out.exists() and not (tmp_path / "cache").exists()

    fix = CliRunner().invoke(app, ["eval", "propose", *args], env=NO_LLM_ENV)
    assert fix.exit_code == 0, fix.output
    assert "Cases: 1 x 1" in fix.output and "Calls to make: 2" in fix.output
    assert "no prices configured" in fix.output


# ------------------------------------------------------------------ the report file


def test_each_command_keeps_the_other_section(tmp_path, source_cases) -> None:
    out = tmp_path / "bench" / "agent_eval.md"
    diag = agent_eval.run_eval(
        source_cases, MeteredClient(FakeClient({c.case_id: right(c) for c in source_cases}))
    )
    meta = agent_eval.run_meta(model="m", real=False, cases=4, repeats=1, seed=0, filters={})
    diag.meta = meta
    agent_eval.write_report(out, "diagnose", diag.to_dict())
    fix = agent_eval.FixEvalReport(model="m", results=[], meta=meta)
    report = agent_eval.write_report(out, "propose", fix.to_dict())
    saved = json.loads(out.with_suffix(".json").read_text(encoding="utf-8"))
    assert saved == report and {"diagnose", "propose"} <= set(saved)
    assert saved["diagnose"]["meta"]["git_commit"]  # the tests run in a git checkout
    md = out.read_text(encoding="utf-8")
    assert "# Diagnosis" in md and "# Fix proposals" in md
    assert agent_eval.SYNTHETIC_NOTE in md and "Model: `m`" in md


# ------------------------------------------------------------------ fixes


@pytest.mark.slow  # two proposals, each tested in a git worktree
def test_propose_eval_is_a_dry_run_with_failure_reasons(
    tmp_path, project_repo, monkeypatch
) -> None:
    from guardian.agent import propose as propose_module

    def no_github(*args, **kwargs):
        raise AssertionError("the eval must never reach GitHub")

    monkeypatch.setattr(propose_module.GitHubClient, "from_env", no_github)
    good, broken = representative(SPEC, "fallback_protected"), representative(SPEC, "leaf")
    cases = agent_eval.generate_cases(
        SPEC, tmp_path / "cases", pairs=[(good, "code_bug"), (broken, "code_bug")]
    )
    responses = {}
    for c in cases:
        responses[c.case_id] = answer("code_bug", ("E1",))
        fix = revert_fix(SPEC, c.block) if c.block == good else broken_fix(SPEC, c.block)
        responses[f"{c.case_id}:fix"] = fix
    before = repo_state(project_repo)
    client = MeteredClient(FakeClient(responses, model="scripted"))
    report = agent_eval.run_fix_eval(
        cases, client, spec=SPEC, spec_path=project_repo / SPEC_REL, repeats=2
    )
    assert repo_state(project_repo) == before
    by_block = {(r.case.block, r.repeat): r for r in report.results}
    assert by_block[(good, 0)].promoted and by_block[(good, 1)].promoted
    assert not by_block[(broken, 0)].promoted
    assert by_block[(broken, 0)].failure in (agent_eval.TESTS_FAILED, agent_eval.SHADOW_FAILED)
    assert report.failures() == {by_block[(broken, 0)].failure: 2}
    assert report.agreement()["unanimous"] == 2
    assert report.to_dict()["usage"]["calls"] == 2 * 2 * 2  # diagnosis + fix, per repeat
    assert "## Failure reasons" in report.to_markdown()


def test_misdiagnosed_code_bugs_fail_before_any_fix(tmp_path, project_repo, source_cases) -> None:
    (case,) = [c for c in source_cases if c.fault == "code_bug"]
    client = FakeClient({case.case_id: answer("schema_change", ("E1",))})
    report = agent_eval.run_fix_eval(
        [case], client, spec=SPEC, spec_path=project_repo / SPEC_REL, workdir=tmp_path
    )
    (result,) = report.results
    assert not result.promoted and result.failure == "misdiagnosed as schema_change"
    # The agent may still propose a fix for the cause it diagnosed (e.g. a schema
    # update); the eval scores it as a misdiagnosis whatever that proposal is.
    keys = [r.key for r in client.calls]
    assert keys[0] == case.case_id and set(keys[1:]) <= {f"{case.case_id}:fix"}


# ------------------------------------------------------------------ redaction


def _redacted_values(root, spec) -> set[str]:
    """Every value of every redacted column in the case's clean baseline."""
    columns = {c for b in spec.blocks for c in b.redact_columns}
    store = LocalParquetSnapshotStore(root)
    values: set[str] = set()
    for block in spec.block_names:
        if not store.exists(block, agent_eval.BASELINE_RUN):
            continue
        df = store.read(block, agent_eval.BASELINE_RUN)
        for column in columns & set(df.columns):
            values |= {str(v) for v in df[column] if not pd.isna(v) and len(str(v)) >= 5}
    return values


def test_no_redacted_value_ever_reaches_a_cached_prompt(tmp_path, project_repo) -> None:
    """Includes blocks that carry a redacted column without declaring it."""
    redacted = {c for b in SPEC.blocks for c in b.redact_columns}
    assert redacted, "the demo spec must declare redact_columns for this test to mean anything"
    probe = agent_eval.generate_cases(SPEC, tmp_path / "probe", pairs=[])
    store = LocalParquetSnapshotStore(tmp_path / "probe" / "baseline")
    carriers = [
        b
        for b in SPEC.block_names
        if redacted & set(store.read(b, agent_eval.BASELINE_RUN).columns)
    ]
    assert any(not SPEC.block(b).redact_columns for b in carriers), carriers  # undeclared too
    assert probe == []
    pairs = [(b, f) for b in carriers for f in agent_eval.FAULT_TYPES]
    cases = agent_eval.generate_cases(SPEC, tmp_path / "cases", pairs=pairs)

    cache = ResponseCache(tmp_path / "cache")
    responses = {c.case_id: answer(c.label, ("E1",)) for c in cases}
    responses |= {f"{c.case_id}:fix": revert_fix(SPEC, c.block) for c in cases}
    client = MeteredClient(FakeClient(responses, model="real-model"), cache=cache)
    agent_eval.run_eval(cases, client)
    fix_cases = [c for c in cases if c.fault == "code_bug"][:1]
    agent_eval.run_fix_eval(
        fix_cases, client, spec=SPEC, spec_path=project_repo / SPEC_REL, workdir=tmp_path / "fx"
    )

    values = _redacted_values(tmp_path / "cases" / "baseline", SPEC)
    assert len(values) > 50
    entries = cache.entries()
    assert {e["key"] for e in entries} >= {c.case_id for c in cases}
    assert any(e["key"].endswith(":fix") for e in entries)
    for entry in entries:
        sent = entry["system"] + entry["prompt"]
        leaked = sorted(v for v in values if v in sent)
        assert not leaked, (entry["key"], leaked[:5])
