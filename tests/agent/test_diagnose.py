"""Diagnosis: response validation, clients and config. All LLM calls use fakes (no network)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest

from guardian.agent.diagnose import (
    ACCEPTED,
    BAD_CITATION,
    DOWNGRADED,
    FAILED,
    INVALID_RESPONSE,
    LLM_ERROR,
    REFUSAL,
    RESPONSE_SCHEMA,
    SYSTEM_PROMPT,
    AnthropicClient,
    FakeClient,
    LLMConfig,
    LLMConfigError,
    LLMError,
    LLMRefusal,
    LLMRequest,
    RecordingClient,
    diagnose,
    diagnose_bundle,
    make_client,
)
from guardian.agent.evidence import EvidenceBundle, EvidenceItem
from guardian.core.events import EventKind
from guardian.core.guardian import Guardian

from ..helpers.roles import representative
from .helpers import SPEC, answer, clean_then_fault, run, with_block

BUNDLE = EvidenceBundle(
    pipeline="p",
    block="blk",
    run_id="r9",
    items=tuple(EvidenceItem(f"E{i}", "k", "t", {}) for i in range(1, 6)),
)
KEY = "blk/r9"


def ask(*responses: str) -> tuple:
    client = FakeClient({KEY: list(responses)})
    return diagnose_bundle(BUNDLE, client), client


# ---------------------------------------------------------------- validation


def test_valid_answer_is_accepted() -> None:
    d, client = ask(answer("schema_change", ("E2", "E5")))
    assert (d.status, d.root_cause, d.confidence, d.attempts) == (ACCEPTED, "schema_change", 0.8, 1)
    assert d.claims[0].evidence == ("E2", "E5") and d.rejection is None
    (request,) = client.calls
    assert request.system == SYSTEM_PROMPT and request.schema == RESPONSE_SCHEMA
    assert BUNDLE.to_json(indent=None) in request.prompt


def test_fenced_json_is_accepted() -> None:
    d, _ = ask(f"```json\n{answer('code_bug')}\n```")
    assert d.status == ACCEPTED and d.root_cause == "code_bug"


def test_unknown_with_no_claims_is_accepted() -> None:
    d, _ = ask(
        json.dumps({"root_cause": "unknown", "confidence": 0.2, "summary": "?", "claims": []})
    )
    assert d.status == ACCEPTED and d.root_cause == "unknown"


def test_invalid_json_is_retried_once_then_accepted() -> None:
    d, client = ask("not json", answer("upstream_data_drift"))
    assert (d.status, d.root_cause, d.attempts) == (ACCEPTED, "upstream_data_drift", 2)
    assert len(client.calls) == 2
    assert "previous answer was rejected (invalid JSON" in client.calls[1].prompt


@pytest.mark.parametrize(
    "bad",
    [
        "not json",
        "[1, 2]",
        json.dumps({"root_cause": "code_bug"}),
        answer("gremlins"),
        answer("code_bug", confidence=1.5),
        answer("code_bug", confidence=True),
        json.dumps({"root_cause": "code_bug", "confidence": 0.5, "summary": "s", "claims": []}),
        json.dumps(
            {
                "root_cause": "code_bug",
                "confidence": 0.5,
                "summary": "s",
                "claims": [{"statement": "x", "evidence": "E1"}],
            }
        ),
    ],
)
def test_invalid_after_retry_is_downgraded_to_unknown(bad: str) -> None:
    d, client = ask(bad, bad)
    assert (d.status, d.root_cause, d.rejection, d.attempts) == (
        DOWNGRADED,
        "unknown",
        INVALID_RESPONSE,
        2,
    )
    assert len(client.calls) == 2
    assert d.reason.startswith("invalid response after one retry")
    assert d.claims == () and d.confidence == 0.0


@pytest.mark.parametrize(
    ("evidence", "unknown"),
    [(("E1", "E99"), ("E99",)), (("E6",), ("E6",)), ((), ()), (("e1",), ("e1",))],
)
def test_citing_nonexistent_evidence_downgrades_without_retry(evidence, unknown) -> None:
    d, client = ask(answer("code_bug", evidence), answer("code_bug"))
    assert (d.status, d.root_cause, d.rejection) == (DOWNGRADED, "unknown", BAD_CITATION)
    assert d.unknown_evidence == unknown
    assert len(client.calls) == 1  # rejected, not retried
    assert d.proposed["root_cause"] == "code_bug"  # kept for audit only
    assert d.claims == ()


def test_refusal_and_llm_errors_are_recorded_not_raised() -> None:
    class Refuses:
        model = "m"

        def complete(self, request):
            raise LLMRefusal("declined")

    class Broken:
        model = "m"

        def complete(self, request):
            raise LLMError("connection reset")

    refused = diagnose_bundle(BUNDLE, Refuses())
    assert (refused.status, refused.rejection, refused.root_cause) == (
        DOWNGRADED,
        REFUSAL,
        "unknown",
    )
    failed = diagnose_bundle(BUNDLE, Broken())
    assert (failed.status, failed.rejection, failed.reason) == (
        FAILED,
        LLM_ERROR,
        "connection reset",
    )
    missing = diagnose_bundle(BUNDLE, FakeClient({}))
    assert missing.status == FAILED and "no recorded response" in missing.reason


# ---------------------------------------------------------------- clients


def test_fake_client_serves_recordings_in_order(tmp_path) -> None:
    client = FakeClient({"a": ["1", "2"], "b": "x"}, default="d", model="rec")
    req = lambda key: LLMRequest(key=key, system="s", prompt="p")  # noqa: E731
    assert [client.complete(req("a")) for _ in range(3)] == ["1", "2", "2"]
    assert client.complete(req("b")) == "x" and client.complete(req("zzz")) == "d"

    recorder = RecordingClient(FakeClient({"a": "one"}, model="m1"))
    recorder.complete(req("a"))
    path = recorder.save(tmp_path / "rec.json")
    replay = FakeClient.from_file(path)
    assert replay.model == "m1" and replay.complete(req("a")) == "one"


class _Stream:
    def __init__(self, message):
        self.message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self.message


class _StubSDK:
    """Records the kwargs of messages.stream and returns a canned message."""

    def __init__(self, message):
        self.kwargs = None
        self.messages = SimpleNamespace(stream=self._stream)
        self._message = message

    def _stream(self, **kwargs):
        self.kwargs = kwargs
        return _Stream(self._message)


def _message(stop_reason="end_turn", *texts):
    content = [SimpleNamespace(type="thinking", thinking="hmm")]
    content += [SimpleNamespace(type="text", text=t) for t in texts]
    return SimpleNamespace(stop_reason=stop_reason, content=content)


def test_anthropic_client_request_shape_and_text() -> None:
    sdk = _StubSDK(_message("end_turn", '{"a":', " 1}"))
    client = AnthropicClient("configured-model", max_tokens=1234, sdk_client=sdk)
    text = client.complete(LLMRequest(key="k", system="sys", prompt="hello"))
    assert text == '{"a": 1}'  # text blocks only, thinking skipped
    kw = sdk.kwargs
    assert kw["model"] == "configured-model" and kw["max_tokens"] == 1234
    assert kw["system"] == "sys"
    assert kw["messages"] == [{"role": "user", "content": "hello"}]
    assert kw["output_config"] == {"format": {"type": "json_schema", "schema": RESPONSE_SCHEMA}}
    assert kw["thinking"] == {"type": "adaptive"}

    no_thinking = _StubSDK(_message("end_turn", "{}"))
    AnthropicClient("m", thinking=False, sdk_client=no_thinking).complete(
        LLMRequest(key="k", system="s", prompt="p")
    )
    assert "thinking" not in no_thinking.kwargs


def test_anthropic_client_refusal_and_errors() -> None:
    with pytest.raises(LLMRefusal):
        AnthropicClient("m", sdk_client=_StubSDK(_message("refusal"))).complete(
            LLMRequest(key="k", system="s", prompt="p")
        )

    class Exploding:
        messages = SimpleNamespace(stream=lambda **kw: (_ for _ in ()).throw(OSError("down")))

    with pytest.raises(LLMError, match="down"):
        AnthropicClient("m", sdk_client=Exploding()).complete(
            LLMRequest(key="k", system="s", prompt="p")
        )
    with pytest.raises(LLMConfigError):
        AnthropicClient("", sdk_client=object())


def test_config_comes_from_the_environment(tmp_path) -> None:
    env = {
        "GUARDIAN_LLM_PROVIDER": "anthropic",
        "GUARDIAN_LLM_MODEL": "model-from-env",
        "GUARDIAN_LLM_API_KEY_ENV": "MY_KEY",
        "GUARDIAN_LLM_MAX_TOKENS": "999",
        "GUARDIAN_LLM_THINKING": "off",
    }
    config = LLMConfig.from_env(env)
    assert (config.provider, config.model, config.api_key_env, config.max_tokens) == (
        "anthropic",
        "model-from-env",
        "MY_KEY",
        999,
    )
    assert config.thinking is False
    assert LLMConfig.from_env(env, model="override").model == "override"

    with pytest.raises(LLMConfigError, match="GUARDIAN_LLM_PROVIDER"):
        make_client(env={})
    with pytest.raises(LLMConfigError, match="GUARDIAN_LLM_MODEL"):
        make_client(env={"GUARDIAN_LLM_PROVIDER": "anthropic"})
    with pytest.raises(LLMConfigError, match="MY_KEY"):
        make_client(env=env)
    with pytest.raises(LLMConfigError, match="unknown LLM provider"):
        make_client(env={"GUARDIAN_LLM_PROVIDER": "other"})
    with pytest.raises(LLMConfigError, match="GUARDIAN_LLM_FAKE_RESPONSES"):
        make_client(env={"GUARDIAN_LLM_PROVIDER": "fake"})

    recordings = tmp_path / "r.json"
    recordings.write_text(json.dumps({"model": "rec", "responses": {}}), encoding="utf-8")
    fake = make_client(
        env={"GUARDIAN_LLM_PROVIDER": "fake", "GUARDIAN_LLM_FAKE_RESPONSES": str(recordings)}
    )
    assert isinstance(fake, FakeClient) and fake.model == "rec"

    pytest.importorskip("anthropic")
    real = make_client(env={**env, "MY_KEY": "sk-test-not-used"})
    assert isinstance(real, AnthropicClient) and real.model == "model-from-env"  # no call made


# ---------------------------------------------------------------- end to end


def test_diagnose_writes_files_logs_an_event_and_minimizes_the_prompt(tmp_path) -> None:
    block = representative(SPEC, "source")
    root = tmp_path / "g"
    run(root, SPEC, "r0")
    with Guardian(SPEC, root) as g:
        text_columns = [
            c
            for c, t in g.snapshots.read(block, "r0").dtypes.items()
            if pd.api.types.is_string_dtype(t) or pd.api.types.is_object_dtype(t)
        ]
    spec = with_block(SPEC, block, redact_columns=tuple(text_columns))
    clean_then_fault(root, spec, block, "code_bug")
    client = FakeClient({f"{block}/r2": answer("code_bug", ("E1",))}, model="rec")
    with Guardian(spec, root) as g:
        result = diagnose(g, block, client=client, git=False)  # default: latest ROLLBACK
        events = g.events.query(kind=EventKind.DIAGNOSIS, block=block)
        payloads = [r.payload() for r in g.quarantine.list(block=block, run_id="r2")]
    d = result.diagnosis
    assert (d.run_id, d.root_cause, d.status, d.model) == ("r2", "code_bug", ACCEPTED, "rec")
    base = root / "diagnoses" / block / "r2"
    assert result.evidence_path == base / "evidence.json"
    assert result.diagnosis_path == base / "diagnosis.json"
    saved = json.loads(result.diagnosis_path.read_text(encoding="utf-8"))
    assert saved["root_cause"] == "code_bug" and saved["advisory"] is True
    assert [e.data["root_cause"] for e in events] == ["code_bug"]

    prompt = client.calls[0].prompt
    secrets = {
        p[c].strip()
        for p in payloads
        for c in text_columns
        if isinstance(p.get(c), str) and len(p[c].strip()) >= 4 and not p[c].strip().isdigit()
    }
    assert secrets and not [s for s in secrets if s in prompt]
