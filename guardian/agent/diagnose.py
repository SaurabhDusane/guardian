"""Root-cause diagnosis of a failed block run, by an LLM, from an evidence bundle.

The model gets the bundle (see evidence.py) and must answer with JSON:
``root_cause`` (one of ROOT_CAUSES), ``confidence`` in [0, 1], a ``summary`` and a
list of ``claims``, each citing evidence IDs. The answer is checked before it is
trusted:

- invalid JSON or a malformed answer is retried once; if the retry fails too, the
  diagnosis is downgraded to ``unknown`` with the reason recorded;
- a claim citing an evidence ID that is not in the bundle (or citing nothing) gets
  the whole answer rejected: the diagnosis is downgraded to ``unknown`` and the model's
  answer is kept only as ``proposed``, for audit.

The agent is advisory. It writes the bundle and the diagnosis under
``<root>/diagnoses/<block>/<run_id>/`` and logs a DIAGNOSIS event; it never edits code,
merges, or promotes.

The LLM sits behind the ``LLMClient`` protocol. ``AnthropicClient`` calls the Claude
API; ``FakeClient`` replays recorded responses (used by every test: no network in
pytest). Provider, model and API key come from the environment (``LLMConfig``), never
from code.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from guardian.agent.evidence import (
    DEFAULT_SAMPLE_SIZE,
    EvidenceBundle,
    build_evidence,
    default_run,
    diagnosis_dir,
)
from guardian.core.events import EventKind
from guardian.core.guardian import Guardian
from guardian.core.models import GuardianError

ROOT_CAUSES = ("upstream_data_drift", "schema_change", "code_bug", "unknown")
UNKNOWN = "unknown"
DIAGNOSIS_FILE = "diagnosis.json"

# Diagnosis status.
ACCEPTED = "accepted"
DOWNGRADED = "downgraded"  # the model answered, but the answer was not trusted
FAILED = "failed"  # no usable answer: the LLM call itself failed

# Why an answer was not trusted.
BAD_CITATION = "bad_citation"
INVALID_RESPONSE = "invalid_response"
REFUSAL = "refusal"
LLM_ERROR = "llm_error"

ENV_PROVIDER = "GUARDIAN_LLM_PROVIDER"
ENV_MODEL = "GUARDIAN_LLM_MODEL"
ENV_API_KEY_VAR = "GUARDIAN_LLM_API_KEY_ENV"  # names the variable holding the key
ENV_FAKE_RESPONSES = "GUARDIAN_LLM_FAKE_RESPONSES"
ENV_MAX_TOKENS = "GUARDIAN_LLM_MAX_TOKENS"
ENV_THINKING = "GUARDIAN_LLM_THINKING"
DEFAULT_API_KEY_VAR = "ANTHROPIC_API_KEY"
DEFAULT_MAX_TOKENS = 16000
PROVIDERS = ("anthropic", "fake")

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "root_cause": {"type": "string", "enum": list(ROOT_CAUSES)},
        "confidence": {"type": "number"},
        "summary": {"type": "string"},
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "statement": {"type": "string"},
                    "evidence": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["statement", "evidence"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["root_cause", "confidence", "summary", "claims"],
    "additionalProperties": False,
}

SYSTEM_PROMPT = """\
You diagnose why one block (one step) of a data pipeline failed on one run. You are \
advisory: you explain the failure; you do not fix, promote, merge or edit anything.

You get an evidence bundle as JSON. Every item in it has an ID (E1, E2, ...). Decide \
the single most likely root cause:
- upstream_data_drift: the data changed in content while the block's code and its \
output schema did not. Examples: out-of-range or sentinel values, unexpected \
categories, bursts of nulls.
- schema_change: the structure of the output changed. Examples: columns missing, \
renamed or added, or a column's type changed, so the output no longer matches the \
declared or last good schema.
- code_bug: the block's own implementation is at fault. Examples: its code changed \
since the last good run and the failures follow from that change, or an error was \
raised from inside its code.
- unknown: the evidence does not support any of the above with reasonable confidence.

Rules:
- Every claim must cite the IDs of the evidence items that support it, and only IDs \
that appear in the bundle. A claim citing an ID that does not exist gets the whole \
diagnosis rejected.
- Use only what the bundle shows; do not invent values. Values shown as <redacted> \
are masked on purpose.
- confidence is your probability, from 0 to 1, that root_cause is correct.
- Reply with only a JSON object: {"root_cause": "...", "confidence": 0.0, \
"summary": "...", "claims": [{"statement": "...", "evidence": ["E1"]}]}.
"""


class LLMError(GuardianError):
    """The LLM call failed (transport, API or configuration error)."""


class LLMConfigError(LLMError):
    """The LLM provider is not configured (or its SDK is not installed)."""


class LLMRefusal(LLMError):
    """The model declined to answer."""


# ------------------------------------------------------------------ clients


@dataclass(frozen=True)
class LLMRequest:
    key: str  # identifies the case (FakeClient looks responses up by it)
    system: str
    prompt: str
    schema: Mapping[str, Any] = field(default_factory=lambda: RESPONSE_SCHEMA)


@runtime_checkable
class LLMClient(Protocol):
    model: str

    def complete(self, request: LLMRequest) -> str:
        """Return the model's raw text answer; raise LLMError on failure."""
        ...


class AnthropicClient:
    """Claude via the official ``anthropic`` SDK, with JSON-schema structured output."""

    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        thinking: bool = True,
        sdk_client: Any = None,
    ) -> None:
        if not model:
            raise LLMConfigError(f"no model configured: set {ENV_MODEL}")
        self.model = model
        self.max_tokens = max_tokens
        self.thinking = thinking
        if sdk_client is None:
            try:
                import anthropic
            except ImportError as exc:
                raise LLMConfigError(
                    "the anthropic SDK is not installed: uv sync --extra agent"
                ) from exc
            sdk_client = anthropic.Anthropic(api_key=api_key)
        self._client = sdk_client

    def complete(self, request: LLMRequest) -> str:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": request.system,
            "messages": [{"role": "user", "content": request.prompt}],
            "output_config": {"format": {"type": "json_schema", "schema": dict(request.schema)}},
        }
        if self.thinking:
            kwargs["thinking"] = {"type": "adaptive"}
        try:
            # Streamed, so a long answer cannot hit the non-streaming request timeout.
            with self._client.messages.stream(**kwargs) as stream:
                response = stream.get_final_message()
        except Exception as exc:
            raise LLMError(_describe_api_error(exc)) from exc
        if response.stop_reason == "refusal":
            raise LLMRefusal("the model declined to answer")
        return "".join(
            getattr(block, "text", "") for block in response.content if block.type == "text"
        )


def _describe_api_error(exc: Exception) -> str:
    try:
        import anthropic
    except ImportError:
        return f"{type(exc).__name__}: {exc}"
    if isinstance(exc, anthropic.NotFoundError):
        return f"model not found (check {ENV_MODEL}): {exc}"
    if isinstance(exc, anthropic.AuthenticationError):
        return f"authentication failed (check the API key variable): {exc}"
    if isinstance(exc, anthropic.RateLimitError):
        return f"rate limited: {exc}"
    if isinstance(exc, anthropic.APIStatusError):
        return f"API error {exc.status_code}: {exc}"
    if isinstance(exc, anthropic.APIConnectionError):
        return f"could not reach the API: {exc}"
    return f"{type(exc).__name__}: {exc}"


class FakeClient:
    """Replays recorded responses, looked up by request key; no network.

    ``responses`` maps a key to one response or to a sequence served in order (the
    last one repeats), so a retry can get a different answer. ``default`` serves keys
    with no recording; without it an unknown key raises LLMError. Every request is kept
    in ``calls``.
    """

    def __init__(
        self,
        responses: Mapping[str, str | Sequence[str]] | None = None,
        *,
        default: str | Sequence[str] | None = None,
        model: str = "fake",
    ) -> None:
        self.model = model
        self._responses = {k: _as_list(v) for k, v in (responses or {}).items()}
        self._default = _as_list(default) if default is not None else None
        self._served: Counter[str] = Counter()
        self.calls: list[LLMRequest] = []

    def complete(self, request: LLMRequest) -> str:
        self.calls.append(request)
        answers = self._responses.get(request.key, self._default)
        if not answers:
            raise LLMError(f"no recorded response for {request.key!r}")
        i = self._served[request.key]
        self._served[request.key] += 1
        return answers[min(i, len(answers) - 1)]

    @classmethod
    def from_file(cls, path: Path | str) -> FakeClient:
        """Load recordings: ``{"model": ..., "responses": {key: text | [text, ...]}}``."""
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            raw.get("responses", {}), default=raw.get("default"), model=raw.get("model", "fake")
        )


def _as_list(value: str | Sequence[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


class RecordingClient:
    """Passes requests to ``inner`` and records every answer, for later replay with
    FakeClient (``save`` writes the file ``FakeClient.from_file`` reads)."""

    def __init__(self, inner: LLMClient) -> None:
        self.inner = inner
        self.model = inner.model
        self.recorded: dict[str, list[str]] = {}

    def complete(self, request: LLMRequest) -> str:
        text = self.inner.complete(request)
        self.recorded.setdefault(request.key, []).append(text)
        return text

    def save(self, path: Path | str) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"model": self.model, "responses": self.recorded}
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path


@dataclass(frozen=True)
class LLMConfig:
    """Which LLM to use, from environment variables (overridable, e.g. by CLI flags)."""

    provider: str | None = None
    model: str | None = None
    api_key_env: str = DEFAULT_API_KEY_VAR
    fake_responses: str | None = None
    max_tokens: int = DEFAULT_MAX_TOKENS
    thinking: bool = True

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **overrides: Any) -> LLMConfig:
        env = os.environ if env is None else env
        values: dict[str, Any] = {
            "provider": env.get(ENV_PROVIDER) or None,
            "model": env.get(ENV_MODEL) or None,
            "api_key_env": env.get(ENV_API_KEY_VAR) or DEFAULT_API_KEY_VAR,
            "fake_responses": env.get(ENV_FAKE_RESPONSES) or None,
        }
        if env.get(ENV_MAX_TOKENS):
            values["max_tokens"] = int(env[ENV_MAX_TOKENS])
        if env.get(ENV_THINKING):
            values["thinking"] = env[ENV_THINKING].strip().lower() not in ("0", "off", "false")
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)


def make_client(config: LLMConfig | None = None, env: Mapping[str, str] | None = None) -> LLMClient:
    """Build the configured client; raise LLMConfigError, naming what to set, if unset."""
    env = os.environ if env is None else env
    config = config or LLMConfig.from_env(env)
    if config.provider is None:
        raise LLMConfigError(
            f"no LLM provider configured: set {ENV_PROVIDER} to one of {list(PROVIDERS)}"
        )
    if config.provider == "anthropic":
        if not config.model:
            raise LLMConfigError(f"no model configured: set {ENV_MODEL}")
        api_key = env.get(config.api_key_env)
        if not api_key:
            raise LLMConfigError(f"no API key: set {config.api_key_env}")
        return AnthropicClient(
            config.model, api_key=api_key, max_tokens=config.max_tokens, thinking=config.thinking
        )
    if config.provider == "fake":
        if not config.fake_responses:
            raise LLMConfigError(
                f"the fake provider needs {ENV_FAKE_RESPONSES} (a recordings file)"
            )
        return FakeClient.from_file(config.fake_responses)
    raise LLMConfigError(
        f"unknown LLM provider {config.provider!r}; expected one of {list(PROVIDERS)}"
    )


# ------------------------------------------------------------------ diagnosis


@dataclass(frozen=True)
class Claim:
    statement: str
    evidence: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"statement": self.statement, "evidence": list(self.evidence)}


@dataclass(frozen=True)
class Diagnosis:
    pipeline: str
    block: str
    run_id: str
    root_cause: str
    confidence: float
    summary: str
    claims: tuple[Claim, ...]
    status: str  # ACCEPTED, DOWNGRADED or FAILED
    model: str
    attempts: int
    rejection: str | None = None  # BAD_CITATION, INVALID_RESPONSE, REFUSAL or LLM_ERROR
    reason: str | None = None  # why it was downgraded or failed
    unknown_evidence: tuple[str, ...] = ()  # cited IDs that are not in the bundle
    proposed: dict[str, Any] | None = None  # the model's answer, when not trusted

    @property
    def accepted(self) -> bool:
        return self.status == ACCEPTED

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "block": self.block,
            "run_id": self.run_id,
            "advisory": True,
            "root_cause": self.root_cause,
            "confidence": self.confidence,
            "summary": self.summary,
            "claims": [c.to_dict() for c in self.claims],
            "status": self.status,
            "rejection": self.rejection,
            "reason": self.reason,
            "unknown_evidence": list(self.unknown_evidence),
            "proposed": self.proposed,
            "model": self.model,
            "attempts": self.attempts,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> Diagnosis:
        return cls(
            pipeline=raw["pipeline"],
            block=raw["block"],
            run_id=raw["run_id"],
            root_cause=raw["root_cause"],
            confidence=raw["confidence"],
            summary=raw["summary"],
            claims=tuple(Claim(c["statement"], tuple(c["evidence"])) for c in raw["claims"]),
            status=raw["status"],
            model=raw["model"],
            attempts=raw["attempts"],
            rejection=raw.get("rejection"),
            reason=raw.get("reason"),
            unknown_evidence=tuple(raw.get("unknown_evidence", ())),
            proposed=raw.get("proposed"),
        )

    @classmethod
    def load(cls, root: Path | str, block: str, run_id: str) -> Diagnosis | None:
        """The diagnosis saved for (block, run_id), if any."""
        path = diagnosis_dir(root, block, run_id) / DIAGNOSIS_FILE
        if not path.exists():
            return None
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def write(self, root: Path | str) -> Path:
        path = diagnosis_dir(root, self.block, self.run_id) / DIAGNOSIS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json() + "\n", encoding="utf-8", newline="\n")
        return path


class ResponseError(ValueError):
    """The answer is not valid JSON or does not have the required shape."""


_FENCE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def parse_response(text: str) -> dict[str, Any]:
    """Parse and shape-check an answer; raise ResponseError with the problem."""
    text = (text or "").strip()
    fenced = _FENCE.match(text)
    if fenced:
        text = fenced.group(1)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResponseError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ResponseError("the answer must be a JSON object")
    missing = [k for k in ("root_cause", "confidence", "summary", "claims") if k not in payload]
    if missing:
        raise ResponseError(f"missing field(s) {missing}")
    if payload["root_cause"] not in ROOT_CAUSES:
        raise ResponseError(
            f"root_cause {payload['root_cause']!r} is not one of {list(ROOT_CAUSES)}"
        )
    confidence = payload["confidence"]
    if isinstance(confidence, bool) or not isinstance(confidence, int | float):
        raise ResponseError("confidence must be a number")
    if not 0.0 <= float(confidence) <= 1.0:
        raise ResponseError(f"confidence {confidence} is not in [0, 1]")
    if not isinstance(payload["summary"], str):
        raise ResponseError("summary must be a string")
    claims = payload["claims"]
    if not isinstance(claims, list):
        raise ResponseError("claims must be a list")
    for i, claim in enumerate(claims):
        if (
            not isinstance(claim, dict)
            or not isinstance(claim.get("statement"), str)
            or not isinstance(claim.get("evidence"), list)
            or not all(isinstance(e, str) for e in claim["evidence"])
        ):
            raise ResponseError(f"claim {i} must be {{'statement': str, 'evidence': [str]}}")
    if payload["root_cause"] != UNKNOWN and not claims:
        raise ResponseError("a root cause other than 'unknown' needs at least one claim")
    return payload


def check_citations(payload: Mapping[str, Any], bundle: EvidenceBundle) -> list[str]:
    """Problems with the claims' citations: unknown IDs, or claims citing nothing."""
    known = set(bundle.ids)
    problems = []
    for i, claim in enumerate(payload["claims"]):
        cited = [e.strip() for e in claim["evidence"]]
        if not cited:
            problems.append(f"claim {i} cites no evidence")
        problems.extend(e for e in cited if e not in known)
    return problems


def build_prompt(bundle: EvidenceBundle) -> str:
    return (
        f"Diagnose block {bundle.block!r} of pipeline {bundle.pipeline!r} on run "
        f"{bundle.run_id!r}. The evidence bundle has items {bundle.ids[0]}.."
        f"{bundle.ids[-1]}.\n\n<evidence>\n{bundle.to_json(indent=None)}\n</evidence>"
    )


def diagnose_bundle(
    bundle: EvidenceBundle, client: LLMClient, *, key: str | None = None
) -> Diagnosis:
    """Ask ``client`` for a diagnosis of ``bundle`` and validate it (one retry)."""
    key = key or f"{bundle.block}/{bundle.run_id}"
    prompt = build_prompt(bundle)
    base = {"pipeline": bundle.pipeline, "block": bundle.block, "run_id": bundle.run_id}
    model = getattr(client, "model", "?")
    error = ""
    for attempt in (1, 2):
        request = LLMRequest(key=key, system=SYSTEM_PROMPT, prompt=prompt)
        if attempt == 2:
            request = LLMRequest(
                key=key,
                system=SYSTEM_PROMPT,
                prompt=f"{prompt}\n\nYour previous answer was rejected ({error}). "
                "Answer again with only the JSON object.",
            )
        try:
            text = client.complete(request)
        except LLMRefusal as exc:
            return _untrusted(base, model, attempt, DOWNGRADED, REFUSAL, str(exc))
        except LLMError as exc:
            return _untrusted(base, model, attempt, FAILED, LLM_ERROR, str(exc))
        try:
            payload = parse_response(text)
        except ResponseError as exc:
            error = str(exc)
            continue
        bad = check_citations(payload, bundle)
        if bad:
            unknown = tuple(dict.fromkeys(b for b in bad if not b.startswith("claim ")))
            return _untrusted(
                base,
                model,
                attempt,
                DOWNGRADED,
                BAD_CITATION,
                "claims cite evidence that is not in the bundle: " + ", ".join(bad),
                unknown=unknown,
                proposed=payload,
            )
        return Diagnosis(
            **base,
            root_cause=payload["root_cause"],
            confidence=round(float(payload["confidence"]), 4),
            summary=payload["summary"],
            claims=tuple(
                Claim(c["statement"], tuple(e.strip() for e in c["evidence"]))
                for c in payload["claims"]
            ),
            status=ACCEPTED,
            model=model,
            attempts=attempt,
        )
    return _untrusted(
        base, model, 2, DOWNGRADED, INVALID_RESPONSE, f"invalid response after one retry: {error}"
    )


def _untrusted(
    base: dict[str, str],
    model: str,
    attempts: int,
    status: str,
    rejection: str,
    reason: str,
    *,
    unknown: tuple[str, ...] = (),
    proposed: dict[str, Any] | None = None,
) -> Diagnosis:
    return Diagnosis(
        **base,
        root_cause=UNKNOWN,
        confidence=0.0,
        summary=f"diagnosis {status}: {reason}",
        claims=(),
        status=status,
        model=model,
        attempts=attempts,
        rejection=rejection,
        reason=reason,
        unknown_evidence=unknown,
        proposed=proposed,
    )


@dataclass(frozen=True)
class DiagnosisResult:
    diagnosis: Diagnosis
    bundle: EvidenceBundle
    evidence_path: Path | None
    diagnosis_path: Path | None


def diagnose(
    g: Guardian,
    block: str,
    run_id: str | None = None,
    *,
    client: LLMClient,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    git: bool = True,
    key: str | None = None,
    write: bool = True,
) -> DiagnosisResult:
    """Build the evidence for ``block``'s run (default: its latest ROLLBACK), diagnose
    it, write both under ``<root>/diagnoses/<block>/<run_id>/`` and log the result."""
    run_id = run_id or default_run(g, block)
    bundle = build_evidence(g, block, run_id, sample_size=sample_size, git=git)
    evidence_path = bundle.write(g.root) if write else None
    diagnosis = diagnose_bundle(bundle, client, key=key)
    diagnosis_path = diagnosis.write(g.root) if write else None
    g.events.emit(
        EventKind.DIAGNOSIS,
        block=block,
        run_id=run_id,
        root_cause=diagnosis.root_cause,
        confidence=diagnosis.confidence,
        status=diagnosis.status,
        rejection=diagnosis.rejection,
        model=diagnosis.model,
        path=str(diagnosis_path) if diagnosis_path else None,
    )
    return DiagnosisResult(diagnosis, bundle, evidence_path, diagnosis_path)


def auto_diagnoser(
    client: LLMClient | Callable[[], LLMClient] | None = None,
) -> Callable[[Guardian, str, str], DiagnosisResult]:
    """A ``Guardian(diagnoser=...)`` hook for blocks with ``auto_diagnose: true``.

    The client is built from the environment on first use, so a pipeline whose blocks
    never roll back never needs LLM settings. Errors propagate to Guardian, which logs
    them as a WARN event and carries on.
    """
    cache: list[LLMClient] = []

    def get_client() -> LLMClient:
        if not cache:
            if client is None:
                cache.append(make_client())
            elif isinstance(client, LLMClient):
                cache.append(client)
            else:
                cache.append(client())
        return cache[0]

    def run(g: Guardian, block: str, run_id: str) -> DiagnosisResult:
        return diagnose(g, block, run_id, client=get_client())

    return run
