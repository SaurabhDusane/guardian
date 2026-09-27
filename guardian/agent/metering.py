"""Metering and caching of LLM calls for evaluations: tokens, latency, cost, and a
response cache on disk so re-runs and resumed runs never pay twice for a call.

``MeteredClient`` wraps any ``LLMClient``. Each call is looked up in the
``ResponseCache`` first, by (model, prompt hash, sample): the prompt hash covers the
system prompt, the prompt and the response schema; the sample number tells repeats
of the same prompt apart (``--repeats``), so repeat k of a resumed run reuses repeat
k's answer and not repeat 0's. A miss calls the inner client and stores the answer
with the prompt that produced it (already redacted: prompts are built from evidence
bundles), the tokens and the latency. Errors are never cached.

Token counts come from the API when the client reports them (``last_usage``); for
clients that do not (the fake one), and for dry runs, they are estimated offline from
text length (``estimate_tokens``) and flagged as estimates. Prices are never
hardcoded: they come from the environment or the command line (``Prices``).
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from guardian.agent.diagnose import LLMClient, LLMRequest, Usage

ENV_PRICE_INPUT = "GUARDIAN_LLM_PRICE_INPUT_PER_MTOK"
ENV_PRICE_OUTPUT = "GUARDIAN_LLM_PRICE_OUTPUT_PER_MTOK"
ENV_CACHE_DIR = "GUARDIAN_LLM_CACHE_DIR"
DEFAULT_CACHE_DIR = Path(".guardian") / "llm-cache"
# Offline token estimate: characters per token for this JSON-heavy English text. An
# approximation; the API's own counts replace it for every call actually made.
CHARS_PER_TOKEN = 3.5
CACHE_FORMAT = 1


def estimate_tokens(text: str) -> int:
    return math.ceil(len(text) / CHARS_PER_TOKEN) if text else 0


def request_tokens(request: LLMRequest) -> int:
    """Estimated input tokens of a request (system prompt, prompt and schema)."""
    return estimate_tokens(request.system + request.prompt + json.dumps(dict(request.schema)))


def prompt_hash(request: LLMRequest) -> str:
    payload = json.dumps(
        {"system": request.system, "prompt": request.prompt, "schema": dict(request.schema)},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ prices


@dataclass(frozen=True)
class Prices:
    """USD per million tokens, from the environment or explicit values."""

    input_per_mtok: float | None = None
    output_per_mtok: float | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        input_per_mtok: float | None = None,
        output_per_mtok: float | None = None,
    ) -> Prices:
        env = os.environ if env is None else env

        def read(name: str, override: float | None) -> float | None:
            if override is not None:
                value = override
            elif env.get(name):
                try:
                    value = float(env[name])
                except ValueError as exc:
                    raise ValueError(f"{name} must be a number (USD per million tokens)") from exc
            else:
                return None
            if value < 0:
                raise ValueError(f"{name} must not be negative")
            return value

        return cls(read(ENV_PRICE_INPUT, input_per_mtok), read(ENV_PRICE_OUTPUT, output_per_mtok))

    @property
    def known(self) -> bool:
        return self.input_per_mtok is not None and self.output_per_mtok is not None

    def cost(self, input_tokens: int, output_tokens: int) -> float | None:
        if not self.known:
            return None
        assert self.input_per_mtok is not None and self.output_per_mtok is not None
        return (input_tokens * self.input_per_mtok + output_tokens * self.output_per_mtok) / 1e6

    def describe(self) -> str:
        if not self.known:
            return f"unknown: set {ENV_PRICE_INPUT} and {ENV_PRICE_OUTPUT} (USD per million tokens)"
        return (
            f"${self.input_per_mtok:g} / ${self.output_per_mtok:g} "
            "per million input / output tokens"
        )

    def to_dict(self) -> dict[str, float | None]:
        return {"input_per_mtok": self.input_per_mtok, "output_per_mtok": self.output_per_mtok}


# ------------------------------------------------------------------ calls and cache


@dataclass(frozen=True)
class CallRecord:
    """One LLM call made (or served from the cache) during an eval."""

    key: str
    sample: int
    model: str
    prompt_hash: str
    input_tokens: int
    output_tokens: int
    estimated: bool  # token counts estimated offline, not reported by the API
    latency_s: float  # of the original call, also when served from the cache
    cached: bool  # served from the cache: not paid for in this run

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class ResponseCache:
    """Answers on disk: ``<dir>/<model>/<prompt hash>-s<sample>.json``."""

    def __init__(self, directory: Path | str) -> None:
        self.dir = Path(directory)

    def path(self, model: str, phash: str, sample: int) -> Path:
        return self.dir / (_UNSAFE.sub("_", model) or "_") / f"{phash}-s{sample}.json"

    def get(self, model: str, phash: str, sample: int) -> dict[str, Any] | None:
        path = self.path(model, phash, sample)
        if not path.exists():
            return None
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None  # a torn write: treated as a miss and overwritten
        if entry.get("format") != CACHE_FORMAT or entry.get("model") != model:
            return None
        return entry

    def put(
        self,
        request: LLMRequest,
        *,
        model: str,
        phash: str,
        sample: int,
        response: str,
        usage: Usage,
        latency_s: float,
    ) -> Path:
        path = self.path(model, phash, sample)
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "format": CACHE_FORMAT,
            "model": model,
            "prompt_hash": phash,
            "sample": sample,
            "key": request.key,
            "created": datetime.now(UTC).isoformat(timespec="seconds"),
            "system": request.system,
            "prompt": request.prompt,
            "response": response,
            "usage": asdict(usage),
            "latency_s": latency_s,
        }
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(entry, indent=1, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
        return path

    def entries(self) -> list[dict[str, Any]]:
        out = []
        for path in sorted(self.dir.rglob("*.json")):
            try:
                out.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
        return out


class MeteredClient:
    """An LLMClient that meters every call and serves repeats from ``cache``.

    ``read_cache=False`` (``--no-cache``) always calls the model, and still stores the
    fresh answers. ``sample`` is the repeat being run; the eval sets it.
    """

    def __init__(
        self,
        inner: LLMClient,
        *,
        cache: ResponseCache | None = None,
        read_cache: bool = True,
    ) -> None:
        self.inner = inner
        self.model = inner.model
        self.cache = cache
        self.read_cache = read_cache
        self.sample = 0
        self.calls: list[CallRecord] = []

    def cached(self, request: LLMRequest) -> bool:
        """Whether ``request`` would be served from the cache (used by dry runs)."""
        return bool(
            self.cache
            and self.read_cache
            and self.cache.get(self.model, prompt_hash(request), self.sample)
        )

    def complete(self, request: LLMRequest) -> str:
        phash = prompt_hash(request)
        if self.cache is not None and self.read_cache:
            entry = self.cache.get(self.model, phash, self.sample)
            if entry is not None:
                usage = entry["usage"]
                self._record(request, phash, Usage(**usage), entry["latency_s"], cached=True)
                return entry["response"]
        start = time.perf_counter()
        text = self.inner.complete(request)
        latency = round(time.perf_counter() - start, 4)
        usage = getattr(self.inner, "last_usage", None) or Usage(
            request_tokens(request), estimate_tokens(text), estimated=True
        )
        if self.cache is not None:
            self.cache.put(
                request,
                model=self.model,
                phash=phash,
                sample=self.sample,
                response=text,
                usage=usage,
                latency_s=latency,
            )
        self._record(request, phash, usage, latency, cached=False)
        return text

    def _record(
        self, request: LLMRequest, phash: str, usage: Usage, latency: float, *, cached: bool
    ) -> None:
        self.calls.append(
            CallRecord(
                key=request.key,
                sample=self.sample,
                model=self.model,
                prompt_hash=phash,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                estimated=usage.estimated,
                latency_s=latency,
                cached=cached,
            )
        )


def default_cache_dir(env: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    return Path(env.get(ENV_CACHE_DIR) or DEFAULT_CACHE_DIR)
