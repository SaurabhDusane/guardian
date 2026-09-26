"""Opt-in: the diagnosis contract against a real model (network and an API key).

Deselected by default (marker ``llm``) and skipped unless GUARDIAN_LLM_LIVE=1, with
GUARDIAN_LLM_PROVIDER, GUARDIAN_LLM_MODEL and the API key variable set:

    GUARDIAN_LLM_LIVE=1 GUARDIAN_LLM_PROVIDER=anthropic GUARDIAN_LLM_MODEL=<model> \\
        ANTHROPIC_API_KEY=... uv run pytest -m llm

It checks that a real answer honors the contract (a known root cause, confidence in
[0, 1], citations that exist); how often the answer is *right* is what
`guardian eval diagnose` measures.
"""

from __future__ import annotations

import os

import pytest

from guardian.agent.diagnose import ACCEPTED, ROOT_CAUSES, diagnose_bundle, make_client
from guardian.agent.eval import generate_cases

from ..helpers.roles import representative
from .helpers import SPEC

pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        os.environ.get("GUARDIAN_LLM_LIVE") != "1",
        reason="real-model test: set GUARDIAN_LLM_LIVE=1 and the GUARDIAN_LLM_* settings",
    ),
]


def test_a_real_model_answers_within_the_contract(tmp_path) -> None:
    client = make_client()
    block = representative(SPEC, "fallback_protected")
    (case,) = generate_cases(SPEC, tmp_path, blocks=[block], faults=["schema_drift"])
    diagnosis = diagnose_bundle(case.bundle, client, key=case.case_id)
    assert diagnosis.root_cause in ROOT_CAUSES
    assert 0.0 <= diagnosis.confidence <= 1.0
    if diagnosis.status == ACCEPTED:
        cited = {e for claim in diagnosis.claims for e in claim.evidence}
        assert cited and cited <= set(case.bundle.ids)
    else:
        assert diagnosis.reason  # a downgrade always says why
