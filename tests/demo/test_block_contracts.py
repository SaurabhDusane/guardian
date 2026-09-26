"""Per-block unit tests: every version of a block, and every fallback adapter standing in
for it, turns clean inputs into output its schema fully accepts.

The demo spec points each block's `tests` here, so a proposed fix (a new version added
to the spec) is covered automatically. Parametrized over the spec's blocks: no block is
named in this file.
"""

from __future__ import annotations

import functools
import tempfile

import pandas as pd
import pytest

from guardian.core.guardian import Guardian
from guardian.core.models import PipelineSpec
from guardian.runner.spec_loader import topological_order

from ..scenarios.runners import scenario_spec

SPEC = scenario_spec(rows=150)


@functools.cache
def clean_outputs(spec: PipelineSpec) -> tuple[Guardian, dict[str, pd.DataFrame]]:
    """Each block's live version run bare, in order, on clean generated data."""
    g = Guardian(spec, tempfile.mkdtemp(prefix="guardian-contract-"))
    outputs: dict[str, pd.DataFrame] = {}
    for name in topological_order(spec):
        inputs = g.prepare_inputs(name, [outputs[u] for u in spec.block(name).inputs])
        outputs[name] = g.version_fn(name)(*inputs)
    g.close()
    return g, outputs


@pytest.mark.parametrize("block", SPEC.block_names)
def test_block_contract(block: str) -> None:
    g, outputs = clean_outputs(SPEC)
    spec = SPEC.block(block)
    validator = g.validator_for(block)
    inputs = g.prepare_inputs(block, [outputs[u] for u in spec.inputs])
    for version in list(spec.versions) or [None]:
        out = g.version_fn(block, version)(*inputs)
        result = validator.validate(out)
        assert result.schema_error is None, (version, result.schema_error)
        assert result.bad.empty, (version, result.bad.head())
        assert len(out) > 0, version
    for dependent in SPEC.dependents(block):
        edge = dependent.fallback_for(block)
        if edge is None:
            continue
        stand_in = outputs[edge.source]
        if edge.adapter:
            stand_in = g.resolve(edge.adapter)(stand_in)
        result = validator.validate(stand_in)
        assert result.schema_error is None and result.bad.empty, edge
