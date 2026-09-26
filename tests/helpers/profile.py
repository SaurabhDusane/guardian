"""Per-block facts derived by running a spec's blocks bare (no Guardian) on clean data."""

from __future__ import annotations

import functools
from dataclasses import dataclass

import pandas as pd

from guardian.core.models import PipelineSpec
from guardian.core.refs import load_ref
from guardian.runner.spec_loader import topological_order


@dataclass(frozen=True)
class BlockProfile:
    block: str
    rows: int
    derived_columns: tuple[str, ...]  # output columns not present in any input
    row_preserving: bool  # exactly one input, one output row per input row (same index)


@functools.cache
def block_profiles(spec: PipelineSpec) -> dict[str, BlockProfile]:
    """Profile every block. The spec's data must be clean (bare blocks can't skip bad rows)."""
    outputs: dict[str, pd.DataFrame] = {}
    profiles: dict[str, BlockProfile] = {}
    for name in topological_order(spec):
        block = spec.block(name)
        inputs = [outputs[u] for u in block.inputs]
        out = load_ref(block.fn)(*inputs, **block.params)
        outputs[name] = out
        seen = {c for frame in inputs for c in frame.columns}
        derived = tuple(c for c in out.columns if c not in seen) or tuple(out.columns)
        preserving = len(inputs) == 1 and out.index.equals(inputs[0].index)
        profiles[name] = BlockProfile(name, len(out), derived, preserving)
    return profiles
