"""Select blocks by their role in the DAG, so tests never hardcode block names."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from guardian.core.models import PipelineSpec
from guardian.runner.spec_loader import topological_order

ROLES = (
    "source",  # no inputs
    "leaf",  # no dependents
    "fallback_protected",  # at least one dependent has a fallback edge replacing it
    "unprotected",  # has dependents, none with a fallback edge replacing it
    "fallback_source",  # is the `source` of some fallback edge
    "multi_dependent",  # two or more dependents
)


def dependents(spec: PipelineSpec, block: str) -> list[str]:
    """Blocks that list ``block`` as an input, in topological order."""
    return [b for b in topological_order(spec) if block in spec.block(b).inputs]


def descendants(spec: PipelineSpec, block: str) -> list[str]:
    """Every block downstream of ``block`` through inputs, in topological order."""
    found: set[str] = set()
    frontier = [block]
    while frontier:
        for d in dependents(spec, frontier.pop()):
            if d not in found:
                found.add(d)
                frontier.append(d)
    return [b for b in topological_order(spec) if b in found]


def ancestors(spec: PipelineSpec, block: str) -> list[str]:
    """Every block upstream of ``block`` through inputs, in topological order."""
    return [b for b in topological_order(spec) if block in descendants(spec, b)]


def blocks_by_role(spec: PipelineSpec) -> dict[str, list[str]]:
    """Map every role in ROLES to its blocks (topological order; roles may overlap)."""
    fallback_sources = {e.source for b in spec.blocks for e in b.fallbacks}
    roles: dict[str, list[str]] = {role: [] for role in ROLES}
    for name in topological_order(spec):
        block = spec.block(name)
        deps = dependents(spec, name)
        protected = any(spec.block(d).fallback_for(name) is not None for d in deps)
        if not block.inputs:
            roles["source"].append(name)
        if not deps:
            roles["leaf"].append(name)
        if protected:
            roles["fallback_protected"].append(name)
        if deps and not protected:
            roles["unprotected"].append(name)
        if name in fallback_sources:
            roles["fallback_source"].append(name)
        if len(deps) >= 2:
            roles["multi_dependent"].append(name)
    return roles


def roles_of(spec: PipelineSpec, block: str) -> set[str]:
    return {role for role, blocks in blocks_by_role(spec).items() if block in blocks}


def representative(spec: PipelineSpec, role: str) -> str:
    """One block for ``role``: the one with the fewest other roles, then topological order.

    Preferring the most "pure" block keeps a role's scenario about that role.
    """
    candidates = blocks_by_role(spec)[role]
    if not candidates:
        raise LookupError(f"no block in spec {spec.name!r} has role {role!r}")
    return min(candidates, key=lambda b: (len(roles_of(spec, b)), candidates.index(b)))


def present_roles(spec: PipelineSpec) -> list[str]:
    roles = blocks_by_role(spec)
    return [role for role in ROLES if roles[role]]


# ---------------------------------------------------------------- default vs full suite
#
# Role- and block-parametrized tests run for every role (block) in the full suite. The
# default (fast) run keeps one of them per test, and tests/scenarios/test_smoke.py
# covers every role in the default run, so each role is still exercised there.

DEFAULT_ROLE = "fallback_protected"


def role_params(roles: Sequence[str], default: str = DEFAULT_ROLE) -> list:
    """``roles`` as pytest params; all but ``default`` are marked slow."""
    keep = default if default in roles else roles[0]
    return [
        pytest.param(role, id=role, marks=() if role == keep else pytest.mark.slow)
        for role in roles
    ]


def block_params(spec: PipelineSpec, role: str = DEFAULT_ROLE) -> list:
    """Every block of ``spec`` as pytest params; all but the ``role`` representative slow."""
    keep = representative(spec, role) if blocks_by_role(spec)[role] else spec.block_names[0]
    return [
        pytest.param(block, id=block, marks=() if block == keep else pytest.mark.slow)
        for block in spec.block_names
    ]
