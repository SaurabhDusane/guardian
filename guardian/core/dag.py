"""DAG structure queries: dependents, ancestors and each block's role, for any spec."""

from __future__ import annotations

from guardian.core.models import PipelineSpec

ROLES = (
    "source",  # no inputs
    "leaf",  # no dependents
    "fallback_protected",  # at least one dependent has a fallback edge replacing it
    "unprotected",  # has dependents, none with a fallback edge replacing it
    "fallback_source",  # is the `source` of some fallback edge
    "multi_dependent",  # two or more dependents
)


def dependents(spec: PipelineSpec, block: str) -> list[str]:
    """Blocks that list ``block`` as an input, in spec order."""
    spec.block(block)
    return [b.name for b in spec.blocks if block in b.inputs]


def ancestors(spec: PipelineSpec, block: str) -> list[str]:
    """Every block upstream of ``block`` (inputs and fallback sources), in spec order."""
    found: set[str] = set()
    frontier = [block]
    while frontier:
        current = spec.block(frontier.pop())
        for upstream in (*current.inputs, *(e.source for e in current.fallbacks)):
            if upstream not in found:
                found.add(upstream)
                frontier.append(upstream)
    found.discard(block)
    return [b for b in spec.block_names if b in found]


def roles_of(spec: PipelineSpec, block: str) -> list[str]:
    """The roles of ``block``, in ROLES order (a block may have several)."""
    node = spec.block(block)
    deps = dependents(spec, block)
    protected = any(spec.block(d).fallback_for(block) is not None for d in deps)
    has = {
        "source": not node.inputs,
        "leaf": not deps,
        "fallback_protected": protected,
        "unprotected": bool(deps) and not protected,
        "fallback_source": any(e.source == block for b in spec.blocks for e in b.fallbacks),
        "multi_dependent": len(deps) >= 2,
    }
    return [role for role in ROLES if has[role]]


def blocks_by_role(spec: PipelineSpec) -> dict[str, list[str]]:
    """Map every role in ROLES to its blocks, in spec order (roles may overlap)."""
    roles: dict[str, list[str]] = {role: [] for role in ROLES}
    for name in spec.block_names:
        for role in roles_of(spec, name):
            roles[role].append(name)
    return roles
