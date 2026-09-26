from pathlib import Path

import pytest

from guardian.core.models import BlockSpec, FallbackEdge, PipelineSpec
from guardian.runner.spec_loader import load_spec

from ..helpers.roles import (
    ROLES,
    blocks_by_role,
    dependents,
    descendants,
    present_roles,
    representative,
    roles_of,
)

DEMO_SPEC = Path(__file__).parents[2] / "guardian" / "demo" / "pipeline.yaml"


def spec(*blocks: BlockSpec) -> PipelineSpec:
    return PipelineSpec(name="t", blocks=blocks)


# a -> b -> d ; a -> c -> d ; e (isolated) ; d falls back from c to b
DIAMOND = spec(
    BlockSpec("a", "m:f"),
    BlockSpec("b", "m:f", inputs=("a",)),
    BlockSpec("c", "m:f", inputs=("a",)),
    BlockSpec("d", "m:f", inputs=("b", "c"), fallbacks=(FallbackEdge("c", "b"),)),
    BlockSpec("e", "m:f"),
)


def test_roles_on_a_diamond() -> None:
    roles = blocks_by_role(DIAMOND)
    assert set(roles) == set(ROLES)
    assert roles["source"] == ["a", "e"]
    assert roles["leaf"] == ["e", "d"]  # topological order: e is ready in the first wave
    assert roles["fallback_protected"] == ["c"]
    assert roles["unprotected"] == ["a", "b"]
    assert roles["fallback_source"] == ["b"]
    assert roles["multi_dependent"] == ["a"]


def test_isolated_block_is_source_and_leaf_only() -> None:
    assert roles_of(DIAMOND, "e") == {"source", "leaf"}


def test_protected_and_unprotected_are_exclusive_and_cover_blocks_with_dependents() -> None:
    roles = blocks_by_role(DIAMOND)
    with_deps = {b for b in DIAMOND.block_names if dependents(DIAMOND, b)}
    assert set(roles["fallback_protected"]) | set(roles["unprotected"]) == with_deps
    assert not set(roles["fallback_protected"]) & set(roles["unprotected"])


def test_partially_protected_block_counts_as_protected() -> None:
    """Protected if at least one dependent has a fallback for it."""
    s = spec(
        BlockSpec("x", "m:f"),
        BlockSpec("alt", "m:f"),
        BlockSpec("p", "m:f", inputs=("x",), fallbacks=(FallbackEdge("x", "alt"),)),
        BlockSpec("q", "m:f", inputs=("x",)),
    )
    assert "x" in blocks_by_role(s)["fallback_protected"]
    assert "x" not in blocks_by_role(s)["unprotected"]
    assert "x" in blocks_by_role(s)["multi_dependent"]


def test_fallback_source_needs_no_dependents() -> None:
    s = spec(
        BlockSpec("x", "m:f"),
        BlockSpec("alt", "m:f"),
        BlockSpec("p", "m:f", inputs=("x",), fallbacks=(FallbackEdge("x", "alt"),)),
    )
    assert roles_of(s, "alt") == {"source", "leaf", "fallback_source"}


def test_dependents_and_descendants() -> None:
    assert dependents(DIAMOND, "a") == ["b", "c"]
    assert descendants(DIAMOND, "a") == ["b", "c", "d"]
    assert descendants(DIAMOND, "d") == []


def test_representative_prefers_the_purest_block() -> None:
    assert representative(DIAMOND, "leaf") == "d"  # e is also a source
    assert representative(DIAMOND, "source") == "e"  # a has 3 roles, e has 2
    assert representative(DIAMOND, "unprotected") == "b"  # a also source/multi
    with pytest.raises(LookupError):
        representative(spec(BlockSpec("a", "m:f")), "fallback_source")


def test_demo_spec_has_every_role() -> None:
    demo = load_spec(DEMO_SPEC)
    assert present_roles(demo) == list(ROLES)
    # every role's representative really has that role
    for role in ROLES:
        assert role in roles_of(demo, representative(demo, role))
