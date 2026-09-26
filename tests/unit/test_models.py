from pathlib import Path

import pytest

from guardian.core.models import (
    DEFAULT_STORAGE_ROOT,
    Action,
    BlockSpec,
    DataRef,
    Decision,
    FallbackEdge,
    PipelineSpec,
    RunContext,
    validate_name,
)


def test_validate_name_accepts_safe_names() -> None:
    for name in ["b1_ingest", "run-2024.01.01", "A9"]:
        assert validate_name(name) == name


@pytest.mark.parametrize("bad", ["", "../x", "a/b", "a\\b", "c:d", ".hidden", "-x", "a..b"])
def test_validate_name_rejects_unsafe_names(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_name(bad)


def test_block_spec_normalizes_lists_and_finds_fallback() -> None:
    edge = FallbackEdge(replaces="b6", source="b5", adapter="m:f")
    spec = BlockSpec(name="b8", fn="m:agg", inputs=["b6"], fallbacks=[edge])  # type: ignore[arg-type]
    assert spec.inputs == ("b6",)
    assert spec.fallback_for("b6") == edge
    assert spec.fallback_for("other") is None
    hash(spec)  # frozen + tuples -> hashable


@pytest.mark.parametrize("threshold", [-0.1, 1.5])
def test_block_spec_rejects_bad_threshold(threshold: float) -> None:
    with pytest.raises(ValueError, match="quarantine_threshold"):
        BlockSpec(name="b", fn="m:f", quarantine_threshold=threshold)


def test_block_spec_rejects_fallback_for_non_input() -> None:
    with pytest.raises(ValueError, match="not one of its inputs"):
        BlockSpec(name="b", fn="m:f", inputs=("a",), fallbacks=(FallbackEdge("z", "y"),))


def test_block_spec_rejects_two_fallbacks_for_same_input() -> None:
    with pytest.raises(ValueError, match="more than one fallback"):
        BlockSpec(
            name="b",
            fn="m:f",
            inputs=("a",),
            fallbacks=(FallbackEdge("a", "x"), FallbackEdge("a", "y")),
        )


def test_fallback_edge_cannot_replace_itself() -> None:
    with pytest.raises(ValueError):
        FallbackEdge(replaces="a", source="a")


def test_pipeline_spec_lookup_and_dependents() -> None:
    a = BlockSpec(name="a", fn="m:a")
    b = BlockSpec(name="b", fn="m:b", inputs=("a",))
    c = BlockSpec(name="c", fn="m:c", inputs=("a", "b"))
    pipe = PipelineSpec(name="p", blocks=[a, b, c])  # type: ignore[arg-type]
    assert pipe.block_names == ("a", "b", "c")
    assert pipe.block("b") is b
    assert pipe.dependents("a") == (b, c)
    with pytest.raises(KeyError):
        pipe.block("zzz")


def test_pipeline_spec_rejects_duplicate_blocks() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        PipelineSpec(name="p", blocks=(BlockSpec("a", "m:a"), BlockSpec("a", "m:b")))


def test_dataref_rerouted() -> None:
    assert not DataRef("a", "r1").rerouted
    assert not DataRef("a", "r1", requested="a").rerouted
    assert DataRef("b5", "r1", requested="b6", adapter="m:f").rerouted


def test_decision_bad_fraction() -> None:
    assert Decision("b", "r", Action.PASS).bad_fraction == 0.0
    d = Decision("b", "r", Action.ROLLBACK, total_rows=10, good_rows=7, bad_rows=3)
    assert d.bad_fraction == pytest.approx(0.3)


def test_run_context_defaults() -> None:
    ctx = RunContext(run_id="r1")
    assert ctx.storage_root == DEFAULT_STORAGE_ROOT == Path(".guardian")
    assert ctx.started_at.tzinfo is not None
    assert isinstance(RunContext("r2", storage_root="x").storage_root, Path)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RunContext(run_id="bad/id")
