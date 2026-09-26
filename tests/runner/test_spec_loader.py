from pathlib import Path

import pytest
import yaml

from guardian.runner.spec_loader import SpecError, load_spec, parse_spec, topological_order

DEMO_SPEC = Path(__file__).parents[2] / "guardian" / "demo" / "pipeline.yaml"


def spec(*blocks: dict) -> dict:
    return {"name": "p", "blocks": list(blocks)}


def test_loads_demo_spec() -> None:
    s = load_spec(DEMO_SPEC)
    assert s.name == "demo"
    b8 = s.block("b8_aggregate")
    assert b8.inputs == ("b6_enrich",)
    edge = b8.fallback_for("b6_enrich")
    assert edge.source == "b5_normalize" and edge.adapter == "demo.blocks:b5_to_b6_shape"
    assert s.block("b1_ingest").params["rows"] == 500
    assert s.block("b1_ingest").quarantine_threshold == 0.2
    assert all(b.merge_key for b in s.blocks)  # every demo block declares one
    assert s.block("b8_aggregate").merge_key == ("order_date", "region", "segment")


def test_topological_order_respects_inputs_and_fallback_sources() -> None:
    s = parse_spec(
        spec(
            {
                "name": "c",
                "fn": "m:f",
                "inputs": ["b"],
                "fallbacks": [{"replaces": "b", "source": "x"}],
            },
            {"name": "b", "fn": "m:f", "inputs": ["a"]},
            {"name": "x", "fn": "m:f"},
            {"name": "a", "fn": "m:f"},
        )
    )
    order = topological_order(s)
    assert order.index("a") < order.index("b") < order.index("c")
    assert order.index("x") < order.index("c")


def test_rejects_cycle() -> None:
    with pytest.raises(SpecError, match="cycle"):
        parse_spec(
            spec(
                {"name": "a", "fn": "m:f", "inputs": ["c"]},
                {"name": "b", "fn": "m:f", "inputs": ["a"]},
                {"name": "c", "fn": "m:f", "inputs": ["b"]},
            )
        )


def test_rejects_cycle_through_fallback_edge() -> None:
    with pytest.raises(SpecError, match="cycle"):
        parse_spec(
            spec(
                {"name": "a", "fn": "m:f"},
                {
                    "name": "b",
                    "fn": "m:f",
                    "inputs": ["a"],
                    "fallbacks": [{"replaces": "a", "source": "c"}],
                },
                {"name": "c", "fn": "m:f", "inputs": ["b"]},
            )
        )


def test_rejects_self_input() -> None:
    with pytest.raises(SpecError):
        parse_spec(spec({"name": "a", "fn": "m:f", "inputs": ["a"]}))


def test_rejects_unknown_input() -> None:
    with pytest.raises(SpecError, match="unknown input 'ghost'"):
        parse_spec(spec({"name": "a", "fn": "m:f", "inputs": ["ghost"]}))


def test_rejects_fallback_to_nonexistent_block() -> None:
    with pytest.raises(SpecError, match="nonexistent block 'ghost'"):
        parse_spec(
            spec(
                {"name": "a", "fn": "m:f"},
                {
                    "name": "b",
                    "fn": "m:f",
                    "inputs": ["a"],
                    "fallbacks": [{"replaces": "a", "source": "ghost"}],
                },
            )
        )


def test_rejects_fallback_for_non_input() -> None:
    with pytest.raises(SpecError, match="not one of its inputs"):
        parse_spec(
            spec(
                {"name": "a", "fn": "m:f"},
                {"name": "x", "fn": "m:f"},
                {"name": "b", "fn": "m:f", "fallbacks": [{"replaces": "a", "source": "x"}]},
            )
        )


@pytest.mark.parametrize(
    "raw,match",
    [
        ([], "top level"),
        ({"name": "p"}, "blocks"),
        ({"name": "p", "blocks": [{"name": "a"}]}, "'fn'"),
        ({"name": "p", "blocks": [{"name": "a", "fn": "m:f", "typo": 1}]}, "unknown key"),
        (
            {"name": "p", "blocks": [{"name": "a", "fn": "m:f", "quarantine_threshold": "x"}]},
            "number",
        ),
        (
            {"name": "p", "blocks": [{"name": "a", "fn": "m:f", "quarantine_threshold": 2}]},
            r"\[0, 1\]",
        ),
        ({"name": "p", "blocks": [{"name": "a/b", "fn": "m:f"}]}, "invalid block name"),
        (
            {"name": "p", "blocks": [{"name": "a", "fn": "m:f"}, {"name": "a", "fn": "m:g"}]},
            "duplicate",
        ),
        ({"name": "p", "blocks": [{"name": "a", "fn": "m:f", "inputs": "b"}]}, "list"),
        ({"name": "p", "blocks": [{"name": "a", "fn": "m:f", "merge_key": "id"}]}, "merge_key"),
        ({"name": "p", "blocks": [{"name": "a", "fn": "m:f", "merge_key": []}]}, "merge_key"),
    ],
)
def test_rejects_malformed(raw, match) -> None:
    with pytest.raises(SpecError, match=match):
        parse_spec(raw)


def test_invalid_yaml(tmp_path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("name: [unclosed", encoding="utf-8")
    with pytest.raises(SpecError, match="invalid YAML"):
        load_spec(path)


def test_roundtrip_from_file(tmp_path) -> None:
    path = tmp_path / "p.yaml"
    path.write_text(yaml.safe_dump(spec({"name": "a", "fn": "m:f"})), encoding="utf-8")
    assert load_spec(path).block_names == ("a",)
