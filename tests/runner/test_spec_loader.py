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


FALLBACK_TO_C = {
    "name": "b",
    "fn": "m:f",
    "inputs": ["a"],
    "fallbacks": [{"replaces": "a", "source": "c"}],
}


@pytest.mark.parametrize(
    ("blocks", "match"),
    [
        pytest.param(
            [
                {"name": "a", "fn": "m:f", "inputs": ["c"]},
                {"name": "b", "fn": "m:f", "inputs": ["a"]},
                {"name": "c", "fn": "m:f", "inputs": ["b"]},
            ],
            "cycle",
            id="cycle",
        ),
        pytest.param(
            [
                {"name": "a", "fn": "m:f"},
                FALLBACK_TO_C,
                {"name": "c", "fn": "m:f", "inputs": ["b"]},
            ],
            "cycle",
            id="cycle_through_fallback_edge",
        ),
        pytest.param(
            [{"name": "a", "fn": "m:f", "inputs": ["a"]}], "lists itself", id="self_input"
        ),
        pytest.param(
            [{"name": "a", "fn": "m:f", "inputs": ["ghost"]}],
            "unknown input 'ghost'",
            id="unknown_input",
        ),
        pytest.param(
            [
                {"name": "a", "fn": "m:f"},
                {**FALLBACK_TO_C, "fallbacks": [{"replaces": "a", "source": "ghost"}]},
            ],
            "nonexistent block 'ghost'",
            id="fallback_to_nonexistent_block",
        ),
        pytest.param(
            [
                {"name": "a", "fn": "m:f"},
                {"name": "x", "fn": "m:f"},
                {"name": "b", "fn": "m:f", "fallbacks": [{"replaces": "a", "source": "x"}]},
            ],
            "not one of its inputs",
            id="fallback_for_non_input",
        ),
    ],
)
def test_rejects_invalid_dag(blocks, match) -> None:
    with pytest.raises(SpecError, match=match):
        parse_spec(spec(*blocks))


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


def test_versions_active_load_and_shadow_policy() -> None:
    s = parse_spec(
        spec(
            {
                "name": "a",
                "load": "m:load",
                "versions": {"v1": "m:f", "v2": "m:g"},
                "active": "v2",
                "merge_key": ["id"],
                "quarantine_threshold": 0.1,
                "shadow": {"required_runs": 5, "max_changed_fraction": 0.05},
            },
            {"name": "b", "fn": "m:f", "inputs": ["a"]},
        )
    )
    a = s.block("a")
    assert a.fn == "m:g" and a.active == "v2" and dict(a.versions) == {"v1": "m:f", "v2": "m:g"}
    assert a.load == "m:load"
    policy = a.shadow_policy()
    assert (policy.required_runs, policy.max_changed_fraction) == (5, 0.05)
    assert policy.min_pass_rate == pytest.approx(0.9)  # default: 1 - quarantine_threshold
    b = s.block("b")
    assert b.versions == {} and b.active is None and b.shadow.required_runs == 3


def test_demo_gives_every_role_a_v2_and_a_v_bad() -> None:
    from ..helpers.roles import ROLES, blocks_by_role

    demo = load_spec(DEMO_SPEC)
    roles = blocks_by_role(demo)
    for role in ROLES:
        assert any({"v2", "v_bad"} <= set(demo.block(b).versions) for b in roles[role]), role


@pytest.mark.parametrize(
    "block,match",
    [
        ({"name": "a", "versions": {"v1": "m:f"}}, "requires 'active'"),
        ({"name": "a", "versions": {"v1": "m:f"}, "active": "v9"}, "not one of"),
        ({"name": "a", "fn": "m:x", "versions": {"v1": "m:f"}, "active": "v1"}, "conflicts"),
        ({"name": "a", "fn": "m:f", "active": "v1"}, "requires 'versions'"),
        ({"name": "a", "versions": ["m:f"], "active": "v1"}, "'versions' must map"),
        ({"name": "a", "fn": "m:f", "shadow": {"required_runs": 0}}, "required_runs"),
        ({"name": "a", "fn": "m:f", "shadow": {"typo": 1}}, "unknown key"),
        ({"name": "a", "fn": "m:f", "load": 3}, "'load'"),
    ],
)
def test_rejects_bad_version_specs(block, match) -> None:
    with pytest.raises(SpecError, match=match):
        parse_spec(spec(block))


def test_load_only_on_source_blocks() -> None:
    with pytest.raises(SpecError, match="only source blocks"):
        parse_spec(
            spec(
                {"name": "a", "fn": "m:f"},
                {"name": "b", "fn": "m:f", "inputs": ["a"], "load": "m:l"},
            )
        )


def test_annotate_quality_flag() -> None:
    s = parse_spec(spec({"name": "a", "fn": "m:f", "annotate_quality": True}))
    assert s.block("a").annotate_quality is True
    assert parse_spec(spec({"name": "a", "fn": "m:f"})).block("a").annotate_quality is False
    with pytest.raises(SpecError, match="true or false"):
        parse_spec(spec({"name": "a", "fn": "m:f", "annotate_quality": "yes"}))


def test_auto_diagnose_and_redact_columns() -> None:
    s = parse_spec(
        spec({"name": "a", "fn": "m:f", "auto_diagnose": True, "redact_columns": ["email"]})
    )
    assert s.block("a").auto_diagnose is True
    assert s.block("a").redact_columns == ("email",)
    plain = parse_spec(spec({"name": "a", "fn": "m:f"})).block("a")
    assert plain.auto_diagnose is False and plain.redact_columns == ()
    with pytest.raises(SpecError, match="true or false"):
        parse_spec(spec({"name": "a", "fn": "m:f", "auto_diagnose": "yes"}))
    for bad in ("email", [1], [""]):
        with pytest.raises(SpecError, match="redact_columns"):
            parse_spec(spec({"name": "a", "fn": "m:f", "redact_columns": bad}))


def test_block_tests_key() -> None:
    s = parse_spec(spec({"name": "a", "fn": "m:f", "tests": ["tests/x.py::t[a]"]}))
    assert s.block("a").tests == ("tests/x.py::t[a]",)
    assert parse_spec(spec({"name": "a", "fn": "m:f"})).block("a").tests == ()
    for bad in ("tests/x.py", [1], [""]):
        with pytest.raises(SpecError, match="tests"):
            parse_spec(spec({"name": "a", "fn": "m:f", "tests": bad}))


def test_demo_blocks_all_declare_unit_tests() -> None:
    demo = load_spec(Path(__file__).parents[2] / "guardian" / "demo" / "pipeline.yaml")
    for block in demo.blocks:
        assert block.tests == (
            f"tests/demo/test_block_contracts.py::test_block_contract[{block.name}]",
        )


def test_drift_policy() -> None:
    assert parse_spec(spec({"name": "a", "fn": "m:f"})).block("a").drift is None
    default = parse_spec(spec({"name": "a", "fn": "m:f", "drift": True})).block("a").drift
    assert default is not None and default.window == 5 and default.fail.psi == 0.25
    custom = (
        parse_spec(
            spec(
                {
                    "name": "a",
                    "fn": "m:f",
                    "drift": {
                        "window": 3,
                        "min_history": 1,
                        "warn": {"psi": 0.2},
                        "fail": {"psi": 0.4, "z": 8},
                        "exclude": ["id"],
                    },
                }
            )
        )
        .block("a")
        .drift
    )
    assert (custom.window, custom.warn.psi, custom.warn.z, custom.fail.z) == (3, 0.2, None, 8.0)
    assert custom.exclude == ("id",)
    for bad, match in (
        ("yes", "must be true or a mapping"),
        ({"windw": 3}, "unknown key"),
        ({"warn": 0.1}, "psi and/or z"),
        ({"warn": {"psi": 0.5}, "fail": {"psi": 0.1}}, "must not exceed"),
        ({"columns": "a"}, "list of column names"),
        ({"window": 0}, "positive integer"),
    ):
        with pytest.raises(SpecError, match=match):
            parse_spec(spec({"name": "a", "fn": "m:f", "drift": bad}))
