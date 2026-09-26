import subprocess
import sys

import pytest

from guardian.runner.spec_loader import load_spec

dg = pytest.importorskip("dagster")

from guardian.adapters.dagster import PIPELINE_JOB, REPLAY_JOB  # noqa: E402
from guardian.adapters.dagster.definitions import DEMO_SPEC, definitions_for  # noqa: E402

pytestmark = [pytest.mark.dagster, pytest.mark.slow]


def test_assets_mirror_the_yaml_spec(tmp_path) -> None:
    spec = load_spec(DEMO_SPEC)
    defs = definitions_for(DEMO_SPEC, tmp_path)
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert keys == set(spec.block_names)
    for block in spec.blocks:
        parents = {k.to_user_string() for k in graph.get(dg.AssetKey(block.name)).parent_keys}
        fallbacks = {e.source for e in block.fallbacks}
        assert parents == set(block.inputs) | fallbacks
        checks = {c.name for c in graph.get(dg.AssetKey(block.name)).check_keys}
        # every block has the validation check; versioned blocks also the shadow check
        assert checks == {"guardian_validation"} | (
            {"guardian_shadow"} if block.versions else set()
        )
    assert defs.resolve_job_def(PIPELINE_JOB) is not None
    assert defs.resolve_job_def(REPLAY_JOB) is not None


def test_module_defs_are_lazy(tmp_path) -> None:
    code = (
        "import guardian.adapters.dagster.definitions as d; "
        "from pathlib import Path; assert not Path('.guardian').exists(); "
        "d.defs(); assert Path('.guardian').exists()"
    )
    subprocess.run([sys.executable, "-c", code], cwd=tmp_path, check=True)


def test_candidate_runs_inside_the_same_materialization(tmp_path) -> None:
    """Shadow comparison happens in the block's own materialization, reported as
    materialization metadata and as the guardian_shadow asset check."""
    from guardian.adapters.dagster import build_definitions
    from guardian.adapters.dagster.io_manager import RUN_ID_TAG
    from guardian.core.events import EventKind
    from guardian.core.guardian import Guardian

    from ..helpers.roles import representative
    from ..scenarios.runners import scenario_spec

    spec = scenario_spec()
    x = representative(spec, "fallback_protected")
    with Guardian(spec, tmp_path) as g:
        g.shadow_start(x, "v_bad")
        defs = build_definitions(g)
        result = defs.resolve_job_def("guardian_pipeline").execute_in_process(
            tags={RUN_ID_TAG: "d1"}
        )
        assert result.success
        (mat,) = [
            m
            for m in result.get_asset_materialization_events()
            if m.asset_key.to_user_string() == x
        ]
        meta = {k: v.value for k, v in mat.materialization.metadata.items()}
        assert meta["guardian_action"] == "PASS" and meta["guardian_version"] == "v1"
        assert meta["shadow_version"] == "v_bad" and meta["shadow_mode"] == "PARITY"
        assert meta["shadow_within_tolerance"] is False and meta["shadow_changed"] > 0
        assert meta["shadow_changed_columns"]

        checks = {
            (ev.asset_key.to_user_string(), ev.check_name): ev
            for ev in result.get_asset_check_evaluations()
        }
        shadow_check = checks[(x, "guardian_shadow")]
        assert not shadow_check.passed
        assert shadow_check.metadata["status"].value == "in shadow"
        assert checks[(x, "guardian_validation")].passed  # the live output is fine
        # blocks without a candidate report that nothing is in shadow
        others = [k for k in checks if k[1] == "guardian_shadow" and k[0] != x]
        assert others and all(
            checks[k].metadata["status"].value == "no candidate in shadow" for k in others
        )
        # one SHADOW run event, in this Dagster run, for the shadowed block only
        runs = [e for e in g.events.query(kind=EventKind.SHADOW) if e.data["action"] == "run"]
        assert [(e.block, e.run_id) for e in runs] == [(x, "d1")]


def test_quality_and_provenance_as_materialization_metadata(tmp_path) -> None:
    from guardian.adapters.dagster import build_definitions
    from guardian.adapters.dagster.io_manager import RUN_ID_TAG
    from guardian.core.guardian import Guardian
    from guardian.demo.faults import apply_faults, crash

    from ..helpers.roles import dependents, representative
    from ..scenarios.runners import scenario_spec

    spec = scenario_spec()
    x = representative(spec, "fallback_protected")
    d = next(d for d in dependents(spec, x) if spec.block(d).fallback_for(x))
    for run_id, faults in (("d0", {}), ("d1", {x: [crash()]})):
        faulted, registry = apply_faults(spec, faults)
        with Guardian(faulted, tmp_path, registry=registry) as g:
            result = (
                build_definitions(g)
                .resolve_job_def("guardian_pipeline")
                .execute_in_process(tags={RUN_ID_TAG: run_id})
            )
            assert result.success
            metadata = {
                m.asset_key.to_user_string(): {
                    k: v.value for k, v in m.materialization.metadata.items()
                }
                for m in result.get_asset_materialization_events()
            }
    meta = metadata[d]
    assert meta["guardian_quality"] == "FALLBACK"
    assert "via" in meta["guardian_inputs"] and "(FALLBACK)" in meta["guardian_inputs"]
    prov = meta["guardian_provenance"]
    assert prov["quality"] == "FALLBACK" and prov["inputs"][0]["read"] == "FALLBACK"
    assert "guardian_quality" not in metadata[x]  # crashed: no output, no provenance
