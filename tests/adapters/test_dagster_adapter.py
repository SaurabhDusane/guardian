import subprocess
import sys

import pytest

from guardian.runner.spec_loader import load_spec

dg = pytest.importorskip("dagster")

from guardian.adapters.dagster import PIPELINE_JOB, REPLAY_JOB  # noqa: E402
from guardian.adapters.dagster.definitions import DEMO_SPEC, definitions_for  # noqa: E402

pytestmark = pytest.mark.dagster


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
        assert checks == {"guardian_validation"}
    assert defs.resolve_job_def(PIPELINE_JOB) is not None
    assert defs.resolve_job_def(REPLAY_JOB) is not None


def test_module_defs_are_lazy(tmp_path) -> None:
    code = (
        "import guardian.adapters.dagster.definitions as d; "
        "from pathlib import Path; assert not Path('.guardian').exists(); "
        "d.defs(); assert Path('.guardian').exists()"
    )
    subprocess.run([sys.executable, "-c", code], cwd=tmp_path, check=True)
