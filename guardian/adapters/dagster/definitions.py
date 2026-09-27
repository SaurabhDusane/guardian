"""Build Dagster Definitions from the same YAML pipeline spec the standalone runner uses.

``dagster dev -m guardian.adapters.dagster.definitions`` serves the demo pipeline;
set ``GUARDIAN_ROOT`` to choose the storage root (default ``.guardian``).
"""

# No `from __future__ import annotations`: Dagster inspects the context annotation.

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg

from guardian.adapters.dagster.checks import (
    check_result,
    check_spec,
    shadow_check_result,
    shadow_check_spec,
)
from guardian.adapters.dagster.io_manager import (
    BLOCK_METADATA_KEY,
    GuardianIOManager,
    guardian_run_id,
)
from guardian.adapters.dagster.replay import REPLAY_IO_MANAGER_KEY, build_replay_job
from guardian.agent.diagnose import auto_diagnoser
from guardian.core.guardian import Guardian
from guardian.core.models import DEFAULT_STORAGE_ROOT, BlockSpec, PipelineSpec
from guardian.observability import install as install_observability
from guardian.runner.spec_loader import load_spec

PIPELINE_JOB = "guardian_pipeline"
DEMO_SPEC = Path(__file__).resolve().parents[2] / "demo" / "pipeline.yaml"


def _block_asset(block: BlockSpec) -> dg.AssetsDefinition:
    fallback_sources = [e.source for e in block.fallbacks if e.source not in block.inputs]

    @dg.asset(
        name=block.name,
        ins={
            upstream: dg.AssetIn(key=upstream, metadata={BLOCK_METADATA_KEY: block.name})
            for upstream in block.inputs
        },
        # Fallback sources are ordering-only dependencies: they must be materialized
        # before this block in case its input has to be rerouted to them.
        deps=fallback_sources,
        check_specs=[check_spec(block.name)]
        + ([shadow_check_spec(block.name)] if block.versions else []),
        required_resource_keys={"guardian"},
        description=f"Guardian block {block.name} ({block.fn})",
    )
    def _asset(context: dg.AssetExecutionContext, **inputs: Any):
        guardian: Guardian = context.resources.guardian
        run_id = guardian_run_id(context.run)
        frames = [inputs[name] for name in block.inputs]
        result = guardian.begin_block(block.name, run_id, frames)
        # handle_output runs here: live decision, then the shadow candidate (core)
        yield dg.Output(result)
        yield check_result(guardian, block.name, run_id, result)
        if block.versions:
            yield shadow_check_result(guardian, block.name, run_id)

    return _asset


def build_definitions(guardian: Guardian) -> dg.Definitions:
    """Definitions for ``guardian.spec``: one asset per block plus the replay job.

    The Guardian instance is shared by the assets, the IO manager and the replay job.
    """
    assets = [_block_asset(block) for block in guardian.spec.blocks]
    return dg.Definitions(
        assets=assets,
        jobs=[
            dg.define_asset_job(PIPELINE_JOB, selection=dg.AssetSelection.all()),
            build_replay_job(),
        ],
        resources={
            "guardian": dg.ResourceDefinition.hardcoded_resource(guardian),
            "io_manager": dg.IOManagerDefinition.hardcoded_io_manager(GuardianIOManager(guardian)),
            REPLAY_IO_MANAGER_KEY: dg.mem_io_manager,
        },
    )


def definitions_for(
    spec: PipelineSpec | Path | str = DEMO_SPEC,
    root: Path | str = DEFAULT_STORAGE_ROOT,
    registry: Mapping[str, Any] | None = None,
) -> dg.Definitions:
    if not isinstance(spec, PipelineSpec):
        spec = load_spec(spec)
    # Blocks with auto_diagnose get an advisory diagnosis after a ROLLBACK (core calls
    # the hook; LLM settings come from the environment).
    guardian = Guardian(spec, root, registry=registry, diagnoser=auto_diagnoser())
    install_observability(guardian)
    guardian.close()  # release the event DB until first use; it reopens lazily
    return build_definitions(guardian)


@dg.definitions
def defs() -> dg.Definitions:
    """Lazily built demo Definitions (nothing touches storage at import time)."""
    return definitions_for(DEMO_SPEC, os.environ.get("GUARDIAN_ROOT", DEFAULT_STORAGE_ROOT))
