"""GuardianIOManager: Dagster storage hooks mapped onto the Guardian facade.

- ``handle_output`` -> ``Guardian.handle_result`` (``on_output`` for a DataFrame,
  ``on_crash`` for a crash, nothing for a skipped block).
- ``load_input``    -> ``Guardian.resolve_input`` + ``Guardian.read``; the upstream
  block is the input's asset key. A missing safe input is returned as a
  ``BlockSkipped(blocked=True)`` value instead of failing the step, so the
  consumer can report BLOCKED and the rest of the pipeline keeps running.
"""

from __future__ import annotations

from typing import Any

import dagster as dg

from guardian.core.guardian import Guardian
from guardian.core.models import BlockSkipped, NoSafeInputError

RUN_ID_TAG = "guardian/run_id"
BLOCK_METADATA_KEY = "guardian_block"


def guardian_run_id(run: dg.DagsterRun) -> str:
    """The Guardian run id for a Dagster run: the ``guardian/run_id`` tag, else the run id."""
    return run.tags.get(RUN_ID_TAG, run.run_id)


def block_name(asset_key: dg.AssetKey) -> str:
    return asset_key.to_user_string()


class GuardianIOManager(dg.IOManager):
    def __init__(self, guardian: Guardian) -> None:
        self.guardian = guardian

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        block = block_name(context.asset_key)
        run_id = guardian_run_id(context.step_context.dagster_run)
        decision = self.guardian.handle_result(block, run_id, obj)
        if decision is not None:
            context.add_output_metadata(
                {
                    "guardian_action": decision.action.value,
                    "guardian_run_id": run_id,
                    "rows_total": decision.total_rows,
                    "rows_promoted": decision.good_rows if decision.action.value == "PASS" else 0,
                    "rows_bad": decision.bad_rows,
                }
            )

    def load_input(self, context: dg.InputContext) -> Any:
        upstream = block_name(context.asset_key)
        consumer = (context.definition_metadata or {}).get(
            BLOCK_METADATA_KEY
        ) or context.op_def.name
        run_id = guardian_run_id(context.step_context.dagster_run)
        try:
            ref = self.guardian.resolve_input(consumer, upstream, run_id)
        except NoSafeInputError as exc:
            return BlockSkipped(str(exc), blocked=True)
        return self.guardian.read(ref)
