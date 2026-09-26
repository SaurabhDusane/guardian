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
from guardian.core.provenance import Provenance
from guardian.core.shadow import ShadowRun

RUN_ID_TAG = "guardian/run_id"
BLOCK_METADATA_KEY = "guardian_block"


def guardian_run_id(run: dg.DagsterRun) -> str:
    """The Guardian run id for a Dagster run: the ``guardian/run_id`` tag, else the run id."""
    return run.tags.get(RUN_ID_TAG, run.run_id)


def block_name(asset_key: dg.AssetKey) -> str:
    return asset_key.to_user_string()


def provenance_metadata(record: Provenance) -> dict[str, Any]:
    """A snapshot's quality and provenance summary as Dagster metadata."""
    return {
        "guardian_quality": record.quality.value,
        "guardian_inputs": "; ".join(
            f"{i.upstream}: {i.how} ({i.quality.value})" for i in record.inputs
        )
        or "-",
        "guardian_provenance": dg.MetadataValue.json(record.as_dict()),
    }


def shadow_metadata(run: ShadowRun) -> dict[str, Any]:
    """A shadow comparison as flat Dagster metadata (prefixed ``shadow_``)."""
    c = run.comparison
    return {
        "shadow_version": run.version,
        "shadow_mode": run.mode.value,
        "shadow_within_tolerance": run.within_tolerance,
        "shadow_pass_rate": float(run.pass_rate),
        "shadow_rows": run.rows_passed,
        "shadow_added": c.added if c else -1,
        "shadow_removed": c.removed if c else -1,
        "shadow_changed": c.changed if c else -1,
        "shadow_changed_fraction": float(c.changed_fraction) if c else -1.0,
        "shadow_changed_columns": ", ".join(c.changed_columns) if c else "",
        "shadow_notes": "; ".join(run.notes),
    }


class GuardianIOManager(dg.IOManager):
    def __init__(self, guardian: Guardian) -> None:
        self.guardian = guardian

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        block = block_name(context.asset_key)
        run_id = guardian_run_id(context.step_context.dagster_run)
        # Live result, then the shadow candidate on the same inputs, in this
        # materialization; all of it is core logic.
        version = self.guardian.active_version(block)  # before any auto-promotion
        decision, shadow = self.guardian.complete_block(block, run_id, obj)
        metadata: dict[str, Any] = {"guardian_run_id": run_id}
        if decision is not None:
            metadata.update(
                {
                    "guardian_action": decision.action.value,
                    "guardian_version": version or "-",
                    "rows_total": decision.total_rows,
                    "rows_promoted": decision.good_rows if decision.action.value == "PASS" else 0,
                    "rows_bad": decision.bad_rows,
                }
            )
        record = self.guardian.provenance.get(block, run_id)
        if record is not None:
            metadata.update(provenance_metadata(record))
        if shadow is not None:
            metadata.update(shadow_metadata(shadow))
        context.add_output_metadata(metadata)

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
