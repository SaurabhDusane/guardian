"""Replay job: re-run a fixed block on its quarantined records."""

# No `from __future__ import annotations`: Dagster inspects the context annotation.

import dagster as dg

REPLAY_JOB = "guardian_replay"
# The replay summary is a plain dict, not a block output: keep it out of Guardian's
# IO manager (which only stores block assets).
REPLAY_IO_MANAGER_KEY = "guardian_replay_io"


def build_replay_job() -> dg.JobDefinition:
    @dg.op(
        name="guardian_replay_op",
        config_schema={"block": str},
        required_resource_keys={"guardian"},
        out=dg.Out(dict, io_manager_key=REPLAY_IO_MANAGER_KEY),
    )
    def replay_op(context: dg.OpExecutionContext) -> dict:
        guardian = context.resources.guardian
        result = guardian.replay(context.op_config["block"])
        summary = {
            "block": result.block,
            "replayed": result.replayed,
            "still_failing": result.still_failing,
            "snapshot_run_id": result.snapshot.run_id if result.snapshot else None,
            "merged": result.merged,
        }
        context.add_output_metadata({k: v if v is not None else "" for k, v in summary.items()})
        return summary

    @dg.job(name=REPLAY_JOB, description="Replay quarantined records for one block.")
    def replay_job() -> None:
        replay_op()

    return replay_job


def replay_run_config(block: str) -> dict:
    return {"ops": {"guardian_replay_op": {"config": {"block": block}}}}
