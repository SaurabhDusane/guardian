"""Dagster adapter for Guardian. Requires the optional ``dagster`` extra.

All repair logic (validation, quarantine, rollback, rerouting, replay) lives in
``guardian.core``; this package only maps Dagster's hooks onto the Guardian facade.
"""

from guardian.adapters.dagster.definitions import PIPELINE_JOB, build_definitions
from guardian.adapters.dagster.io_manager import GuardianIOManager
from guardian.adapters.dagster.replay import REPLAY_JOB, build_replay_job

__all__ = [
    "PIPELINE_JOB",
    "REPLAY_JOB",
    "GuardianIOManager",
    "build_definitions",
    "build_replay_job",
]
