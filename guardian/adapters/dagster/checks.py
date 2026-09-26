"""Asset checks surfacing Guardian's validation decision for each block."""

from __future__ import annotations

from typing import Any

import dagster as dg

from guardian.core.guardian import Guardian
from guardian.core.models import Action, BlockSkipped

CHECK_NAME = "guardian_validation"


def check_spec(block: str) -> dg.AssetCheckSpec:
    return dg.AssetCheckSpec(
        CHECK_NAME,
        asset=block,
        description="Guardian validation: PASS promotes a snapshot; ROLLBACK keeps the "
        "last-good snapshot and quarantines the rows.",
    )


def check_result(guardian: Guardian, block: str, run_id: str, result: Any) -> dg.AssetCheckResult:
    """Build the check result after the IO manager has handled ``result``."""
    if isinstance(result, BlockSkipped):
        outcome = "BLOCKED" if result.blocked else "SKIPPED"
        return dg.AssetCheckResult(
            check_name=CHECK_NAME,
            asset_key=block,
            passed=not result.blocked,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"outcome": outcome, "reason": result.reason, "guardian_run_id": run_id},
        )
    decision = guardian.decisions[(block, run_id)]
    passed = decision.action is Action.PASS
    spec = guardian.spec.block(block)
    return dg.AssetCheckResult(
        check_name=CHECK_NAME,
        asset_key=block,
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN if passed else dg.AssetCheckSeverity.ERROR,
        metadata={
            "outcome": decision.action.value,
            "reason": decision.reason or "",
            "guardian_run_id": run_id,
            "rows_total": decision.total_rows,
            "rows_good": decision.good_rows,
            "rows_bad": decision.bad_rows,
            "bad_fraction": float(decision.bad_fraction),
            "quarantine_threshold": spec.quarantine_threshold,
            "last_good_run_id": decision.snapshot.run_id if decision.snapshot else "",
        },
    )


SHADOW_CHECK_NAME = "guardian_shadow"


def shadow_check_spec(block: str) -> dg.AssetCheckSpec:
    return dg.AssetCheckSpec(
        SHADOW_CHECK_NAME,
        asset=block,
        description="Guardian shadow comparison: the candidate version, run on the same "
        "live inputs, is within the block's shadow tolerances.",
    )


def shadow_check_result(guardian: Guardian, block: str, run_id: str) -> dg.AssetCheckResult:
    """The shadow comparison of this materialization (or that nothing is in shadow)."""
    from guardian.adapters.dagster.io_manager import shadow_metadata

    run = guardian.shadow_results.get((block, run_id))
    if run is None:
        candidate = guardian.shadow_candidate(block)
        return dg.AssetCheckResult(
            check_name=SHADOW_CHECK_NAME,
            asset_key=block,
            passed=True,
            metadata={
                "status": "no candidate in shadow"
                if candidate is None
                else "candidate did not run (no usable inputs)",
                "active_version": guardian.active_version(block) or "-",
                "guardian_run_id": run_id,
            },
        )
    return dg.AssetCheckResult(
        check_name=SHADOW_CHECK_NAME,
        asset_key=block,
        passed=run.within_tolerance,
        severity=dg.AssetCheckSeverity.WARN,  # a bad candidate never affects live data
        metadata={
            **shadow_metadata(run),
            "status": "promoted" if guardian.active_version(block) == run.version else "in shadow",
            "active_version": guardian.active_version(block) or "-",
            "guardian_run_id": run_id,
        },
    )
