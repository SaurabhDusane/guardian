"""Reroute planning: decide which snapshot a block reads for a given upstream.

Pure logic over callbacks, so it is trivially testable and storage-agnostic.
"""

from __future__ import annotations

from collections.abc import Callable

from guardian.core.models import BlockStatus, DataRef, NoSafeInputError, PipelineSpec

StatusLookup = Callable[[str], BlockStatus]
LastGoodLookup = Callable[[str], DataRef | None]


def plan_input(
    spec: PipelineSpec,
    block: str,
    upstream: str,
    status_of: StatusLookup,
    last_good_of: LastGoodLookup,
) -> DataRef:
    """Return the snapshot ``block`` should read in place of ``upstream``'s output.

    - upstream HEALTHY: upstream's last-good snapshot.
    - upstream DEGRADED/OUT: the fallback source's last-good (with the edge's adapter)
      if a fallback edge exists and that source has one; otherwise upstream's last-good
      (marked stale).
    - no candidate has a last-good snapshot: NoSafeInputError.
    """
    consumer = spec.block(block)
    if upstream not in consumer.inputs:
        raise ValueError(f"{upstream!r} is not an input of block {block!r}")

    status = status_of(upstream)
    if status is BlockStatus.HEALTHY:
        ref = last_good_of(upstream)
        if ref is None:
            raise NoSafeInputError(
                f"{block!r} needs {upstream!r}, which has no last-good snapshot yet"
            )
        return DataRef(block=ref.block, run_id=ref.run_id, requested=upstream)

    edge = consumer.fallback_for(upstream)
    if edge is not None:
        fallback = last_good_of(edge.source)
        if fallback is not None:
            return DataRef(
                block=fallback.block,
                run_id=fallback.run_id,
                requested=upstream,
                adapter=edge.adapter,
                stale=status_of(edge.source) is not BlockStatus.HEALTHY,
            )

    ref = last_good_of(upstream)
    if ref is not None:
        return DataRef(block=ref.block, run_id=ref.run_id, requested=upstream, stale=True)

    tried = [upstream] + ([edge.source] if edge is not None else [])
    raise NoSafeInputError(
        f"{block!r} cannot read {upstream!r} ({status.value}): "
        f"no last-good snapshot for any of {tried}"
    )
