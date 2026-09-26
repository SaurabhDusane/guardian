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
    - upstream DEGRADED/OUT (including a source block, which has no inputs itself):
      1. a fallback edge exists for (block, upstream) AND its fallback source is HEALTHY
         with a last-good snapshot: read that, through the edge's adapter;
      2. otherwise upstream's last-good snapshot, marked stale, with no adapter. This
         covers the fallback source being DEGRADED/OUT too: an unhealthy fallback is
         never used;
      3. otherwise NoSafeInputError.
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
    fallback_note = ""
    if edge is not None:
        source_status = status_of(edge.source)
        fallback = last_good_of(edge.source)
        if source_status is BlockStatus.HEALTHY and fallback is not None:
            return DataRef(
                block=fallback.block,
                run_id=fallback.run_id,
                requested=upstream,
                adapter=edge.adapter,
            )
        fallback_note = (
            f"; fallback source {edge.source!r} is {source_status.value}"
            if source_status is not BlockStatus.HEALTHY
            else f"; fallback source {edge.source!r} has no last-good snapshot"
        )

    ref = last_good_of(upstream)
    if ref is not None:
        return DataRef(block=ref.block, run_id=ref.run_id, requested=upstream, stale=True)

    raise NoSafeInputError(
        f"{block!r} cannot read {upstream!r} ({status.value}): {upstream!r} has no "
        f"last-good snapshot{fallback_note or '; no fallback edge'}"
    )
