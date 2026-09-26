"""Guardian events as OpenLineage run events (spec 2-0-2), with the standard library only.

Each block run is an OpenLineage run of job ``<pipeline>.<block>``, a child (``parent``
facet) of the pipeline run's job ``<pipeline>``:

- START when Guardian first reports on the block for that run;
- COMPLETE (PASS), FAIL (ROLLBACK or BLOCKED) or ABORT (SKIPPED: taken OUT) from the
  block's BLOCK_OUTCOME event.

Inputs are the snapshots the block actually read. A fallback read names the fallback
source's dataset and a stale read the older snapshot (``version`` facet = the run id
read); the custom ``guardian_input`` facet says which upstream it stands in for, how it
was read (FRESH / STALE / FALLBACK), through which adapter, and the upstream's status.
A promoted output carries ``outputStatistics`` (row count) and ``guardian_quality``.
The run's ``guardian_decision`` facet holds the outcome, status, reason, version and
row counts.
"""

from __future__ import annotations

import json
import sys
import urllib.request
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from guardian.core.events import Event, EventKind

PRODUCER = "https://github.com/saurabhdusane/guardian"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
_FACETS = "https://openlineage.io/spec/facets"
PARENT_SCHEMA = f"{_FACETS}/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet"
ERROR_SCHEMA = f"{_FACETS}/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"
VERSION_SCHEMA = (
    f"{_FACETS}/1-0-1/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"
)
STATS_SCHEMA = (
    f"{_FACETS}/1-0-2/OutputStatisticsOutputDatasetFacet.json"
    "#/$defs/OutputStatisticsOutputDatasetFacet"
)
GUARDIAN_FACET_SCHEMA = f"{PRODUCER}/blob/main/README.md#observability"

# Events emitted while a block runs live. Others that name a block (replays, shadow runs,
# promotions, status changes) are not block runs and never produce a BLOCK_OUTCOME.
RUN_EVENTS = frozenset(
    {
        EventKind.BLOCK_STARTED,
        EventKind.RESOLVE,
        EventKind.REROUTE,
        EventKind.VALIDATION,
        EventKind.ERROR,
        EventKind.BLOCK_OUTCOME,
    }
)
EVENT_TYPES = {"PASS": "COMPLETE", "ROLLBACK": "FAIL", "BLOCKED": "FAIL", "SKIPPED": "ABORT"}
_RUN_NAMESPACE = uuid.UUID("5a1ad0e5-6ae5-4b6e-9d2e-6d7f1c0a9e11")

Transport = Callable[[dict[str, Any]], None]


def run_uuid(*parts: str) -> str:
    """A stable OpenLineage runId (a UUID) for a pipeline run or a block run."""
    return str(uuid.uuid5(_RUN_NAMESPACE, "/".join(parts)))


def _facet(schema: str, **fields: Any) -> dict[str, Any]:
    return {"_producer": PRODUCER, "_schemaURL": schema, **fields}


def _time(ts: datetime) -> str:
    return ts.isoformat()


# ------------------------------------------------------------------ transports


class MemoryTransport:
    """Keeps events in ``events`` (for tests)."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def __call__(self, event: dict[str, Any]) -> None:
        self.events.append(event)


class FileTransport:
    """Appends one JSON event per line."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def __call__(self, event: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(event, sort_keys=True) + "\n")


class ConsoleTransport:
    def __call__(self, event: dict[str, Any]) -> None:
        print(json.dumps(event, sort_keys=True), file=sys.stderr)


class HttpTransport:
    """POSTs events to an OpenLineage HTTP endpoint (e.g. Marquez: /api/v1/lineage)."""

    def __init__(self, url: str, *, endpoint: str = "api/v1/lineage", api_key: str | None = None):
        self.url = f"{url.rstrip('/')}/{endpoint.lstrip('/')}"
        self.api_key = api_key

    def __call__(self, event: dict[str, Any]) -> None:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(
            self.url, data=json.dumps(event).encode("utf-8"), headers=headers, method="POST"
        )
        with urllib.request.urlopen(request, timeout=10):
            pass


# ------------------------------------------------------------------ emitter


class OpenLineageEmitter:
    """An EventLogger listener that turns Guardian events into OpenLineage run events."""

    def __init__(
        self,
        pipeline: str,
        transport: Transport,
        *,
        namespace: str = "guardian",
        dataset_namespace: str | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.transport = transport
        self.namespace = namespace
        self.dataset_namespace = dataset_namespace or namespace
        self._started: set[tuple[str, str]] = set()

    # -------------------------------------------------------------- mapping

    def __call__(self, event: Event) -> None:
        if event.kind is EventKind.RUN_STARTED and event.run_id:
            self._send(self._pipeline_event("START", event))
        elif event.kind is EventKind.RUN_FINISHED and event.run_id:
            self._send(self._pipeline_event("COMPLETE", event))
        elif event.block and event.run_id and event.kind in RUN_EVENTS:
            if (event.block, event.run_id) not in self._started:
                self._started.add((event.block, event.run_id))
                self._send(self._block_event("START", event))
            if event.kind is EventKind.BLOCK_OUTCOME:
                self._started.discard((event.block, event.run_id))
                outcome = event.data.get("outcome", "PASS")
                self._send(self._block_event(EVENT_TYPES.get(outcome, "COMPLETE"), event))

    def _send(self, payload: dict[str, Any]) -> None:
        self.transport(payload)

    def _base(self, event_type: str, event: Event, job: str, run_id: str) -> dict[str, Any]:
        return {
            "eventType": event_type,
            "eventTime": _time(event.ts),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": run_id, "facets": {}},
            "job": {"namespace": self.namespace, "name": job, "facets": {}},
            "inputs": [],
            "outputs": [],
        }

    def _pipeline_event(self, event_type: str, event: Event) -> dict[str, Any]:
        assert event.run_id is not None
        payload = self._base(
            event_type, event, self.pipeline, run_uuid(self.pipeline, event.run_id)
        )
        payload["run"]["facets"]["guardian_run"] = _facet(
            GUARDIAN_FACET_SCHEMA,
            run_id=event.run_id,
            outcomes=event.data.get("outcomes", {}),
        )
        return payload

    def dataset(self, block: str) -> str:
        return f"{self.pipeline}.{block}"

    def _block_event(self, event_type: str, event: Event) -> dict[str, Any]:
        assert event.block is not None and event.run_id is not None
        block, run_id = event.block, event.run_id
        payload = self._base(
            event_type, event, self.dataset(block), run_uuid(self.pipeline, block, run_id)
        )
        facets = payload["run"]["facets"]
        facets["parent"] = _facet(
            PARENT_SCHEMA,
            run={"runId": run_uuid(self.pipeline, run_id)},
            job={"namespace": self.namespace, "name": self.pipeline},
        )
        if event.kind is not EventKind.BLOCK_OUTCOME:
            return payload
        d = event.data
        facets["guardian_decision"] = _facet(
            GUARDIAN_FACET_SCHEMA,
            run_id=run_id,
            outcome=d.get("outcome"),
            status=d.get("status"),
            reason=d.get("reason"),
            version=d.get("version"),
            quality=d.get("quality"),
            rows_out=d.get("rows_out"),
            rows_promoted=d.get("rows_promoted"),
            rows_quarantined=d.get("rows_quarantined"),
        )
        if event_type == "FAIL" and d.get("reason"):
            facets["errorMessage"] = _facet(
                ERROR_SCHEMA, message=str(d["reason"]), programmingLanguage="python"
            )
        payload["inputs"] = [self._input(i) for i in d.get("inputs", [])]
        if d.get("outcome") == "PASS":
            payload["outputs"] = [
                {
                    "namespace": self.dataset_namespace,
                    "name": self.dataset(block),
                    "facets": {
                        "version": _facet(VERSION_SCHEMA, datasetVersion=run_id),
                        "guardian_quality": _facet(GUARDIAN_FACET_SCHEMA, quality=d.get("quality")),
                    },
                    "outputFacets": {
                        "outputStatistics": _facet(
                            STATS_SCHEMA, rowCount=int(d.get("rows_promoted") or 0)
                        )
                    },
                }
            ]
        return payload

    def _input(self, i: dict[str, Any]) -> dict[str, Any]:
        """The snapshot actually read: the upstream's, a stale one, or a fallback's."""
        return {
            "namespace": self.dataset_namespace,
            "name": self.dataset(i["block"]),
            "facets": {
                "version": _facet(VERSION_SCHEMA, datasetVersion=i["run_id"]),
                "guardian_input": _facet(
                    GUARDIAN_FACET_SCHEMA,
                    upstream=i["upstream"],
                    read=i["read"],
                    quality=i["quality"],
                    source=f"{i['block']}@{i['run_id']}",
                    adapter=i.get("adapter"),
                    upstream_status=i.get("upstream_status"),
                    fallback=i["read"] == "FALLBACK",
                    stale=i["read"] == "STALE",
                ),
            },
        }
