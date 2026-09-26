"""OpenLineage run events from Guardian events, under both runners (in-memory transport)."""

from __future__ import annotations

import uuid

import pytest

from guardian.demo.faults import crash
from guardian.observability.openlineage import (
    PRODUCER,
    SCHEMA_URL,
    MemoryTransport,
    OpenLineageEmitter,
    run_uuid,
)

from ..helpers.roles import dependents, representative
from .helpers import RUNNERS, SPEC, execute

by_runner = pytest.mark.parametrize("runner", RUNNERS)


def emitter() -> tuple[OpenLineageEmitter, MemoryTransport]:
    transport = MemoryTransport()
    return OpenLineageEmitter(SPEC.name, transport, namespace="test"), transport


def terminal(events: list[dict], block: str) -> dict:
    (event,) = [
        e
        for e in events
        if e["job"]["name"] == f"{SPEC.name}.{block}" and e["eventType"] != "START"
    ]
    return event


def inputs_by_upstream(event: dict) -> dict[str, dict]:
    return {i["facets"]["guardian_input"]["upstream"]: i for i in event["inputs"]}


@by_runner
def test_every_block_run_is_a_well_formed_run(tmp_path, runner: str) -> None:
    listener, transport = emitter()
    execute(runner, tmp_path / "g", "r1", [listener])
    events = transport.events
    for e in events:
        assert e["producer"] == PRODUCER and e["schemaURL"] == SCHEMA_URL
        assert e["eventType"] in ("START", "COMPLETE", "FAIL", "ABORT")
        uuid.UUID(e["run"]["runId"])
        assert e["job"]["namespace"] == "test" and e["eventTime"]
    for block in SPEC.block_names:
        runs = [e for e in events if e["job"]["name"] == f"{SPEC.name}.{block}"]
        assert [e["eventType"] for e in runs] == ["START", "COMPLETE"], block
        done = runs[-1]
        assert done["run"]["runId"] == run_uuid(SPEC.name, block, "r1") == runs[0]["run"]["runId"]
        parent = done["run"]["facets"]["parent"]
        assert parent["run"]["runId"] == run_uuid(SPEC.name, "r1")
        assert parent["job"] == {"namespace": "test", "name": SPEC.name}
        decision = done["run"]["facets"]["guardian_decision"]
        assert decision["outcome"] == "PASS" and decision["quality"] == "FRESH"
        (output,) = done["outputs"]
        assert output["name"] == f"{SPEC.name}.{block}"
        assert output["facets"]["version"]["datasetVersion"] == "r1"
        assert output["outputFacets"]["outputStatistics"]["rowCount"] == decision["rows_promoted"]
        assert decision["rows_promoted"] > 0
        assert [inputs_by_upstream(done)] and set(inputs_by_upstream(done)) == set(
            SPEC.block(block).inputs
        )
        for i in done["inputs"]:
            facet = i["facets"]["guardian_input"]
            assert (facet["read"], facet["fallback"], facet["stale"]) == ("FRESH", False, False)
            assert i["facets"]["version"]["datasetVersion"] == "r1"
    if runner == "standalone":  # the standalone runner also reports the pipeline run
        pipeline = [e for e in events if e["job"]["name"] == SPEC.name]
        assert [e["eventType"] for e in pipeline] == ["START", "COMPLETE"]


@by_runner
def test_fallback_and_stale_inputs_are_facets(tmp_path, runner: str) -> None:
    protected = representative(SPEC, "fallback_protected")
    reader = next(d for d in dependents(SPEC, protected) if SPEC.block(d).fallback_for(protected))
    edge = SPEC.block(reader).fallback_for(protected)
    root = tmp_path / "g"
    execute(runner, root, "r1", [])
    listener, transport = emitter()
    execute(runner, root, "r2", [listener], {protected: [crash()]})

    failed = terminal(transport.events, protected)
    assert failed["eventType"] == "FAIL" and failed["outputs"] == []
    assert failed["run"]["facets"]["errorMessage"]["message"] == "crash"
    read = inputs_by_upstream(terminal(transport.events, reader))[protected]
    assert read["name"] == f"{SPEC.name}.{edge.source}"  # the dataset actually read
    facet = read["facets"]["guardian_input"]
    assert (facet["read"], facet["fallback"], facet["adapter"]) == ("FALLBACK", True, edge.adapter)
    assert facet["upstream_status"] == "DEGRADED"
    reader_decision = terminal(transport.events, reader)["run"]["facets"]["guardian_decision"]
    assert reader_decision["quality"] == "FALLBACK"

    # An unprotected block's crash: its dependents read its last good snapshot (stale).
    unprotected = representative(SPEC, "unprotected")
    listener, transport = emitter()
    execute(runner, root, "r3", [listener], {unprotected: [crash()]})
    for d in dependents(SPEC, unprotected):
        read = inputs_by_upstream(terminal(transport.events, d))[unprotected]
        facet = read["facets"]["guardian_input"]
        assert (facet["read"], facet["stale"]) == ("STALE", True)
        assert read["facets"]["version"]["datasetVersion"] == facet["source"].split("@")[1]
        assert read["facets"]["version"]["datasetVersion"] != "r3"


def test_skipped_and_blocked_blocks(tmp_path) -> None:
    source = representative(SPEC, "source")
    listener, transport = emitter()
    execute("standalone", tmp_path / "g", "r1", [listener], out=[source])
    assert terminal(transport.events, source)["eventType"] == "ABORT"
    blocked = dependents(SPEC, source)[0]  # no last good anywhere yet
    event = terminal(transport.events, blocked)
    assert event["eventType"] == "FAIL"
    assert event["run"]["facets"]["guardian_decision"]["outcome"] == "BLOCKED"
