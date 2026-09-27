# Observability

Guardian can export what it does to standard tools: OpenLineage run events to lineage
catalogs such as Marquez, and OpenTelemetry spans to any tracing backend. Both are off by
default, are configured through environment variables, and work under both runners (the
CLI and the Dagster definitions install them).

```bash
uv sync --extra observability     # OpenTelemetry SDK and OTLP exporter; OpenLineage needs nothing
```

| variable | effect |
|---|---|
| `GUARDIAN_OPENLINEAGE_URL` | POST run events to `<url>/api/v1/lineage` (Marquez) |
| `GUARDIAN_OPENLINEAGE_FILE` | or append them to a JSON-lines file |
| `GUARDIAN_OPENLINEAGE_CONSOLE=1` | or print them |
| `GUARDIAN_OPENLINEAGE_NAMESPACE` | job and dataset namespace (default `guardian`) |
| `GUARDIAN_OPENLINEAGE_API_KEY` | optional bearer token |
| `GUARDIAN_OTEL=otlp` / `console` | export spans over OTLP/HTTP, or print them |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | standard OpenTelemetry settings |

Core offers one generic hook: every event goes to registered listeners before sampling,
so sampling limits what Guardian stores, not what exporters see. An exporter that raises
is counted in `listener_errors` and never fails a run. Every block outcome, under either
runner, emits a `BLOCK_OUTCOME` event with the decision, status, quality, row counts and
input provenance. The exporters live in `guardian/observability/`, which core never
imports.

## OpenLineage

Each block run is a run of job `<pipeline>.<block>`, a child (through the `parent` facet)
of the pipeline run's job `<pipeline>`. It emits START when Guardian first reports on the
block, then:

| Guardian outcome | OpenLineage event |
|---|---|
| PASS | COMPLETE |
| ROLLBACK, BLOCKED | FAIL (with `errorMessage`) |
| SKIPPED (block OUT) | ABORT |

Inputs are the snapshots actually read, with a `version` facet naming the run read. A
custom `guardian_input` facet says which upstream the input stands in for, how it was
read (FRESH, STALE or FALLBACK), through which adapter, and the upstream's status. A
promoted output carries `outputStatistics.rowCount` and `guardian_quality`, and the run
carries a `guardian_decision` facet. When b6 crashes, b8 reads b5 through the fallback
adapter:

```json
{
  "eventType": "COMPLETE",
  "job": "demo.b8_aggregate",
  "inputs": [{
    "name": "demo.b5_normalize",
    "facets": {
      "version": {"datasetVersion": "r2"},
      "guardian_input": {"upstream": "b6_enrich", "read": "FALLBACK", "fallback": true,
                         "stale": false, "source": "b5_normalize@r2",
                         "adapter": "demo.blocks:b5_to_b6_shape",
                         "upstream_status": "DEGRADED", "quality": "FALLBACK"}
    }
  }]
}
```

## OpenTelemetry

There is one span per block run, `guardian.block <block>`, from Guardian's first report
on the block to its outcome. Its attributes are `guardian.decision`, `guardian.status`,
`guardian.quality`, `guardian.version`, `guardian.reason`, the row counts
`guardian.rows.out`, `.promoted` and `.quarantined`, and `guardian.inputs`
(`upstream <- block@run (READ)`) with counts of stale and fallback inputs. ROLLBACK and
BLOCKED spans have status ERROR. Under the standalone runner, block spans are children of
a `guardian.run <pipeline>` span.

## Local viewing stack

[`observability/docker-compose.yml`](../observability/docker-compose.yml) runs:

| service | image (pinned) | ports |
|---|---|---|
| Marquez API, and its UI | `marquezproject/marquez:0.51.1`, `marquezproject/marquez-web:0.51.1` | API :5000 (admin :5001), UI :3000 |
| Postgres for Marquez | `postgres:14.24` | internal |
| Tempo | `grafana/tempo:3.0.3` | OTLP/HTTP :4318, API :3200 |
| Grafana, Tempo preconfigured | `grafana/grafana:13.2.2` | :3001 |

Each tag was the latest release on Docker Hub on 2026-09-27, except Postgres, which stays
on the 14 line that Marquez's own setup uses. Override a tag with `POSTGRES_VERSION`,
`MARQUEZ_VERSION`, `TEMPO_VERSION` or `GRAFANA_VERSION`.

One command brings the stack up, runs the demo against it (r1 clean, then r2 with the
fallback-protected block crashing), and prints three links: the Marquez lineage graph of
`demo.b8_aggregate`, the Marquez overview of the demo's namespace, and a Grafana Explore
link that opens the r2 trace in Tempo.

```bash
uv sync --extra observability
uv run python scripts/observability_demo.py          # leaves the stack running
uv run python scripts/observability_demo.py --down   # stop it and delete its data
```

To point your own runs at the stack:

```bash
export GUARDIAN_OPENLINEAGE_URL=http://localhost:5000
export GUARDIAN_OTEL=otlp OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

Screenshot placeholder: the Marquez lineage graph of `demo.b8_aggregate` on r2, reading
`demo.b5_normalize` through the fallback (the script's first link).

Screenshot placeholder: the Grafana / Tempo trace of run r2, with one `guardian.run demo`
span, eight `guardian.block` children, and `b6_enrich` in ERROR (the script's third link).

## Tests

The unit and scenario tests use in-memory exporters (`tests/observability/`): a memory
transport and the OpenTelemetry SDK's `InMemorySpanExporter`, under both the standalone
runner and the Dagster adapter. They check START/COMPLETE pairs with parent facets for
every block, FALLBACK and STALE input facets picked by DAG role, the FAIL, ABORT and
BLOCKED mappings, span attributes and parents, off-by-default configuration, the file
transport through the CLI, and that a failing exporter does not fail the run.

[`tests/docker/test_observability_stack.py`](../tests/docker/test_observability_stack.py)
(`uv run pytest -m docker`) runs against the real stack. It brings the stack up under its
own compose project, waits for every service to be healthy, runs the same demo, and
checks that Marquez has every job and the fallback consumer's FALLBACK input facet, and
that Tempo returns the r2 trace with exactly one span per block and ERROR on the crashed
block only. The stack is always torn down, volumes included, even when a check fails.
The nightly workflow runs it on Ubuntu.
