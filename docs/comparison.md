# Comparison with existing tools

Guardian overlaps with data-quality and observability tools, and it is meant to sit next
to them rather than replace them. Most of those tools answer "is this data good?" and
then alert, or stop the pipeline. Guardian's part starts after the answer is "no": it
decides what downstream consumers read in the meantime, keeps the rejected rows, and
brings them back once the block is fixed.

## Great Expectations

Great Expectations validates a batch of data against a suite of declarative expectations
(not null, in a set, between two values, a column's mean in a range, and many more). A
checkpoint runs the validation and triggers actions such as storing results, updating
the Data Docs site, or sending a notification. It has a much larger library of checks
than Guardian, which relies on Pandera schemas, and it works with pandas, Spark and SQL
databases.

Great Expectations reports; what happens to a failing batch is up to the pipeline around
it, which usually either fails the run or carries on. Guardian can use the same kind of
checks (any `Validator`), and adds what happens next: the failing block's output is not
promoted, consumers read its last-good snapshot or a declared fallback, bad rows are
quarantined one by one with the rule and reason, and they can be replayed later.

## dbt tests

dbt tests are SQL queries that return failing rows, either generic (`unique`,
`not_null`, `accepted_values`, `relationships`) or custom. `dbt build` runs each model's
tests right after the model and skips downstream nodes when a test fails at severity
`error`, which stops bad data from spreading. `store_failures` keeps the failing rows in
a table for inspection, and model contracts, unit tests and model versions cover schema
guarantees, logic tests and versioned models. dbt runs inside the warehouse, at a scale
Guardian does not attempt.

A failed dbt test skips the downstream models, so they keep serving whatever the previous
run built, or nothing. Guardian keeps the pipeline running and records each consumer's
choice explicitly: a fallback edge with an adapter, or a stale snapshot flagged as STALE
in provenance, which you can query later with `guardian impact`. Stored failures in dbt
are for inspection; Guardian's quarantine is a queue of records that `replay` re-runs
through the fixed block and merges back by key, with each record marked REPLAYED.
The write-audit-publish pattern, often built with dbt on table formats that support
branches, is the closest thing to Guardian's promote-on-pass.

## Dagster asset checks

Dagster asset checks attach data-quality checks to assets and show them in the UI, with
WARN or ERROR severity. A blocking check stops downstream assets from materializing when
it fails. Dagster also gives the asset graph, lineage, scheduling, partitions and
freshness checks. Guardian's Dagster adapter uses asset checks itself: every block has a
`guardian_validation` check and every versioned block a `guardian_shadow` check.

A blocking asset check stops the flow; Guardian keeps it going. Its IO manager decides at
read time which snapshot each consumer gets, so a failed block leaves its dependents on
last-good or fallback data instead of skipping them, and the quarantine and replay work
the same as in the standalone runner. See
[Design decisions](design-decisions.md#rerouting-in-dagster-happens-where-data-is-read-not-in-the-graph)
for why rerouting happens in the IO manager rather than in the graph.

## Data observability platforms

Platforms such as Monte Carlo, Bigeye, Metaplane, Anomalo and Soda monitor tables in a
warehouse or lake for freshness, volume, schema changes and distribution anomalies,
usually with learned baselines rather than hand-written rules. They add lineage across
many systems, impact analysis, alert routing and incident workflows, and some offer
circuit breakers that stop a pipeline when a check fails. They cover a whole data
estate, which Guardian does not.

They watch data after it lands and tell people about it; Guardian acts inside one
pipeline, before a block's output is promoted. Its drift detection (PSI and z-score
against the block's last promoted snapshots) is simple next to these platforms'
anomaly models. What it adds is the action taken at the moment a check fails.

## What Guardian adds

- Containment: a failing block's output is never promoted, and its consumers keep
  reading a known-good snapshot or a declared fallback, so the rest of the pipeline
  keeps producing current output.
- Record-level quarantine and replay: every rejected row is stored with its rule and
  reason, nothing is dropped, and once the block is fixed `replay` recovers the rows the
  fixed code can now produce, exactly once, by `merge_key`.
- Fallback rerouting: a consumer can declare an alternative source and an adapter for
  an input, used only while that input is unhealthy and only if the alternative is
  healthy itself, and recorded in provenance as FALLBACK.
- Verified promotion: a new implementation of a block runs in shadow on live inputs,
  is compared row by row with the live one, becomes active only after enough matching
  runs or an explicit approval, and can be rolled back without rewriting data.

## Limitations

Guardian is a single-process library that runs on pandas, so a block's output has to fit
in memory; the benchmark's largest scale is one million rows. Storage is local Parquet files and
DuckDB, with no warehouse integration, no multi-writer coordination beyond atomic file
replacement, and no UI of its own beyond the CLI and Dagster. Its checks are Pandera
schemas plus a simple drift test, not a library of hundreds of expectations. The
diagnosis and fix agent is advisory, has been evaluated only on synthetic injected
faults, and its real-model accuracy is not measured yet.
