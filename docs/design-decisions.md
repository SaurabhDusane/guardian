# Design decisions

## The core does not know about orchestrators

All repair and promotion semantics live in `guardian.core`: plain Python over pandas,
Pandera, Parquet and DuckDB. It never imports dagster, and `tests/unit/test_layering.py`
enforces that. Each adapter only maps its orchestrator's hooks onto the `Guardian`
facade: the standalone executor calls `resolve_input`, `compute` and `handle_result` in
topological order, and the Dagster IO manager calls the same methods from `load_input`
and `handle_output`.

The alternative was to build Guardian as a Dagster feature, with the logic in the IO
manager and sensors, and a separate implementation for the standalone runner. That would
tie the semantics (when to promote, what to quarantine, which snapshot a consumer reads)
to one scheduler, and every rule would need testing twice. With one core, the scenario
suite is parametrized over a runner fixture and runs unchanged against both runners, and
a third orchestrator such as Airflow or Prefect needs a thin adapter rather than a
rewrite. When the Dagster adapter needed crash handling, the logic went into core
(`Guardian.compute` returns a `BlockCrash` value), and the standalone runner now uses it
too.

## Rerouting in Dagster happens where data is read, not in the graph

Guardian leaves the Dagster asset graph as declared. `GuardianIOManager.load_input` asks
core which snapshot a consumer should read: the upstream's last-good, the fallback
source's snapshot through the adapter, or a stale copy. A failing block does not fail its
step; `Guardian.compute` catches the crash, the IO manager records the rollback, and the
step completes so downstream assets still run. Fallback sources are declared as extra
`deps`, so they are always materialized before a consumer that might need them.

The alternative was to reroute by changing the graph, with conditional branches, dynamic
outputs, or one job per combination of failures. Dagster's graph is static and a failed
step skips everything downstream, so that would mean many jobs, and the degraded path
would look different from the healthy one in the UI and in lineage.

The cost is that Dagster records a successful materialization for a block Guardian
rolled back or skipped. The `guardian_validation` asset check (failed, severity ERROR,
with outcome, reason, row counts and threshold) and the `guardian_action` output
metadata carry what actually happened.

## A rejected batch quarantines every row

On a rollback, the rows that were still valid are quarantined too, under rule
`rollback`, and replay brings them back. The alternative was to quarantine only the bad
rows and discard the rest of the rejected batch, but then those good rows would be
neither in a promoted snapshot nor in quarantine. This follows from the invariant the
whole design depends on: every row handed to `on_output` ends up in exactly one place,
the promoted snapshot for that run or quarantine, and after a replay a `REPLAYED` row is
held by the replay snapshot its record points at. Guardian can only act automatically
if it never makes data disappear.

## Quarantine is append-only

Replay changes a record's status from `QUARANTINED` to `REPLAYED` and never deletes it.
Deleting replayed records would keep the table smaller, but every recovery would lose
its audit trail, and a replay interrupted halfway could not tell what it had already
done.

## Replay merges by key or not at all

With a `merge_key`, recovered rows are upserted into a copy of the last-good snapshot.
Without one, they go to a separate snapshot, last-good is left alone, and a WARN event
says so. Appending recovered rows to the last-good snapshot would be simpler and would
satisfy "no loss", but that snapshot may already hold an earlier version of the same
row, which breaks "exactly once". Upserting by key satisfies both, and it makes replay
idempotent.

The invariant is tested at three levels. Hypothesis property tests
(`tests/unit/test_invariant.py`) generate random frames with bad rows, NaNs,
unconvertible text, repeated index labels and missing columns, across thresholds, and
check that promoted plus quarantined row ids equal the input ids, then that each input id
is held exactly once after one and two replays, with and without a `merge_key`. Every
scenario also checks each block, reading back from the stored events, snapshots and
quarantine rather than trusting the runner's report, and checks the chain end to end:
every ingested row is in the final snapshot or quarantined somewhere upstream.

## Tests select blocks by DAG role

Guardian's code never names a block, and the tests follow the same rule.
`blocks_by_role(spec)` classifies every block by its place in the DAG, and each scenario
runs against the representative block of each role, with expectations derived from the
spec: a dependent with a healthy fallback edge reads through the adapter, any other
dependent reads stale data, and a leaf reroutes nothing.

| Role | Meaning | Demo block used |
|---|---|---|
| `source` | no inputs | b1_ingest |
| `leaf` | no dependents | b7_customers (b8 also qualifies) |
| `fallback_protected` | a dependent has a fallback edge replacing it | b6_enrich |
| `unprotected` | has dependents, none with a fallback edge replacing it | b2_parse (b1 to b5 qualify) |
| `fallback_source` | is the source of some fallback edge | b5_normalize |
| `multi_dependent` | two or more dependents | b5_normalize |

The alternative was to write scenarios against named demo blocks, which is quicker to
write. But a test tied to `b6_enrich` only shows that b6 works; a test tied to a role
holds for any spec that has that role, and it would catch a rule that only works for the
block it was written against. `tests/unit/test_roles.py` checks that the demo spec
contains every role.

## What I'd do differently

<!-- SAURABH: write this -->
<!-- Note: the decisions above (or others) you would change with hindsight, and why. -->

## Hardest problem

<!-- SAURABH: write this -->
<!-- Note: the problem that took the most thought to get right, and how it was solved. -->
