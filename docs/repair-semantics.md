# Repair semantics

This is the contract every adapter honors. The scenario suite in
[`tests/scenarios/`](../tests/scenarios/) checks it for both runners, standalone and
Dagster.

## A block's output: `on_output(block, run_id, df) -> Decision`

Guardian validates `df` against the block's Pandera schema and splits it into good and
bad rows, each bad row with a `rule_name` and a `reason`. A missing column or a
frame-wide check is a schema-level failure. Bad rows go to quarantine with the block,
run id, rule, reason, timestamp and the original row as JSON, with status `QUARANTINED`.

If the bad-row fraction is at or below the block's `quarantine_threshold`, the good rows
become the immutable snapshot `(block, run_id)`, that snapshot becomes the block's
last-good, the block is `HEALTHY` and the decision is PASS.

Above the threshold, or on a schema-level failure, nothing is promoted: the block
becomes `DEGRADED`, the decision is ROLLBACK, and consumers keep reading the previous
last-good snapshot. The rows that were still good are quarantined too, under rule
`rollback`, so none is lost. A block function that raises, or does not return a
DataFrame, is a ROLLBACK with reason `crash`.

## A consumer's input: `resolve_input(block, upstream) -> DataRef`

| Upstream status | Fallback edge for (block, upstream)? | Reads |
|---|---|---|
| `HEALTHY` | n/a | upstream's latest last-good snapshot |
| `DEGRADED` or `OUT` | yes, and the fallback source is `HEALTHY` with a last-good | the fallback source's last-good, passed through the edge's `adapter` |
| `DEGRADED` or `OUT` | no, or the fallback source is itself `DEGRADED`/`OUT` (or has no snapshot) | upstream's last-good, marked `stale`, with no adapter |
| `DEGRADED` or `OUT` | upstream has no last-good either | `NoSafeInputError`: the block is reported `BLOCKED` and the rest of the pipeline continues |
| `HEALTHY` | upstream has never been promoted | `NoSafeInputError`, as above |

An unhealthy fallback source is never used, even if it has a snapshot. When several
blocks are unhealthy at once, a consumer reads stale data from the block it actually
depends on. A source block that is `DEGRADED` or `OUT` needs no special case: its
dependents follow the same table. Every resolution emits a `RESOLVE` or `REROUTE` event
naming the snapshot actually read.

## Recovery

`replay(block)` re-runs the block's fixed function on its `QUARANTINED` records and
validates the result. Records whose rows now pass are marked `REPLAYED`; the rest stay
`QUARANTINED`. Records are never deleted, and the result reports `replayed` and
`still_failing`.

Where recovered rows go depends on the block's `merge_key`. With one (for example
`merge_key: [order_id]`), they are upserted into a copy of the last-good snapshot: on a
key conflict the replayed row wins, and among replayed rows the newest record wins. The
result is a new snapshot, promoted to last-good, and the block becomes `HEALTHY`.
Without a `merge_key`, Guardian cannot tell a correction from a duplicate, so it writes
the recovered rows to their own snapshot, leaves last-good alone, and logs a `WARN`.

Replay only consumes `QUARANTINED` records, so a second call finds nothing to do. A
replay that dies after writing its snapshot but before marking records can be re-run:
the keyed upsert produces the same rows, not duplicates.

`set_block_status(block, HEALTHY | OUT)` is the human control. An `OUT` block is
skipped and its consumers reroute. Only Guardian sets `DEGRADED`.

Structured JSON events go to `events.jsonl` and a DuckDB table. Routine events are
sampled at `--sample-rate`; `PROMOTION`, `WARN`, `ERROR`, `ROLLBACK`, `REROUTE` and
`QUARANTINE` are always kept.

## Walkthrough: heavy corruption in one block

The demo pipeline turns messy e-commerce orders into daily revenue by region and
customer segment, plus a per-customer summary:

```
b1_ingest → b2_parse → b3_standardize → b4_clean → b5_normalize ─┬→ b6_enrich → b8_aggregate
                                                                  │      └─ fallback for b6 ─┘
                                                                  │         (adapter b5_to_b6_shape)
                                                                  └→ b7_customers
```

The walkthrough uses `b6_enrich`, but nothing in Guardian is specific to it. Every
command takes a block name, and what happens to a failing block's dependents depends
only on its role in the DAG. The second example below runs the same kind of failure on
`b2_parse`, which has no fallback.

### 1. A normal run

The raw data is messy on purpose: 57 rows are quarantined in b1 to b4, all under their
thresholds, so every block passes.

```
$ guardian run demo/pipeline.yaml --run-id r1
                                    Pipeline 'demo' - run r1
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━┓
┃ block          ┃ outcome ┃ status  ┃ rows in ┃ promoted ┃ quarantined ┃ read from      ┃ note ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━┩
│ b1_ingest      │ PASS    │ HEALTHY │       0 │      498 │           2 │ -              │      │
│ b2_parse       │ PASS    │ HEALTHY │     498 │      475 │          23 │ b1_ingest      │      │
│ b3_standardize │ PASS    │ HEALTHY │     475 │      462 │          13 │ b2_parse       │      │
│ b4_clean       │ PASS    │ HEALTHY │     462 │      443 │          19 │ b3_standardize │      │
│ b5_normalize   │ PASS    │ HEALTHY │     443 │      443 │           0 │ b4_clean       │      │
│ b6_enrich      │ PASS    │ HEALTHY │     443 │      443 │           0 │ b5_normalize   │      │
│ b7_customers   │ PASS    │ HEALTHY │     443 │      115 │           0 │ b5_normalize   │      │
│ b8_aggregate   │ PASS    │ HEALTHY │     443 │      130 │           0 │ b6_enrich      │      │
└────────────────┴─────────┴─────────┴─────────┴──────────┴─────────────┴────────────────┴──────┘
run r1: 8 blocks, 57 rows quarantined. Storage: .guardian
```

### 2. b6 goes bad

A fault corrupts `region` and `segment` in 50% of b6's output, far above its 10%
threshold.

```
$ guardian run demo/pipeline.yaml --run-id r2 --fault b6_enrich:corrupt:0.5:region,segment
                                                     Pipeline 'demo' - run r2
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ block          ┃ outcome  ┃ status   ┃ rows in ┃ promoted ┃ quarantined ┃ read from                 ┃ note                     ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ b1_ingest      │ PASS     │ HEALTHY  │       0 │      498 │           2 │ -                         │                          │
│ b2_parse       │ PASS     │ HEALTHY  │     498 │      475 │          23 │ b1_ingest                 │                          │
│ b3_standardize │ PASS     │ HEALTHY  │     475 │      462 │          13 │ b2_parse                  │                          │
│ b4_clean       │ PASS     │ HEALTHY  │     462 │      443 │          19 │ b3_standardize            │                          │
│ b5_normalize   │ PASS     │ HEALTHY  │     443 │      443 │           0 │ b4_clean                  │                          │
│ b6_enrich      │ ROLLBACK │ DEGRADED │     443 │        0 │         443 │ b5_normalize              │ bad-row fraction 0.5011  │
│                │          │          │         │          │             │                           │ exceeds threshold 0.1    │
│ b7_customers   │ PASS     │ HEALTHY  │     443 │      115 │           0 │ b5_normalize              │                          │
│ b8_aggregate   │ PASS     │ HEALTHY  │     443 │       98 │           0 │ b5_normalize (fallback    │                          │
│                │          │          │         │          │             │ for b6_enrich)            │                          │
└────────────────┴──────────┴──────────┴─────────┴──────────┴─────────────┴───────────────────────────┴──────────────────────────┘
run r2: 8 blocks, 500 rows quarantined. Storage: .guardian
```

This one run shows all three repair actions. b6's r2 output is not promoted and its
last-good stays at r1. All 443 rows are kept in quarantine: the 222 corrupt ones under
rule `region:isin(['NA', 'EU'])`, the other 221 under `rollback` because the batch was
rejected. b8 reads this run's b5 output through `b5_to_b6_shape`, so revenue is current;
only the segment breakdown is lost (segments are `unassigned` on that path, which is why
b8 has 98 rows instead of 130). b7 does not depend on b6 and is unaffected.

The event log records why:

```json
{"kind": "ROLLBACK", "block": "b6_enrich", "run_id": "r2",
 "data": {"reason": "bad-row fraction 0.5011 exceeds threshold 0.1", "last_good_run_id": "r1"}}
{"kind": "QUARANTINE", "block": "b6_enrich", "run_id": "r2",
 "data": {"rows": 443, "rules": {"region:isin(['NA', 'EU'])": 222, "rollback": 221}}}
{"kind": "REROUTE", "block": "b8_aggregate", "run_id": "r2",
 "data": {"upstream": "b6_enrich", "upstream_status": "DEGRADED", "source": "b5_normalize",
          "source_run_id": "r2", "adapter": "demo.blocks:b5_to_b6_shape", "stale": false}}
```

### 3. Status

b6 is `DEGRADED`, still serving r1, with 443 rows in quarantine. The counts for b1 to b4
add up over both runs.

```
$ guardian status
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ block          ┃ status   ┃ version ┃ shadow ┃ last-good run ┃ quarantined ┃ replayed ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ b1_ingest      │ HEALTHY  │ v1      │ -      │ r2            │ 4           │ 0        │
│ b2_parse       │ HEALTHY  │ v1      │ -      │ r2            │ 46          │ 0        │
│ b3_standardize │ HEALTHY  │ -       │ -      │ r2            │ 26          │ 0        │
│ b4_clean       │ HEALTHY  │ -       │ -      │ r2            │ 38          │ 0        │
│ b5_normalize   │ HEALTHY  │ v1      │ -      │ r2            │ 0           │ 0        │
│ b6_enrich      │ DEGRADED │ v1      │ -      │ r1            │ 443         │ 0        │
│ b7_customers   │ HEALTHY  │ v1      │ -      │ r2            │ 0           │ 0        │
│ b8_aggregate   │ HEALTHY  │ -       │ -      │ r2            │ 0           │ 0        │
└────────────────┴──────────┴─────────┴────────┴───────────────┴─────────────┴──────────┘
```

### 4. Fix and replay

The fault applied to one run only, so the fixed b6 is the real one. Replay re-derives
`region` and `segment` from the intact columns and all 443 records pass. b6 declares
`merge_key: [order_id]`, so the recovered rows are upserted into its r1 snapshot. r1 and
r2 hold the same generated orders, so the new snapshot has 443 rows with unique
`order_id`s, not 886. A second replay has nothing to do:

```
$ guardian replay b6_enrich
b6_enrich: replayed 443, still failing 0, upserted into new last-good snapshot replay-20260926T055359-f98e54

$ guardian replay b6_enrich
b6_enrich: replayed 0, still failing 0 (nothing to replay)

$ guardian status
│ b6_enrich      │ HEALTHY │ v1      │ -      │ replay-20260926T055359-f98e54 │ 0           │ 443      │
```

The 443 records are still in quarantine, now marked `REPLAYED`.

### 5. Refresh b8

b8's r2 output came from the fallback path. Re-running only b8 reads the recovered b6
snapshot and gives the same 130 rows as the clean r1 run.
`test_descendants_after_replay_match_clean_run` checks this under both runners.

```
$ guardian run demo/pipeline.yaml --run-id r3 --only b8_aggregate
                                                Pipeline 'demo' - run r3
┏━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┓
┃ block        ┃ outcome ┃ status  ┃ rows in ┃ promoted ┃ quarantined ┃ read from                               ┃ note ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━┩
│ b8_aggregate │ PASS    │ HEALTHY │     443 │      130 │           0 │ b6_enrich@replay-20260926T055359-f98e54 │      │
└──────────────┴─────────┴─────────┴─────────┴──────────┴─────────────┴─────────────────────────────────────────┴──────┘
run r3: 1 blocks, 0 rows quarantined. Storage: .guardian
```

Without a `merge_key`, replay does not append (that would have duplicated all 443
orders here). It writes the recovered rows to a separate snapshot, leaves last-good
alone, and warns:

```
$ guardian replay b6_enrich
b6_enrich: replayed 443, still failing 0
WARN: no merge_key declared; replayed rows written to separate snapshot replay-20260926T022401-2c26b9, last-good unchanged
```

Every demo block declares a `merge_key`. Under Dagster the failing block's
`guardian_validation` check fails with `outcome=ROLLBACK`, and the IO manager resolves
its dependents' inputs the same way.

## The same failure on b2_parse

b2 has no fallback edge, so when it crashes its dependent b3 reads b2's last-good
snapshot, marked stale, with no adapter. Everything downstream runs on data that is one
run old at b2:

```
$ guardian run demo/pipeline.yaml --run-id r4 --fault b2_parse:crash
                                        Pipeline 'demo' - run r4
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ block          ┃ outcome  ┃ status   ┃ rows in ┃ promoted ┃ quarantined ┃ read from           ┃ note  ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ b1_ingest      │ PASS     │ HEALTHY  │       0 │      498 │           2 │ -                   │       │
│ b2_parse       │ ROLLBACK │ DEGRADED │     498 │        0 │           0 │ b1_ingest           │ crash │
│ b3_standardize │ PASS     │ HEALTHY  │     475 │      462 │          13 │ b2_parse@r2 (stale) │       │
│ b4_clean       │ PASS     │ HEALTHY  │     462 │      443 │          19 │ b3_standardize      │       │
│ b5_normalize   │ PASS     │ HEALTHY  │     443 │      443 │           0 │ b4_clean            │       │
│ b6_enrich      │ PASS     │ HEALTHY  │     443 │      443 │           0 │ b5_normalize        │       │
│ b7_customers   │ PASS     │ HEALTHY  │     443 │      115 │           0 │ b5_normalize        │       │
│ b8_aggregate   │ PASS     │ HEALTHY  │     443 │      130 │           0 │ b6_enrich           │       │
└────────────────┴──────────┴──────────┴─────────┴──────────┴─────────────┴─────────────────────┴───────┘
run r4: 8 blocks, 34 rows quarantined. Storage: .guardian
```

A crash produces no rows, so nothing is quarantined for b2. The next run without the
fault passes, b2 returns to `HEALTHY`, and b3 reads it fresh again.
