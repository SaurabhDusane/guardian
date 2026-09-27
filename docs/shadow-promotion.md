# Shadow promotion

A block can have several implementations. A new one runs in shadow next to the live one,
on the same data, and becomes active only after it has matched the live output or been
approved by a person. [`tests/scenarios/test_shadow.py`](../tests/scenarios/test_shadow.py)
runs every rule below against one block of each DAG role, under both runners.

```yaml
  - name: b6_enrich
    versions:
      v1: demo.blocks:enrich        # live
      v2: demo.blocks:enrich_v2     # candidate: handles " c0042"-style ids too
      v_bad: demo.blocks:enrich_bad # an off-by-one in the VIP rule
    active: v1
    merge_key: [order_id]           # needed to diff and to replay
    shadow:                         # optional; these are the defaults
      required_runs: 3
      max_changed_fraction: 0.01
      # min_pass_rate: 1 - quarantine_threshold
```

A source block also declares `load:` (b1 does). The loader runs once per run and every
version receives the same loaded frame.

## How a candidate runs

`guardian shadow start <block> <version>` registers a candidate. On every run it executes
on the same resolved live inputs as the active version, also when the block is `OUT`, and
also when an upstream block has a candidate of its own: candidates never read each
other. Its output goes to a separate store, `.guardian/candidates/<version>/`, which
`resolve_input` never reads, so no consumer ever sees a candidate.

Each run is compared in one of two modes. In parity mode the live version passed, and
the candidate is diffed against the live output row by row on `merge_key`: added,
removed and changed rows, the changed columns, and per-column stats (null rate, mean,
distinct count). In absolute mode the live version is `DEGRADED` or `OUT`, so there is
no baseline, and the candidate is judged on its own validation pass rate.

## Promotion

A candidate auto-promotes after `required_runs` consecutive parity runs within
tolerance. Absolute mode, or a shadow started with `--expect-diff` for an intended
behavior change, needs `guardian shadow promote <block> --approve`. Approval never
promotes a candidate whose latest run fails `min_pass_rate`.

Promoting makes the candidate the active version in the version registry, which
overrides the spec's `active`. It marks the block `HEALTHY`, so dependents go back to
their normal edges, and replays the block's quarantine through the new version.

A promotion is recorded as `PROMOTING` first and completes only at the end. If it is
interrupted, the old version stays live, `guardian status` shows `PROMOTING`, and
running `guardian shadow promote <block>` again resumes it. `guardian shadow rollback
<block>` makes the previous version active again. Snapshots are immutable, so neither
promotion nor rollback rewrites data. Every snapshot records the version that produced
it (see [Blast radius](blast-radius.md)).

## Example: b6_enrich

A subtly wrong candidate is caught on its first run, and the live output is untouched:

```
$ guardian shadow start b6_enrich v_bad
b6_enrich: shadowing v_bad next to live v1

$ guardian run demo/pipeline.yaml --run-id r2

$ guardian shadow status b6_enrich
      b6_enrich: candidate v_bad vs live v1 (max changed 1.00%, min pass rate 90.00%, 3 runs to auto-promote)
┏━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━┓
┃ run ┃ mode   ┃ live rows ┃ cand. rows ┃ added ┃ removed ┃ changed ┃ changed % ┃ changed columns ┃ pass rate ┃ ok ┃
┡━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━┩
│ r2  │ PARITY │       443 │        443 │     0 │       0 │     184 │    41.53% │ segment         │   100.00% │ no │
└─────┴────────┴───────────┴────────────┴───────┴─────────┴─────────┴───────────┴─────────────────┴───────────┴────┘

$ guardian shadow promote b6_enrich
promoting 'b6_enrich' to 'v_bad' needs approval: it has 0 consecutive parity run(s) within tolerance; 3 are required; latest run: changed fraction 0.4153 > max_changed_fraction 0.01 (columns: segment). Use `guardian shadow promote b6_enrich --approve`.

$ guardian shadow stop b6_enrich
b6_enrich: stopped shadowing v_bad
```

The real improvement matches the live output exactly (0 changed rows) and auto-promotes
after its third run:

```
$ guardian shadow start b6_enrich v2
b6_enrich: shadowing v2 next to live v1

$ guardian run demo/pipeline.yaml --run-id r3
$ guardian run demo/pipeline.yaml --run-id r4
$ guardian shadow status
                            Pipeline 'demo' - blocks in shadow
┏━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ block     ┃ live ┃ candidate ┃ runs ┃ last mode ┃ streak ┃ promotion                   ┃
┡━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ b6_enrich │ v1   │ v2        │ 2    │ PARITY    │ 2/3    │ auto after 3 ok parity runs │
└───────────┴──────┴───────────┴──────┴───────────┴────────┴─────────────────────────────┘

$ guardian run demo/pipeline.yaml --run-id r5

$ guardian status
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ block          ┃ status  ┃ version       ┃ shadow ┃ last-good run ┃ quarantined ┃ replayed ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ b1_ingest      │ HEALTHY │ v1            │ -      │ r5            │ 10          │ 0        │
│ b2_parse       │ HEALTHY │ v1            │ -      │ r5            │ 115         │ 0        │
│ b3_standardize │ HEALTHY │ -             │ -      │ r5            │ 65          │ 0        │
│ b4_clean       │ HEALTHY │ -             │ -      │ r5            │ 95          │ 0        │
│ b5_normalize   │ HEALTHY │ v1            │ -      │ r5            │ 0           │ 0        │
│ b6_enrich      │ HEALTHY │ v2 (spec: v1) │ -      │ r5            │ 0           │ 0        │
│ b7_customers   │ HEALTHY │ v1            │ -      │ r5            │ 0           │ 0        │
│ b8_aggregate   │ HEALTHY │ -             │ -      │ r5            │ 0           │ 0        │
└────────────────┴─────────┴───────────────┴────────┴───────────────┴─────────────┴──────────┘

$ guardian shadow rollback b6_enrich
b6_enrich: rolled back v2 -> v1; last-good is r5
```

The event log keeps the audit trail, and `PROMOTION` events are never sampled out:
`PROMOTE PROMOTING auto v1→v2`, `PROMOTE COMPLETED auto v1→v2`, then
`ROLLBACK COMPLETED v2→v1`.

## Example: an OUT block in absolute mode

With the live version out of service there is no baseline, so the candidate is judged on
its pass rate and promotion needs an approval. Promoting brings `b2_parse` back to
`HEALTHY` and takes b3 off its stale read:

```
$ guardian set-status b2_parse OUT
$ guardian shadow start b2_parse v2
$ guardian run demo/pipeline.yaml --run-id r6
│ b2_parse       │ SKIPPED │ OUT     │       0 │        0 │           0 │ b1_ingest           │ taken out (status OUT) │
│ b3_standardize │ PASS    │ HEALTHY │     475 │      462 │          13 │ b2_parse@r5 (stale) │                        │

$ guardian shadow status b2_parse
┃ run ┃ mode     ┃ live rows ┃ cand. rows ┃ added ┃ removed ┃ changed ┃ changed % ┃ changed columns ┃ pass rate ┃ ok  ┃
│ r6  │ ABSOLUTE │         - │        475 │     - │       - │       - │         - │ -               │    95.38% │ yes │

$ guardian shadow promote b2_parse
promoting 'b2_parse' to 'v2' needs approval: the active version is not healthy, so there is no baseline (absolute mode). Use `guardian shadow promote b2_parse --approve`.

$ guardian shadow promote b2_parse --approve
b2_parse: promoted v1 -> v2 (approved); replayed 0 quarantined record(s) through v2, still failing 115; b2_parse is HEALTHY

$ guardian run demo/pipeline.yaml --run-id r7
│ b2_parse       │ PASS    │ HEALTHY │     498 │      475 │          23 │ b1_ingest      │      │
│ b3_standardize │ PASS    │ HEALTHY │     475 │      462 │          13 │ b2_parse       │      │
```

The 115 records that still fail are b2's unparseable rows from earlier runs, such as
`"not a date"`. v2 recovers only day-first dates and epoch timestamps, and the demo data
has none.

## In Dagster

The candidate runs inside the same asset materialization as the live version: the asset
body prepares the inputs through core, and `GuardianIOManager.handle_output` calls
`Guardian.complete_block`, which records the live decision and then runs the
comparison. The result appears as materialization metadata (`shadow_version`,
`shadow_mode`, `shadow_changed_fraction`, `shadow_changed_columns`, `shadow_pass_rate`)
and as a `guardian_shadow` asset check on every versioned block, which fails with
severity WARN when the candidate is out of tolerance. Promotion, rollback and approval
go through the core or the CLI; a promotion made from the CLI takes effect on the next
Dagster run.
