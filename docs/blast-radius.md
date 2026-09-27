# Blast radius

When a block goes bad, the next question is what it touched. Guardian records the
provenance of every snapshot it writes in `.guardian/provenance.duckdb`: the block
version that produced it, the snapshot each input was actually read from (`block@run_id`,
which is the upstream's own snapshot, a fallback source's snapshot with its adapter, or a
stale last-good one), and a quality of `FRESH`, `STALE` or `FALLBACK`. It also records
every block's outcome per run: PASS, ROLLBACK, SKIPPED or BLOCKED.

A snapshot's quality is the worst of its inputs (FALLBACK > STALE > FRESH), and an
input's quality includes the quality of the snapshot it read, so degradation propagates
downstream.

`guardian impact <block> [--since <run_id>]` lists the block's own degraded runs, every
downstream snapshot that read it while it was unhealthy, and everything that read those
snapshots in turn. `guardian lineage <block> <run_id>` prints the upstream provenance
tree of one snapshot. Both work for any block.

Quality is not written into your data by default. A block can opt in with
`annotate_quality: true`, which appends a `_guardian_quality` column to its promoted
output; the demo does this for `b7_customers`. In Dagster the same information is
attached to each materialization as `guardian_quality`, `guardian_inputs` (a one-line
summary) and `guardian_provenance` (the full record as JSON).

## Example

Four demo runs: r1 is clean, r2 crashes `b6_enrich`, r3 crashes `b5_normalize`, and r4
is clean again. While b6 is down, b8 falls back to b5 through the adapter, so the damage
stops there:

```
$ guardian impact b6_enrich
┏━━━━━━━━━━━━━━┳━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ snapshot     ┃ run ┃ relation   ┃ quality / outcome ┃ how                                                            ┃
┡━━━━━━━━━━━━━━╇━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ b6_enrich    │ r2  │ self       │ ROLLBACK          │ crash                                                          │
│ b8_aggregate │ r2  │ downstream │ FALLBACK          │ read b6_enrich: fallback to b5_normalize@r2 via                │
│              │     │            │                   │ demo.blocks:b5_to_b6_shape                                     │
└──────────────┴─────┴────────────┴───────────────────┴────────────────────────────────────────────────────────────────┘
b6_enrich: 1 degraded run(s); 1 downstream snapshot(s) in 1 block(s) touched.

$ guardian lineage b8_aggregate r2
b8_aggregate@r2  FALLBACK  (run)
└── as b6_enrich, via adapter demo.blocks:b5_to_b6_shape: b5_normalize@r2  FRESH  (run, version v1)
    └── b4_clean@r2  FRESH  (run)
        └── b3_standardize@r2  FRESH  (run)
            └── b2_parse@r2  FRESH  (run, version v1)
                └── b1_ingest@r2  FRESH  (run, version v1)
```

b5 has two dependents and no fallback edge replaces it, so its crash reaches further. b6
and b7 read the previous b5 snapshot (STALE), and b8 inherits that through b6:

```
$ guardian impact b5_normalize
┏━━━━━━━━━━━━━━┳━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ snapshot     ┃ run ┃ relation   ┃ quality / outcome ┃ how                                      ┃
┡━━━━━━━━━━━━━━╇━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ b5_normalize │ r3  │ self       │ ROLLBACK          │ crash                                    │
│ b6_enrich    │ r3  │ downstream │ STALE             │ read b5_normalize: stale b5_normalize@r2 │
│ b7_customers │ r3  │ downstream │ STALE             │ read b5_normalize: stale b5_normalize@r2 │
│ b8_aggregate │ r3  │ downstream │ STALE             │ read b6_enrich@r3                        │
└──────────────┴─────┴────────────┴───────────────────┴──────────────────────────────────────────┘
b5_normalize: 1 degraded run(s); 3 downstream snapshot(s) in 3 block(s) touched.

$ guardian lineage b8_aggregate r3
b8_aggregate@r3  STALE  (run)
└── b6_enrich@r3  STALE  (run, version v1)
    └── stale b5_normalize (DEGRADED): b5_normalize@r2  FRESH  (run, version v1)
        └── b4_clean@r2  FRESH  (run)
            └── b3_standardize@r2  FRESH  (run)
                └── b2_parse@r2  FRESH  (run, version v1)
                    └── b1_ingest@r2  FRESH  (run, version v1)

$ guardian impact b5_normalize --since r4
b5_normalize: 0 degraded run(s); 0 downstream snapshot(s) in 0 block(s) touched.
```

A leaf block has nothing downstream, so its impact is only itself:

```
$ guardian run demo/pipeline.yaml --run-id r5 --fault b8_aggregate:crash
$ guardian impact b8_aggregate
┏━━━━━━━━━━━━━━┳━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ snapshot     ┃ run ┃ relation ┃ quality / outcome ┃ how   ┃
┡━━━━━━━━━━━━━━╇━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ b8_aggregate │ r5  │ self     │ ROLLBACK          │ crash │
└──────────────┴─────┴──────────┴───────────────────┴───────┘
b8_aggregate: 1 degraded run(s); 0 downstream snapshot(s) in 0 block(s) touched.
```

`b7_customers` opts into the annotation, so its r3 output carries
`_guardian_quality = STALE` and its r1 output `FRESH`.

## Tests

[`tests/scenarios/test_provenance.py`](../tests/scenarios/test_provenance.py) runs for
every DAG role under both runners. A fallback reader is FALLBACK, unprotected dependents
are STALE, and a source fault degrades the whole downstream subgraph. When the fallback
source and the block it replaces are both down, the reader is STALE with no adapter, and
after a promotion and replay the next run is FRESH end to end.

A Hypothesis property test places random faults (crash, corruption, OUT) over several
runs, then checks `impact` for every block against an upward search over the recorded
lineage (`impact` itself searches downward), the runner's own outcomes, the DAG (only
descendants are touched, and every dependent that ran is), and the quality flags.
