# Drift detection

Validation catches rows that are wrong. Some failures only show in aggregate: every row
passes the schema, but the output as a whole no longer looks like the block's output. A
currency feed stuck on one value, amounts that doubled, or a category that quietly took
over are examples. Any block can opt into statistical drift detection:

```yaml
- name: b6_enrich
  drift: true                 # defaults, or:
  # drift:
  #   window: 5               # learn from the last 5 promoted snapshots
  #   min_history: 2          # no verdict until 2 exist
  #   warn: {psi: 0.1, z: 3}  # log a DRIFT event
  #   fail: {psi: 0.25, z: 6} # treat as a validation failure (ROLLBACK)
  #   columns: [...]          # default: numeric + categorical (<= max_categories values)
  #   exclude: [...]
```

## Profile, score and verdict

Guardian learns a profile from the block's last `window` promoted snapshots. Per column
it records the null rate, plus the mean, standard deviation and quantile bins for numeric
columns, or the category frequencies for categorical ones. Merge-key columns are row
identity, not a distribution, so they are not profiled; neither are datetimes, nor text
with more than `max_categories` distinct values.

After validation passes, each run's good rows are scored against the profile. Every
tracked column gets a PSI (population stability index): categorical columns compare
category shares, numeric columns compare the share of rows in each of the profile's
quantile bins. Nulls get a bin of their own, so a null burst moves the PSI, and an unseen
category counts as `__other__`. Numeric columns also get the z-score of the mean shift,
`|mean_now − mean_ref| / (std_ref / √n)`. It is sensitive on large outputs; set `z` to
`null`, or raise it, to rely on PSI alone.

A column reaching a `warn` threshold makes the run WARN: a DRIFT event is logged (never
sampled out) and the output is promoted. Reaching a `fail` threshold makes it FAIL, which
is handled like a validation failure: nothing is promoted, the block becomes DEGRADED,
dependents read the fallback or the stale last-good snapshot, and every row is
quarantined under rule `drift`. If the new distribution is in fact correct, `guardian
replay` accepts it: replay re-validates rows but does not re-check drift, so it is the
human override.

Every check is written to `.guardian/drift/<block>/<run_id>.json`, and `guardian drift
<block> [--run <run_id>]` shows it. The agent's evidence bundle includes a `drift` item
with each column's PSI and z, the thresholds and the reference runs; redacted columns
show scores only.

## Example

The demo turns drift detection on for `b6_enrich`. The `drift` fault skews an output
while every row stays schema-valid: in half the rows, each column takes a value it
already holds elsewhere (a number's maximum, a category's most frequent value), and the
merge key is left alone.

```
$ guardian run demo/pipeline.yaml --run-id r1   # r1..r3: clean, the baseline
$ guardian run demo/pipeline.yaml --run-id r4 --fault b6_enrich:drift
  ... b6_enrich │ ROLLBACK │ DEGRADED │ 443 │ 0 │ 443 │ b5_normalize │ distribution drift: ...
$ guardian drift b6_enrich
                           Drift of b6_enrich on r4 vs r1, r2, r3
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ column         ┃ kind        ┃ PSI   ┃ z     ┃ nulls ref -> now ┃ mean ref -> now ┃ level ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━┩
│ amount_usd     │ numeric     │ 1.088 │ 59.11 │ 0.0% -> 0.0%     │ 92.42 -> 286.3  │ FAIL  │
│ unit_price_usd │ numeric     │ 1.054 │ 62.98 │ 0.0% -> 0.0%     │ 60.87 -> 237.4  │ FAIL  │
│ quantity       │ numeric     │ 0.975 │ 20.41 │ 0.0% -> 0.0%     │ 2.005 -> 3.016  │ FAIL  │
│ country        │ categorical │ 0.498 │       │ 0.0% -> 0.0%     │                 │ FAIL  │
│ status         │ categorical │ 0.264 │       │ 0.0% -> 0.0%     │                 │ FAIL  │
│ order_size     │ categorical │ 0.204 │       │ 0.0% -> 0.0%     │                 │ WARN  │
│ region         │ categorical │ 0.137 │       │ 0.0% -> 0.0%     │                 │ WARN  │
│ segment        │ categorical │ 0.098 │       │ 0.0% -> 0.0%     │                 │ OK    │
└────────────────┴─────────────┴───────┴───────┴──────────────────┴─────────────────┴───────┘
FAIL: distribution drift: amount_usd (PSI 1.088 >= 0.25; z 59.11 >= 6.0), unit_price_usd (PSI 1.054
>= 0.25; z 62.98 >= 6.0), quantity (PSI 0.975 >= 0.25; z 20.41 >= 6.0) and 4 more
```

The clean run r3 scores PSI 0.000 on every column. `order_id` (the merge key) and
`customer_id` (too many distinct values) are not tracked.

## Tests

[`tests/scenarios/test_drift.py`](../tests/scenarios/test_drift.py) runs for every DAG
role under both runners. With schema validation alone, the drifted output is promoted
with nothing quarantined. With a drift policy the same fault is a ROLLBACK: dependents
follow the fallback and stale rules, no row is lost, a FAIL event is logged, and the
evidence bundle carries the details. Below the fail threshold it only warns. Unit tests
cover the PSI and z math, null bursts, unseen categories, thresholds, history windows and
policy validation.
