# Guardian

Guardian is a self-healing maintenance layer that attaches to an existing ETL pipeline.
It snapshots every block's output and validates it. When a block goes bad, the
pipeline keeps flowing instead of stopping: consumers are rolled back to the
block's last-good snapshot, failing records are quarantined (never dropped), and
dependents are rerouted through declared fallback edges. Once the block is fixed,
its quarantined records are replayed into a new snapshot. The same core runs as a
standalone YAML-driven DAG runner and as a Dagster plugin, and both modes run the
identical pipeline spec.

## Architecture

```mermaid
flowchart LR
    spec["pipeline.yaml<br/>(blocks, schemas, thresholds,<br/>fallback edges)"]

    subgraph adapters["Adapters (thin; no repair logic)"]
        direction TB
        standalone["Standalone runner<br/>spec_loader · executor · CLI"]
        dagster["Dagster plugin<br/>GuardianIOManager · asset checks · replay job"]
    end

    subgraph core["guardian.core (never imports dagster)"]
        direction TB
        facade["Guardian facade<br/>begin/complete_block · on_output · on_crash<br/>resolve_input · replay · set_block_status<br/>shadow · promote · rollback"]
        validation["validation<br/>Pandera: good / bad rows"]
        planner["planner<br/>which snapshot to read"]
        facade --> validation
        facade --> planner
    end

    subgraph storage["Storage root (./.guardian/)"]
        direction TB
        snaps[("snapshots/<br/>immutable Parquet<br/>+ last-good pointers")]
        quar[("quarantine.duckdb<br/>QUARANTINED / REPLAYED")]
        events[("events.jsonl + events.duckdb<br/>sampled structured events")]
        status[("block_status.json<br/>HEALTHY / DEGRADED / OUT")]
        versions[("versions.duckdb + shadow.duckdb<br/>active versions, promotions,<br/>shadow comparisons")]
        prov[("provenance.duckdb<br/>inputs, versions, quality<br/>per snapshot; block runs")]
        cands[("candidates/<br/>shadow outputs (never read<br/>by consumers)")]
    end

    spec --> standalone
    spec --> dagster
    standalone --> facade
    dagster --> facade
    facade --> snaps
    facade --> quar
    facade --> events
    facade --> status
    facade --> versions
    facade --> prov
    facade --> cands
```

| Layer | Modules | Responsibility |
|---|---|---|
| Core | `guardian/core/` | Models, validation, snapshot and quarantine stores, event log, reroute planner, version registry (`versions.py`), shadow comparison and policy (`shadow.py`), provenance, impact and lineage (`provenance.py`), `Guardian` facade. All repair and promotion semantics live here. |
| Standalone adapter | `guardian/runner/` | YAML → `PipelineSpec` (DAG checks), topological executor, `guardian` CLI. |
| Dagster adapter | `guardian/adapters/dagster/` | IO manager (`handle_output` → core, `load_input` → `resolve_input`), `guardian_validation` and `guardian_shadow` asset checks, `guardian_replay` job, `Definitions` built from the same YAML. |
| Demo | `guardian/demo/` | Messy synthetic orders behind a `DatasetLoader` interface, blocks b1–b8 (every DAG role is represented), schemas, fault injection. |

## Quickstart

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

### Standalone

```bash
uv sync                                        # core + dev tools
uv run guardian run demo/pipeline.yaml         # run the demo; prints a per-block summary
uv run guardian status                         # health, last-good run, quarantine counts
uv run guardian set-status b6_enrich OUT       # take a block out for optimization
uv run guardian run demo/pipeline.yaml         # b8 now reads b5 through the fallback adapter
uv run guardian set-status b6_enrich HEALTHY
uv run guardian replay b2_parse                # re-run a (fixed) block on its quarantine
uv run guardian shadow start b6_enrich v2      # shadow a new version of any block
uv run guardian shadow status                  # ...then promote / rollback / stop
uv run guardian impact b5_normalize            # what did a degraded block touch?
uv run guardian lineage b8_aggregate <run_id>  # where did a snapshot come from?
uv run pytest                                  # unit tests + scenario suite
```

`demo/pipeline.yaml` is looked up in the working directory first, then inside the
installed package, so the command works from anywhere. Storage defaults to `./.guardian/`
(use `--root` to change it). `guardian run` also takes `--run-id`, `--sample-rate`, and
`--fault` for fault injection, and `--only BLOCK` to re-run selected blocks against the
current last-good snapshots (see the walkthrough below).

### Dagster

```bash
uv sync --extra dagster                        # or: uv pip install -e ".[dagster]"
uv run pytest                                  # scenario suite now also runs under Dagster
```

In-process, from Python:

```python
from guardian.adapters.dagster.definitions import DEMO_SPEC, definitions_for
from guardian.adapters.dagster.io_manager import RUN_ID_TAG

defs = definitions_for(DEMO_SPEC, ".guardian")  # same YAML as the standalone runner
result = defs.resolve_job_def("guardian_pipeline").execute_in_process(tags={RUN_ID_TAG: "d1"})
for check in result.get_asset_check_evaluations():
    print(check.asset_key.to_user_string(), check.metadata["outcome"].value)

defs.resolve_job_def("guardian_replay").execute_in_process(
    run_config={"ops": {"guardian_replay_op": {"config": {"block": "b6_enrich"}}}}
)
```

With the Dagster UI (`dagster-webserver` comes with the extra):

```bash
GUARDIAN_ROOT=.guardian uv run dagster dev -m guardian.adapters.dagster.definitions
```

Every block is an asset with a `guardian_validation` check. `guardian_pipeline`
materializes all of them, and `guardian_replay` replays one block. Both modes share the
same storage format, so `guardian status --spec demo/pipeline.yaml --root <root>` works on a
root that Dagster wrote.

## Repair semantics

Every adapter must honor the following contract, and the scenario suite enforces it for
both runners.

**On a block's output: `on_output(block, run_id, df) -> Decision`**

1. Validate `df` against the block's Pandera schema. Rows are split into good and bad,
   and each bad row gets a `rule_name` and a `reason`. A frame-level failure (a missing
   column, a frame-wide check) is a *schema-level* failure.
2. Bad rows go to quarantine with `block, run_id, rule_name, reason, ts, status`
   (`QUARANTINED`) and the original row as `payload_json`.
3. If the bad-row fraction is at or below the block's `quarantine_threshold`, the good
   rows are written as the immutable snapshot `(block, run_id)` and promoted to
   last-good. The block becomes `HEALTHY` and the decision is **PASS**.
4. Above the threshold, or on a schema-level failure, nothing is promoted. The block
   becomes `DEGRADED` and the decision is **ROLLBACK**, so consumers keep reading the
   previous last-good snapshot. The rows that were still good are quarantined too
   (rule `rollback`) so nothing is lost.
5. If the block function raises (or does not return a DataFrame), the result is
   **ROLLBACK** with reason `crash`.

**On a consumer's input: `resolve_input(block, upstream) -> DataRef`**

| Upstream status | Fallback edge for (block, upstream)? | Reads |
|---|---|---|
| `HEALTHY` | n/a | upstream's latest last-good snapshot |
| `DEGRADED` or `OUT` | yes, and the fallback source is `HEALTHY` with a last-good | the fallback source's last-good, passed through the edge's `adapter` |
| `DEGRADED` or `OUT` | no, or the fallback source is itself `DEGRADED`/`OUT` (or has no snapshot) | upstream's last-good, marked `stale`, with **no** adapter |
| `DEGRADED` or `OUT` | upstream has no last-good either | `NoSafeInputError`; the block is reported `BLOCKED` and the rest of the pipeline continues |
| `HEALTHY` | upstream has never been promoted | `NoSafeInputError` (as above) |

An unhealthy fallback source is never used, even if it has a snapshot: when several
blocks are unhealthy at once, a consumer falls back to stale data from the block it
actually depends on. A source block (no inputs) that is `DEGRADED` or `OUT` needs no
special case: its dependents follow the same table.

Every resolution emits a `RESOLVE` or `REROUTE` event naming the source actually read.

**Recovery**

- `replay(block)` re-runs the block's (fixed) function on its `QUARANTINED` records and
  validates the result. Records whose rows now pass are marked `REPLAYED`; the rest stay
  `QUARANTINED`. Records are never deleted. The result reports `replayed` and
  `still_failing`. Where the recovered rows go depends on the block's `merge_key`:
  - **With a `merge_key`** (for example `merge_key: [order_id]` in the YAML), the
    recovered rows are *upserted* into a copy of the last-good snapshot. On a key
    conflict the replayed row wins, and among replayed rows the newest quarantine record
    wins. The result is written as a new snapshot and promoted to last-good, and the
    block becomes `HEALTHY`.
  - **Without one**, Guardian cannot tell a correction from a duplicate, so it does not
    merge. The recovered rows are written to their own snapshot, last-good is left
    unchanged, and a `WARN` event says the merge was skipped.
- Replay is **idempotent**. It only consumes `QUARANTINED` records, so running it again
  finds nothing to do and changes nothing. A replay that dies after writing its snapshot
  but before marking records can simply be re-run: the keyed upsert produces the same
  rows, not duplicates.
- `set_block_status(block, HEALTHY | OUT)` is the human control. An `OUT` block is
  skipped and its consumers reroute. `DEGRADED` is only ever set by Guardian.

**Events.** Structured JSON events go to `events.jsonl` and a DuckDB table. Routine
events are sampled at `--sample-rate`, while `PROMOTION`, `WARN`, `ERROR`, `ROLLBACK`,
`REROUTE` and `QUARANTINE` are always kept.

## Walkthrough: heavy corruption in one block (scenario 3)

The demo pipeline turns messy e-commerce orders into daily revenue by region and
customer segment, plus a per-customer summary:

```
b1_ingest → b2_parse → b3_standardize → b4_clean → b5_normalize ─┬→ b6_enrich → b8_aggregate
                                                                  │      └─ fallback for b6 ─┘
                                                                  │         (adapter b5_to_b6_shape)
                                                                  └→ b7_customers
```

**This walkthrough uses b6, but nothing in Guardian is specific to b6.** Every command
takes the block name as an argument and works for any block in the spec. What happens to
a failing block's dependents depends only on its role in the DAG: a fallback edge that
replaces it, no fallback, or no dependents at all. The scenario suite runs the same
failures against one block of every role (see [Testing by DAG role](#testing-by-dag-role)),
and [a second example below](#the-same-flow-on-another-block-b2_parse) runs the flow on
`b2_parse`, which has no fallback.

**1. A normal run.** The raw data is deliberately messy: the 57 rows quarantined in b1–b4
fall under their thresholds, so every block passes.

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

**2. b6 goes bad.** We inject a fault that corrupts the `region` and `segment` columns
in 50% of b6's output. That is far above b6's 10% threshold.

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

This single run shows all three repair actions:

- **Rollback.** b6's r2 output is not promoted, and b6's last-good stays at r1.
- **Quarantine.** All 443 rows are kept. The 222 corrupt rows are stored under rule
  `region:isin(['NA', 'EU'])`, and the other 221 under `rollback` because the batch as a
  whole was rejected.
- **Reroute.** b8 reads *this run's* b5 output through `b5_to_b6_shape`, so the revenue
  numbers are current. Only the segment breakdown is lost: segments are `unassigned` on
  that path, which is why b8 has 98 rows instead of 130. b7 does not depend on b6 and is
  unaffected.

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

**3. Status.** b6 is `DEGRADED`, still serving r1, with 443 rows waiting in quarantine.
The `version` and `shadow` columns come into play in [Shadow promotion](#shadow-promotion).
(The counts for b1–b4 add up over both runs.)

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

**4. Fix and replay.** The fault was only for that run, so the "fixed" b6 is simply the
real one. Replay re-derives `region` and `segment` from the intact columns, and all 443
records pass. b6 declares `merge_key: [order_id]`, so the recovered rows are upserted into
b6's last-good snapshot (r1). r1 and r2 contain the same generated orders, so every
recovered row replaces its r1 version, and the new snapshot has 443 rows with unique
`order_id`s, not 886. A second replay has nothing left to do:

```
$ guardian replay b6_enrich
b6_enrich: replayed 443, still failing 0, upserted into new last-good snapshot replay-20260926T055359-f98e54

$ guardian replay b6_enrich
b6_enrich: replayed 0, still failing 0 (nothing to replay)

$ guardian status
│ b6_enrich      │ HEALTHY │ v1      │ -      │ replay-20260926T055359-f98e54 │ 0           │ 443      │
```

b6 is `HEALTHY` again, with a new promoted snapshot. Its 443 records are still in
quarantine, now marked `REPLAYED`.

**5. Refresh b8.** b8's r2 output was computed on the fallback path. Re-running just b8
reads the recovered b6 snapshot, and the result is identical to the clean r1 aggregates:
the same 130 rows with the same values. The scenario suite asserts this under both runners
for the fallback-protected block's descendants
(`test_descendants_after_replay_match_clean_run`).

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

> **Note: replay needs a `merge_key` to merge.** Without one, replay does not append
> (appending would have duplicated all 443 orders here). It writes the recovered rows to
> a separate snapshot, leaves last-good alone, and warns:
>
> ```
> $ guardian replay b6_enrich
> b6_enrich: replayed 443, still failing 0
> WARN: no merge_key declared; replayed rows written to separate snapshot replay-20260926T022401-2c26b9, last-good unchanged
> ```
>
> The matching `WARN` event, which is never sampled out, records the snapshot name and the
> unchanged last-good run. Every demo block declares a `merge_key`.

The same scenario runs under Dagster in `tests/scenarios`. There, the failing block's
`guardian_validation` check fails with `outcome=ROLLBACK`, and its dependents' inputs are
resolved by the IO manager.

### The same flow on another block: b2_parse

b2 has no fallback edge (role `unprotected`), so when it crashes its dependent b3 reads
b2's last-good snapshot, marked `(stale)`, with no adapter. Everything downstream still
runs, on data that is one run old at b2:

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

b2 is `DEGRADED` and nothing was quarantined, because a crash produces no rows. Once b2
is fixed (here: the next run without the fault), it passes, returns to `HEALTHY`, and b3
reads it fresh again. `guardian replay b2_parse` works the same way as for b6 whenever b2
has quarantined rows.

## Shadow promotion

Any block can have several implementations. A new one runs **in shadow** next to the
live one on real data, and is promoted only when it has proven itself. Nothing in Guardian
is specific to a block: every command takes the block name, and scenarios 7a–7g run
against one block of every DAG role, under both runners
([`tests/scenarios/test_shadow.py`](tests/scenarios/test_shadow.py)).

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

A source block also declares `load:` (b1 does): the loader runs once per run and every
version receives the same loaded frame.

**How it works**

- **Candidates run on live inputs.** `guardian shadow start <block> <version>` registers
  a candidate. On every run it is executed on the same resolved live inputs as the
  active version, even when the block is `OUT`, and even if an upstream block has its
  own candidate: candidates never read each other.
- **Consumers never see candidates.** A candidate's output goes to a separate candidate
  store (`.guardian/candidates/<version>/…`) that `resolve_input` never reads.
- **Every run is compared.**
  - *Parity mode* (the live version passed on this run): a row-level diff against the
    live output on `merge_key`, counting added, removed and changed rows and naming the
    changed columns, plus per-column stats (null rate, mean, distinct count).
  - *Absolute mode* (the live version is `DEGRADED` or `OUT`, so there is no trustworthy
    baseline): the candidate's validation pass rate plus stats.
- **Promotion policy.** A candidate auto-promotes only after `required_runs` consecutive
  parity runs within tolerance. Absolute mode, or `shadow start --expect-diff` (an
  intended behaviour change), needs `guardian shadow promote <block> --approve`.
  Approval never promotes a candidate that fails `min_pass_rate` on its latest run.
- **Promoting** makes the new version active in the version registry, which overrides
  the spec's `active`. It also marks the block `HEALTHY`, so its dependents go back to
  their normal edges, and replays its quarantine through the new version (idempotent
  via `merge_key`).
- **Crash-safe.** A promotion is recorded as `PROMOTING` first, and completes only at
  the end. If it is interrupted, the old version stays live, `guardian status` shows
  `PROMOTING`, and running `guardian shadow promote <block>` again resumes it.
- **Reversible.** `guardian shadow rollback <block>` makes the previous version active
  again. Snapshots are immutable, so no data is rewritten.
- **Provenance.** Every snapshot records its version and where each input came from,
  with a quality of `FRESH`, `STALE` or `FALLBACK`. The worst input quality propagates
  downstream.

### Example: b6_enrich

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

The genuine improvement matches the live output exactly (0 changed rows) and
auto-promotes after its third run:

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

The event log has the audit trail. `PROMOTION` events are never sampled out:
`PROMOTE PROMOTING auto v1→v2`, `PROMOTE COMPLETED auto v1→v2`, then
`ROLLBACK COMPLETED v2→v1`.

### The same on another block: an OUT block in absolute mode

With the live version out of service there is no baseline, so the candidate is judged
on its own pass rate, and promotion needs an explicit approval. Promoting brings the
block back to `HEALTHY` and b3 off its stale read:

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

(The 115 records that still fail are b2's genuinely unparseable rows from earlier runs,
such as `"not a date"`; v2 recovers only day-first dates and epoch timestamps, and the
demo data has none of those.)

### In Dagster

The candidate runs inside the same asset materialization as the live version: the asset
body prepares the inputs through core, and `GuardianIOManager.handle_output` calls
`Guardian.complete_block`, which records the live decision and then runs the shadow
comparison. The comparison appears in two places:

- **Materialization metadata** (`shadow_version`, `shadow_mode`,
  `shadow_changed_fraction`, `shadow_changed_columns`, `shadow_pass_rate`, …).
- **A `guardian_shadow` asset check** on every versioned block. It fails with severity
  WARN when the candidate is out of tolerance; a bad candidate never affects live data.

Promotion, rollback and approval are human actions that go through the core or the CLI,
and a promotion completed from the CLI takes effect on the next Dagster run.

## Blast radius

When a block goes bad, the next question is *what did it touch?* Guardian records
provenance for every snapshot it writes, in `.guardian/provenance.duckdb`:

- the block version that produced it;
- for each input, the snapshot actually read (`block@run_id`). That is the upstream's
  own snapshot, a fallback source's snapshot and adapter, or a stale last-good one;
- a quality flag of `FRESH`, `STALE` or `FALLBACK`.

A snapshot's quality is the worst of its inputs (FALLBACK > STALE > FRESH), and each
input's quality includes the quality of the snapshot it read, so degradation propagates
downstream. Every block's outcome per run (PASS, ROLLBACK, SKIPPED, BLOCKED) is recorded
too.

Two commands answer the question for **any block**:

- `guardian impact <block> [--since <run_id>]` lists the block's own degraded runs,
  then every downstream snapshot that read it while it was unhealthy, and transitively
  everything that read those snapshots.
- `guardian lineage <block> <run_id>` prints the upstream provenance tree of one
  snapshot.

Quality is not written into your data by default. A block can opt in with
`annotate_quality: true`, which appends a `_guardian_quality` column to its promoted
output; the demo does this for `b7_customers`. In Dagster, the same information is
attached to each materialization as `guardian_quality`, `guardian_inputs` (a one-line
summary) and `guardian_provenance` (the full record, as JSON).

### Example

Four demo runs: r1 is clean, r2 crashes `b6_enrich`, r3 crashes `b5_normalize`, and r4
is clean again. When b6 is down, b8 falls back to b5 through the adapter, so the damage
is contained:

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

b5 has two dependents and no fallback edge replaces it, so its crash reaches further.
b6 and b7 read the previous b5 snapshot (STALE), and b8 inherits that through b6:

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
`_guardian_quality = STALE`, while its r1 output carries `FRESH`.

**How it is tested.** Scenarios 8a–8f run for every DAG role, under both runners
([`tests/scenarios/test_provenance.py`](tests/scenarios/test_provenance.py)):
- a fallback reader is FALLBACK;
- unprotected dependents are STALE;
- a source fault degrades the whole downstream subgraph;
- when both the fallback source and the block it replaces are down, the reader is STALE
  and gets no adapter;
- after a shadow promotion and replay, the next run is FRESH end to end.

8f is a hypothesis property test. It places random faults (crash, corruption, OUT) over
several runs, then checks `impact` for every block against four independent checks:
- an upward search over the recorded lineage (`impact` itself searches downward);
- the runner's own outcomes;
- the DAG (only descendants are touched, and every dependent that ran is);
- the quality flags.

## Automated diagnosis

Once Guardian has contained a failure, someone still has to find out *why* it happened.
`guardian diagnose <block> [--run <run_id>]` gathers the evidence for any block's run
(by default its latest ROLLBACK) and asks an LLM for a root cause. The answer is
**advisory**. The agent only reads Guardian's stores and writes under
`.guardian/diagnoses/`. It never edits code, merges, or promotes.

### The evidence bundle

`guardian/agent/evidence.py` builds a bundle in which every item has a stable ID (E1, E2,
...). The model must cite these IDs. A bundle contains:

- the run's outcome;
- each failed validation rule with its row count;
- a capped sample of the quarantined rows (20 by default, `--sample-size`);
- the output schema compared with the last good run and with the declared schema;
- per-column stats (nulls, distinct values, min/max/mean or top values) for the run's
  good rows, its bad rows and the last good snapshot;
- whether the block's **code** changed since its last good run. Every block run now
  records a fingerprint of the function that actually ran, and its source is kept in
  `.guardian/code/`, so the bundle can show the diff;
- the git log and diff of the block's source file (resolved from its `fn` path) since
  its last promotion;
- where each input came from on that run, and its quality (from provenance);
- the block's DAG roles, inputs, dependents and fallback edges;
- recent events for the block and its upstreams, taken from the DAG.

The bundle is deterministic: the same stored state gives the same JSON, and later runs
or earlier diagnoses do not change it. It is written to
`.guardian/diagnoses/<block>/<run_id>/evidence.json`, and `--evidence-only` stops there
without calling an LLM. Data is minimized before it can leave the machine:

- samples are capped;
- long text is cut;
- values of the block's `redact_columns` are masked everywhere, including inside
  validation messages and events.

The demo redacts customer emails in `b1_ingest` and `b4_clean`.

### The answer, and how it is checked

The model must return JSON with four fields:

- `root_cause`: one of `upstream_data_drift`, `schema_change`, `code_bug` or
  `unknown`;
- `confidence`, from 0 to 1;
- `summary`;
- `claims`, each citing evidence IDs.

With the Anthropic client this shape is enforced as a JSON-schema structured output.
Guardian still validates every answer:

- **Invalid JSON or a malformed answer** is retried once. If it fails again, the
  diagnosis is downgraded to `unknown` and the reason is recorded.
- **A claim that cites an ID not in the bundle**, or cites nothing, gets the whole
  answer rejected. The diagnosis becomes `unknown`, and the model's answer is kept
  only as `proposed`, for audit.
- **A refusal or an API error** is recorded as the diagnosis; it is never raised
  into the pipeline.

The result goes to `diagnosis.json` next to the evidence, and a DIAGNOSIS event is
logged. DIAGNOSIS events are never sampled out.

### Configuration

The provider, model and key come from the environment, never from code:

| variable | meaning |
|---|---|
| `GUARDIAN_LLM_PROVIDER` | `anthropic`, or `fake` to replay recorded answers |
| `GUARDIAN_LLM_MODEL` | model name passed to the provider |
| `GUARDIAN_LLM_API_KEY_ENV` | name of the variable holding the key (default `ANTHROPIC_API_KEY`) |
| `GUARDIAN_LLM_FAKE_RESPONSES` | recordings file for the `fake` provider |
| `GUARDIAN_LLM_MAX_TOKENS`, `GUARDIAN_LLM_THINKING` | optional (defaults `16000`, adaptive thinking `on`) |

The Anthropic client uses the official SDK, an optional extra: `uv sync --extra agent`.
The `--provider`, `--model` and `--fake-responses` flags override the variables.

**Automatic diagnosis.** Add `auto_diagnose: true` to a block to diagnose it after every
ROLLBACK. This works in both runners: the hook lives in core and is called from
`complete_block`. A diagnosis that fails, for example because no provider is
configured, is logged as a WARN event and the run carries on exactly as it would
without it. Scenarios 9a–9c check this for every DAG role under both runners.

### Example: a bad deploy of b6_enrich

```
$ guardian run demo/pipeline.yaml --run-id r1
$ guardian run demo/pipeline.yaml --run-id r2 --fault b6_enrich:code_bug
$ guardian diagnose b6_enrich --evidence-only
┏━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ id  ┃ kind              ┃ item                                                                     ┃
┡━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ E1  │ run_outcome       │ b6_enrich on run r2: ROLLBACK                                            │
│ E2  │ validation_rule   │ rule order_size:isin(['small', 'medium', 'large']) failed on 222 row(s)  │
│ E3  │ validation_rule   │ rule region:isin(['NA', 'EU']) failed on 222 row(s)                      │
│ E4  │ quarantine_sample │ sample of 20 of 222 quarantined bad row(s)                               │
│ E5  │ schema_diff       │ output schema vs last good run and declared schema                       │
│ E6  │ column_stats      │ column order_id: good rows vs bad rows vs last good snapshot             │
│ ... │                   │                                                                          │
│ E17 │ code_change       │ the block's code on this run vs its last good run                        │
│ E18 │ git_history       │ git history of the block's source since its last promotion               │
│ E19 │ input             │ input b5_normalize: FRESH                                                │
│ E20 │ dag               │ b6_enrich's place in the DAG                                             │
│ E21 │ event             │ QUARANTINE event for b1_ingest on r1                                     │
│ ... │                   │                                                                          │
│ E40 │ event             │ ROLLBACK event for b6_enrich on r2                                       │
└─────┴───────────────────┴──────────────────────────────────────────────────────────────────────────┘
```

Two of those items, as written to `evidence.json` (trimmed):

```json
{"id": "E2", "kind": "validation_rule", "data": {"rule": "order_size:isin(['small', 'medium', 'large'])",
  "rows": 222, "fraction_of_output": 0.5011, "example_reasons": ["column 'region' failed isin(['NA', 'EU'])
  (value='EU '); column 'order_size' failed isin(['small', 'medium', 'large']) (value='large ')", ...]}}
{"id": "E17", "kind": "code_change", "data": {"changed": true, "last_good_run": "r1",
  "last_good": {"function": "guardian.demo.blocks:enrich", "fingerprint": "cbadb335608d8be1", ...},
  "this_run": {"function": "guardian.demo.refactor:rewrite.<locals>.block", ...},
  "diff": ["--- last good (guardian.demo.blocks:enrich)", "+++ this run (...)",
           "-    out[\"region\"] = out[\"country\"].map(REGION_BY_COUNTRY)", ...,
           "+                out.iloc[selected, j] = values.iloc[selected].map(",
           "+                    lambda v: v + \" \" if isinstance(v, str) else v", ...]}}
```

With a provider configured, `guardian diagnose b6_enrich` prints the answer below. This
particular answer is **not** model output. No API key was available where this README
was written, so it is a hand-written recording, replayed with `--provider fake`, that
shows the format. Replace it with a real run once one exists.

```
$ guardian diagnose b6_enrich
Diagnosis of b6_enrich on r2 (advisory, model illustration)
root cause: code_bug   confidence: 0.85   status: accepted
b6_enrich's code changed between r1 and r2; half its rows now carry padded category labels while its
input and output schema are unchanged.
┏━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ # ┃ claim                                                                           ┃ evidence   ┃
┡━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 1 │ 222 of 443 rows fail region and order_size isin checks, with values like 'EU '  │ E1, E2, E3 │
│   │ and 'large '.                                                                   │            │
│ 2 │ The output has the same columns as the last good run.                           │ E5         │
│ 3 │ The function that ran differs from the one that produced r1.                    │ E17        │
│ 4 │ The input was a fresh, healthy b5_normalize snapshot.                           │ E19        │
└───┴─────────────────────────────────────────────────────────────────────────────────┴────────────┘
Evidence: .guardian/diagnoses/b6_enrich/r2/evidence.json
Diagnosis: .guardian/diagnoses/b6_enrich/r2/diagnosis.json
```

### Evaluation

`guardian eval diagnose` builds one labeled case for **every block × fault type**. It
runs the pipeline once cleanly, then, for each case, copies that state, injects the
fault into the block, re-runs it and builds the bundle:

| fault | what it does | label |
|---|---|---|
| `schema_drift` | renames a required column of the block's output | `schema_change` |
| `corrupt_rows` | writes invalid values into half the rows | `upstream_data_drift` |
| `null_burst` | nulls a non-nullable column in half the rows | `upstream_data_drift` |
| `code_bug` | swaps in a buggy rewrite of the block's function (new code, same input) | `code_bug` |

Columns are chosen from each block's clean output and declared schema, never by name.
The report gives:

- accuracy overall, by DAG role and by fault type;
- a confusion matrix;
- the rate of answers rejected for bad citations.

It is written to `bench/agent_eval.md`, with a `.json` next to it. `--record FILE`
saves the model's answers, so a real run can later be replayed offline with
`--provider fake --fake-responses FILE`.

In pytest the eval only ever runs against `FakeClient`, with no network. The tests
check two things:

- every case really fails, and its bundle carries the signal that separates its label
  (a schema diff, a code change, or neither). A correct diagnosis is possible from the
  evidence alone, and the fault's name never appears in it;
- the metrics, confusion matrix and rejection rate match an independent tally of
  scripted answers: correct and wrong causes, bad citations, invalid JSON.

**Real-model results: not yet recorded.** The table is produced by:

```
GUARDIAN_LLM_PROVIDER=anthropic GUARDIAN_LLM_MODEL=<model> ANTHROPIC_API_KEY=... \
  uv run guardian eval diagnose --record bench/agent_eval_answers.json
```

No API key was available in the environment that built this phase, so no numbers are
reported here rather than invented ones. Paste the summary table from
`bench/agent_eval.md` here after the first run.

## Design decisions

### Why the core is orchestrator-agnostic

The value of Guardian is in its repair semantics: when to promote, what to quarantine,
which snapshot a consumer reads. Those semantics should not depend on who schedules the
blocks. `guardian.core` is plain Python over pandas, Pandera, Parquet and DuckDB. It
never imports dagster, and a layering test (`tests/unit/test_layering.py`) enforces
that. Each adapter only maps its orchestrator's hooks onto the `Guardian` facade:

- The standalone executor calls `resolve_input`, `compute` and `handle_result` in
  topological order.
- The Dagster IO manager calls the same methods from `load_input` and `handle_output`.

This means a third orchestrator (Airflow, Prefect) needs a thin adapter, not a
reimplementation. It also makes the contract testable once for all modes: the scenario
suite is parametrized over a runner fixture, and every scenario, including the
no-silent-loss check, runs unchanged against both runners. When the Dagster adapter
needed crash handling, that logic went into core (`Guardian.compute` returns a
`BlockCrash` value), and the standalone runner now uses it too.

### Why rerouting happens at the data level in Dagster

Dagster's graph is static: the edge b6 → b8 exists whether b6 is healthy or not, and a
failed step makes Dagster skip everything downstream. Rerouting by rewriting the graph
(conditional branches, dynamic outputs, alternate jobs) would mean a different Dagster
job per failure combination. The degraded path would also look different from the
healthy one in the UI and in lineage.

Instead, Guardian leaves the asset graph as declared and reroutes where the data is read.
`GuardianIOManager.load_input` asks core which snapshot b8 should actually read: b6's
last-good, b5's snapshot through the adapter, or a stale copy. A failing block does not
fail its step. `Guardian.compute` catches the crash, the IO manager records the
rollback, and the step completes so that downstream assets still run. Fallback sources are
declared as extra `deps`, so they are always materialized before the consumer that might
need them.

The cost is that Dagster records a successful materialization for a block that Guardian
rolled back or skipped. The truth is carried by the `guardian_validation` asset check
(failed, severity ERROR, with outcome, reason, row counts and threshold) and by
`guardian_action` output metadata. This is a deliberate trade: the pipeline keeps flowing,
and the UI still shows exactly which block is degraded and why.

### The no-silent-loss invariant (exactly once)

> Every row handed to `on_output` ends up in exactly one place: the promoted snapshot for
> that run, or quarantine. After a replay, a `REPLAYED` row is held by the replay
> snapshot its record points at. So at any moment each input row is held exactly once,
> across promoted snapshots plus un-replayed quarantine.

Guardian can only be trusted to act automatically if it never makes data disappear. Two
design choices follow from the invariant:

- **Rejected batches quarantine every row.** On a rollback, the rows that were still
  valid are also quarantined (rule `rollback`). Otherwise they would be in neither place.
  Replay brings them back.
- **Quarantine is append-only.** Replay changes a record's status from `QUARANTINED` to
  `REPLAYED` and never deletes it, so every recovery can be audited.
- **Replay merges by key or not at all.** Appending recovered rows to a snapshot that
  may already hold an earlier version of the same row would satisfy "no loss" but break
  "exactly once". Upserting by `merge_key` satisfies both, and it also makes replay
  idempotent.

The invariant is tested at three levels:

- **Property tests** (`tests/unit/test_invariant.py`, hypothesis) generate random frames
  with random bad rows, NaNs, text that can't be converted, repeated index labels and
  missing columns, across thresholds.
  - The first test checks that the promoted plus quarantined row ids exactly equal the
    input ids.
  - The second then fixes the block and replays twice, with and without a `merge_key`,
    and with and without an earlier run to merge into. After every step, each input id
    of each run must be held exactly once. The second replay must not write anything.
- **Per-block check in every scenario:** for each block, rows handed over = promoted +
  quarantined. This is read back from the stored events, snapshots and quarantine, so it
  does not trust the runner's own report. A second check, by `merge_key`, confirms that
  no row is held twice, including rows that were replayed.
- **Chain checks in every scenario:** for row-wise blocks, the rows a block received
  equal the rows of the snapshot it read. End to end, every ingested row is either in
  the final snapshot or quarantined somewhere upstream.

### Testing by DAG role

Guardian's code never names a block: every behaviour comes from the spec. The tests follow
the same rule. `tests/helpers/roles.py` classifies every block of a spec by its role in
the DAG:

| Role | Meaning | Demo block used |
|---|---|---|
| `source` | no inputs | b1_ingest |
| `leaf` | no dependents | b7_customers (b8 also qualifies) |
| `fallback_protected` | a dependent has a fallback edge replacing it | b6_enrich |
| `unprotected` | has dependents, none with a fallback edge replacing it | b2_parse (b1–b5 qualify) |
| `fallback_source` | is the source of some fallback edge | b5_normalize |
| `multi_dependent` | two or more dependents | b5_normalize |

`representative(spec, role)` picks the block with the fewest other roles. The minor
corruption, heavy failure (corruption and schema drift), crash, taken-OUT and replay
scenarios each run once per role, under both runners. Expectations are derived from the
spec, not written per block:

- **All roles:** every other block passes on this run's data.
- **Dependents:** a dependent with a healthy fallback edge reads through the adapter;
  any other dependent reads `STALE` data.
- **Leaf:** nothing reroutes.
- **Replay:** faults corrupt only the columns a block derives itself. Replay recovers
  every row the fixed block can re-derive, recovers the rolled-back batch's collateral
  rows for every role, and leaves the rest quarantined.

The multi-failure rules (the fallback source is unhealthy too; no last-good anywhere)
have their own scenarios. The demo spec contains every role;
`tests/unit/test_roles.py` checks that.

## Results

Demo pipeline at 100,000 generated rows, 5 runs per configuration, measured before
`b7_customers` was added to the demo (so the pipeline had 7 blocks), on a 4-core Intel Xeon
@ 2.10 GHz cloud container (Python 3.11, pandas 3.0, DuckDB 1.5). The figures are medians
with the min to max range. Full details, machine specs and raw samples are in
[`bench/results.md`](bench/results.md); reproduce with `uv run python bench/run_bench.py`
(`--rows`, `--reps`).

| Metric | Setup | Result |
|---|---|---|
| Throughput during outage | Rows/s through the whole pipeline while b6 crashes on every run and b8 is rerouted to b5, vs. a clean run. The same 87,517 rows reach b8 in both. | **23,362 rows/s** during the outage (22,322 to 23,648) vs. **22,358 rows/s** clean (22,150 to 22,620): **104%** of the clean run. There is no throughput penalty; b6's own work is skipped. |
| Recovery time | Time for `replay b6_enrich` after a heavy-corruption outage (87,517 quarantined rows), until b6 is HEALTHY with the upserted snapshot promoted. | **2.55 s** in-process (2.47 to 2.60). **3.72 s** via the CLI (3.68 to 4.36), which includes starting Python and importing pandas, Pandera, pyarrow and DuckDB. |
| Logging overhead | The same blocks on clean data, with Guardian at event sample rate 1.0 and 0.1, vs. a bare run without Guardian. | Bare **2.67 s**. Guardian **3.41 s** at 1.0 and **3.41 s** at 0.1, so **+28%** in total (validation, Parquet snapshots, bookkeeping). The logging share (1.0 vs. 0.1) is **+1 ms**, below the ±73 ms run-to-run spread. A run emits only 36 events, per block and decision rather than per row. |

**Reading the numbers**
- **Outage throughput:** reading b5 through the adapter costs no more than running b6, so
  the pipeline delivers at full speed during the outage. What degrades is data richness
  (segments are `unassigned`), not throughput.
- **Recovery:** replaying about 88k rows is a few seconds, dominated by re-running b6 and
  validating the recovered rows.
- **Overhead:** Guardian's cost comes from validation and snapshots, not from logging.
  At this event volume the sample rate hardly matters; it would start to matter only with
  far more blocks or with per-row events.

## Project layout

```
guardian/
  core/        models, validation, snapshots, quarantine, events, planner, versions,
               code (fingerprints), dag (roles),
               shadow, provenance, guardian facade
  runner/      spec_loader, executor, cli
  adapters/dagster/  io_manager, checks, replay, definitions
  agent/       evidence bundles, LLM diagnosis (Anthropic + fake clients), eval
  demo/        data_gen, schemas, blocks, faults, refactor (code_bug), pipeline.yaml
bench/
  run_bench.py throughput / recovery / overhead benchmarks -> bench/results.md
  agent_eval.md  written by `guardian eval diagnose` (real model; manual)
tests/
  helpers/     blocks_by_role (DAG roles) and block profiles, shared by the tests
  unit/        per-module core tests, invariant property test, layering test, role helper
  runner/      spec loader, executor, CLI
  demo/        demo blocks and fault injection
  adapters/    Dagster adapter wiring
  agent/       evidence, diagnosis validation and clients, eval (FakeClient only)
  scenarios/   fault-injection suite, parametrized over DAG roles and both runners
  bench/       smoke test for the benchmark script
```
