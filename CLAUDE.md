# Guardian: Self-Healing Maintenance Layer for Data Pipelines

## What this project is
A maintenance layer that attaches to an existing ETL pipeline. It snapshots every
block's output, validates it, and on failure does three things: it rolls downstream
consumers back to the block's last-good snapshot, quarantines the failing records,
and reroutes dependents through declared fallback edges so the pipeline keeps flowing.
Quarantined records are replayed once the block is fixed.

It ships in two modes that share ONE core:
1. **Standalone**: a minimal DAG runner driven by a YAML pipeline spec.
2. **Dagster plugin**: a custom IO manager + asset checks + a replay job.

The core must never import dagster. Adapters depend on core, never the reverse.

## Repo layout
```
guardian/
  core/
    models.py        # BlockSpec, PipelineSpec, FallbackEdge, Decision, DataRef, RunContext
    validation.py    # Validator protocol + Pandera-based implementation
    snapshots.py     # SnapshotStore protocol + LocalParquetSnapshotStore
    quarantine.py    # QuarantineStore protocol + DuckDBQuarantineStore
    events.py        # structured JSON event logger (sampled "short logging")
    planner.py       # reroute planning: which source a block reads from right now
    guardian.py      # Guardian facade: on_output, resolve_input, replay, set_block_status
  runner/
    spec_loader.py   # YAML -> PipelineSpec, validates DAG (no cycles, fallbacks exist)
    executor.py      # topological executor calling Guardian hooks
    cli.py           # `guardian run spec.yaml`, `guardian replay <block>`, `guardian status`
  adapters/
    dagster/
      io_manager.py  # GuardianIOManager: handle_output -> on_output, load_input -> resolve_input
      checks.py      # asset checks surfacing validation results
      replay.py      # replay job/op
      definitions.py # example Definitions wiring the demo pipeline
  demo/
    data_gen.py      # messy synthetic dataset generator (behind a DatasetLoader interface)
    blocks.py        # demo block functions B1..B6 (pure functions: DataFrame -> DataFrame)
    pipeline.yaml    # demo spec with at least one fallback edge
    faults.py        # fault injection: corrupt rows, schema drift, block crash, null bursts
tests/
  unit/              # per-module tests for core
  scenarios/         # shared fault-injection suite, parametrized over BOTH runners
```

## Core semantics (the contract every adapter must honor)
- Every block output is written as an immutable snapshot keyed by (block, run_id).
- `on_output(block, run_id, df) -> Decision`:
  - Validate df against the block's schema/rules.
  - Split into good rows and bad rows. Bad rows go to quarantine with
    block, run_id, rule_name, reason, timestamp, and the original row as JSON.
  - If the bad-row fraction is at or below the block's `quarantine_threshold`, snapshot the
    good rows, mark that snapshot last-good, and return PASS.
  - If it exceeds the threshold, or validation raises a schema-level failure, do NOT
    promote. Mark the block DEGRADED and return ROLLBACK. Downstream consumers read
    the previous last-good snapshot.
  - If the block crashes (exception), treat it as ROLLBACK with reason "crash".
- `resolve_input(block, upstream) -> DataRef`:
  - If upstream is HEALTHY: its latest last-good snapshot.
  - If upstream is DEGRADED or OUT (manually taken out for optimization):
    - If a fallback edge exists for (block, upstream), read the fallback source's
      last-good snapshot and apply the edge's declared `adapter` transform.
    - Otherwise read upstream's last-good snapshot (stale but safe).
    - If no last-good snapshot exists anywhere, raise NoSafeInputError.
  - Every resolution emits an event recording which source was actually used.
- `replay(block) -> ReplayResult`: re-run the block's function on its quarantined
  records, validate the result, merge passing rows into a new snapshot, and mark the replayed
  records as REPLAYED (never delete them). Return counts: replayed, still_failing.
- `set_block_status(block, HEALTHY | OUT)` lets a human take a block out for optimization.
- Invariant, tested everywhere: no input record is ever silently lost. Every input row
  ends up in exactly one of: a promoted snapshot, or quarantine.

## Pipeline spec (YAML)
```yaml
name: demo
blocks:
  - name: b1_ingest
    fn: demo.blocks:ingest
    schema: demo.schemas:RawSchema
    quarantine_threshold: 0.2
  - name: b6_enrich
    fn: demo.blocks:enrich
    inputs: [b4_clean]
  - name: b8_aggregate
    fn: demo.blocks:aggregate
    inputs: [b6_enrich]
    fallbacks:
      - replaces: b6_enrich
        source: b5_normalize
        adapter: demo.blocks:b5_to_b6_shape
```

## Stack and conventions
- Python 3.11+, managed with `uv`. pandas for frames, Pandera for validation,
  pyarrow for Parquet, duckdb for quarantine and event storage, PyYAML, typer for the CLI.
- Dagster is an OPTIONAL extra (`uv pip install -e ".[dagster]"`); core tests must pass without it.
- Dev machine is Windows 11: use pathlib everywhere, no shell-specific commands in code,
  and no hardcoded forward-slash paths.
- Type hints everywhere, dataclasses or pydantic for models, Protocols for storage backends.
- Logging: structured JSON events via core/events.py. The sample rate is configurable; errors,
  rollbacks, reroutes, and quarantines are ALWAYS logged (never sampled out).
- Storage root defaults to `./.guardian/` and is configurable.
- pytest; keep tests fast (small frames). `ruff` for lint/format.

## Definition of done for today
1. `uv run pytest` is green, including the scenario suite against the standalone runner.
2. `uv run guardian run demo/pipeline.yaml` executes the demo end to end.
3. Scenario suite also passes against the Dagster adapter (skipped cleanly if dagster is not installed).
4. README has a quickstart for both modes and a failure-scenario walkthrough.

## Working rules for Claude Code
- Work phase by phase; do not start the next phase until the current one's tests pass.
- Before writing code in a new phase, state the plan briefly, then implement.
- Never weaken a test to make it pass; fix the code or flag the ambiguity.
- Keep adapters thin. If you find repair logic creeping into an adapter, move it to core.
- Commit at the end of every phase with a descriptive message.

## Block-agnostic rules (apply to all code, v1 and v2)
- No block name may appear in guardian/core, guardian/runner, or guardian/adapters. All
  behavior is driven by the pipeline spec. Block names appear only in demo/ and tests.
- Every CLI command that acts on a block takes the block name as an argument and works for
  any block in the spec, with a clear error for unknown names.
- Tests select blocks by DAG role using a shared helper, `blocks_by_role(spec)`, which returns:
  - source: no inputs;
  - leaf: no dependents;
  - fallback_protected: at least one dependent has a fallback edge replacing it;
  - unprotected: has dependents, none with a fallback edge replacing it;
  - fallback_source: is the `source` of some fallback edge;
  - multi_dependent: two or more dependents.
  Behavior tests are parametrized over every role present in the demo spec.
- The demo spec must contain at least one block for each role above.

## Resolution when several blocks are unhealthy at once
- A fallback edge is used only if its fallback source is HEALTHY. If the fallback source is
  also DEGRADED/OUT, the consumer reads the replaced upstream's last-good snapshot (STALE)
  without applying the adapter. If neither has a last-good snapshot: NoSafeInputError.
- A source block has no inputs to resolve; when it is DEGRADED/OUT, its dependents follow
  the normal rules (fallback edge if one exists and is healthy, else stale).

## v2 invariants (apply to every phase from 7 on)
- Consumers NEVER read candidate (shadow) snapshots. Only promoted snapshots are visible
  to resolve_input.
- A candidate always reads the LIVE (promoted) inputs, even if an upstream block also has a
  candidate in shadow. Several blocks may be in shadow at the same time, independently.
- Promotion is reversible: `guardian shadow rollback <block>` restores the previous active
  version. Snapshots stay immutable, so no data is rewritten.
- Every snapshot records provenance: which source each input came from and its quality
  (FRESH / STALE / FALLBACK). Quality propagates downstream as the worst of a block's inputs.
- The diagnosis agent is advisory. It never edits code on the default branch, never merges,
  and never promotes. Every change it proposes goes through a PR plus a passing shadow run.
- Every agent claim must cite items from its evidence bundle by ID. Claims citing
  nonexistent evidence are rejected and the diagnosis is downgraded to `unknown`.
- Data sent to an LLM is minimized: capped samples, and columns listed in `redact_columns`
  are masked before leaving the machine.
- The LLM provider, model name, and API key come from config/env vars. Never hardcode them.
- All LLM-dependent tests use a fake client with recorded responses; no network in pytest.
