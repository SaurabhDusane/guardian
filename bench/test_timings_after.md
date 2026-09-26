# Test suite timings: after

Same machine and extras as [the baseline](test_timings_before.md) (4 CPUs,
dagster + agent + observability). `-n auto` uses pytest-xdist with 4 workers.

## Before / after

| suite | command | tests | wall clock |
|---|---|---|---|
| before: everything, serial (then the default) | `uv run pytest` | 788 | 41m3.898s |
| **default**, serial | `uv run pytest` | 459 | **4m47.824s** |
| **default**, parallel | `uv run pytest -n auto` | 459 | **1m43.896s** |
| full, serial | `uv run pytest -m "not llm and not docker"` | 798 | 30m17.207s |
| full, parallel | `uv run pytest -m "not llm and not docker" -n auto` | 798 | 10m51.176s |

Results: default serial `459 passed, 342 deselected in 286.62s (0:04:46)`; default parallel `459 passed in 103.47s (0:01:43)`; full serial `798 passed, 3 deselected in 1815.54s (0:30:15)`; full parallel `798 passed in 650.81s (0:10:50)`.

The full suite has 13 more tests than before, not fewer: quick default variants of
the three property tests, the per-role smoke scenario, and the opt-in `llm` and
`docker` tests (3 tests, deselected in both suites). No test or assertion was removed.

## What changed

1. **Markers.** `slow`, `dagster`, `llm` and `docker`. The default run is
   `-m 'not slow and not llm and not docker'`.
   - Every Dagster runner and adapter test is `dagster` + `slow`.
   - Role- and block-parametrized tests keep one representative (role
     `fallback_protected`, or its block) in the default run; the other roles and
     blocks are `slow`.
   - `tests/scenarios/test_smoke.py` runs a contain → replay → recover scenario for
     **every** role under the standalone runner, so the default run still covers every
     DAG role.
   - The Hypothesis property tests (no silent loss, exactly-once replay, 8f impact)
     each have a default variant with fewer examples; the full example counts are `slow`.
   - The full eval matrix (every block × fault type, fix success) and the benchmark
     smoke run are `slow`.
2. **One DuckDB connection per store per unit of work.** Stores used to open and
   close their file for every operation. Now a store keeps a connection open inside
   `Guardian.session()`, which is lazy, nested and released at the end. Sessions wrap
   a pipeline run, `complete_block` (which covers the Dagster IO manager), `on_output`,
   `replay`, `promote` and `rollback_version`. Outside a session nothing holds the
   files, as before. A standalone pipeline run went from 1.42s to 0.52s.
3. **Shared read-only state.**
   - `scenario_spec()` is cached per session: it is an immutable spec, and every test
     still gets its own `tmp_path` storage root.
   - The scenario runner's reads share one connection per store.
   - One ephemeral Dagster instance serves the session (per xdist worker). This is
     safe because the adapter keeps all state in Guardian's stores and never reads the
     instance's records; the instance only accumulates run history.
4. **pytest-xdist.** Every test already used its own storage root. The one real
   xdist problem was output formatting: rich fixes the CLI console's width from
   COLUMNS at import, and xdist workers have no terminal, so tables were 80 columns
   wide and truncated cells that CLI tests assert on. A session fixture pins the
   width. The git-based agent tests get one throwaway project copy per worker.

**Data size was not shrunk**, because it doesn't matter here. A standalone
pipeline run takes the same time at 200, 100 or 60 rows per source (0.52s, 0.55s,
0.55s): per-block fixed costs (DuckDB statements, pandera schema setup, parquet I/O)
dominate, not data volume. Smaller data would have saved nothing and risked the
threshold semantics the scenarios depend on (fractions of rows).

## What dominates now

- **Full suite:** the Dagster runner (149 tests, 923s of 1813s summed,
  51%). `execute_in_process` costs about 1.9s per pipeline run against
  about 0.5s for the standalone executor. Almost all of it is Dagster's own step
  orchestration and event log, not Guardian. The only way to lower it further is
  to run fewer Dagster executions, which would cut coverage, so it stays.
- **Default suite:** per-test pipeline runs (0.5s each). The slowest default tests are
  the multi-run scenarios (self-healing, the representative-role shadow and
  provenance scenarios).

## Default suite: time by test file (serial)

| file | tests | seconds |
|---|---|---|
| tests.runner.test_cli | 28 | 43.0 |
| tests.scenarios.test_shadow | 8 | 35.6 |
| tests.agent.test_propose | 13 | 31.1 |
| tests.scenarios.test_scenarios | 13 | 26.3 |
| tests.agent.test_evidence | 14 | 22.2 |
| tests.scenarios.test_smoke | 6 | 20.5 |
| tests.scenarios.test_provenance | 7 | 18.5 |
| tests.agent.test_safety | 48 | 18.2 |
| tests.unit.test_guardian | 38 | 9.1 |
| tests.scenarios.test_drift | 3 | 8.9 |
| tests.scenarios.test_self_healing | 1 | 8.4 |
| tests.unit.test_promotion | 17 | 8.1 |
| tests.scenarios.test_diagnosis | 3 | 4.6 |
| tests.unit.test_invariant | 2 | 3.7 |
| tests.unit.test_provenance | 11 | 3.7 |
| tests.agent.test_diagnose | 22 | 3.1 |
| tests.demo.test_demo | 7 | 2.9 |
| tests.agent.test_eval | 1 | 2.6 |
| tests.observability.test_openlineage | 3 | 2.3 |
| tests.observability.test_otel | 2 | 2.2 |

## Default suite: the 30 slowest tests (serial)

| seconds | phase | test |
|---|---|---|
| 8.37 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[fallback_protected]` |
| 7.16 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation_quick[standalone]` |
| 6.09 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-fallback_protected]` |
| 5.51 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[standalone-during_replay-fallback_protected]` |
| 5.33 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[standalone-after_replay-fallback_protected]` |
| 4.18 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[standalone-fallback_protected]` |
| 4.14 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[standalone-fallback_protected]` |
| 4.14 | call | `tests/scenarios/test_scenarios.py::test_descendants_after_replay_match_clean_run[standalone-after_clean_run]` |
| 4.02 | call | `tests/runner/test_cli.py::test_eval_fix_command` |
| 3.85 | call | `tests/scenarios/test_shadow.py::test_7c_bad_candidate_is_never_promoted[standalone-fallback_protected]` |
| 3.84 | call | `tests/agent/test_propose.py::test_rejected_plans` |
| 3.80 | call | `tests/scenarios/test_scenarios.py::test_replay_after_fix[standalone-fallback_protected]` |
| 3.78 | call | `tests/scenarios/test_provenance.py::test_8e_after_promotion_and_replay_next_run_is_fresh[standalone-fallback_protected]` |
| 3.77 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[unprotected]` |
| 3.75 | call | `tests/scenarios/test_drift.py::test_11b_drift_detection_catches_it[standalone-fallback_protected]` |
| 3.75 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[fallback_protected]` |
| 3.73 | call | `tests/runner/test_cli.py::test_propose_command` |
| 3.73 | call | `tests/scenarios/test_scenarios.py::test_descendants_after_replay_match_clean_run[standalone-first_run]` |
| 3.65 | call | `tests/agent/test_propose.py::test_open_pr_pushes_only_an_agent_branch` |
| 3.37 | call | `tests/scenarios/test_shadow.py::test_7b_healthy_parity_shadow_auto_promotes[standalone-fallback_protected]` |
| 3.31 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[fallback_source]` |
| 3.28 | call | `tests/agent/test_propose.py::test_a_fix_that_breaks_the_block_is_recorded_as_failed` |
| 3.25 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[source]` |
| 3.21 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[multi_dependent]` |
| 3.21 | call | `tests/agent/test_propose.py::test_code_bug_proposal_is_a_verified_new_version[fallback_protected]` |
| 3.19 | call | `tests/scenarios/test_smoke.py::test_smoke_contain_replay_recover[leaf]` |
| 3.16 | call | `tests/agent/test_propose.py::test_schema_change_gets_a_new_schema` |
| 3.16 | call | `tests/agent/test_propose.py::test_with_proposed_version_loads_the_fix_as_if_merged` |
| 3.15 | call | `tests/agent/test_propose.py::test_a_failed_fix_opens_no_pr` |
| 3.10 | call | `tests/agent/test_propose.py::test_schema_change_can_get_a_new_fallback_adapter` |

## Full suite: time by test file (serial)

| file | tests | seconds |
|---|---|---|
| tests.scenarios.test_shadow | 96 | 642.4 |
| tests.scenarios.test_scenarios | 86 | 270.4 |
| tests.scenarios.test_provenance | 36 | 211.9 |
| tests.scenarios.test_drift | 36 | 179.1 |
| tests.agent.test_evidence | 54 | 93.1 |
| tests.runner.test_cli | 49 | 80.5 |
| tests.scenarios.test_diagnosis | 26 | 68.0 |
| tests.agent.test_propose | 25 | 49.3 |
| tests.scenarios.test_self_healing | 6 | 46.5 |
| tests.agent.test_eval | 6 | 35.9 |
| tests.unit.test_invariant | 4 | 27.2 |
| tests.scenarios.test_smoke | 6 | 22.5 |
| tests.agent.test_safety | 48 | 17.3 |
| tests.unit.test_guardian | 38 | 10.3 |
| tests.bench.test_run_bench | 1 | 8.7 |
| tests.unit.test_promotion | 17 | 8.5 |
| tests.observability.test_openlineage | 5 | 8.2 |
| tests.adapters.test_dagster_adapter | 4 | 6.7 |
| tests.observability.test_otel | 3 | 5.4 |
| tests.unit.test_provenance | 11 | 4.0 |

## Full suite: the 40 slowest tests (serial)

| seconds | phase | test |
|---|---|---|
| 45.42 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation[dagster]` |
| 22.05 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation[standalone]` |
| 19.47 | call | `tests/agent/test_eval.py::test_fix_success_rate_by_role` |
| 15.14 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation_quick[dagster]` |
| 13.78 | setup | `tests/agent/test_eval.py::test_a_case_for_every_block_and_fault_type` |
| 13.12 | call | `tests/unit/test_invariant.py::test_exactly_once_through_replay` |
| 12.66 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-unprotected]` |
| 12.16 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-fallback_protected]` |
| 12.11 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-fallback_source]` |
| 12.09 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-leaf]` |
| 11.86 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-source]` |
| 11.50 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-multi_dependent]` |
| 9.54 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-fallback_protected]` |
| 9.49 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-multi_dependent]` |
| 9.42 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-unprotected]` |
| 9.38 | call | `tests/unit/test_invariant.py::test_no_silent_loss` |
| 9.37 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-unprotected]` |
| 9.32 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-source]` |
| 9.24 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[unprotected]` |
| 9.15 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-fallback_protected]` |
| 9.15 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-leaf]` |
| 9.13 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-multi_dependent]` |
| 9.11 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-fallback_source]` |
| 8.98 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-fallback_protected]` |
| 8.91 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-fallback_source]` |
| 8.88 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[fallback_protected]` |
| 8.88 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-source]` |
| 8.73 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-multi_dependent]` |
| 8.68 | call | `tests/bench/test_run_bench.py::test_run_bench_smoke` |
| 8.65 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-fallback_source]` |
| 8.53 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-multi_dependent]` |
| 8.51 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-leaf]` |
| 8.43 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-leaf]` |
| 8.41 | call | `tests/scenarios/test_shadow.py::test_7c_bad_candidate_is_never_promoted[dagster-unprotected]` |
| 8.38 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-source]` |
| 8.34 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-leaf]` |
| 8.33 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-source]` |
| 8.31 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-unprotected]` |
| 8.30 | call | `tests/scenarios/test_shadow.py::test_7c_bad_candidate_is_never_promoted[dagster-source]` |
| 8.30 | call | `tests/scenarios/test_shadow.py::test_7c_bad_candidate_is_never_promoted[dagster-leaf]` |
