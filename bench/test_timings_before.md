# Test suite timings: before

Baseline before the Phase 13 speed-up: the whole suite in one serial `uv run pytest`
(the default run at the time: no markers, no xdist) on the development container
(4 CPUs), with the dagster, agent and observability extras installed.

Command: `uv run pytest -p no:cacheprovider --durations=40 --junitxml=...`

## Suite

| metric | value |
|---|---|
| result | 788 passed in 2458.41s (0:40:58) |
| wall clock | 41m3.898s |
| sum of test durations | 2436s |
| Dagster runner / adapter tests | 148 tests, 1006s (41%) |
| everything else | 640 tests, 1430s |

## Time by test file

| file | tests | seconds |
|---|---|---|
| tests.scenarios.test_shadow | 96 | 855.1 |
| tests.scenarios.test_scenarios | 86 | 359.5 |
| tests.scenarios.test_provenance | 34 | 254.8 |
| tests.scenarios.test_drift | 36 | 242.7 |
| tests.agent.test_evidence | 54 | 169.5 |
| tests.runner.test_cli | 49 | 132.3 |
| tests.scenarios.test_diagnosis | 26 | 92.8 |
| tests.scenarios.test_self_healing | 6 | 66.3 |
| tests.agent.test_propose | 25 | 66.2 |
| tests.agent.test_eval | 6 | 38.2 |
| tests.agent.test_safety | 48 | 34.3 |
| tests.unit.test_invariant | 2 | 22.7 |
| tests.unit.test_promotion | 17 | 16.5 |
| tests.bench.test_run_bench | 1 | 16.2 |
| tests.unit.test_guardian | 38 | 11.2 |
| tests.observability.test_openlineage | 5 | 10.9 |
| tests.adapters.test_dagster_adapter | 4 | 8.5 |
| tests.observability.test_otel | 3 | 7.9 |
| tests.agent.test_diagnose | 22 | 5.8 |
| tests.unit.test_provenance | 11 | 5.4 |

## The 40 slowest tests

| seconds | phase | test |
|---|---|---|
| 49.98 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation[dagster]` |
| 38.88 | call | `tests/scenarios/test_provenance.py::test_8f_impact_matches_an_independent_computation[standalone]` |
| 19.40 | call | `tests/agent/test_eval.py::test_fix_success_rate_by_role` |
| 16.24 | call | `tests/bench/test_run_bench.py::test_run_bench_smoke` |
| 15.07 | setup | `tests/agent/test_eval.py::test_a_case_for_every_block_and_fault_type` |
| 14.24 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-fallback_source]` |
| 13.77 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-unprotected]` |
| 13.58 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-fallback_protected]` |
| 13.54 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-source]` |
| 13.48 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-multi_dependent]` |
| 12.91 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[dagster-leaf]` |
| 12.86 | call | `tests/unit/test_invariant.py::test_exactly_once_through_replay` |
| 12.40 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[fallback_protected]` |
| 12.13 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-unprotected]` |
| 12.12 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[unprotected]` |
| 11.67 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-fallback_protected]` |
| 11.35 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-fallback_source]` |
| 11.23 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-multi_dependent]` |
| 11.14 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-source]` |
| 11.04 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[source]` |
| 10.90 | call | `tests/scenarios/test_shadow.py::test_7a_out_block_absolute_shadow_approved_promotion[standalone-leaf]` |
| 10.68 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-unprotected]` |
| 10.60 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-fallback_protected]` |
| 10.40 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[fallback_source]` |
| 10.38 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-fallback_source]` |
| 10.31 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-unprotected]` |
| 10.27 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-fallback_source]` |
| 10.16 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[multi_dependent]` |
| 10.16 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-source]` |
| 10.15 | call | `tests/scenarios/test_self_healing.py::test_10_self_healing_loop[leaf]` |
| 10.07 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-leaf]` |
| 9.97 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-fallback_protected]` |
| 9.96 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-leaf]` |
| 9.90 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-multi_dependent]` |
| 9.84 | call | `tests/scenarios/test_shadow.py::test_7f_two_blocks_in_shadow_read_live_inputs_and_promote_independently[dagster-fallback_source]` |
| 9.83 | call | `tests/unit/test_invariant.py::test_no_silent_loss` |
| 9.79 | call | `tests/scenarios/test_shadow.py::test_7d_promote_then_rollback_restores_previous_version[dagster-source]` |
| 9.73 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-after_replay-multi_dependent]` |
| 9.71 | call | `tests/scenarios/test_shadow.py::test_7c_bad_candidate_is_never_promoted[dagster-unprotected]` |
| 9.67 | call | `tests/scenarios/test_shadow.py::test_7g_interrupted_promotion_is_resumable[dagster-during_replay-leaf]` |

## What dominated

- **Dagster runner variants**: 148 tests and about 41% of the time. Each one
  materializes the demo pipeline several times in-process.
- **Per-operation DuckDB connections**: in one profiled standalone pipeline run
  (1.42s), about a third of the time was `duckdb.connect` and close. Every store
  operation opened and closed its file.
- **Exhaustive matrices**: most scenario tests run once per DAG role (6) and per
  runner (2), and agent and CLI tests once per role or per block.
- **Property tests and the eval matrix**: 8f (about 40s per runner), the Hypothesis
  invariants (about 22s), generating all 32 eval cases (about 15s) and the fix eval
  (about 19s).
