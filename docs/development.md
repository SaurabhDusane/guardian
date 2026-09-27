# Development

```bash
uv sync --extra dagster --extra agent --extra observability   # everything the tests use
uv run ruff check . && uv run ruff format --check .           # lint and format
```

Tests are split by markers, declared in `pyproject.toml`:

| marker | what | in the default run? |
|---|---|---|
| `slow` | Dagster runner scenarios, every role and block beyond the representative one, full property-test example counts, the eval matrix, the benchmark smoke run | no |
| `dagster` | needs the `dagster` extra (every Dagster runner test is also `slow`) | no |
| `llm` | calls a real LLM API (network, API key, and `GUARDIAN_LLM_LIVE=1`) | no |
| `docker` | needs Docker (the local observability stack) | no |

| command | runs |
|---|---|
| `uv run pytest` | the default suite: `-m "not slow and not llm and not docker"` |
| `uv run pytest -n auto` | the same, in parallel (pytest-xdist) |
| `uv run pytest -m "not llm and not docker" -n auto` | the full suite: both runners, every DAG role and block, full property tests, eval matrix |
| `uv run pytest -m slow` | only the slow tests |
| `uv run pytest -m dagster` | only the Dagster tests |
| `GUARDIAN_LLM_LIVE=1 GUARDIAN_LLM_PROVIDER=anthropic GUARDIAN_LLM_MODEL=... ANTHROPIC_API_KEY=... uv run pytest -m llm` | the real-model contract test |
| `uv run pytest -m docker` | the observability stack end to end (`tests/docker/`; skips without a Docker daemon) |

A later `-m` replaces the default one. Role- and block-parametrized tests keep one
representative in the default run (the `fallback_protected` role, or its block), and the
other roles and blocks run in the full suite. `tests/scenarios/test_smoke.py` still runs
a contain, replay and recover scenario for every DAG role in the default run, and the
Hypothesis property tests run fewer examples there. Every test has its own storage root,
so `-n auto` is safe.

## CI

[`ci.yml`](../.github/workflows/ci.yml) runs on every push and pull request: `ruff
check` and `ruff format --check`, then the default suite with `-n auto` on Ubuntu and
Windows with Python 3.11 and 3.12, uploading a JUnit report per job.
[`nightly.yml`](../.github/workflows/nightly.yml) runs the full suite on Ubuntu and
Windows every night and on demand, plus the `docker` tests on Ubuntu, and lists failing
tests in the job summary. `llm` tests never run in CI and no API key is used.

## Test timings

Measured on a 4-CPU container when the default and full suites were split:

| suite | serial | `-n auto` |
|---|---|---|
| default (459 tests) | 4 min 48 s | 1 min 44 s |
| full (798 tests) | 30 min 17 s | 10 min 51 s |
| before this split (788 tests, all serial) | 41 min 04 s | n/a |

In the full suite, Dagster's in-process execution dominates: about 1.9 s per pipeline
run, against 0.5 s standalone. Details are in
[`bench/test_timings_before.md`](../bench/test_timings_before.md) and
[`bench/test_timings_after.md`](../bench/test_timings_after.md).

## Project layout

```
guardian/
  core/        models, validation, snapshots, quarantine, events, planner, versions,
               code (fingerprints), dag (roles), drift (profiles, PSI/z-score),
               shadow, provenance, guardian facade
  runner/      spec_loader, executor, cli
  adapters/dagster/  io_manager, checks, replay, definitions
  agent/       evidence bundles, LLM diagnosis (Anthropic + fake clients), fix
               proposals (propose), safety guards, eval, metering (LLM cache, tokens, cost)
  observability/  OpenLineage run events and OpenTelemetry spans (optional, via env)
  demo/        data_gen, schemas, blocks, faults, refactor (code_bug), pipeline.yaml
observability/  docker-compose with Marquez, Tempo and Grafana for local viewing
docs/          one page per topic, linked from the README
bench/
  run_bench.py  overhead / throughput / recovery / shadow benchmarks -> results.json, results.md
  update_readme.py  README Results and agent-eval tables <- results.json, agent_eval.json
  agent_eval.json / agent_eval.md  written by `guardian eval diagnose|propose --real` (manual)
  test_timings_before.md / test_timings_after.md  test suite profiling
tests/
  helpers/     blocks_by_role (DAG roles) and block profiles, shared by the tests
  unit/        per-module core tests, invariant property test, layering test, role helper
  runner/      spec loader, executor, CLI
  demo/        demo blocks and fault injection
  adapters/    Dagster adapter wiring
  agent/       evidence, diagnosis validation and clients, eval (FakeClient only)
  observability/  OpenLineage and OpenTelemetry exporters (in-memory exporters)
  scenarios/   fault-injection suite, parametrized over DAG roles and both runners
  bench/       smoke test for the benchmark script (slow)
```
