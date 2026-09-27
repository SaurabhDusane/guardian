# Benchmarks

`bench/run_bench.py` measures four things on the demo pipeline at 10k, 100k and 1M
generated rows, and `bench/update_readme.py` copies the results into the README's
Results table from `bench/results.json`, so no number there is typed by hand. The full
report, with every sample, the interquartile ranges, the machine, the blocks used and the
methodology, is written to `bench/results.md`. Every block is chosen by DAG role, never
by name.

```bash
uv sync --extra observability
uv run python bench/run_bench.py              # 10k, 100k and 1M rows, 5 repeats each
uv run python bench/update_readme.py
```

Overhead compares one pipeline run on clean data with (a), the bare block functions in
topological order. (b) is Guardian's core (validation, Parquet snapshots, quarantine,
events sampled at 0.1) with provenance switched off, (c) adds provenance, and (d) adds
both exporters, OpenLineage to a file and OpenTelemetry to an in-memory exporter.

Throughput is generated rows per second of pipeline wall time on the demo's messy data.
It compares a clean run with a run where the fallback-protected block crashes, so its
consumer reads the fallback source through the adapter, and one where an unprotected
block crashes, so its dependents read its stale last-good snapshot.

Recovery is the wall time of `guardian shadow promote <block> --approve`, run as its own
process, until the block is HEALTHY on the candidate version with its quarantine
replayed, with 1%, 10% and 50% of the block's rows quarantined. The floor row is the
time `guardian status` takes on the same storage, which is the process start and imports
every CLI command pays.

Shadow cost is one run with a block's candidate version in shadow against the same run
with none.

`uv run python bench/run_bench.py --quick` (10k rows, 2 repeats, about 2 minutes) is a
sanity check. It writes `bench/results-quick.*`, which is not committed and which
`update_readme.py` refuses. Other options are `--scales 10000 100000`, `--repeats N` and
`--only overhead throughput recovery shadow`.
