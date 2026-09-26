"""Guardian benchmarks on the demo pipeline.

    uv run python bench/run_bench.py                     # 100k rows, 5 reps
    uv run python bench/run_bench.py --rows 20000 --reps 3 --out /tmp/results.md

Measures:
1. Throughput during an outage: rows/s through the whole pipeline for a clean run vs.
   a run where b6 crashes and b8 is rerouted to b5 through the fallback adapter.
2. Recovery time: wall time of `replay b6_enrich` after a heavy-corruption outage,
   until b6 is HEALTHY with the replayed snapshot promoted (in-process API and CLI).
3. Logging overhead: pipeline time with Guardian at event sample rate 1.0 and 0.1 vs.
   a bare run that calls the same block functions directly, without Guardian.

Each configuration runs ``--reps`` times in a fresh storage root; configurations are
interleaved per repetition so drift affects them equally. One untimed warm-up run
absorbs import and first-call costs.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pandas as pd

from guardian.core.events import EventKind
from guardian.core.guardian import Guardian
from guardian.core.models import Action, BlockStatus, PipelineSpec
from guardian.core.refs import load_ref
from guardian.demo.faults import apply_faults, corrupt_rows, crash
from guardian.runner.executor import Executor, Outcome
from guardian.runner.spec_loader import load_spec, topological_order

REPO = Path(__file__).resolve().parents[1]
DEMO_SPEC = REPO / "guardian" / "demo" / "pipeline.yaml"


# ---------------------------------------------------------------- helpers


def demo_spec(rows: int, error_rate: float | None = None) -> PipelineSpec:
    """The demo spec with b1 generating ``rows`` rows (and optionally clean data)."""
    spec = load_spec(DEMO_SPEC)
    blocks = []
    for block in spec.blocks:
        if block.name == "b1_ingest":
            params = {**block.params, "rows": rows}
            if error_rate is not None:
                params["error_rate"] = error_rate
            block = dataclasses.replace(block, params=params)
        blocks.append(block)
    return dataclasses.replace(spec, blocks=tuple(blocks))


@contextmanager
def fresh_root() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="guardian-bench-", ignore_cleanup_errors=True) as d:
        yield Path(d)


def timed(fn: Callable[[], Any]) -> tuple[float, Any]:
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


@dataclass
class Series:
    """Samples for one configuration."""

    name: str
    unit: str
    samples: list[float] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)

    @property
    def median(self) -> float:
        return statistics.median(self.samples)

    @property
    def spread(self) -> str:
        lo, hi = min(self.samples), max(self.samples)
        pct = (hi - lo) / self.median * 100 if self.median else 0.0
        return f"{fmt(lo)} - {fmt(hi)} (±{pct / 2:.1f}%)"


def fmt(x: float) -> str:
    if x >= 1000:
        return f"{x:,.0f}"
    if x >= 10:
        return f"{x:.1f}"
    return f"{x:.2f}"


def run_pipeline(g: Guardian, run_id: str):
    return Executor(g).run(run_id)


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------- 1. throughput


def bench_throughput(rows: int, reps: int) -> list[Series]:
    spec = demo_spec(rows)
    outage_spec, outage_registry = apply_faults(spec, {"b6_enrich": [crash("bench outage")]})
    clean = Series("Clean run", "rows/s")
    outage = Series("b6 crashed, b8 rerouted to b5", "rows/s")
    configs = [(clean, spec, {}), (outage, outage_spec, outage_registry)]

    for rep in range(reps):
        for series, s, registry in configs if rep % 2 == 0 else configs[::-1]:
            with fresh_root() as root, Guardian(s, root, registry=registry) as g:
                seconds, report = timed(lambda g=g: run_pipeline(g, "r1"))
                ingested = report.get("b1_ingest").decision.total_rows
                b8 = report.get("b8_aggregate")
                assert b8.outcome is Outcome.PASS
                if series is outage:
                    b6 = report.get("b6_enrich")
                    assert b6.outcome is Outcome.ROLLBACK and b6.reason == "crash"
                    assert b8.rerouted and b8.sources[0].block == "b5_normalize"
                else:
                    assert report.ok and not b8.rerouted
                series.samples.append(ingested / seconds)
                series.notes.setdefault("seconds", []).append(seconds)
                series.notes["rows_ingested"] = ingested
                series.notes["rows_into_b8"] = b8.rows_in
        log(f"throughput rep {rep + 1}/{reps}")
    return [clean, outage]


# ---------------------------------------------------------------- 2. recovery


def _prepare_outage(spec: PipelineSpec, root: Path) -> int:
    """Clean run r0, then r1 with 50% of b6 corrupted -> ROLLBACK. Returns #quarantined."""
    with Guardian(spec, root) as g:
        run_pipeline(g, "r0")
    faulted, registry = apply_faults(
        spec, {"b6_enrich": [corrupt_rows(0.5, columns=["region", "segment"], seed=1)]}
    )
    with Guardian(faulted, root, registry=registry) as g:
        report = run_pipeline(g, "r1")
        assert report.get("b6_enrich").outcome is Outcome.ROLLBACK
        assert g.status("b6_enrich") is BlockStatus.DEGRADED
        return report.get("b6_enrich").quarantined


def _assert_recovered(spec: PipelineSpec, root: Path, expected_rows: int) -> None:
    with Guardian(spec, root) as g:
        assert g.status("b6_enrich") is BlockStatus.HEALTHY
        ref = g.snapshots.last_good("b6_enrich")
        assert ref is not None and ref.run_id.startswith("replay")
        df = g.snapshots.read(ref.block, ref.run_id)
        assert len(df) == expected_rows and df["order_id"].is_unique


def bench_recovery(rows: int, reps: int) -> list[Series]:
    spec = demo_spec(rows)
    api = Series("replay b6_enrich (in-process API)", "s")
    cli = Series("guardian replay b6_enrich (CLI, incl. interpreter start)", "s")

    for rep in range(reps):
        for series in (api, cli) if rep % 2 == 0 else (cli, api):
            with fresh_root() as root:
                quarantined = _prepare_outage(spec, root)
                if series is api:
                    with Guardian(spec, root) as g:
                        seconds, result = timed(lambda g=g: g.replay("b6_enrich"))
                        assert result.merged and result.still_failing == 0
                        assert result.replayed == quarantined
                else:
                    cmd = [
                        sys.executable, "-m", "guardian.runner.cli", "replay", "b6_enrich",
                        "--spec", str(DEMO_SPEC), "--root", str(root),
                    ]  # fmt: skip
                    seconds, proc = timed(
                        lambda cmd=cmd: subprocess.run(cmd, capture_output=True, text=True)
                    )
                    assert proc.returncode == 0, proc.stderr
                    assert "upserted into new last-good snapshot" in proc.stdout, proc.stdout
                _assert_recovered(spec, root, quarantined)
                series.samples.append(seconds)
                series.notes["records_replayed"] = quarantined
        log(f"recovery rep {rep + 1}/{reps}")
    return [api, cli]


# ---------------------------------------------------------------- 3. logging overhead


def _bare_run(spec: PipelineSpec) -> int:
    """The same block functions in topological order, no Guardian at all."""
    outputs: dict[str, pd.DataFrame] = {}
    for name in topological_order(spec):
        block = spec.block(name)
        fn = load_ref(block.fn)
        outputs[name] = fn(*[outputs[u] for u in block.inputs], **block.params)
    return len(outputs["b1_ingest"])


def bench_overhead(rows: int, reps: int) -> list[Series]:
    spec = demo_spec(rows, error_rate=0.0)  # bare blocks cannot survive bad rows
    bare = Series("Bare run (no Guardian)", "s")
    full = Series("Guardian, sample rate 1.0", "s")
    tenth = Series("Guardian, sample rate 0.1", "s")

    def guardian_run(series: Series, rate: float) -> float:
        with fresh_root() as root, Guardian(spec, root, sample_rate=rate) as g:
            seconds, report = timed(lambda: run_pipeline(g, "r1"))
            assert report.ok
            decisions = [r.decision for r in report.blocks]
            assert all(d.action is Action.PASS and d.bad_rows == 0 for d in decisions)
            events = g.events.query()
            series.notes.setdefault("events_kept", []).append(len(events))
            series.notes.setdefault("events_sampled_out", []).append(g.events.sampled_out)
            assert not any(e.kind is EventKind.ERROR for e in events)
        return seconds

    order = [
        (bare, lambda: timed(lambda: _bare_run(spec))[0]),
        (full, lambda: guardian_run(full, 1.0)),
        (tenth, lambda: guardian_run(tenth, 0.1)),
    ]
    for rep in range(reps):
        k = rep % len(order)  # rotate so no configuration always runs first
        for series, fn in order[k:] + order[:k]:
            series.samples.append(fn())
        log(f"overhead rep {rep + 1}/{reps}")
    return [bare, full, tenth]


# ---------------------------------------------------------------- report


def machine_specs() -> dict[str, str]:
    cpu = platform.processor() or platform.machine()
    memory = "unknown"
    cpuinfo, meminfo = Path("/proc/cpuinfo"), Path("/proc/meminfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal"):
                memory = f"{int(line.split()[1]) / 1024 / 1024:.1f} GiB"
                break
    packages = ["pandas", "pandera", "pyarrow", "duckdb", "numpy"]
    return {
        "Date": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "OS": platform.platform(),
        "CPU": f"{cpu} ({os.cpu_count()} logical cores visible)",
        "Memory": memory,
        "Python": platform.python_version(),
        "Libraries": ", ".join(f"{p} {version(p)}" for p in packages),
    }


def table(series: list[Series]) -> str:
    lines = [
        "| Configuration | Median | Min - max (± half-range) | Unit |",
        "|---|---:|---|---|",
    ]
    lines += [f"| {s.name} | **{fmt(s.median)}** | {s.spread} | {s.unit} |" for s in series]
    return "\n".join(lines)


def raw(series: list[Series]) -> str:
    return "\n".join(f"- {s.name}: " + ", ".join(fmt(x) for x in s.samples) for s in series)


def write_report(path: Path, rows: int, reps: int, results: dict[str, list[Series]]) -> str:
    clean, outage = results["throughput"]
    api, cli = results["recovery"]
    bare, full, tenth = results["overhead"]
    logging_cost = full.median - tenth.median
    # Run-to-run noise: the larger half-range of the two Guardian configurations.
    noise = max((max(x.samples) - min(x.samples)) / 2 for x in (full, tenth))
    within = "smaller" if abs(logging_cost) <= noise else "larger"
    events_full = statistics.median(full.notes["events_kept"])
    events_tenth = statistics.median(tenth.notes["events_kept"])
    specs = machine_specs()

    summary = {
        "throughput_clean": clean.median,
        "throughput_outage": outage.median,
        "outage_ratio": outage.median / clean.median,
        "pre_b8_loss": 1 - clean.notes["rows_into_b8"] / clean.notes["rows_ingested"],
        "recovery_api": api.median,
        "recovery_cli": cli.median,
        "records_replayed": api.notes["records_replayed"],
        "overhead_full": (full.median / bare.median - 1) * 100,
        "overhead_tenth": (tenth.median / bare.median - 1) * 100,
        "logging_cost": logging_cost,
        "logging_cost_pct": logging_cost / bare.median * 100,
    }
    body = (
        f"""# Guardian benchmark results

Generated by `bench/run_bench.py --rows {rows} --reps {reps}`. Each configuration was run
{reps} times in a fresh storage root, interleaved across repetitions, after one untimed
warm-up. Medians are reported with the min - max range; ± is half the range as a
percentage of the median.

## Machine

| | |
|---|---|
"""
        + "\n".join(f"| {k} | {v} |" for k, v in specs.items())
        + f"""

These numbers come from a shared cloud container, not a dedicated machine: expect some
noise from neighbours, and compare configurations within this file rather than
across machines.

## 1. Throughput during an outage

Rows ingested by b1 per second of end-to-end pipeline wall time ({clean.notes["rows_ingested"]:,}
generated rows of messy demo data, of which b1-b4 quarantine {summary["pre_b8_loss"]:.1%}
as usual). In the outage run b6 raises on every run; Guardian rolls it back and b8
reads this run's b5 output through `b5_to_b6_shape`
({outage.notes["rows_into_b8"]:,} rows reach b8 in both configurations).

{table([clean, outage])}

Throughput during the outage is **{summary["outage_ratio"]:.0%}** of the clean run, with
the same number of rows reaching b8. (In the outage configuration b6 crashes before doing
any work, so it neither validates nor snapshots its output.)

## 2. Recovery time

Setup (untimed): a clean run r0, then run r1 with 50% of b6's rows corrupted, so b6 is
rolled back and all {summary["records_replayed"]:,} of its r1 rows are quarantined. Timed:
`replay b6_enrich` until it returns, after which b6 is verified to be HEALTHY with the
replay snapshot promoted as last-good ({summary["records_replayed"]:,} rows, unique
`order_id`s, upserted into r0's snapshot).

{table([api, cli])}

The CLI figure includes starting Python and importing pandas, Pandera, pyarrow and duckdb
(the difference between the two rows).

## 3. Logging overhead

Same blocks, same {rows:,} rows of *clean* demo data (`error_rate=0`), because the bare
blocks cannot survive the messy rows Guardian would normally quarantine (b5 fails to cast
NaN quantities to integers). The bare run calls the block functions in topological order
with no validation, snapshots, quarantine, or events.

{table([bare, full, tenth])}

- Guardian's total overhead over the bare run: **+{summary["overhead_full"]:.0f}%** at sample
  rate 1.0 and **+{summary["overhead_tenth"]:.0f}%** at 0.1. This is Pandera validation, Parquet
  snapshots, status/last-good bookkeeping and event logging together.
- The event-logging share (median at 1.0 minus median at 0.1) is
  **{logging_cost * 1000:+.0f} ms per run ({summary["logging_cost_pct"]:+.1f}% of the bare
  run)**, {within} than the run-to-run spread of the Guardian runs (±{noise * 1000:.0f} ms).
  A run keeps {events_full:.0f} events at 1.0 and {events_tenth:.0f} at 0.1: events are
  emitted per block and per decision, not per row.

## Raw samples

### Throughput (rows/s)
{raw([clean, outage])}

### Recovery (s)
{raw([api, cli])}

### Overhead (s)
{raw([bare, full, tenth])}
"""
    )
    path.write_text(body, encoding="utf-8")
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--out", type=Path, default=REPO / "bench" / "results.md")
    args = parser.parse_args()

    log(f"warm-up ({args.rows:,} rows)")
    with fresh_root() as root, Guardian(demo_spec(args.rows), root) as g:
        run_pipeline(g, "warmup")
    _bare_run(demo_spec(args.rows, error_rate=0.0))

    results = {
        "throughput": bench_throughput(args.rows, args.reps),
        "recovery": bench_recovery(args.rows, args.reps),
        "overhead": bench_overhead(args.rows, args.reps),
    }
    write_report(args.out, args.rows, args.reps, results)
    log(f"wrote {args.out}")
    for group in results.values():
        for s in group:
            print(f"  {s.name:<58} median {fmt(s.median):>10} {s.unit}   {s.spread}")


if __name__ == "__main__":
    main()
