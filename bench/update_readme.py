"""Fill in the README's Results table from bench/results.json (no hand-typed numbers).

    uv run python bench/update_readme.py            # rewrite the table in README.md
    uv run python bench/update_readme.py --check    # exit 1 if the README is out of date

The table sits between the ``<!-- bench-results:start -->`` and
``<!-- bench-results:end -->`` markers. Results from ``--quick`` runs or unfinished runs
are refused unless ``--allow-partial`` is given. ``run_bench.py`` uses
``render_results_md`` from here to write bench/results.md, so both documents are rendered
from the same JSON by the same code.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

BENCH = Path(__file__).resolve().parent
REPO = BENCH.parent
START = "<!-- bench-results:start -->"
END = "<!-- bench-results:end -->"


# ---------------------------------------------------------------- lookups and formats


class Results:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.meta = data["meta"]
        self.scales: list[int] = sorted({s["scale"] for s in data["series"]})
        self._series = {(s["measurement"], s["scale"], s["config"]): s for s in data["series"]}
        self._derived = {
            (d["measurement"], d["scale"], d["config"], d["metric"]): d["value"]
            for d in data["derived"]
        }

    def series(self, measurement: str, scale: int, config: str) -> dict[str, Any] | None:
        return self._series.get((measurement, scale, config))

    def derived(self, measurement: str, scale: int, config: str, metric: str) -> float | None:
        return self._derived.get((measurement, scale, config, metric))

    def configs(self, measurement: str) -> list[str]:
        seen: dict[str, None] = {}
        for s in self.data["series"]:
            if s["measurement"] == measurement:
                seen.setdefault(s["config"])
        return list(seen)

    def label(self, measurement: str, config: str) -> str:
        return next(
            s["label"]
            for s in self.data["series"]
            if s["measurement"] == measurement and s["config"] == config
        )


def num(x: float) -> str:
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    if abs(x) >= 100:
        return f"{x:.0f}"
    if abs(x) >= 10:
        return f"{x:.1f}"
    return f"{x:.2f}"


def rows(n: int) -> str:
    return f"{n / 1_000_000:g}M" if n >= 1_000_000 else f"{n / 1000:g}k" if n >= 1000 else str(n)


def value(s: dict[str, Any]) -> str:
    """Median with its unit and interquartile range."""
    unit = " s" if s["unit"] == "s" else f" {s['unit']}"
    return f"{num(s['median'])}{unit} (IQR {num(s['iqr'])})"


def pct(x: float | None, *, signed: bool = True) -> str:
    if x is None:
        return "n/a"
    return f"{x:+.1f}%" if signed else f"{x:.0f}%"


# ---------------------------------------------------------------- the summary table


def summary_table(r: Results) -> str:
    """One row per quotable figure, one column per scale."""
    header = "| Measurement | " + " | ".join(f"{rows(s)} rows" for s in r.scales) + " |"
    lines = [header, "|---|" + "---:|" * len(r.scales)]

    def row(name: str, cell) -> None:
        cells = []
        for scale in r.scales:
            try:
                cells.append(cell(scale))
            except (KeyError, TypeError):
                cells.append("n/a")
        if any(c != "n/a" for c in cells):
            lines.append(f"| {name} | " + " | ".join(cells) + " |")

    def skipped_or(measurement: str, config: str, scale: int, fmt) -> str:
        s = r.series(measurement, scale, config)
        if s is None:
            raise KeyError(config)
        return "skipped" if s.get("skipped") else fmt(s)

    row("Bare run, no Guardian (a)", lambda n: value(r.series("overhead", n, "bare")))
    for config, name in (
        ("core", "Guardian core only (b)"),
        ("provenance", "+ provenance (c)"),
        ("observability", "+ observability exporters (d)"),
    ):
        row(
            f"{name}: added over (a)",
            lambda n, c=config: skipped_or(
                "overhead",
                c,
                n,
                lambda s, n=n, c=c: (
                    f"**{pct(r.derived('overhead', n, c, 'added_pct'))}** ({num(s['median'])} s)"
                ),
            ),
        )
    row("Throughput, clean run", lambda n: value(r.series("throughput", n, "clean")))
    for config, name in (
        ("protected_crash", "Throughput, fallback-protected block crashed"),
        ("unprotected_crash", "Throughput, unprotected block crashed"),
    ):
        row(
            name,
            lambda n, c=config: (
                f"{value(r.series('throughput', n, c))}, "
                f"**{pct(r.derived('throughput', n, c, 'pct_of_clean'), signed=False)}** of clean"
            ),
        )
    for config in r.configs("recovery"):
        if config == "cli_floor":
            continue
        row(
            f"Recovery, {config} of rows quarantined",
            lambda n, c=config: (
                f"**{value(r.series('recovery', n, c))}** "
                f"({r.series('recovery', n, c)['notes']['rows_quarantined']:,} rows)"
            ),
        )
    row(
        "Recovery floor: CLI start-up (`guardian status`)",
        lambda n: value(r.series("recovery", n, "cli_floor")),
    )
    row(
        "Shadow cost: one block in shadow vs. none",
        lambda n: (
            f"**{pct(r.derived('shadow', n, 'one', 'added_pct'))}** "
            f"(+{num(r.derived('shadow', n, 'one', 'added_s'))} s)"
        ),
    )
    return "\n".join(lines)


def provenance_line(r: Results) -> str:
    m, machine = r.meta, r.meta["machine"]
    commit = (m.get("git_commit") or "unknown")[:10] + (
        " (dirty tree)" if m.get("git_dirty") else ""
    )
    cores = f"{machine.get('physical_cores') or '?'} cores / {machine.get('logical_cores')} threads"
    ram = f"{machine['ram_gib']} GiB RAM" if machine.get("ram_gib") else "RAM unknown"
    return (
        f"Measured {m['started'][:10]} at commit `{commit}` on {machine['cpu']} "
        f"({cores}, {ram}), {machine['os']}, {machine['python']}. "
        f"Median of {m['repeats']} repeats after 1 warm-up, fresh storage root per repeat; "
        "IQR = interquartile range."
    )


def render_readme_block(r: Results) -> str:
    return f"{START}\n{provenance_line(r)}\n\n{summary_table(r)}\n{END}"


# ---------------------------------------------------------------- results.md


def methodology(r: Results) -> str:
    m = r.meta
    fractions = ", ".join(f"{f:.0%}" for f in m["quarantine_fractions"])
    return f"""## Methodology

- **Scales:** {", ".join(f"{s:,}" for s in m["scales"])} rows generated by the source block of
  the demo pipeline (`guardian/demo/pipeline.yaml`), one scale at a time.
- **Repeats:** for every configuration, one untimed warm-up, then {m["repeats"]} measured
  repeats. Within a repeat the configurations run one after another, in an order rotated
  from repeat to repeat, so slow drift of the machine hits them all alike.
- **Isolation:** every warm-up and repeat gets a fresh temporary storage root (snapshots,
  quarantine, events, provenance, versions) and fresh Guardian instances; nothing on disk
  is shared between repeats. The Python process is shared (imports and module caches are
  warm after the warm-up); `gc.collect()` runs before each repeat.
- **Statistics:** the median and the interquartile range (Q3 - Q1, inclusive quartiles:
  with 5 samples, the 2nd and 4th values). Percentages compare medians.
- **What is timed:** the pipeline run itself (`Executor.run`, source loading included)
  for measurements 1, 2 and 4, and the `guardian shadow promote --approve` process for 3.
  Opening the Guardian stores and the untimed setup runs are excluded.
- **Blocks** are chosen by DAG role with `blocks_by_role` (see "Blocks" below), never by
  name.

1. **Overhead breakdown** (clean data, `error_rate=0`: the bare functions cannot survive
   the rows Guardian would quarantine). (a) the block functions in topological order; (b)
   Guardian with validation, snapshots and quarantine, events sampled at
   {m["sample_rate"]}, provenance switched off (a no-op provenance store); (c) (b) with
   provenance; (d) (c) with the OpenLineage exporter writing to a JSON-lines file and the
   OpenTelemetry span exporter writing to an in-memory exporter. Added cost = median /
   median of (a) - 1.
2. **Throughput during an outage** (the demo's messy data). Rows/s = generated rows /
   wall time of the run. Each timed run follows an untimed clean run in the same root, so
   last-good snapshots exist. Outage runs crash the fallback-protected block (its consumer
   reads the fallback source through the adapter) or an unprotected block (its dependents
   read its stale last-good snapshot). Every run's rerouting is asserted.
3. **Recovery time** (clean data). Untimed setup: a clean run; a run in which the block's
   live version garbles {fractions} of its rows (the columns it derives itself), with its
   quarantine threshold raised just above that fraction so exactly those rows are
   quarantined; the block taken OUT; its candidate version started in shadow and run
   once. Timed: `guardian shadow promote <block> --approve` in a new process, which
   replays the quarantine through the candidate, makes it active and marks the block
   HEALTHY. Afterwards the status, active version and replay counts are asserted. The
   CLI floor row is `guardian status` on the same root: process start, imports and
   opening the stores, which every CLI command pays.
4. **Shadow cost** (messy data). One run with the block's candidate version in shadow
   (it runs on the same inputs, is validated, written to the candidate store and compared
   row by row with the live output), vs. the same run with no shadow.
"""


def detail_tables(r: Results) -> str:
    out = []
    titles = {
        "overhead": "1. Overhead breakdown",
        "throughput": "2. Throughput during an outage",
        "recovery": "3. Recovery time",
        "shadow": "4. Shadow cost",
    }
    for measurement, title in titles.items():
        configs = r.configs(measurement)
        if not configs:
            continue
        out.append(f"## {title}\n")
        out.append("| Rows | Configuration | Median | IQR | Q1 - Q3 | Relative | Samples |")
        out.append("|---:|---|---:|---:|---|---:|---|")
        for scale in r.scales:
            for config in configs:
                s = r.series(measurement, scale, config)
                if s is None:
                    continue
                if s.get("skipped"):
                    out.append(f"| {scale:,} | {s['label']} | skipped: {s['skipped']} | | | | |")
                    continue
                rel = {
                    "overhead": ("added_pct", True),
                    "throughput": ("pct_of_clean", False),
                    "shadow": ("added_pct", True),
                }.get(measurement)
                relative = ""
                if rel:
                    v = r.derived(measurement, scale, config, rel[0])
                    relative = "" if v is None else pct(v, signed=rel[1])
                samples = ", ".join(num(x) for x in s["samples"])
                out.append(
                    f"| {scale:,} | {s['label']} | {num(s['median'])} {s['unit']} | "
                    f"{num(s['iqr'])} | {num(s['q1'])} - {num(s['q3'])} | {relative} | "
                    f"{samples} |"
                )
        notes = [
            f"- {rows(s['scale'])}, {s['label']}: "
            + ", ".join(f"{k} = {v}" for k, v in s["notes"].items())
            for s in r.data["series"]
            if s["measurement"] == measurement and s["notes"]
        ]
        if notes:
            out.append("\nNotes recorded by the runs:\n")
            out += notes
        out.append("")
    return "\n".join(out)


def render_results_md(data: dict[str, Any]) -> str:
    r = Results(data)
    m, machine = r.meta, r.meta["machine"]
    warning = ""
    if m.get("quick"):
        warning = (
            "> **Quick run (`--quick`): a sanity check, not a measurement.** Do not quote "
            "these numbers.\n\n"
        )
    elif not m.get("complete"):
        warning = "> **Incomplete run:** the benchmark stopped before the end.\n\n"
    machine_rows = {
        "CPU": machine["cpu"],
        "Cores": f"{machine.get('physical_cores') or 'unknown'} physical, "
        f"{machine.get('logical_cores')} logical",
        "RAM": f"{machine['ram_gib']} GiB" if machine.get("ram_gib") else "unknown",
        "OS": machine["os"],
        "Python": machine["python"],
        "uv": machine["uv"],
        **{p: v for p, v in machine["packages"].items()},
        "Git commit": f"{m.get('git_commit')}"
        + (" (uncommitted changes)" if m.get("git_dirty") else ""),
        "Started / finished": f"{m['started']} / {m.get('finished') or '-'}",
        "Command": f"`{m['command']}`",
    }
    blocks = "\n".join(
        f"| {role} | {', '.join(v) if isinstance(v, list) else v} |"
        for role, v in data["blocks"].items()
    )
    return (
        f"# Guardian benchmark results\n\n{warning}"
        "Generated by `bench/run_bench.py` from `results.json`; the README's Results table is "
        "filled in from the same file by `bench/update_readme.py`.\n\n"
        f"## Summary\n\n{provenance_line(r)}\n\n{summary_table(r)}\n\n"
        f"{methodology(r)}\n## Machine\n\n| | |\n|---|---|\n"
        + "\n".join(f"| {k} | {v} |" for k, v in machine_rows.items())
        + f"\n\n## Blocks (chosen by DAG role)\n\n| Role | Block |\n|---|---|\n{blocks}\n\n"
        + detail_tables(r)
    )


# ---------------------------------------------------------------- README


def update_readme(readme: str, data: dict[str, Any]) -> str:
    start, end = readme.find(START), readme.find(END)
    if start < 0 or end < start:
        raise SystemExit(f"README has no {START} ... {END} section")
    return readme[:start] + render_readme_block(Results(data)) + readme[end + len(END) :]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--results", type=Path, default=BENCH / "results.json")
    parser.add_argument("--readme", type=Path, default=REPO / "README.md")
    parser.add_argument("--check", action="store_true", help="exit 1 if the README is stale")
    parser.add_argument(
        "--allow-partial", action="store_true", help="accept --quick or unfinished results"
    )
    args = parser.parse_args(argv)
    if not args.results.exists():
        parser.error(f"{args.results} not found: run bench/run_bench.py first")
    data = json.loads(args.results.read_text(encoding="utf-8"))
    meta = data["meta"]
    if (meta.get("quick") or not meta.get("complete")) and not args.allow_partial:
        parser.error(
            f"{args.results} is from a {'--quick' if meta.get('quick') else 'unfinished'} run; "
            "the README should quote a full run (--allow-partial to override)"
        )
    readme = args.readme.read_text(encoding="utf-8")
    updated = update_readme(readme, data)
    if args.check:
        if updated != readme:
            print(f"{args.readme} is out of date: run bench/update_readme.py", file=sys.stderr)
            return 1
        return 0
    args.readme.write_text(updated, encoding="utf-8", newline="\n")
    print(f"updated the Results table in {args.readme}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
