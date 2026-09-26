"""Command-line entry point: `guardian run|replay|status|set-status`."""

from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

import guardian as guardian_pkg
from guardian.core.guardian import Guardian
from guardian.core.models import DEFAULT_STORAGE_ROOT, BlockStatus, QuarantineStatus
from guardian.runner.executor import Executor, Outcome, RunReport
from guardian.runner.spec_loader import SpecError, load_spec

app = typer.Typer(
    help="Guardian: self-healing maintenance layer for data pipelines.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)
console = Console()

STATE_FILE = "cli_state.json"

RootOption = typer.Option(DEFAULT_STORAGE_ROOT, "--root", help="Guardian storage root.")
SpecOption = typer.Option(
    None, "--spec", help="Pipeline spec (defaults to the spec of the last `guardian run`)."
)

_OUTCOME_STYLE = {
    Outcome.PASS: "green",
    Outcome.ROLLBACK: "yellow",
    Outcome.SKIPPED: "dim",
    Outcome.BLOCKED: "red",
}
_STATUS_STYLE = {
    BlockStatus.HEALTHY: "green",
    BlockStatus.DEGRADED: "yellow",
    BlockStatus.OUT: "cyan",
}


def find_spec(spec: Path) -> Path:
    """Resolve ``spec`` as given, falling back to a path inside the guardian package.

    This makes ``guardian run demo/pipeline.yaml`` work from any directory.
    """
    if spec.exists():
        return spec.resolve()
    packaged = Path(guardian_pkg.__file__).parent / spec
    if not spec.is_absolute() and packaged.exists():
        return packaged.resolve()
    raise typer.BadParameter(f"spec file not found: {spec}")


def _save_state(root: Path, spec: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / STATE_FILE).write_text(json.dumps({"spec": str(spec)}), encoding="utf-8")


def _spec_for(root: Path, spec: Path | None) -> Path:
    if spec is not None:
        return find_spec(spec)
    state = root / STATE_FILE
    if state.exists():
        return Path(json.loads(state.read_text(encoding="utf-8"))["spec"])
    raise typer.BadParameter("no --spec given and no previous `guardian run` in this root")


def _open(
    root: Path, spec_path: Path, sample_rate: float = 1.0, faults: list[str] | None = None
) -> Guardian:
    try:
        spec = load_spec(spec_path)
    except SpecError as exc:
        console.print(f"[red]Invalid spec:[/red] {exc}")
        raise typer.Exit(code=2) from exc
    registry: dict = {}
    if faults:
        from guardian.demo.faults import apply_faults, parse_fault

        by_block: dict[str, list] = {}
        try:
            for text in faults:
                block, fault = parse_fault(text)
                by_block.setdefault(block, []).append(fault)
            spec, registry = apply_faults(spec, by_block)
        except (KeyError, ValueError) as exc:
            raise typer.BadParameter(str(exc), param_hint="--fault") from exc
    return Guardian(spec, root, sample_rate=sample_rate, registry=registry)


@app.command()
def run(
    spec: Path = typer.Argument(..., help="Path to the pipeline YAML spec."),
    root: Path = RootOption,
    run_id: str | None = typer.Option(None, "--run-id", help="Run id (default: generated)."),
    sample_rate: float = typer.Option(
        1.0, "--sample-rate", min=0.0, max=1.0, help="Sample rate for routine events."
    ),
    fault: list[str] | None = typer.Option(
        None,
        "--fault",
        help="Inject a fault into any block for this run (repeatable): "
        "BLOCK:corrupt:FRACTION[:COL,COL] | BLOCK:null:COL:FRACTION | BLOCK:drop:COL[,COL] | "
        "BLOCK:rename:OLD=NEW | BLOCK:crash.",
    ),
    only: list[str] | None = typer.Option(
        None, "--only", help="Run only these blocks (repeatable); others are not executed."
    ),
) -> None:
    """Run a pipeline spec end to end and print a per-block summary."""
    spec_path = find_spec(spec)
    with _open(root, spec_path, sample_rate, fault) as g:
        try:
            report = Executor(g).run(run_id, only=only)
        except KeyError as exc:
            raise typer.BadParameter(str(exc.args[0]), param_hint="--only") from exc
        _save_state(root, spec_path)
        console.print(render_report(report))
        quarantined = sum(r.quarantined for r in report.blocks)
        console.print(
            f"run [bold]{report.run_id}[/bold]: {len(report.blocks)} blocks, "
            f"{quarantined} rows quarantined. Storage: {root}"
        )


def render_report(report: RunReport) -> Table:
    table = Table(title=f"Pipeline {report.pipeline!r} - run {report.run_id}")
    for column, justify in [
        ("block", "left"),
        ("outcome", "left"),
        ("status", "left"),
        ("rows in", "right"),
        ("promoted", "right"),
        ("quarantined", "right"),
        ("read from", "left"),
        ("note", "left"),
    ]:
        table.add_column(column, justify=justify)  # type: ignore[arg-type]
    for r in report.blocks:
        sources = ", ".join(
            ref.block
            + (f"@{ref.run_id}" if ref.run_id != report.run_id else "")
            + (f" (fallback for {ref.requested})" if ref.rerouted else "")
            + (" (stale)" if ref.stale else "")
            for ref in r.sources
        )
        table.add_row(
            r.block,
            f"[{_OUTCOME_STYLE[r.outcome]}]{r.outcome.value}[/]",
            f"[{_STATUS_STYLE[r.status]}]{r.status.value}[/]",
            str(r.rows_in),
            str(r.rows_out),
            str(r.quarantined),
            sources or "-",
            r.reason or "",
        )
    return table


@app.command()
def replay(
    block: str = typer.Argument(..., help="Block whose quarantine to replay."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Replay quarantined records for a block (after it has been fixed)."""
    with _open(root, _spec_for(root, spec)) as g:
        try:
            result = g.replay(block)
        except KeyError as exc:
            console.print(f"[red]{exc.args[0]}[/red]")
            raise typer.Exit(code=2) from exc
        line = (
            f"{block}: replayed [green]{result.replayed}[/green], "
            f"still failing [yellow]{result.still_failing}[/yellow]"
        )
        if result.snapshot and result.merged:
            line += f", upserted into new last-good snapshot {result.snapshot.run_id}"
        elif result.snapshot:
            line += (
                f"\n[yellow]WARN[/yellow]: no merge_key declared; replayed rows written to "
                f"separate snapshot {result.snapshot.run_id}, last-good unchanged"
            )
        elif result.replayed == 0 and result.still_failing == 0:
            line += " (nothing to replay)"
        console.print(line)


@app.command()
def status(spec: Path | None = SpecOption, root: Path = RootOption) -> None:
    """Show block health, last-good snapshots and quarantine counts."""
    with _open(root, _spec_for(root, spec)) as g:
        table = Table(title=f"Pipeline {g.spec.name!r} - status")
        for column in ("block", "status", "last-good run", "quarantined", "replayed"):
            table.add_column(column)
        for name, block_status in g.all_statuses().items():
            last_good = g.snapshots.last_good(name)
            table.add_row(
                name,
                f"[{_STATUS_STYLE[block_status]}]{block_status.value}[/]",
                last_good.run_id if last_good else "-",
                str(g.quarantine.count(block=name, status=QuarantineStatus.QUARANTINED)),
                str(g.quarantine.count(block=name, status=QuarantineStatus.REPLAYED)),
            )
        console.print(table)


@app.command("set-status")
def set_status(
    block: str = typer.Argument(..., help="Block name."),
    new_status: str = typer.Argument(..., help="HEALTHY or OUT."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Take a block OUT for optimization, or return it to HEALTHY."""
    with _open(root, _spec_for(root, spec)) as g:
        try:
            g.set_block_status(block, new_status.upper())
        except (KeyError, ValueError) as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=2) from exc
        console.print(f"{block} is now {g.status(block).value}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
