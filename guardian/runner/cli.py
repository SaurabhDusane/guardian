"""Command-line entry point: `guardian run|replay|status|set-status|shadow|impact|lineage|
drift|diagnose|propose|eval`."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from rich.tree import Tree

import guardian as guardian_pkg
from guardian.agent.diagnose import (
    ACCEPTED,
    FAILED,
    DiagnosisResult,
    LLMConfig,
    auto_diagnoser,
    diagnose,
    make_client,
)
from guardian.agent.evidence import DEFAULT_SAMPLE_SIZE, EvidenceBundle, build_evidence, default_run
from guardian.core.guardian import Guardian
from guardian.core.models import (
    DEFAULT_STORAGE_ROOT,
    BlockStatus,
    DataRef,
    GuardianError,
    QuarantineStatus,
)
from guardian.core.provenance import LineageNode
from guardian.core.provenance import impact as compute_impact
from guardian.core.provenance import lineage as compute_lineage
from guardian.core.shadow import ShadowMode
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
    return Guardian(
        spec, root, sample_rate=sample_rate, registry=registry, diagnoser=auto_diagnoser()
    )


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
    """Show block health, versions, last-good snapshots and quarantine counts."""
    with _open(root, _spec_for(root, spec)) as g:
        table = Table(title=f"Pipeline {g.spec.name!r} - status")
        for column in (
            "block",
            "status",
            "version",
            "shadow",
            "last-good run",
            "quarantined",
            "replayed",
        ):
            table.add_column(column)
        for name, block_status in g.all_statuses().items():
            last_good = g.snapshots.last_good(name)
            candidate = g.shadow_candidate(name)
            table.add_row(
                name,
                f"[{_STATUS_STYLE[block_status]}]{block_status.value}[/]",
                describe_version(g, name),
                candidate.version if candidate else "-",
                last_good.run_id if last_good else "-",
                str(g.quarantine.count(block=name, status=QuarantineStatus.QUARANTINED)),
                str(g.quarantine.count(block=name, status=QuarantineStatus.REPLAYED)),
            )
        console.print(table)


def describe_version(g: Guardian, block: str) -> str:
    """The live version; notes the spec's ``active`` when the registry overrides it,
    and an unfinished promotion."""
    spec_active = g.spec.block(block).active
    live = g.active_version(block)
    if live is None:
        return "-"
    text = live if live == spec_active else f"{live} (spec: {spec_active})"
    pending = g.versions.pending(block)
    if pending is not None:
        text += f" -> {pending.to_version} [yellow]PROMOTING[/]"
    return text


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


# ---------------------------------------------------------------- shadow promotion

shadow_app = typer.Typer(
    help="Shadow a candidate version of any block, then promote it or roll back.",
    no_args_is_help=True,
)
app.add_typer(shadow_app, name="shadow")


@contextmanager
def _guarded(root: Path, spec: Path | None):
    """Open Guardian and turn expected errors into clean CLI failures."""
    with _open(root, _spec_for(root, spec)) as g:
        try:
            yield g
        except KeyError as exc:
            console.print(f"[red]{exc.args[0]}[/red]")
            raise typer.Exit(code=2) from exc
        except GuardianError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc


@shadow_app.command("start")
def shadow_start(
    block: str = typer.Argument(..., help="Any block in the spec."),
    version: str = typer.Argument(..., help="One of the block's declared versions."),
    expect_diff: bool = typer.Option(
        False, "--expect-diff", help="An intended behaviour change: promotion needs --approve."
    ),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Run VERSION of BLOCK as a shadow candidate on every subsequent run."""
    with _guarded(root, spec) as g:
        g.shadow_start(block, version, expect_diff=expect_diff)
        console.print(
            f"{block}: shadowing {version} next to live {g.active_version(block)}"
            + (" (expecting differences: promotion needs --approve)" if expect_diff else "")
        )


@shadow_app.command("stop")
def shadow_stop(
    block: str = typer.Argument(..., help="Any block in the spec."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Stop shadowing BLOCK's candidate without promoting it."""
    with _guarded(root, spec) as g:
        shadow = g.shadow_stop(block)
        console.print(f"{block}: stopped shadowing {shadow.version}")


@shadow_app.command("status")
def shadow_status(
    block: str | None = typer.Argument(None, help="Show one block's per-run comparisons."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """List blocks in shadow, or print BLOCK's per-run comparison table."""
    with _guarded(root, spec) as g:
        if block is None:
            console.print(render_shadows(g))
        else:
            console.print(render_shadow_runs(g, block))


@shadow_app.command("promote")
def shadow_promote(
    block: str = typer.Argument(..., help="Any block in the spec."),
    approve: bool = typer.Option(
        False, "--approve", help="Approve a promotion the automatic policy does not allow."
    ),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Promote BLOCK's shadow candidate (or resume an interrupted promotion)."""
    with _guarded(root, spec) as g:
        result = g.promote(block, approve=approve)
        console.print(
            f"{block}: {'resumed and completed' if result.resumed else 'promoted'} "
            f"{result.from_version} -> [green]{result.to_version}[/green] ({result.reason}); "
            f"replayed {result.replay.replayed} quarantined record(s) through "
            f"{result.to_version}, still failing {result.replay.still_failing}; "
            f"{block} is {g.status(block).value}"
        )


@shadow_app.command("rollback")
def shadow_rollback(
    block: str = typer.Argument(..., help="Any block in the spec."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Make BLOCK's previous version active again (no data is rewritten)."""
    with _guarded(root, spec) as g:
        rollback = g.rollback_version(block)
        console.print(
            f"{block}: rolled back {rollback.from_version} -> "
            f"[green]{rollback.to_version}[/green]; last-good is "
            f"{(g.snapshots.last_good(block) or DataRef(block, '-')).run_id}"
        )


def render_shadows(g: Guardian) -> Table:
    table = Table(title=f"Pipeline {g.spec.name!r} - blocks in shadow")
    for column in ("block", "live", "candidate", "runs", "last mode", "streak", "promotion"):
        table.add_column(column)
    for shadow in g.shadows.active():
        runs = g.shadows.runs(shadow.id)
        policy = g.spec.block(shadow.block).shadow_policy()
        streak = 0
        for r in reversed(runs):
            if r.mode is not ShadowMode.PARITY or not r.within_tolerance:
                break
            streak += 1
        if shadow.expect_diff:
            promotion = "needs --approve (expect-diff)"
        elif runs and runs[-1].mode is ShadowMode.ABSOLUTE:
            promotion = "needs --approve (absolute)"
        else:
            promotion = f"auto after {policy.required_runs} ok parity runs"
        table.add_row(
            shadow.block,
            describe_version(g, shadow.block),
            shadow.version,
            str(len(runs)),
            runs[-1].mode.value if runs else "-",
            f"{streak}/{policy.required_runs}",
            promotion,
        )
    if not table.rows:
        table.add_row("(none)", "", "", "", "", "", "")
    return table


def render_shadow_runs(g: Guardian, block: str) -> Table:
    shadow, runs = g.shadow_runs(block)
    if shadow is None:
        raise GuardianError(f"block {block!r} has no candidate in shadow")
    policy = g.spec.block(block).shadow_policy()
    table = Table(
        title=f"{block}: candidate {shadow.version} vs live {g.active_version(block)} "
        f"(max changed {policy.max_changed_fraction:.2%}, min pass rate "
        f"{policy.min_pass_rate:.2%}, {policy.required_runs} runs to auto-promote)"
    )
    for column, justify in [
        ("run", "left"),
        ("mode", "left"),
        ("live rows", "right"),
        ("cand. rows", "right"),
        ("added", "right"),
        ("removed", "right"),
        ("changed", "right"),
        ("changed %", "right"),
        ("changed columns", "left"),
        ("pass rate", "right"),
        ("ok", "left"),
    ]:
        table.add_column(column, justify=justify)  # type: ignore[arg-type]
    for r in runs:
        c = r.comparison
        table.add_row(
            r.run_id,
            r.mode.value,
            str(c.rows_active) if c else "-",
            str(r.rows_passed),
            str(c.added) if c else "-",
            str(c.removed) if c else "-",
            str(c.changed) if c else "-",
            f"{c.changed_fraction:.2%}" if c else "-",
            ", ".join(c.changed_columns) if c and c.changed_columns else "-",
            f"{r.pass_rate:.2%}",
            "[green]yes[/]" if r.within_tolerance else "[red]no[/]",
        )
    return table


# ---------------------------------------------------------------- provenance

_QUALITY_STYLE = {"FRESH": "green", "STALE": "yellow", "FALLBACK": "magenta"}


def _quality(value: str) -> str:
    return f"[{_QUALITY_STYLE.get(value, 'red')}]{value}[/]"


@app.command("impact")
def impact_cmd(
    block: str = typer.Argument(..., help="Any block in the spec."),
    since: str | None = typer.Option(None, "--since", help="Only runs from this run id on."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """List every snapshot touched by a degraded state of BLOCK (its blast radius)."""
    with _guarded(root, spec) as g:
        g.spec.block(block)
        entries = compute_impact(g.provenance, block, since)
        table = Table(title=f"Impact of {block}" + (f" since {since}" if since else ""))
        for column in ("snapshot", "run", "relation", "quality / outcome", "how"):
            table.add_column(column)
        for e in entries:
            table.add_row(
                e.block,
                e.run_id,
                e.relation,
                _quality(e.quality) if e.relation == "downstream" else f"[red]{e.quality}[/]",
                escape(e.reason),
            )
        console.print(table)
        downstream = [e for e in entries if e.relation == "downstream"]
        runs = sorted({e.run_id for e in entries if e.relation == "self"})
        console.print(
            f"{block}: {len(runs)} degraded run(s); {len(downstream)} downstream snapshot(s) "
            f"in {len({e.block for e in downstream})} block(s) touched."
        )


@app.command("lineage")
def lineage_cmd(
    block: str = typer.Argument(..., help="Any block in the spec."),
    run_id: str = typer.Argument(..., help="The run whose snapshot to trace."),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Print the upstream provenance tree of BLOCK's snapshot from RUN_ID."""
    with _guarded(root, spec) as g:
        g.spec.block(block)
        console.print(render_lineage(compute_lineage(g.provenance, block, run_id)))


def _lineage_label(node: LineageNode) -> str:
    p = node.provenance
    via = node.via
    prefix = ""
    if via is not None and via.read.value == "FALLBACK":
        prefix = f"as {via.upstream}, via adapter {via.adapter}: "
    elif via is not None and via.read.value == "STALE":
        prefix = f"stale {via.upstream} ({via.upstream_status}): "
    if p is None:
        return f"{prefix}[bold]{node.snapshot_id}[/]  [dim](no provenance recorded)[/]"
    detail = p.kind + (f", version {p.version}" if p.version else "")
    if p.kind == "replay":
        detail += f", recovered rows from {', '.join(p.replayed_from) or '-'}"
    return f"{prefix}[bold]{node.snapshot_id}[/]  {_quality(p.quality.value)}  [dim]({detail})[/]"


def render_lineage(root: LineageNode) -> Tree:
    tree = Tree(_lineage_label(root))

    def add(branch: Tree, node: LineageNode) -> None:
        for parent in node.parents:
            label = _lineage_label(parent)
            if parent.via is None:  # a replay's base snapshot
                label = f"base: {label}"
            add(branch.add(label), parent)

    add(tree, root)
    return tree


@app.command("drift")
def drift_cmd(
    block: str = typer.Argument(..., help="Any block in the spec."),
    run: str | None = typer.Option(
        None, "--run", help="Run to show (default: the block's latest checked run)."
    ),
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Show BLOCK's statistical drift check for a run: PSI and z-score per column."""
    with _guarded(root, spec) as g:
        if g.spec.block(block).drift is None:
            console.print(f"{block} has no drift policy (add `drift:` to it in the spec).")
            raise typer.Exit(code=1)
        runs = [run] if run else [r.run_id for r in reversed(g.provenance.runs(block))]
        report = next((r for r in (g.drift.get(block, x) for x in runs) if r), None)
        if report is None:
            console.print(
                f"[red]no drift check recorded for {block}"
                + (f" on {run}" if run else "")
                + "[/red]"
            )
            raise typer.Exit(code=1)
        style = {"FAIL": "red", "WARN": "yellow"}.get(report["level"], "green")
        reference = ", ".join(report["reference_runs"]) or "-"
        table = Table(title=f"Drift of {block} on {report['run_id']} vs {reference}")
        for column in (
            "column",
            "kind",
            "PSI",
            "z",
            "nulls ref -> now",
            "mean ref -> now",
            "level",
        ):
            table.add_column(column)
        order = {"FAIL": 0, "WARN": 1}
        for c in sorted(report["columns"], key=lambda c: (order.get(c["level"], 2), -c["psi"])):
            mean = (
                f"{c['mean_ref']:.4g} -> {c['mean_now']:.4g}"
                if c.get("mean_ref") is not None and c.get("mean_now") is not None
                else ""
            )
            level_style = {"FAIL": "red", "WARN": "yellow"}.get(c["level"], "green")
            table.add_row(
                escape(c["column"]),
                c["kind"],
                f"{c['psi']:.3f}",
                "" if c["z"] is None else f"{c['z']:.2f}",
                f"{c['null_rate_ref']:.1%} -> {c['null_rate_now']:.1%}",
                mean,
                f"[{level_style}]{c['level']}[/]",
            )
        console.print(table)
        policy = report["policy"]
        console.print(
            f"[{style}]{report['level']}[/]: {escape(report['reason'])}  "
            f"(warn: {policy['warn']}, fail: {policy['fail']})"
        )


ProviderOption = typer.Option(
    None, "--provider", help="LLM provider: anthropic or fake (default: $GUARDIAN_LLM_PROVIDER)."
)
ModelOption = typer.Option(None, "--model", help="Model name (default: $GUARDIAN_LLM_MODEL).")
FakeResponsesOption = typer.Option(
    None,
    "--fake-responses",
    help="Recorded responses for the fake provider (default: $GUARDIAN_LLM_FAKE_RESPONSES).",
)


def _llm_config(provider: str | None, model: str | None, fake: Path | None) -> LLMConfig:
    return LLMConfig.from_env(
        provider=provider, model=model, fake_responses=str(fake) if fake else None
    )


@app.command("diagnose")
def diagnose_cmd(
    block: str = typer.Argument(..., help="Any block in the spec."),
    run: str | None = typer.Option(
        None, "--run", help="Run to diagnose (default: the block's latest ROLLBACK)."
    ),
    sample_size: int = typer.Option(
        DEFAULT_SAMPLE_SIZE, "--sample-size", min=0, help="Quarantined rows to include."
    ),
    evidence_only: bool = typer.Option(
        False, "--evidence-only", help="Only build and write the evidence bundle (no LLM call)."
    ),
    provider: str | None = ProviderOption,
    model: str | None = ModelOption,
    fake_responses: Path | None = FakeResponsesOption,
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Diagnose why BLOCK failed on a run (advisory: nothing is changed).

    Writes the evidence bundle and the diagnosis to <root>/diagnoses/<block>/<run>/.
    """
    with _guarded(root, spec) as g:
        g.spec.block(block)
        run_id = run or default_run(g, block)
        if evidence_only:
            bundle = build_evidence(g, block, run_id, sample_size=sample_size)
            path = bundle.write(g.root)
            console.print(render_evidence(bundle))
            console.print(f"Evidence written to {path}")
            return
        client = make_client(_llm_config(provider, model, fake_responses))
        result = diagnose(g, block, run_id, client=client, sample_size=sample_size)
        render_diagnosis(result)
        if result.diagnosis.status == FAILED:
            raise typer.Exit(code=1)


def render_evidence(bundle: EvidenceBundle) -> Table:
    table = Table(title=f"Evidence for {bundle.block} on {bundle.run_id}")
    table.add_column("id")
    table.add_column("kind")
    table.add_column("item")
    for item in bundle.items:
        table.add_row(item.id, item.kind, escape(item.title))
    return table


def render_diagnosis(result: DiagnosisResult) -> None:
    d = result.diagnosis
    style = "green" if d.status == ACCEPTED else "yellow" if d.status != FAILED else "red"
    console.print(
        f"[bold]Diagnosis of {d.block} on {d.run_id}[/bold] (advisory, model {escape(d.model)})"
    )
    console.print(
        f"root cause: [bold]{d.root_cause}[/bold]   confidence: {d.confidence:.2f}   "
        f"status: [{style}]{d.status}[/]"
    )
    if d.status != ACCEPTED:
        console.print(f"[{style}]{escape(d.reason or '')}[/]")
        if d.proposed:
            console.print(
                f"model proposed: {escape(str(d.proposed.get('root_cause')))} "
                f"(confidence {d.proposed.get('confidence')}), not trusted"
            )
    else:
        console.print(escape(d.summary))
        table = Table(show_header=True)
        table.add_column("#")
        table.add_column("claim")
        table.add_column("evidence")
        for i, claim in enumerate(d.claims, 1):
            table.add_row(str(i), escape(claim.statement), ", ".join(claim.evidence))
        console.print(table)
    console.print(f"Evidence: {result.evidence_path}")
    console.print(f"Diagnosis: {result.diagnosis_path}")


@app.command("propose")
def propose_cmd(
    block: str = typer.Argument(..., help="Any block in the spec."),
    run: str | None = typer.Option(
        None, "--run", help="Run whose failure to fix (default: the block's latest ROLLBACK)."
    ),
    open_pr: bool = typer.Option(
        False,
        "--open-pr/--dry-run",
        help="Open a draft PR if the fix passes its checks (default: dry run, GitHub untouched).",
    ),
    repo: Path | None = typer.Option(
        None, "--repo", help="Git repository to patch (default: the one holding the spec)."
    ),
    provider: str | None = ProviderOption,
    model: str | None = ModelOption,
    fake_responses: Path | None = FakeResponsesOption,
    spec: Path | None = SpecOption,
    root: Path = RootOption,
) -> None:
    """Propose a verified fix for BLOCK's failure (advisory: never merges or promotes).

    Diagnoses the run if needed, applies the proposed change in a temporary git worktree
    on a new branch, runs the block's unit tests and a shadow run on the failing run's
    inputs. Writes the patch and PR body to <root>/proposals/<block>/<run>/.
    """
    from guardian.agent.propose import FAILED, REJECTED, propose

    spec_path = _spec_for(root, spec)
    with _guarded(root, spec) as g:
        g.spec.block(block)
        client = make_client(_llm_config(provider, model, fake_responses))
        proposal = propose(
            g, block, run, client=client, spec_path=spec_path, repo=repo, open_pr=open_pr
        )
        render_proposal(proposal)
        if proposal.status in (FAILED, REJECTED):
            raise typer.Exit(code=1)


def render_proposal(proposal) -> None:
    style = {"ready": "green", "opened": "green", "note": "cyan"}.get(proposal.status, "red")
    console.print(
        f"[bold]Proposal for {proposal.block} on {proposal.run_id}[/bold] "
        f"(root cause {proposal.root_cause}): [{style}]{proposal.status}[/]"
    )
    if proposal.plan:
        console.print(f"action: {proposal.action}   {escape(proposal.plan.rationale)}")
    if proposal.version:
        console.print(f"new version: {proposal.version} -> {proposal.ref}")
    elif proposal.ref:
        console.print(f"new definition: {proposal.ref}")
    if proposal.tests:
        ok = "passed" if proposal.tests.passed else "[red]failed[/]"
        console.print(f"unit tests: {ok} ({escape(proposal.tests.command)})")
    if proposal.shadow:
        table = Table(title="Shadow run on the failing run's inputs")
        for column in proposal.shadow.columns:
            table.add_column(escape(column))
        for row in proposal.shadow.rows:
            table.add_row(*(escape(c) for c in row))
        console.print(table)
        for note in proposal.shadow.notes:
            console.print(escape(note))
    for reason in proposal.reasons:
        console.print(f"[red]{escape(reason)}[/]")
    if proposal.pr_url:
        console.print(f"Draft PR: {proposal.pr_url}")
    if proposal.dir:
        console.print(f"Proposal: {proposal.dir}")
    if proposal.status == "ready":
        console.print("Dry run: GitHub was not touched. Re-run with --open-pr to open a draft PR.")


eval_app = typer.Typer(help="Evaluate Guardian's agents.", no_args_is_help=True)
app.add_typer(eval_app, name="eval")


@eval_app.command("diagnose")
def eval_diagnose_cmd(
    spec: Path = typer.Argument(Path("demo/pipeline.yaml"), help="Pipeline spec to evaluate on."),
    out: Path = typer.Option(
        Path("bench") / "agent_eval.md", "--out", help="Markdown report (a .json goes next to it)."
    ),
    workdir: Path | None = typer.Option(
        None, "--workdir", help="Keep the cases' state here (default: a temporary directory)."
    ),
    blocks: list[str] | None = typer.Option(None, "--block", help="Only these blocks."),
    faults: list[str] | None = typer.Option(None, "--fault-type", help="Only these fault types."),
    record: Path | None = typer.Option(
        None, "--record", help="Save the model's answers, for replay with --provider fake."
    ),
    provider: str | None = ProviderOption,
    model: str | None = ModelOption,
    fake_responses: Path | None = FakeResponsesOption,
) -> None:
    """Diagnose a labeled fault for every block x fault type and score the answers.

    Uses the configured (real) model unless --provider fake is given.
    """
    import tempfile

    from guardian.agent.diagnose import RecordingClient
    from guardian.agent.eval import FAULT_TYPES, generate_cases, run_eval

    spec_path = find_spec(spec)
    try:
        pipeline = load_spec(spec_path)
        client = make_client(_llm_config(provider, model, fake_responses))
    except SpecError as exc:
        console.print(f"[red]Invalid spec:[/red] {exc}")
        raise typer.Exit(code=2) from exc
    except GuardianError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc
    recorder = RecordingClient(client) if record else None
    with tempfile.TemporaryDirectory(prefix="guardian-eval-") as tmp:
        try:
            cases = generate_cases(
                pipeline,
                workdir or Path(tmp),
                blocks=blocks or None,
                faults=faults or FAULT_TYPES,
            )
        except (KeyError, ValueError) as exc:
            console.print(f"[red]{exc.args[0] if exc.args else exc}[/red]")
            raise typer.Exit(code=2) from exc
        console.print(f"Diagnosing {len(cases)} case(s) with model {escape(client.model)}...")
        report = run_eval(cases, recorder or client)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report.to_markdown(), encoding="utf-8", newline="\n")
    out.with_suffix(".json").write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if recorder and record:
        recorder.save(record)
        console.print(f"Recorded answers: {record}")
    correct = sum(r.correct for r in report.results)
    console.print(
        f"accuracy {correct}/{len(report.results)} ({report.accuracy:.1%}); "
        f"rejected for bad citations: {report.rejected}/{len(report.results)}"
    )
    for fault, (c, n) in report.by_fault().items():
        console.print(f"  {fault}: {c}/{n}")
    console.print(f"Report: {out}")


@eval_app.command("fix")
def eval_fix_cmd(
    spec: Path = typer.Argument(Path("demo/pipeline.yaml"), help="Pipeline spec to evaluate on."),
    out: Path = typer.Option(
        Path("bench") / "agent_fix_eval.md", "--out", help="Markdown report (a .json goes next)."
    ),
    repo: Path | None = typer.Option(
        None, "--repo", help="Git repository to propose fixes against (default: the spec's)."
    ),
    blocks: list[str] | None = typer.Option(None, "--block", help="Only these blocks."),
    record: Path | None = typer.Option(
        None, "--record", help="Save the model's answers, for replay with --provider fake."
    ),
    provider: str | None = ProviderOption,
    model: str | None = ModelOption,
    fake_responses: Path | None = FakeResponsesOption,
) -> None:
    """Fix success: inject a code bug into every block, let the agent diagnose and propose
    a fix (dry run), and check that the fix passes shadow promotion."""
    import tempfile

    from guardian.agent.diagnose import RecordingClient
    from guardian.agent.eval import generate_cases, run_fix_eval

    spec_path = find_spec(spec)
    try:
        pipeline = load_spec(spec_path)
        client = make_client(_llm_config(provider, model, fake_responses))
    except SpecError as exc:
        console.print(f"[red]Invalid spec:[/red] {exc}")
        raise typer.Exit(code=2) from exc
    except GuardianError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc
    recorder = RecordingClient(client) if record else None
    with tempfile.TemporaryDirectory(prefix="guardian-fix-eval-") as tmp:
        try:
            cases = generate_cases(pipeline, Path(tmp), blocks=blocks or None, faults=["code_bug"])
        except (KeyError, ValueError) as exc:
            console.print(f"[red]{exc.args[0] if exc.args else exc}[/red]")
            raise typer.Exit(code=2) from exc
        console.print(
            f"Proposing fixes for {len(cases)} case(s) with model {escape(client.model)}..."
        )
        report = run_fix_eval(
            cases, recorder or client, spec=pipeline, spec_path=spec_path, repo=repo
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report.to_markdown(), encoding="utf-8", newline="\n")
    out.with_suffix(".json").write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if recorder and record:
        recorder.save(record)
        console.print(f"Recorded answers: {record}")
    ok = sum(r.promoted for r in report.results)
    console.print(f"fix success {ok}/{len(report.results)} ({report.success_rate:.1%})")
    for role, (c, n) in report.by_role().items():
        console.print(f"  {role}: {c}/{n}")
    console.print(f"Report: {out}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
