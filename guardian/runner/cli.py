"""Command-line entry point: `guardian run|replay|status`."""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(help="Guardian: self-healing maintenance layer for data pipelines.")


@app.command()
def run(spec: Path = typer.Argument(..., help="Path to the pipeline YAML spec.")) -> None:
    """Run a pipeline spec end to end."""
    typer.echo("`guardian run` is not implemented yet.", err=True)
    raise typer.Exit(code=1)


@app.command()
def replay(block: str = typer.Argument(..., help="Block whose quarantine to replay.")) -> None:
    """Replay quarantined records for a block."""
    typer.echo("`guardian replay` is not implemented yet.", err=True)
    raise typer.Exit(code=1)


@app.command()
def status() -> None:
    """Show block health and snapshot status."""
    typer.echo("`guardian status` is not implemented yet.", err=True)
    raise typer.Exit(code=1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
