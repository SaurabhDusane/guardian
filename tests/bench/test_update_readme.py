"""bench/update_readme.py: the README Results table is rendered from results.json only."""

import copy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT / "bench"))

import run_bench  # noqa: E402
import update_readme  # noqa: E402


def series(measurement, config, samples, unit="s", **notes):
    s = run_bench.Series(measurement, 10_000, config, f"label {config}", unit, list(samples))
    s.notes.update(notes)
    return s.summary()


@pytest.fixture
def results() -> dict:
    summaries = [
        series("overhead", "bare", [1.0, 1.0, 1.1, 0.9, 1.0]),
        series("overhead", "core", [1.5] * 5),
        series("overhead", "provenance", [1.6] * 5),
        series("overhead", "observability", [2.0] * 5),
        series("throughput", "clean", [1000.0] * 5, unit="rows/s"),
        series("throughput", "protected_crash", [900.0] * 5, unit="rows/s"),
        series("throughput", "unprotected_crash", [1100.0] * 5, unit="rows/s"),
        series("recovery", "1%", [1.2] * 5, rows_quarantined=100),
        series("recovery", "cli_floor", [1.0] * 5),
        series("shadow", "none", [2.0] * 5),
        series("shadow", "one", [2.5] * 5),
    ]
    return {
        "schema": 1,
        "meta": {
            "quick": False,
            "complete": True,
            "started": "2026-09-27T00:00:00+00:00",
            "finished": "2026-09-27T01:00:00+00:00",
            "command": "python bench/run_bench.py",
            "scales": [10_000],
            "repeats": 5,
            "sample_rate": 0.1,
            "quarantine_fractions": [0.01],
            "git_commit": "0123456789abcdef",
            "git_dirty": False,
            "machine": {
                "cpu": "Test CPU",
                "logical_cores": 8,
                "physical_cores": 4,
                "ram_gib": 16.0,
                "os": "TestOS",
                "python": "CPython 3.11",
                "uv": "uv 0.9",
                "packages": {"pandas": "3.0"},
            },
        },
        "blocks": {"source": ["b1"], "fallback_protected (crashed)": "b6"},
        "series": summaries,
        "derived": run_bench.derive(summaries),
    }


def test_quartiles_are_inclusive() -> None:
    assert run_bench.quartiles([5.0, 1.0, 3.0, 2.0, 4.0]) == (2.0, 3.0, 4.0)
    assert run_bench.quartiles([2.0]) == (2.0, 2.0, 2.0)


def test_derived_figures_come_from_medians(results) -> None:
    derived = {(d["config"], d["metric"]): d["value"] for d in results["derived"]}
    assert derived[("core", "added_pct")] == pytest.approx(50.0)
    assert derived[("observability", "added_pct")] == pytest.approx(100.0)
    assert derived[("protected_crash", "pct_of_clean")] == pytest.approx(90.0)
    assert derived[("one", "added_s")] == pytest.approx(0.5)


def test_table_quotes_every_measurement(results) -> None:
    table = update_readme.summary_table(update_readme.Results(results))
    assert "| Measurement | 10k rows |" in table
    assert "**+50.0%** (1.50 s)" in table  # core over bare
    assert "**+100.0%** (2.00 s)" in table  # + observability
    assert "**90%** of clean" in table
    assert "**1.20 s (IQR 0.00)** (100 rows)" in table
    assert "**+25.0%** (+0.50 s)" in table  # shadow


def test_skipped_configuration_is_marked(results) -> None:
    data = copy.deepcopy(results)
    for s in data["series"]:
        if s["config"] == "observability":
            s["skipped"] = "OpenTelemetry not installed"
    data["derived"] = [d for d in data["derived"] if d["config"] != "observability"]
    table = update_readme.summary_table(update_readme.Results(data))
    assert "+ observability exporters (d): added over (a) | skipped |" in table


def test_update_replaces_only_the_marked_section(results) -> None:
    readme = f"before\n{update_readme.START}\nold numbers\n{update_readme.END}\nafter\n"
    updated = update_readme.update_readme(readme, results)
    assert updated.startswith("before\n") and updated.endswith("\nafter\n")
    assert "old numbers" not in updated and "Test CPU" in updated
    assert update_readme.update_readme(updated, results) == updated  # idempotent


def test_quick_or_unfinished_results_are_refused(tmp_path, results) -> None:
    import json

    readme = tmp_path / "README.md"
    readme.write_text(f"{update_readme.START}\n{update_readme.END}\n", encoding="utf-8")
    for change in ({"quick": True}, {"complete": False}):
        data = copy.deepcopy(results)
        data["meta"].update(change)
        path = tmp_path / "results.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        args = ["--results", str(path), "--readme", str(readme)]
        with pytest.raises(SystemExit):
            update_readme.main(args)
        assert update_readme.main([*args, "--allow-partial"]) == 0
        assert update_readme.main([*args, "--allow-partial", "--check"]) == 0


def test_repo_readme_has_the_results_markers() -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert text.index(update_readme.START) < text.index(update_readme.END)
