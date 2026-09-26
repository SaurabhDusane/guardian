# Guardian

Self-healing maintenance layer for data pipelines. See `CLAUDE.md` for the design.

```
uv sync                      # core + dev tools
uv sync --extra dagster      # include the Dagster adapter
uv run pytest
uv run guardian --help
```
