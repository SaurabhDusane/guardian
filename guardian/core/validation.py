"""Row-level validation: split a frame into good rows and bad rows.

Row-level failures (a check or coercion failing for specific rows) send those rows to
``bad`` with the rule and reason recorded per row. Failures that cannot be attributed
to rows (missing column, frame-level check) are a *schema-level* failure: the whole
output is unusable and ``schema_error`` is set.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError, SchemaErrors

RULE_COL = "_guardian_rule"
REASON_COL = "_guardian_reason"


@dataclass(frozen=True)
class ValidationResult:
    """``good``: rows that passed (coerced by the schema), original index labels kept.

    ``bad``: the original, uncoerced failing rows plus ``RULE_COL`` and ``REASON_COL``.
    ``schema_error``: set when the frame failed as a whole; then ``good`` is empty and
    ``bad`` holds every row.
    """

    good: pd.DataFrame
    bad: pd.DataFrame
    schema_error: str | None = None
    # Positions (0-based, into the validated frame) of the bad rows. Needed to map bad
    # rows back to input rows when the index has duplicate labels. ``None`` means
    # "match by index label", which custom validators may rely on for unique indexes.
    bad_positions: tuple[int, ...] | None = None

    @property
    def ok(self) -> bool:
        return self.schema_error is None


@runtime_checkable
class Validator(Protocol):
    """Splits a frame into good and bad rows (with rules and reasons)."""

    def validate(self, df: pd.DataFrame) -> ValidationResult: ...


def _with_rules(rows: pd.DataFrame, rules: list[str], reasons: list[str]) -> pd.DataFrame:
    out = rows.copy()
    out[RULE_COL] = pd.Series(rules, index=out.index, dtype=object)
    out[REASON_COL] = pd.Series(reasons, index=out.index, dtype=object)
    return out


def schema_failure(df: pd.DataFrame, message: str, rule: str = "schema") -> ValidationResult:
    """A result that rejects every row of ``df`` for a frame-level reason."""
    return ValidationResult(
        good=df.iloc[0:0].copy(),
        bad=_with_rules(df, [rule] * len(df), [message] * len(df)),
        schema_error=message,
        bad_positions=tuple(range(len(df))),
    )


class PassthroughValidator:
    """Accepts every row. Used for blocks without a schema."""

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        return ValidationResult(good=df, bad=_with_rules(df.iloc[0:0], [], []), bad_positions=())


class PanderaValidator:
    def __init__(self, schema: Any) -> None:
        if isinstance(schema, type) and issubclass(schema, pa.DataFrameModel):
            schema = schema.to_schema()
        if not isinstance(schema, pa.DataFrameSchema):
            raise TypeError(f"expected a pandera DataFrameSchema or DataFrameModel, got {schema!r}")
        self.schema = schema

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        # Work on positional labels so duplicate / non-unique indexes are handled.
        work = df.reset_index(drop=True)
        failures: dict[int, list[tuple[str, str]]] = {}
        remaining = work
        while True:
            try:
                coerced = self.schema.validate(remaining, lazy=True)
                break
            except SchemaErrors as exc:
                cases = exc.failure_cases
            except SchemaError as exc:  # non-lazy style error (e.g. strict schema)
                return schema_failure(df, _one_line(str(exc)))
            except Exception as exc:  # checks blowing up on unexpected data
                return schema_failure(df, f"validation raised {type(exc).__name__}: {exc}")

            row_cases = cases[cases["index"].notna()]
            if row_cases.empty:
                frame_cases = cases[cases["index"].isna()]
                return schema_failure(df, _describe_frame_failures(frame_cases))
            for case in row_cases.itertuples(index=False):
                pos = int(case.index)
                failures.setdefault(pos, []).append(_rule_and_reason(case))
            remaining = work.drop(index=list(failures), errors="ignore")
            # Loop: frame-level failures caused only by bad rows (e.g. a check that
            # errors on an uncoercible value) disappear once those rows are removed.

        bad_positions = sorted(failures)
        good = coerced.copy()
        good.index = df.index[good.index.to_numpy()]
        rules, reasons = [], []
        for pos in bad_positions:
            pairs = list(dict.fromkeys(failures[pos]))
            rules.append(";".join(dict.fromkeys(rule for rule, _ in pairs)))
            reasons.append("; ".join(reason for _, reason in pairs))
        bad = _with_rules(df.iloc[bad_positions], rules, reasons)
        return ValidationResult(good=good, bad=bad, bad_positions=tuple(bad_positions))


def _rule_and_reason(case: Any) -> tuple[str, str]:
    column = case.column if isinstance(case.column, str) else None
    rule = f"{column}:{case.check}" if column else str(case.check)
    where = f"column {column!r}" if column else "row"
    return rule, f"{where} failed {case.check} (value={case.failure_case!r})"


def _describe_frame_failures(cases: pd.DataFrame) -> str:
    parts = []
    for case in cases.itertuples(index=False):
        if case.check == "column_in_dataframe":
            parts.append(f"missing column {case.failure_case!r}")
        else:
            target = f"column {case.column!r}" if isinstance(case.column, str) else "frame"
            parts.append(f"{target} failed {case.check}: {_one_line(str(case.failure_case))}")
    return "schema: " + "; ".join(dict.fromkeys(parts))


def _one_line(text: str, limit: int = 300) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."
