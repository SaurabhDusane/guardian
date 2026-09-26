import pandas as pd
import pandera.pandas as pa
import pytest

from guardian.core.validation import (
    REASON_COL,
    RULE_COL,
    PanderaValidator,
    PassthroughValidator,
    Validator,
)

from .conftest import AMOUNT_SCHEMA


class AmountModel(pa.DataFrameModel):
    id: int
    amount: float = pa.Field(ge=0, coerce=True)


@pytest.fixture
def validator() -> PanderaValidator:
    return PanderaValidator(AMOUNT_SCHEMA)


def test_protocol_conformance(validator) -> None:
    assert isinstance(validator, Validator)
    assert isinstance(PassthroughValidator(), Validator)


def test_all_good(validator) -> None:
    df = pd.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]})
    result = validator.validate(df)
    assert result.ok and result.bad.empty
    pd.testing.assert_frame_equal(result.good, df)


def test_splits_bad_rows_with_rule_and_reason(validator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "amount": [1.0, -5.0, 2.0]}, index=[10, 20, 30])
    result = validator.validate(df)
    assert result.ok
    assert list(result.good.index) == [10, 30]
    assert list(result.bad.index) == [20]
    assert result.bad_positions == (1,)
    row = result.bad.iloc[0]
    assert row[RULE_COL] == "amount:greater_than_or_equal_to(0)"
    assert "amount" in row[REASON_COL] and "-5.0" in row[REASON_COL]
    # bad rows keep original columns and values
    assert row["amount"] == -5.0


def test_uncoercible_values_are_row_failures_not_schema_failures(validator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "amount": ["1.5", "abc", "2"]})
    result = validator.validate(df)
    assert result.ok
    assert list(result.bad.index) == [1]
    assert "coerce_dtype" in result.bad.iloc[0][RULE_COL]
    assert result.good["amount"].tolist() == [1.5, 2.0]
    assert result.good["amount"].dtype == "float64"


def test_multiple_failures_on_one_row_yield_one_bad_row() -> None:
    schema = pa.DataFrameSchema({"a": pa.Column(int, [pa.Check.ge(0), pa.Check.lt(-100)])})
    result = PanderaValidator(schema).validate(pd.DataFrame({"a": [-1, 5]}))
    assert list(result.bad.index) == [0, 1]  # one bad row per input row, not per failure
    assert result.bad.loc[0, RULE_COL] == "a:greater_than_or_equal_to(0);a:less_than(-100)"
    assert result.bad.loc[1, RULE_COL] == "a:less_than(-100)"
    assert result.bad.loc[0, REASON_COL].count("failed") == 2


def test_missing_column_is_schema_level(validator) -> None:
    df = pd.DataFrame({"id": [1, 2]})
    result = validator.validate(df)
    assert not result.ok
    assert "missing column 'amount'" in result.schema_error
    assert result.good.empty
    assert len(result.bad) == 2
    assert set(result.bad[RULE_COL]) == {"schema"}


def test_frame_level_check_is_schema_level() -> None:
    schema = pa.DataFrameSchema(
        {"a": pa.Column(int)}, checks=[pa.Check(lambda d: len(d) > 5, name="min_rows")]
    )
    result = PanderaValidator(schema).validate(pd.DataFrame({"a": [1, 2]}))
    assert not result.ok
    assert "min_rows" in result.schema_error


def test_duplicate_index_labels(validator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "amount": [1.0, -1.0, 2.0]}, index=[7, 7, 7])
    result = validator.validate(df)
    assert result.bad_positions == (1,)
    assert len(result.good) == 2 and len(result.bad) == 1
    assert result.good["amount"].tolist() == [1.0, 2.0]


def test_accepts_dataframe_model() -> None:
    result = PanderaValidator(AmountModel).validate(
        pd.DataFrame({"id": [1, 2], "amount": [1.0, -2.0]})
    )
    assert len(result.bad) == 1


def test_rejects_non_schema() -> None:
    with pytest.raises(TypeError):
        PanderaValidator("not a schema")


def test_empty_frame(validator) -> None:
    result = validator.validate(pd.DataFrame({"id": pd.Series([], dtype=int), "amount": []}))
    assert result.ok and result.good.empty and result.bad.empty


def test_passthrough() -> None:
    df = pd.DataFrame({"x": [1, None]})
    result = PassthroughValidator().validate(df)
    assert result.ok and result.bad.empty and result.good is df
