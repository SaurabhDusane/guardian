"""Pandera schemas for the demo pipeline's blocks."""

from __future__ import annotations

import pandera.pandas as pa

CURRENCIES = ["USD", "EUR", "GBP"]
COUNTRIES = ["US", "DE", "GB", "FR"]
REGIONS = ["NA", "EU"]
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

_text = {"nullable": True, "required": True}

RawSchema = pa.DataFrameSchema(
    {
        "order_id": pa.Column(None, pa.Check.str_matches(r"^\d+$"), nullable=False),
        "customer_id": pa.Column(None, **_text),
        "order_ts": pa.Column(None, **_text),
        "amount": pa.Column(None, **_text),
        "currency": pa.Column(None, **_text),
        "quantity": pa.Column(None, **_text),
        "country": pa.Column(None, **_text),
        "email": pa.Column(None, **_text),
        "status": pa.Column(None, **_text),
    },
    name="RawSchema",
)

ParsedSchema = pa.DataFrameSchema(
    {
        "order_id": pa.Column("int64", coerce=True),
        "order_time": pa.Column("datetime64[us, UTC]", nullable=False, coerce=True),
        "amount_value": pa.Column(float, nullable=False, coerce=True),
        "quantity_value": pa.Column(float, nullable=False, coerce=True),
    },
    name="ParsedSchema",
)

StandardizedSchema = pa.DataFrameSchema(
    {
        "currency_code": pa.Column(str, pa.Check.isin(CURRENCIES), nullable=False),
        "country_code": pa.Column(str, pa.Check.isin(COUNTRIES), nullable=False),
        "status_code": pa.Column(str, nullable=False),
    },
    name="StandardizedSchema",
)

CleanSchema = pa.DataFrameSchema(
    {
        "amount_usd": pa.Column(float, pa.Check.gt(0), nullable=False, coerce=True),
        "quantity_value": pa.Column(float, pa.Check.ge(1), nullable=False, coerce=True),
        "email_clean": pa.Column(str, pa.Check.str_matches(EMAIL_RE), nullable=True),
    },
    name="CleanSchema",
)

NormalizedSchema = pa.DataFrameSchema(
    {
        "order_id": pa.Column("int64", coerce=True),
        "customer_id": pa.Column(str, nullable=True),
        "order_date": pa.Column(str, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
        "country": pa.Column(str, pa.Check.isin(COUNTRIES)),
        "status": pa.Column(str),
        "quantity": pa.Column("int64", pa.Check.ge(1), coerce=True),
        "amount_usd": pa.Column(float, pa.Check.gt(0), coerce=True),
        "unit_price_usd": pa.Column(float, pa.Check.gt(0), coerce=True),
    },
    strict=True,
    unique=["order_id"],
    name="NormalizedSchema",
)

EnrichedSchema = pa.DataFrameSchema(
    {
        "region": pa.Column(str, pa.Check.isin(REGIONS)),
        "segment": pa.Column(str, nullable=False),
        "order_size": pa.Column(str, pa.Check.isin(["small", "medium", "large"])),
    },
    name="EnrichedSchema",
)

AggregateSchema = pa.DataFrameSchema(
    {
        "order_date": pa.Column(str),
        "region": pa.Column(str),
        "segment": pa.Column(str),
        "orders": pa.Column("int64", pa.Check.ge(1), coerce=True),
        "units": pa.Column("int64", pa.Check.ge(1), coerce=True),
        "revenue_usd": pa.Column(float, pa.Check.gt(0), coerce=True),
    },
    unique=["order_date", "region", "segment"],
    name="AggregateSchema",
)
