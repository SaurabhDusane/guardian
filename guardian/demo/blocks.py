"""Demo block functions: pure ``DataFrame -> DataFrame`` transforms.

Row-wise blocks (b2..b6) keep the index and can be re-applied to their own output,
which is what makes quarantine replay work for them. Each block keeps the raw input
columns it parses, so a replay can re-derive values from the original text.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from guardian.core.refs import load_ref

DEFAULT_LOADER = "guardian.demo.data_gen:SyntheticOrdersLoader"

FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}
COUNTRY_ALIASES = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "de": "DE",
    "germany": "DE",
    "gb": "GB",
    "uk": "GB",
    "united kingdom": "GB",
    "fr": "FR",
    "france": "FR",
}
REGION_BY_COUNTRY = {"US": "NA", "DE": "EU", "GB": "EU", "FR": "EU"}
STATUS_ALIASES = {"complete": "completed"}


def _text(series: pd.Series) -> pd.Series:
    """Stripped text with blanks as missing."""
    out = series.astype("string").str.strip()
    return out.mask(out == "")


def _number(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return series.astype(float)
    cleaned = _text(series).str.replace(r"[$,\s]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce").astype(float)


# ---------------------------------------------------------------- b1


def ingest(
    replayed: pd.DataFrame | None = None,
    *,
    loader: str = DEFAULT_LOADER,
    **loader_kwargs: Any,
) -> pd.DataFrame:
    """b1: load the raw dataset through a DatasetLoader named by ``loader``.

    On replay Guardian passes the quarantined raw rows back in; a source cannot
    regenerate them, so they are returned unchanged for re-validation.
    """
    if replayed is not None:
        return replayed.copy()
    return load_ref(loader)(**loader_kwargs).load()


# ---------------------------------------------------------------- b2


def parse(df: pd.DataFrame) -> pd.DataFrame:
    """b2: parse text into typed columns (raw columns are kept for replay)."""
    out = df.copy()
    out["order_time"] = pd.to_datetime(
        _text(out["order_ts"]), format="mixed", utc=True, errors="coerce"
    )
    out["amount_value"] = _number(out["amount"])
    out["quantity_value"] = _number(out["quantity"])
    return out


# ---------------------------------------------------------------- b3


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """b3: canonical codes for currency, country, status; normalized email."""
    out = df.copy()
    out["currency_code"] = _text(out["currency"]).str.upper()
    out["country_code"] = _text(out["country"]).str.lower().map(COUNTRY_ALIASES)
    status = _text(out["status"]).str.lower()
    out["status_code"] = status.replace(STATUS_ALIASES)
    out["email_clean"] = _text(out["email"]).str.lower()
    out["customer_id"] = _text(out["customer_id"])
    return out


# ---------------------------------------------------------------- b4


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """b4: business rules - convert to USD, round quantities."""
    out = df.copy()
    rate = out["currency_code"].map(FX_TO_USD).astype(float)
    out["amount_usd"] = (out["amount_value"].astype(float) * rate).round(2)
    out["quantity_value"] = np.floor(out["quantity_value"].astype(float))
    return out


# ---------------------------------------------------------------- b5


NORMALIZED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "country",
    "status",
    "quantity",
    "amount_usd",
    "unit_price_usd",
]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """b5: the canonical, narrow order table."""
    if set(NORMALIZED_COLUMNS) <= set(df.columns) and "order_time" not in df.columns:
        return df[NORMALIZED_COLUMNS].copy()  # already normalized (replay)
    out = pd.DataFrame(index=df.index)
    out["order_id"] = pd.to_numeric(df["order_id"]).astype("int64")
    out["customer_id"] = df["customer_id"].astype("string").astype(object)
    out["order_date"] = pd.to_datetime(df["order_time"], utc=True).dt.strftime("%Y-%m-%d")
    out["country"] = df["country_code"].astype(str)
    out["status"] = df["status_code"].astype(str)
    out["quantity"] = df["quantity_value"].astype("int64")
    out["amount_usd"] = df["amount_usd"].astype(float)
    out["unit_price_usd"] = (out["amount_usd"] / out["quantity"]).round(2)
    return out


# ---------------------------------------------------------------- b6


def _segment(customer_id: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(customer_id.astype("string").str[1:], errors="coerce")
    seg = pd.Series("unknown", index=customer_id.index, dtype=object)
    seg[numeric % 5 == 0] = "vip"
    seg[(numeric % 5 != 0) & numeric.notna()] = "regular"
    return seg


def _order_size(amount_usd: pd.Series) -> pd.Series:
    return pd.cut(
        amount_usd, bins=[-np.inf, 50, 150, np.inf], labels=["small", "medium", "large"]
    ).astype(str)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """b6: add region, customer segment and order-size bucket."""
    out = df.copy()
    out["region"] = out["country"].map(REGION_BY_COUNTRY)
    out["segment"] = _segment(out["customer_id"])
    out["order_size"] = _order_size(out["amount_usd"])
    return out


def b5_to_b6_shape(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback adapter: make b5 output look like b6 output for b8.

    Region and order size are cheap to derive; the (expensive) customer segmentation
    is not available on this path, so it is marked ``unassigned``.
    """
    out = df.copy()
    out["region"] = out["country"].map(REGION_BY_COUNTRY)
    out["segment"] = "unassigned"
    out["order_size"] = _order_size(out["amount_usd"])
    return out


# ---------------------------------------------------------------- b8


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """b8: daily revenue by region and segment (completed orders only).

    Already-aggregated rows (a replay of this block's own output) pass through.
    """
    if "orders" in df.columns and "status" not in df.columns:
        return df.copy()
    done = df[df["status"] == "completed"]
    grouped = (
        done.groupby(["order_date", "region", "segment"], as_index=False)
        .agg(
            orders=("order_id", "count"),
            units=("quantity", "sum"),
            revenue_usd=("amount_usd", "sum"),
        )
        .sort_values(["order_date", "region", "segment"], ignore_index=True)
    )
    grouped["revenue_usd"] = grouped["revenue_usd"].round(2)
    return grouped


# ---------------------------------------------------------------- b7


def customers(df: pd.DataFrame) -> pd.DataFrame:
    """b7: per-customer order summary (orders without a customer id are skipped).

    Already-summarized rows (a replay of this block's own output) pass through.
    """
    if "first_order_date" in df.columns:
        return df.copy()
    known = df[df["customer_id"].notna()]
    summary = (
        known.groupby("customer_id", as_index=False)
        .agg(
            orders=("order_id", "count"),
            revenue_usd=("amount_usd", "sum"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        )
        .sort_values("customer_id", ignore_index=True)
    )
    summary["customer_id"] = summary["customer_id"].astype(str)
    summary["revenue_usd"] = summary["revenue_usd"].round(2)
    return summary
