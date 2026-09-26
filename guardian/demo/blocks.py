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


def load_orders(loader: str = DEFAULT_LOADER, **loader_kwargs: Any) -> pd.DataFrame:
    """b1's loader: the raw dataset from a DatasetLoader named by ``loader``.

    Guardian calls it once per run; every version of b1 receives its frame, so a
    shadow candidate sees exactly the data the live version saw.
    """
    return load_ref(loader)(**loader_kwargs).load()


def ingest(raw: pd.DataFrame) -> pd.DataFrame:
    """b1 v1: accept the loaded rows as they are.

    On replay Guardian passes the quarantined raw rows back in; they are returned
    unchanged for re-validation.
    """
    return raw.copy()


def ingest_v2(raw: pd.DataFrame) -> pd.DataFrame:
    """b1 v2: also normalizes order ids exported as ``" 1042"`` or ``"#1042"``.

    Identical to v1 on well-formed ids; recovers rows v1 would reject.
    """
    out = raw.copy()
    present = out["order_id"].notna()
    ids = out.loc[present, "order_id"].astype(str).str.strip().str.lstrip("#")
    out.loc[present, "order_id"] = ids
    return out


def ingest_bad(raw: pd.DataFrame) -> pd.DataFrame:
    """b1 v_bad: an off-by-one when "normalizing" ids shifts every order id by one."""
    out = raw.copy()
    ids = pd.to_numeric(out["order_id"], errors="coerce")
    out["order_id"] = (ids + 1).astype("Int64").astype("string").astype(object)
    out.loc[ids.isna(), "order_id"] = None
    return out


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


def parse_v2(df: pd.DataFrame) -> pd.DataFrame:
    """b2 v2: as v1, and also parses day-first dates (``31.01.2024``) and epoch seconds.

    Identical to v1 wherever v1 parses a timestamp; recovers some rows v1 rejects.
    """
    out = parse(df)
    missing = out["order_time"].isna()
    if missing.any():
        text = _text(out.loc[missing, "order_ts"])
        epoch = pd.to_numeric(text, errors="coerce")
        from_epoch = pd.to_datetime(epoch, unit="s", utc=True, errors="coerce")
        dayfirst = pd.to_datetime(text, format="%d.%m.%Y", utc=True, errors="coerce")
        out.loc[missing, "order_time"] = from_epoch.fillna(dayfirst)
    return out


def parse_bad(df: pd.DataFrame) -> pd.DataFrame:
    """b2 v_bad: an off-by-one in the parsed quantity (counts from 1 twice)."""
    out = parse(df)
    out["quantity_value"] = out["quantity_value"] + 1
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


def normalize_v2(df: pd.DataFrame) -> pd.DataFrame:
    """b5 v2: as v1, but never divides by a zero quantity (unit price left empty).

    Identical to v1 for every valid row.
    """
    out = normalize(df)
    zero = out["quantity"] == 0
    if zero.any():
        out.loc[zero, "unit_price_usd"] = float("nan")
    return out


def normalize_bad(df: pd.DataFrame) -> pd.DataFrame:
    """b5 v_bad: unit price divides by ``quantity + 1`` (an off-by-one)."""
    out = normalize(df)
    out["unit_price_usd"] = (out["amount_usd"] / (out["quantity"] + 1)).round(2)
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


def enrich_v2(df: pd.DataFrame) -> pd.DataFrame:
    """b6 v2: as v1, but segments customer ids written as ``" c0042"`` too.

    Identical to v1 on well-formed ids.
    """
    out = df.copy()
    ids = out["customer_id"].astype("string").str.strip().str.upper()
    out["region"] = out["country"].map(REGION_BY_COUNTRY)
    out["segment"] = _segment(ids)
    out["order_size"] = _order_size(out["amount_usd"])
    return out


def enrich_bad(df: pd.DataFrame) -> pd.DataFrame:
    """b6 v_bad: VIP customers are ``id % 5 == 1`` instead of ``== 0`` (off-by-one)."""
    out = enrich(df)
    numeric = pd.to_numeric(out["customer_id"].astype("string").str[1:], errors="coerce")
    known = numeric.notna()
    out.loc[known, "segment"] = np.where(numeric[known] % 5 == 1, "vip", "regular")
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


def customers_v2(df: pd.DataFrame) -> pd.DataFrame:
    """b7 v2: as v1, but groups customer ids case- and whitespace-insensitively.

    Identical to v1 on well-formed ids.
    """
    if "first_order_date" in df.columns:
        return df.copy()
    out = df.copy()
    present = out["customer_id"].notna()
    out.loc[present, "customer_id"] = (
        out.loc[present, "customer_id"].astype(str).str.strip().str.upper()
    )
    return customers(out)


def customers_bad(df: pd.DataFrame) -> pd.DataFrame:
    """b7 v_bad: counts one order too many per customer (off-by-one)."""
    out = customers(df)
    if "first_order_date" in df.columns:
        return out
    out["orders"] = out["orders"] + 1
    return out
