"""Messy synthetic e-commerce orders, behind a swappable DatasetLoader interface.

To use a real dataset, implement ``DatasetLoader`` (or use ``CsvLoader``) and point
the ingest block's ``params.loader`` at it in the pipeline spec.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd

RAW_COLUMNS = (
    "order_id",
    "customer_id",
    "order_ts",
    "amount",
    "currency",
    "quantity",
    "country",
    "email",
    "status",
)


@runtime_checkable
class DatasetLoader(Protocol):
    def load(self) -> pd.DataFrame:
        """Return the raw dataset. Columns should match ``RAW_COLUMNS`` (all text)."""
        ...


class CsvLoader:
    """Load a real dataset from CSV, read entirely as text like the synthetic one."""

    def __init__(self, path: str | Path, rename: dict[str, str] | None = None) -> None:
        self.path = Path(path)
        self.rename = rename or {}

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(self.path, dtype=str, keep_default_na=False, na_values=[""])
        return df.rename(columns=self.rename)


class SyntheticOrdersLoader:
    """Deterministic (seeded) messy orders.

    The mess is deliberate and realistic: mixed date formats, currency symbols and
    thousands separators, inconsistent casing and whitespace, country aliases, missing
    ids, negative/zero quantities, invalid emails, unknown currencies, and garbage
    values. Most of it is fixable by the cleaning blocks; ``error_rate`` controls the
    share that is not (which ends up quarantined).
    """

    def __init__(self, rows: int = 500, seed: int = 42, error_rate: float = 0.03) -> None:
        if rows < 0:
            raise ValueError("rows must be >= 0")
        self.rows = rows
        self.seed = seed
        self.error_rate = error_rate

    def load(self) -> pd.DataFrame:
        rng = np.random.default_rng(self.seed)
        n = self.rows

        def pick(options: list[str], p: list[float] | None = None) -> np.ndarray:
            return rng.choice(np.array(options, dtype=object), size=n, p=p)

        def corrupt(values: np.ndarray, bad: list[object], rate: float) -> np.ndarray:
            mask = rng.random(n) < rate
            if mask.any():
                values = values.copy()
                values[mask] = rng.choice(np.array(bad, dtype=object), size=int(mask.sum()))
            return values

        err = self.error_rate
        order_id = np.array([str(1000 + i) for i in range(n)], dtype=object)
        order_id = corrupt(order_id, [None], err / 3)

        customer_id = np.array([f"C{int(c):04d}" for c in rng.integers(1, 120, n)], dtype=object)
        customer_id = corrupt(customer_id, [None, " "], 0.02)

        base = pd.Timestamp("2024-01-01", tz="UTC")
        offsets = pd.to_timedelta(rng.integers(0, 60 * 24 * 3600, n), unit="s")
        stamps = base + offsets
        fmt = rng.integers(0, 3, n)
        order_ts = np.array(
            [
                s.strftime(("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%m/%d/%Y %H:%M")[f])
                for s, f in zip(stamps, fmt, strict=True)
            ],
            dtype=object,
        )
        order_ts = corrupt(order_ts, ["not a date", "", "31/31/2024", None], err)

        value = np.round(rng.gamma(2.0, 40.0, n) + 1, 2)
        style = rng.integers(0, 4, n)
        amount = np.array(
            [
                (f"{v:.2f}", f"${v:,.2f}", f" {v:,.2f} ", f"{v}")[s]
                for v, s in zip(value, style, strict=True)
            ],
            dtype=object,
        )
        amount = corrupt(amount, ["N/A", "-12.50", "abc", None], err)

        currency = pick(["USD", "usd", " USD", "EUR", "eur ", "GBP", "gbp"])
        currency = corrupt(currency, ["XXX", "???"], err / 2)

        quantity = pick(["1", "1", "1", "2", "3", " 2", "4.0"])
        quantity = corrupt(quantity, ["0", "-1", ""], err / 2)

        country = pick(["US", "usa", "United States", "DE", "germany", "GB", "uk", " fr", "FR"])
        country = corrupt(country, ["Atlantis", None], err / 3)

        users = rng.integers(1, 300, n)
        domain = pick(["example.com", "Example.COM", "mail.test"])
        email = np.array([f"User{u}@{d}" for u, d in zip(users, domain, strict=True)], dtype=object)
        email = corrupt(email, ["bob[at]example.com", "no-email", None], err)

        status = pick(["completed", "COMPLETED", "Complete", "cancelled", "refunded", "pending"])

        return pd.DataFrame(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": order_ts,
                "amount": amount,
                "currency": currency,
                "quantity": quantity,
                "country": country,
                "email": email,
                "status": status,
            },
            columns=list(RAW_COLUMNS),
        )
