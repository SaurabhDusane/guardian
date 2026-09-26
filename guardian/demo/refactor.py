"""A plausible-looking but buggy rewrite of any block, used by the ``code_bug`` fault.

It lives in its own module so the code-change evidence shows an ordinary refactor,
not a fault-injection helper.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd


def rewrite(
    base: Callable[..., pd.DataFrame], rows: Callable[[int], np.ndarray]
) -> Callable[..., pd.DataFrame]:
    """``base`` rewritten: same logic, then values in ``rows(n)`` are mishandled."""

    def block(*args: object, **kwargs: object) -> pd.DataFrame:
        out = base(*args, **kwargs).copy()
        selected = rows(len(out))
        for j, column in enumerate(out.columns):
            values = out[column]
            if pd.api.types.is_bool_dtype(values):
                continue
            if pd.api.types.is_numeric_dtype(values):
                out.iloc[selected, j] = -values.iloc[selected]
            elif pd.api.types.is_datetime64_any_dtype(values):
                out.iloc[selected, j] = pd.NaT
            else:
                out.iloc[selected, j] = values.iloc[selected].map(
                    lambda v: v + " " if isinstance(v, str) else v
                )
        return out

    return block
