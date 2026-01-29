from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import polars as pl

from ._base import tracked
from ._polars_compat import ensure_polars, to_pandas
from ._validation import validate_column_exists


@tracked("apply_row", affected_columns=lambda p: [p.get("target", "")])
def apply_row(
    df: pl.DataFrame,
    func: Callable[[pd.Series], Any],
    target: str,
) -> pl.DataFrame:
    """
    Apply a function to each row, store result in target column.

    Note: This operation converts to pandas internally since user lambdas
    expect pandas Series rows. Not vectorizable — use Polars expressions
    where possible for better performance.

    Args:
        df: Input DataFrame
        func: Function that takes a row (pd.Series) and returns a value
        target: Target column name for the result

    Example:
        df = apply_row(df, lambda row: f"{row['first']} {row['last']}", "full_name")
    """
    pdf = to_pandas(df)
    # Ensure None values stay as None (not NaN) for user lambdas
    pdf = pdf.where(pdf.notna(), None)
    pdf[target] = pdf.apply(func, axis=1)
    return ensure_polars(pdf)


def _get_apply_column_target(p: dict) -> list[str]:
    """Get target column for apply_column operation."""
    target = p.get("target")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("apply_column", affected_columns=_get_apply_column_target)
def apply_column(
    df: pl.DataFrame,
    column: str,
    func: Callable[[Any], Any],
    *,
    target: str | None = None,
) -> pl.DataFrame:
    """
    Apply a function to each value in a column.

    Note: Converts to pandas internally for user lambda compatibility.

    Args:
        df: Input DataFrame
        column: Column to apply function to
        func: Function that takes a single value and returns a value
        target: Target column name (default: replace source column)

    Example:
        df = apply_column(df, "phone", lambda x: x.replace("-", "") if x else x)
    """
    validate_column_exists(df, column, "apply_column")
    pdf = to_pandas(df)
    # Ensure None values stay as None (not NaN) for user lambdas
    pdf = pdf.where(pdf.notna(), None)
    target_col = target if target is not None else column
    pdf[target_col] = pdf[column].apply(func)
    return ensure_polars(pdf)


@tracked("transform", affected_columns=lambda p: [])
def transform(
    df: pl.DataFrame,
    func: Callable[[pd.DataFrame], pd.DataFrame],
) -> pl.DataFrame:
    """
    Apply an arbitrary transformation function to the DataFrame.

    Note: Converts to/from pandas for user function compatibility.

    Args:
        df: Input DataFrame
        func: Function that takes a DataFrame and returns a DataFrame

    Example:
        def pivot_data(df):
            return df.pivot_table(index='id', columns='category', values='amount')
        df = transform(df, pivot_data)
    """
    pdf = to_pandas(df)
    pdf = pdf.where(pdf.notna(), None)
    result = func(pdf.copy())
    return ensure_polars(result)


__all__ = ["apply_row", "apply_column", "transform"]
