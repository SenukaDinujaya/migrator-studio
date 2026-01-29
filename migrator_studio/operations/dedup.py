from __future__ import annotations

from typing import Literal

import polars as pl

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


@tracked("drop_duplicates", affected_columns=lambda p: [])
def drop_duplicates(
    df: pl.DataFrame,
    columns: str | list[str],
    keep: Literal["first", "last"] = "first",
) -> pl.DataFrame:
    """
    Remove duplicate rows based on specified columns.

    Args:
        df: Input DataFrame
        columns: Column(s) to check for duplicates
        keep: Which duplicate to keep - 'first' or 'last' (default: 'first')

    Example:
        df = drop_duplicates(df, "customer_id")
    """
    validate_columns_exist(df, columns, "drop_duplicates")

    if isinstance(columns, str):
        columns = [columns]

    return df.unique(subset=columns, keep=keep, maintain_order=True)


@tracked("keep_max", affected_columns=lambda p: [])
def keep_max(
    df: pl.DataFrame,
    by: str | list[str],
    value_column: str,
) -> pl.DataFrame:
    """
    Keep only the row with the maximum value in each group.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        value_column: Column to find max value in

    Example:
        df = keep_max(df, "customer_id", "order_date")
    """
    validate_columns_exist(df, by, "keep_max")
    validate_column_exists(df, value_column, "keep_max")

    if isinstance(by, str):
        by = [by]

    return (
        df.sort(value_column, descending=True)
        .unique(subset=by, keep="first", maintain_order=True)
    )


@tracked("keep_min", affected_columns=lambda p: [])
def keep_min(
    df: pl.DataFrame,
    by: str | list[str],
    value_column: str,
) -> pl.DataFrame:
    """
    Keep only the row with the minimum value in each group.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        value_column: Column to find min value in

    Example:
        df = keep_min(df, "customer_id", "order_date")
    """
    validate_columns_exist(df, by, "keep_min")
    validate_column_exists(df, value_column, "keep_min")

    if isinstance(by, str):
        by = [by]

    return (
        df.sort(value_column, descending=False)
        .unique(subset=by, keep="first", maintain_order=True)
    )


__all__ = ["drop_duplicates", "keep_max", "keep_min"]
