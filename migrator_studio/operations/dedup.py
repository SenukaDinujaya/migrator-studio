from __future__ import annotations

from typing import Literal

import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


@tracked("drop_duplicates", affected_columns=lambda p: [])
def drop_duplicates(
    df: pd.DataFrame,
    columns: str | list[str],
    keep: Literal["first", "last"] = "first",
) -> pd.DataFrame:
    """
    Remove duplicate rows based on specified columns.

    Args:
        df: Input DataFrame
        columns: Column(s) to check for duplicates
        keep: Which duplicate to keep - 'first' or 'last' (default: 'first')

    Example:
        df = drop_duplicates(df, "customer_id")
        df = drop_duplicates(df, ["order_num", "branch"], keep="last")
    """
    validate_columns_exist(df, columns, "drop_duplicates")

    if isinstance(columns, str):
        columns = [columns]

    return df.drop_duplicates(subset=columns, keep=keep).reset_index(drop=True)


@tracked("keep_max", affected_columns=lambda p: [])
def keep_max(
    df: pd.DataFrame,
    by: str | list[str],
    value_column: str,
) -> pd.DataFrame:
    """
    Keep only the row with the maximum value in each group.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        value_column: Column to find max value in

    Example:
        df = keep_max(df, "customer_id", "order_date")  # Keep most recent order
        df = keep_max(df, ["region", "category"], "amount")  # Keep highest amount
    """
    validate_columns_exist(df, by, "keep_max")
    validate_column_exists(df, value_column, "keep_max")

    if isinstance(by, str):
        by = [by]

    # Get index of max value in each group
    idx = df.groupby(by, sort=False)[value_column].idxmax()
    return df.loc[idx].reset_index(drop=True)


@tracked("keep_min", affected_columns=lambda p: [])
def keep_min(
    df: pd.DataFrame,
    by: str | list[str],
    value_column: str,
) -> pd.DataFrame:
    """
    Keep only the row with the minimum value in each group.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        value_column: Column to find min value in

    Example:
        df = keep_min(df, "customer_id", "order_date")  # Keep oldest order
        df = keep_min(df, ["region", "category"], "price")  # Keep lowest price
    """
    validate_columns_exist(df, by, "keep_min")
    validate_column_exists(df, value_column, "keep_min")

    if isinstance(by, str):
        by = [by]

    # Get index of min value in each group
    idx = df.groupby(by, sort=False)[value_column].idxmin()
    return df.loc[idx].reset_index(drop=True)


__all__ = ["drop_duplicates", "keep_max", "keep_min"]
