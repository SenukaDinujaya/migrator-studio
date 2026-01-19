from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Sequence, Union

import pandas as pd

from ._base import tracked
from ._validation import (
    FilterTypeError,
    validate_column_exists,
    validate_comparable,
    validate_date_value,
    validate_membership_values,
)


@tracked("filter_isin")
def filter_isin(
    df: pd.DataFrame,
    column: str,
    values: Sequence[Any],
) -> pd.DataFrame:
    """
    Keep rows where column value is in the given list.

    Args:
        df: Input DataFrame
        column: Column name to filter on
        values: List of values to keep

    Example:
        df = filter_isin(df, "status", ["Active", "Pending"])
    """
    validate_column_exists(df, column, "filter_isin")
    validate_membership_values(df[column], values, "filter_isin")
    return df[df[column].isin(values)].reset_index(drop=True)


@tracked("filter_not_isin")
def filter_not_isin(
    df: pd.DataFrame,
    column: str,
    values: Sequence[Any],
) -> pd.DataFrame:
    """
    Keep rows where column value is NOT in the given list.

    Args:
        df: Input DataFrame
        column: Column name to filter on
        values: List of values to exclude

    Example:
        df = filter_not_isin(df, "status", ["Deleted", "Archived"])
    """
    validate_column_exists(df, column, "filter_not_isin")
    validate_membership_values(df[column], values, "filter_not_isin")
    return df[~df[column].isin(values)].reset_index(drop=True)


@tracked("filter_by_value")
def filter_by_value(
    df: pd.DataFrame,
    column: str,
    *,
    eq: Optional[Any] = None,
    ne: Optional[Any] = None,
    gt: Optional[Any] = None,
    gte: Optional[Any] = None,
    lt: Optional[Any] = None,
    lte: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Filter rows by value conditions. Multiple conditions are ANDed together.

    Args:
        df: Input DataFrame
        column: Column name to filter on
        eq: Keep rows where column == value
        ne: Keep rows where column != value
        gt: Keep rows where column > value
        gte: Keep rows where column >= value
        lt: Keep rows where column < value
        lte: Keep rows where column <= value

    Examples:
        filter_by_value(df, "amount", gt=100)              # amount > 100
        filter_by_value(df, "amount", gte=100, lte=500)    # 100 <= amount <= 500
        filter_by_value(df, "amount", gt=100, ne=200)      # amount > 100 AND != 200
    """
    conditions = {"eq": eq, "ne": ne, "gt": gt, "gte": gte, "lt": lt, "lte": lte}
    provided = {k: v for k, v in conditions.items() if v is not None}

    if not provided:
        raise ValueError(
            "filter_by_value failed: No filter conditions provided. "
            "Specify at least one of: eq, ne, gt, gte, lt, lte."
        )

    validate_column_exists(df, column, "filter_by_value")
    series = df[column]

    # Validate all provided values
    for value in provided.values():
        validate_comparable(series, value, "filter_by_value")

    # Build combined mask - start with all True
    mask = pd.Series([True] * len(df), index=df.index)

    if eq is not None:
        mask &= series == eq
    if ne is not None:
        mask &= series != ne
    if gt is not None:
        mask &= series > gt
    if gte is not None:
        mask &= series >= gte
    if lt is not None:
        mask &= series < lt
    if lte is not None:
        mask &= series <= lte

    return df[mask].reset_index(drop=True)


@tracked("filter_null")
def filter_null(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Keep only rows where column is null/NA.

    Args:
        df: Input DataFrame
        column: Column name to check

    Example:
        df = filter_null(df, "email")  # Keep rows where email is null
    """
    validate_column_exists(df, column, "filter_null")
    return df[df[column].isna()].reset_index(drop=True)


@tracked("filter_not_null")
def filter_not_null(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Keep only rows where column is not null/NA.

    Args:
        df: Input DataFrame
        column: Column name to check

    Example:
        df = filter_not_null(df, "email")  # Keep rows where email has a value
    """
    validate_column_exists(df, column, "filter_not_null")
    return df[df[column].notna()].reset_index(drop=True)


@tracked("filter_date")
def filter_date(
    df: pd.DataFrame,
    column: str,
    *,
    after: Optional[Union[str, datetime, pd.Timestamp]] = None,
    before: Optional[Union[str, datetime, pd.Timestamp]] = None,
    on_or_after: Optional[Union[str, datetime, pd.Timestamp]] = None,
    on_or_before: Optional[Union[str, datetime, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    Filter rows by date conditions. Multiple conditions are ANDed together.

    Args:
        df: Input DataFrame
        column: Date column name
        after: Keep rows where date > value (exclusive)
        before: Keep rows where date < value (exclusive)
        on_or_after: Keep rows where date >= value (inclusive)
        on_or_before: Keep rows where date <= value (inclusive)

    Examples:
        filter_date(df, "order_date", after="2025-01-01")                         # cutoff
        filter_date(df, "order_date", on_or_after="2025-01-01", before="2026-01-01")  # range
    """
    conditions = {
        "after": after,
        "before": before,
        "on_or_after": on_or_after,
        "on_or_before": on_or_before,
    }
    provided = {k: v for k, v in conditions.items() if v is not None}

    if not provided:
        raise ValueError(
            "filter_date failed: No filter conditions provided. "
            "Specify at least one of: after, before, on_or_after, on_or_before."
        )

    validate_column_exists(df, column, "filter_date")

    # Convert column to datetime
    date_col = pd.to_datetime(df[column], errors="coerce")

    # Build combined mask
    mask = pd.Series([True] * len(df), index=df.index)

    if after is not None:
        after_dt = validate_date_value(after, "after", "filter_date")
        mask &= date_col > after_dt

    if before is not None:
        before_dt = validate_date_value(before, "before", "filter_date")
        mask &= date_col < before_dt

    if on_or_after is not None:
        on_or_after_dt = validate_date_value(on_or_after, "on_or_after", "filter_date")
        mask &= date_col >= on_or_after_dt

    if on_or_before is not None:
        on_or_before_dt = validate_date_value(on_or_before, "on_or_before", "filter_date")
        mask &= date_col <= on_or_before_dt

    return df[mask].reset_index(drop=True)


@tracked("sanitize_data")
def sanitize_data(
    df: pd.DataFrame,
    *,
    ignore_cols: list[str] | None = None,
    strip_whitespace: bool = True,
    empty_val: str | None = "",
) -> pd.DataFrame:
    """
    Clean the entire DataFrame by stripping whitespace and optionally replacing empty strings.

    Applies to all string/object columns except those in ignore_cols.

    Args:
        df: Input DataFrame
        ignore_cols: List of column names to skip (default: None)
        strip_whitespace: Strip leading/trailing whitespace from strings (default: True)
        empty_val: Value to use for empty strings (default: "" keeps them as-is).
                   Set to None to convert empty strings to null.

    Example:
        df = sanitize_data(df)  # Strip whitespace, keep empty strings
        df = sanitize_data(df, empty_val=None)  # Strip whitespace, convert empty to null
        df = sanitize_data(df, ignore_cols=["raw_data"])  # Skip certain columns
    """
    result = df.copy()
    ignore_cols = ignore_cols or []

    for col in result.columns:
        if col in ignore_cols:
            continue

        # Only process string/object columns
        if result[col].dtype == "object":
            if strip_whitespace:
                # Strip whitespace, keeping NaN as NaN
                result[col] = result[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )

            if empty_val != "":
                # Convert empty strings to specified value
                result[col] = result[col].apply(
                    lambda x, ev=empty_val: ev if isinstance(x, str) and x == "" else x
                )

    return result
