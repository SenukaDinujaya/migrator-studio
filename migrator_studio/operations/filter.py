from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pandas as pd
import polars as pl

from ._base import tracked
from ._validation import (
    validate_column_exists,
    validate_comparable,
    validate_date_value,
    validate_membership_values,
)


@tracked("filter_isin", affected_columns=lambda p: [])
def filter_isin(
    df: pl.DataFrame,
    column: str,
    values: Sequence[Any],
) -> pl.DataFrame:
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
    validate_membership_values(df, column, values, "filter_isin")
    return df.filter(pl.col(column).is_in(list(values)))


@tracked("filter_not_isin", affected_columns=lambda p: [])
def filter_not_isin(
    df: pl.DataFrame,
    column: str,
    values: Sequence[Any],
) -> pl.DataFrame:
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
    validate_membership_values(df, column, values, "filter_not_isin")
    return df.filter(~pl.col(column).is_in(list(values)))


@tracked("filter_by_value", affected_columns=lambda p: [])
def filter_by_value(
    df: pl.DataFrame,
    column: str,
    *,
    eq: Any | None = None,
    ne: Any | None = None,
    gt: Any | None = None,
    gte: Any | None = None,
    lt: Any | None = None,
    lte: Any | None = None,
) -> pl.DataFrame:
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
        filter_by_value(df, "amount", gt=100)
        filter_by_value(df, "amount", gte=100, lte=500)
    """
    conditions = {"eq": eq, "ne": ne, "gt": gt, "gte": gte, "lt": lt, "lte": lte}
    provided = {k: v for k, v in conditions.items() if v is not None}

    if not provided:
        raise ValueError(
            "filter_by_value failed: No filter conditions provided. "
            "Specify at least one of: eq, ne, gt, gte, lt, lte."
        )

    validate_column_exists(df, column, "filter_by_value")

    for value in provided.values():
        validate_comparable(df, column, value, "filter_by_value")

    col = pl.col(column)
    dtype = df.schema[column]

    def _coerce_value(val: Any) -> Any:
        """Coerce comparison value for Polars datetime columns."""
        if dtype.is_temporal() and isinstance(val, str):
            return pd.to_datetime(val)
        return val

    expr = pl.lit(True)

    if eq is not None:
        expr = expr & (col == _coerce_value(eq))
    if ne is not None:
        expr = expr & (col != _coerce_value(ne))
    if gt is not None:
        expr = expr & (col > _coerce_value(gt))
    if gte is not None:
        expr = expr & (col >= _coerce_value(gte))
    if lt is not None:
        expr = expr & (col < _coerce_value(lt))
    if lte is not None:
        expr = expr & (col <= _coerce_value(lte))

    return df.filter(expr)


@tracked("filter_null", affected_columns=lambda p: [])
def filter_null(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """
    Keep only rows where column is null/NA.

    Args:
        df: Input DataFrame
        column: Column name to check

    Example:
        df = filter_null(df, "email")
    """
    validate_column_exists(df, column, "filter_null")
    return df.filter(pl.col(column).is_null())


@tracked("filter_not_null", affected_columns=lambda p: [])
def filter_not_null(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """
    Keep only rows where column is not null/NA.

    Args:
        df: Input DataFrame
        column: Column name to check

    Example:
        df = filter_not_null(df, "email")
    """
    validate_column_exists(df, column, "filter_not_null")
    return df.filter(pl.col(column).is_not_null())


@tracked("filter_date", affected_columns=lambda p: [])
def filter_date(
    df: pl.DataFrame,
    column: str,
    *,
    after: str | datetime | pd.Timestamp | None = None,
    before: str | datetime | pd.Timestamp | None = None,
    on_or_after: str | datetime | pd.Timestamp | None = None,
    on_or_before: str | datetime | pd.Timestamp | None = None,
) -> pl.DataFrame:
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
        filter_date(df, "order_date", after="2025-01-01")
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

    # Ensure column is datetime — try cast first, then str parse
    dtype = df.schema[column]
    if dtype == pl.Utf8 or dtype == pl.String:
        date_col = pl.col(column).str.to_datetime(strict=False)
    elif dtype.is_temporal():
        date_col = pl.col(column)
    else:
        date_col = pl.col(column).cast(pl.Datetime, strict=False)

    expr = pl.lit(True)

    if after is not None:
        after_dt = validate_date_value(after, "after", "filter_date")
        expr = expr & (date_col > pl.lit(after_dt).cast(pl.Datetime))

    if before is not None:
        before_dt = validate_date_value(before, "before", "filter_date")
        expr = expr & (date_col < pl.lit(before_dt).cast(pl.Datetime))

    if on_or_after is not None:
        on_or_after_dt = validate_date_value(on_or_after, "on_or_after", "filter_date")
        expr = expr & (date_col >= pl.lit(on_or_after_dt).cast(pl.Datetime))

    if on_or_before is not None:
        on_or_before_dt = validate_date_value(on_or_before, "on_or_before", "filter_date")
        expr = expr & (date_col <= pl.lit(on_or_before_dt).cast(pl.Datetime))

    return df.filter(expr)


@tracked("sanitize_data", affected_columns=lambda p: [])
def sanitize_data(
    df: pl.DataFrame,
    *,
    ignore_cols: list[str] | None = None,
    strip_whitespace: bool = True,
    empty_val: str | None = "",
) -> pl.DataFrame:
    """
    Clean the entire DataFrame by stripping whitespace and optionally replacing empty strings.

    Args:
        df: Input DataFrame
        ignore_cols: List of column names to skip (default: None)
        strip_whitespace: Strip leading/trailing whitespace (default: True)
        empty_val: Value to use for empty strings (default: ""). Set to None to convert to null.

    Example:
        df = sanitize_data(df)
        df = sanitize_data(df, empty_val=None)
    """
    ignore_cols = ignore_cols or []
    exprs = []

    for col_name in df.columns:
        if col_name in ignore_cols:
            continue

        dtype = df.schema[col_name]
        if dtype == pl.Utf8 or dtype == pl.String:
            col_expr = pl.col(col_name)
            if strip_whitespace:
                col_expr = col_expr.str.strip_chars()
            if empty_val != "":
                if empty_val is None:
                    col_expr = pl.when(col_expr == "").then(None).otherwise(col_expr)
                else:
                    col_expr = pl.when(col_expr == "").then(pl.lit(empty_val)).otherwise(col_expr)
            exprs.append(col_expr.alias(col_name))

    if exprs:
        return df.with_columns(exprs)
    return df


__all__ = ["filter_isin", "filter_not_isin", "filter_by_value", "filter_null", "filter_not_null", "filter_date", "sanitize_data"]
