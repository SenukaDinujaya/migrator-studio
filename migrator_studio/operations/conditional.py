from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

from ._base import tracked
from ._polars_compat import ensure_polars_series
from ._validation import validate_column_exists, validate_columns_exist


@tracked("where", affected_columns=lambda p: [p.get("column", "")])
def where(
    df: pl.DataFrame,
    column: str,
    condition: pl.Series | pl.Expr | pd.Series,
    then_value: Any,
    else_value: Any = None,
) -> pl.DataFrame:
    """
    Simple if-else assignment.

    Args:
        df: Input DataFrame
        column: Target column name for the result
        condition: Boolean Series or Polars Expr
        then_value: Value when condition is True
        else_value: Value when condition is False (default: None/null)

    Example:
        df = where(df, "priority", df["amount"] > 1000, "High", "Normal")
    """
    if isinstance(condition, pd.Series):
        condition = ensure_polars_series(condition)

    if isinstance(condition, pl.Series):
        return df.with_columns(
            pl.when(condition).then(pl.lit(then_value)).otherwise(pl.lit(else_value)).alias(column)
        )

    # condition is a pl.Expr
    return df.with_columns(
        pl.when(condition).then(pl.lit(then_value)).otherwise(pl.lit(else_value)).alias(column)
    )


@tracked("case", affected_columns=lambda p: [p.get("column", "")])
def case(
    df: pl.DataFrame,
    column: str,
    conditions: list[tuple[pl.Series | pl.Expr | pd.Series, Any]],
    default: Any = None,
) -> pl.DataFrame:
    """
    Multiple condition assignment (like SQL CASE WHEN).

    Args:
        df: Input DataFrame
        column: Target column name for the result
        conditions: List of (condition, value) tuples, evaluated in order
        default: Value when no condition matches (default: None/null)

    Example:
        df = case(df, "priority", [
            (df["amount"] > 1000, "High"),
            (df["amount"] > 500, "Medium"),
        ], default="Low")
    """
    if not conditions:
        return df.with_columns(pl.lit(default).alias(column))

    # Build chained when/then
    first_cond, first_val = conditions[0]
    if isinstance(first_cond, pd.Series):
        first_cond = ensure_polars_series(first_cond)
    expr = pl.when(first_cond).then(pl.lit(first_val))

    for cond, val in conditions[1:]:
        if isinstance(cond, pd.Series):
            cond = ensure_polars_series(cond)
        expr = expr.when(cond).then(pl.lit(val))

    expr = expr.otherwise(pl.lit(default))
    return df.with_columns(expr.alias(column))


def _get_fill_null_target(p: dict) -> list[str]:
    """Get target column for fill_null operation."""
    target = p.get("target")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("fill_null", affected_columns=_get_fill_null_target)
def fill_null(
    df: pl.DataFrame,
    column: str,
    value: Any,
    *,
    target: str | None = None,
) -> pl.DataFrame:
    """
    Fill null/NA values in a column.

    Args:
        df: Input DataFrame
        column: Column with null values to fill
        value: Value to replace nulls with
        target: Target column name (default: replace source column)

    Example:
        df = fill_null(df, "email", "no-email@example.com")
    """
    validate_column_exists(df, column, "fill_null")
    target_col = target if target is not None else column
    return df.with_columns(pl.col(column).fill_null(value).alias(target_col))


@tracked("coalesce", affected_columns=lambda p: [p.get("target", "")])
def coalesce(
    df: pl.DataFrame,
    columns: list[str],
    target: str,
) -> pl.DataFrame:
    """
    Get first non-null value from a list of columns.

    Args:
        df: Input DataFrame
        columns: List of columns to check (in order of priority)
        target: Target column name for the result

    Example:
        df = coalesce(df, ["phone_primary", "phone_mobile"], "contact_phone")
    """
    validate_columns_exist(df, columns, "coalesce")
    return df.with_columns(
        pl.coalesce([pl.col(c) for c in columns]).alias(target)
    )


__all__ = ["where", "case", "fill_null", "coalesce"]
