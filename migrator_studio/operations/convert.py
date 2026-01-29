from __future__ import annotations

from typing import Any

import polars as pl

from ._base import tracked
from ._validation import validate_column_exists


def _get_convert_target(p: dict) -> list[str]:
    """Get target column for convert operations."""
    target = p.get("target")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("to_numeric", affected_columns=_get_convert_target)
def to_numeric(
    df: pl.DataFrame,
    column: str,
    *,
    target: str | None = None,
    errors: str = "coerce",
) -> pl.DataFrame:
    """
    Convert column to numeric type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        errors: How to handle errors - 'coerce' (default) sets invalid to null, 'raise' raises

    Example:
        df = to_numeric(df, "amount")
    """
    validate_column_exists(df, column, "to_numeric")
    target_col = target if target is not None else column
    strict = errors == "raise"
    return df.with_columns(
        pl.col(column).cast(pl.Float64, strict=strict).alias(target_col)
    )


@tracked("to_int", affected_columns=_get_convert_target)
def to_int(
    df: pl.DataFrame,
    column: str,
    *,
    target: str | None = None,
    fill: int = 0,
) -> pl.DataFrame:
    """
    Convert to integer with null handling.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        fill: Value to use for nulls before conversion (default: 0)

    Example:
        df = to_int(df, "quantity")
    """
    validate_column_exists(df, column, "to_int")
    target_col = target if target is not None else column
    return df.with_columns(
        pl.col(column)
        .cast(pl.Float64, strict=False)
        .fill_null(fill)
        .cast(pl.Int64)
        .alias(target_col)
    )


@tracked("to_string", affected_columns=_get_convert_target)
def to_string(
    df: pl.DataFrame,
    column: str,
    *,
    target: str | None = None,
) -> pl.DataFrame:
    """
    Convert column to string type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)

    Example:
        df = to_string(df, "code")
    """
    validate_column_exists(df, column, "to_string")
    target_col = target if target is not None else column
    return df.with_columns(pl.col(column).cast(pl.Utf8).alias(target_col))


@tracked("to_bool", affected_columns=_get_convert_target)
def to_bool(
    df: pl.DataFrame,
    column: str,
    *,
    target: str | None = None,
    true_values: list[Any] | None = None,
) -> pl.DataFrame:
    """
    Convert column to boolean type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        true_values: List of values to treat as True

    Example:
        df = to_bool(df, "active")
    """
    validate_column_exists(df, column, "to_bool")
    target_col = target if target is not None else column

    if true_values is None:
        true_values = [1, "1", "true", "True", "yes", "Yes", "Y", "y"]

    # Cast column to string for comparison to handle mixed int/string true_values
    str_true_values = [str(v) for v in true_values]
    return df.with_columns(
        pl.col(column).cast(pl.Utf8, strict=False).is_in(str_true_values).alias(target_col)
    )


__all__ = ["to_numeric", "to_int", "to_string", "to_bool"]
