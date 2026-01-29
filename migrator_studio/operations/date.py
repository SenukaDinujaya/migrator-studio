from __future__ import annotations

from typing import Literal

import polars as pl

from ._base import tracked
from ._validation import validate_column_exists


def _get_date_target(p: dict) -> list[str]:
    """Get target column for date operations."""
    target = p.get("target")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("parse_date", affected_columns=_get_date_target)
def parse_date(
    df: pl.DataFrame,
    column: str,
    *,
    target: str | None = None,
    format: str | None = None,
) -> pl.DataFrame:
    """
    Parse string column to datetime.

    Args:
        df: Input DataFrame
        column: Column to parse
        target: Target column name (default: replace source column)
        format: Date format string (e.g., '%Y-%m-%d'). If None, Polars infers.

    Example:
        df = parse_date(df, "order_date")
        df = parse_date(df, "date_str", target="date", format="%d/%m/%Y")
    """
    validate_column_exists(df, column, "parse_date")
    target_col = target if target is not None else column

    if format:
        expr = pl.col(column).str.to_datetime(format, strict=False)
    else:
        expr = pl.col(column).str.to_datetime(strict=False)

    return df.with_columns(expr.alias(target_col))


@tracked("format_date", affected_columns=_get_date_target)
def format_date(
    df: pl.DataFrame,
    column: str,
    format: str,
    *,
    target: str | None = None,
) -> pl.DataFrame:
    """
    Format datetime column to string.

    Args:
        df: Input DataFrame
        column: Datetime column to format
        format: Output format string (e.g., '%Y-%m-%d')
        target: Target column name (default: replace source column)

    Example:
        df = format_date(df, "order_date", "%Y-%m-%d")
    """
    validate_column_exists(df, column, "format_date")
    target_col = target if target is not None else column

    # Ensure column is datetime, then format
    expr = pl.col(column).cast(pl.Datetime, strict=False).dt.strftime(format)
    return df.with_columns(expr.alias(target_col))


@tracked("extract_date_part", affected_columns=lambda p: [p.get("target", "")])
def extract_date_part(
    df: pl.DataFrame,
    column: str,
    part: Literal["year", "month", "day", "quarter", "week", "dayofweek"],
    target: str,
) -> pl.DataFrame:
    """
    Extract a component from a datetime column.

    Args:
        df: Input DataFrame
        column: Datetime column to extract from
        part: Component to extract
        target: Target column name

    Example:
        df = extract_date_part(df, "order_date", "year", "order_year")
    """
    validate_column_exists(df, column, "extract_date_part")

    dt_col = pl.col(column).cast(pl.Datetime, strict=False)

    extractors = {
        "year": dt_col.dt.year(),
        "month": dt_col.dt.month(),
        "day": dt_col.dt.day(),
        "quarter": dt_col.dt.quarter(),
        "week": dt_col.dt.week(),
        "dayofweek": dt_col.dt.weekday(),
    }

    if part not in extractors:
        raise ValueError(
            f"extract_date_part: Invalid part '{part}'. "
            f"Valid options: {list(extractors.keys())}"
        )

    return df.with_columns(extractors[part].alias(target))


@tracked("handle_invalid_dates", affected_columns=lambda p: [p.get("column", "")])
def handle_invalid_dates(
    df: pl.DataFrame,
    column: str,
    *,
    fallback: str = "2099-12-31",
) -> pl.DataFrame:
    """
    Replace invalid dates (like 9999-xx-xx) with a fallback value.

    Args:
        df: Input DataFrame
        column: Date column to check
        fallback: Replacement value for invalid dates (default: "2099-12-31")

    Example:
        df = handle_invalid_dates(df, "end_date")
    """
    validate_column_exists(df, column, "handle_invalid_dates")

    str_col = pl.col(column).cast(pl.Utf8)
    return df.with_columns(
        pl.when(str_col.str.starts_with("9999"))
        .then(pl.lit(fallback))
        .otherwise(pl.col(column))
        .alias(column)
    )


__all__ = ["parse_date", "format_date", "extract_date_part", "handle_invalid_dates"]
