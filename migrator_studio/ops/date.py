from __future__ import annotations

from typing import Literal, Optional

import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists


@tracked("parse_date")
def parse_date(
    df: pd.DataFrame,
    column: str,
    *,
    target: Optional[str] = None,
    format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Parse string column to datetime.

    Args:
        df: Input DataFrame
        column: Column to parse
        target: Target column name (default: replace source column)
        format: Date format string (e.g., '%Y-%m-%d'). If None, pandas infers.

    Example:
        df = parse_date(df, "order_date")
        df = parse_date(df, "date_str", target="date", format="%d/%m/%Y")
    """
    validate_column_exists(df, column, "parse_date")
    result = df.copy()
    target_col = target if target is not None else column

    if format:
        result[target_col] = pd.to_datetime(result[column], format=format, errors="coerce")
    else:
        result[target_col] = pd.to_datetime(result[column], errors="coerce")

    return result


@tracked("format_date")
def format_date(
    df: pd.DataFrame,
    column: str,
    format: str,
    *,
    target: Optional[str] = None,
) -> pd.DataFrame:
    """
    Format datetime column to string.

    Args:
        df: Input DataFrame
        column: Datetime column to format
        format: Output format string (e.g., '%Y-%m-%d')
        target: Target column name (default: replace source column)

    Example:
        df = format_date(df, "order_date", "%Y-%m-%d")
        df = format_date(df, "timestamp", "%d %b %Y", target="date_str")
    """
    validate_column_exists(df, column, "format_date")
    result = df.copy()
    target_col = target if target is not None else column

    # Ensure column is datetime
    dt_col = pd.to_datetime(result[column], errors="coerce")
    result[target_col] = dt_col.dt.strftime(format)

    return result


@tracked("extract_date_part")
def extract_date_part(
    df: pd.DataFrame,
    column: str,
    part: Literal["year", "month", "day", "quarter", "week", "dayofweek"],
    target: str,
) -> pd.DataFrame:
    """
    Extract a component from a datetime column.

    Args:
        df: Input DataFrame
        column: Datetime column to extract from
        part: Component to extract - 'year', 'month', 'day', 'quarter', 'week', 'dayofweek'
        target: Target column name for the extracted value

    Example:
        df = extract_date_part(df, "order_date", "year", "order_year")
        df = extract_date_part(df, "order_date", "month", "order_month")
    """
    validate_column_exists(df, column, "extract_date_part")
    result = df.copy()

    # Ensure column is datetime
    dt_col = pd.to_datetime(result[column], errors="coerce")

    extractors = {
        "year": lambda x: x.dt.year,
        "month": lambda x: x.dt.month,
        "day": lambda x: x.dt.day,
        "quarter": lambda x: x.dt.quarter,
        "week": lambda x: x.dt.isocalendar().week,
        "dayofweek": lambda x: x.dt.dayofweek,
    }

    if part not in extractors:
        raise ValueError(
            f"extract_date_part: Invalid part '{part}'. "
            f"Valid options: {list(extractors.keys())}"
        )

    result[target] = extractors[part](dt_col)
    return result


@tracked("handle_invalid_dates")
def handle_invalid_dates(
    df: pd.DataFrame,
    column: str,
    *,
    fallback: str = "2099-12-31",
) -> pd.DataFrame:
    """
    Replace invalid dates (like 9999-xx-xx) with a fallback value.

    Common in legacy systems where 9999-12-31 represents "no end date".

    Args:
        df: Input DataFrame
        column: Date column to check
        fallback: Replacement value for invalid dates (default: "2099-12-31")

    Example:
        df = handle_invalid_dates(df, "end_date")
        df = handle_invalid_dates(df, "due_date", fallback="2050-01-01")
    """
    validate_column_exists(df, column, "handle_invalid_dates")
    result = df.copy()

    # Convert to string for pattern matching
    str_col = result[column].astype(str)

    # Replace 9999 dates
    mask = str_col.str.startswith("9999")
    result.loc[mask, column] = fallback

    return result
