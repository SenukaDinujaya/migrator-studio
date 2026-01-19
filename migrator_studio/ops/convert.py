from __future__ import annotations

from typing import Any, Optional

import pandas as pd

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
    df: pd.DataFrame,
    column: str,
    *,
    target: Optional[str] = None,
    errors: str = "coerce",
) -> pd.DataFrame:
    """
    Convert column to numeric type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        errors: How to handle errors - 'coerce' (default), 'raise', 'ignore'

    Example:
        df = to_numeric(df, "amount")
        df = to_numeric(df, "price", target="price_num", errors="coerce")
    """
    validate_column_exists(df, column, "to_numeric")
    result = df.copy()
    target_col = target if target is not None else column
    result[target_col] = pd.to_numeric(result[column], errors=errors)
    return result


@tracked("to_int", affected_columns=_get_convert_target)
def to_int(
    df: pd.DataFrame,
    column: str,
    *,
    target: Optional[str] = None,
    fill: int = 0,
) -> pd.DataFrame:
    """
    Convert to integer with null handling.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        fill: Value to use for nulls before conversion (default: 0)

    Example:
        df = to_int(df, "quantity")
        df = to_int(df, "count", fill=-1)
    """
    validate_column_exists(df, column, "to_int")
    result = df.copy()
    target_col = target if target is not None else column
    result[target_col] = pd.to_numeric(result[column], errors="coerce").fillna(fill).astype(int)
    return result


@tracked("to_string", affected_columns=_get_convert_target)
def to_string(
    df: pd.DataFrame,
    column: str,
    *,
    target: Optional[str] = None,
) -> pd.DataFrame:
    """
    Convert column to string type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)

    Example:
        df = to_string(df, "code")
        df = to_string(df, "id", target="id_str")
    """
    validate_column_exists(df, column, "to_string")
    result = df.copy()
    target_col = target if target is not None else column
    result[target_col] = result[column].astype(str)
    return result


@tracked("to_bool", affected_columns=_get_convert_target)
def to_bool(
    df: pd.DataFrame,
    column: str,
    *,
    target: Optional[str] = None,
    true_values: Optional[list[Any]] = None,
) -> pd.DataFrame:
    """
    Convert column to boolean type.

    Args:
        df: Input DataFrame
        column: Column to convert
        target: Target column name (default: replace source column)
        true_values: List of values to treat as True
                     (default: [1, "1", "true", "True", "yes", "Yes", "Y", "y"])

    Example:
        df = to_bool(df, "active")
        df = to_bool(df, "flag", true_values=["Y", "Yes", 1])
    """
    validate_column_exists(df, column, "to_bool")
    result = df.copy()
    target_col = target if target is not None else column

    if true_values is None:
        true_values = [1, "1", "true", "True", "yes", "Yes", "Y", "y"]

    result[target_col] = result[column].isin(true_values)
    return result
