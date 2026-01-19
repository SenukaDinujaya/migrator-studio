from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


@tracked("where")
def where(
    df: pd.DataFrame,
    column: str,
    condition: pd.Series,
    then_value: Any,
    else_value: Any = None,
) -> pd.DataFrame:
    """
    Simple if-else assignment using np.where.

    Args:
        df: Input DataFrame
        column: Target column name for the result
        condition: Boolean Series (e.g., df['amount'] > 100)
        then_value: Value when condition is True
        else_value: Value when condition is False (default: None/NaN)

    Example:
        df = where(df, "priority", df["amount"] > 1000, "High", "Normal")
        df = where(df, "discounted", df["qty"] >= 10, True, False)
    """
    result = df.copy()
    result[column] = np.where(condition, then_value, else_value)
    return result


@tracked("case")
def case(
    df: pd.DataFrame,
    column: str,
    conditions: list[tuple[pd.Series, Any]],
    default: Any = None,
) -> pd.DataFrame:
    """
    Multiple condition assignment (like SQL CASE WHEN).

    Args:
        df: Input DataFrame
        column: Target column name for the result
        conditions: List of (condition, value) tuples, evaluated in order
        default: Value when no condition matches (default: None/NaN)

    Example:
        df = case(df, "priority", [
            (df["amount"] > 1000, "High"),
            (df["amount"] > 500, "Medium"),
        ], default="Low")
    """
    result = df.copy()

    # Start with default value
    result[column] = default

    # Apply conditions in reverse order so first match wins
    for condition, value in reversed(conditions):
        result.loc[condition, column] = value

    return result


@tracked("fill_null")
def fill_null(
    df: pd.DataFrame,
    column: str,
    value: Any,
    *,
    target: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fill null/NA values in a column.

    Args:
        df: Input DataFrame
        column: Column with null values to fill
        value: Value to replace nulls with
        target: Target column name (default: replace source column)

    Example:
        df = fill_null(df, "email", "no-email@example.com")
        df = fill_null(df, "quantity", 0)
    """
    validate_column_exists(df, column, "fill_null")
    result = df.copy()
    target_col = target if target is not None else column
    result[target_col] = result[column].fillna(value)
    return result


@tracked("coalesce")
def coalesce(
    df: pd.DataFrame,
    columns: list[str],
    target: str,
) -> pd.DataFrame:
    """
    Get first non-null value from a list of columns.

    Args:
        df: Input DataFrame
        columns: List of columns to check (in order of priority)
        target: Target column name for the result

    Example:
        df = coalesce(df, ["phone_primary", "phone_mobile", "phone_work"], "contact_phone")
        df = coalesce(df, ["MailContact", "ShipContact"], "primary_contact")
    """
    validate_columns_exist(df, columns, "coalesce")
    result = df.copy()

    # Start with first column
    result[target] = result[columns[0]]

    # Fill with subsequent columns
    for col in columns[1:]:
        result[target] = result[target].fillna(result[col])

    return result
