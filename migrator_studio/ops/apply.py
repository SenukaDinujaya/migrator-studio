from __future__ import annotations

from typing import Any, Callable, Optional

import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists


@tracked("apply_row", affected_columns=lambda p: [p.get("target", "")])
def apply_row(
    df: pd.DataFrame,
    func: Callable[[pd.Series], Any],
    target: str,
) -> pd.DataFrame:
    """
    Apply a function to each row, store result in target column.

    Args:
        df: Input DataFrame
        func: Function that takes a row (pd.Series) and returns a value
        target: Target column name for the result

    Example:
        # Create full name from parts
        df = apply_row(df, lambda row: f"{row['first']} {row['last']}", "full_name")

        # Calculate total
        df = apply_row(df, lambda row: row['price'] * row['qty'], "total")

        # Build child table
        def create_items(row):
            items = []
            if pd.notna(row['Item1']):
                items.append({'code': row['Item1'], 'qty': row['Qty1']})
            return items
        df = apply_row(df, create_items, "items")
    """
    result = df.copy()
    result[target] = df.apply(func, axis=1)
    return result


def _get_apply_column_target(p: dict) -> list[str]:
    """Get target column for apply_column operation."""
    target = p.get("target")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("apply_column", affected_columns=_get_apply_column_target)
def apply_column(
    df: pd.DataFrame,
    column: str,
    func: Callable[[Any], Any],
    *,
    target: Optional[str] = None,
) -> pd.DataFrame:
    """
    Apply a function to each value in a column.

    Args:
        df: Input DataFrame
        column: Column to apply function to
        func: Function that takes a single value and returns a value
        target: Target column name (default: replace source column)

    Example:
        # Clean phone numbers
        df = apply_column(df, "phone", lambda x: x.replace("-", "") if x else x)

        # Extract domain from email
        df = apply_column(df, "email", lambda x: x.split("@")[1] if "@" in str(x) else None, target="domain")
    """
    validate_column_exists(df, column, "apply_column")
    result = df.copy()
    target_col = target if target is not None else column
    result[target_col] = result[column].apply(func)
    return result


@tracked("transform", affected_columns=lambda p: [])
def transform(
    df: pd.DataFrame,
    func: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """
    Apply an arbitrary transformation function to the DataFrame.

    This is the escape hatch for complex transformations that don't fit
    other patterns. The function receives the full DataFrame and must
    return a DataFrame.

    Args:
        df: Input DataFrame
        func: Function that takes a DataFrame and returns a DataFrame

    Example:
        # Custom pivot operation
        def pivot_data(df):
            return df.pivot_table(index='id', columns='category', values='amount')
        df = transform(df, pivot_data)

        # Complex multi-step transformation
        def complex_transform(df):
            df = df[df['status'] == 'Active']
            df['total'] = df['price'] * df['qty']
            return df.groupby('region').agg({'total': 'sum'})
        df = transform(df, complex_transform)
    """
    return func(df.copy())
