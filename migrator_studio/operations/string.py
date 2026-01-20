from __future__ import annotations

from typing import Optional

import pandas as pd

from ._base import tracked


def _get_string_target(p: dict) -> list[str]:
    """Get target column for string operations."""
    target = p.get("target_column")
    if target:
        return [target]
    return [p.get("column", "")]


@tracked("str_upper", affected_columns=_get_string_target)
def str_upper(
    df: pd.DataFrame,
    column: str,
    *,
    target_column: Optional[str] = None,
) -> pd.DataFrame:
    """Convert column values to uppercase."""
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.upper()
    return result


@tracked("str_lower", affected_columns=_get_string_target)
def str_lower(
    df: pd.DataFrame,
    column: str,
    *,
    target_column: Optional[str] = None,
) -> pd.DataFrame:
    """Convert column values to lowercase."""
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.lower()
    return result


@tracked("str_strip", affected_columns=_get_string_target)
def str_strip(
    df: pd.DataFrame,
    column: str,
    *,
    target_column: Optional[str] = None,
) -> pd.DataFrame:
    """Strip leading/trailing whitespace from column values."""
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.strip()
    return result


@tracked("str_replace", affected_columns=_get_string_target)
def str_replace(
    df: pd.DataFrame,
    column: str,
    old: str,
    new: str,
    *,
    target_column: Optional[str] = None,
) -> pd.DataFrame:
    """Replace substring in column values."""
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.replace(old, new, regex=False)
    return result


@tracked("str_regex_replace", affected_columns=_get_string_target)
def str_regex_replace(
    df: pd.DataFrame,
    column: str,
    pattern: str,
    replacement: str,
    *,
    target_column: Optional[str] = None,
) -> pd.DataFrame:
    """
    Replace regex pattern in column values.

    Example:
        df = str_regex_replace(df, "phone", r"\\D", "")  # Remove non-digits
    """
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.replace(pattern, replacement, regex=True)
    return result
