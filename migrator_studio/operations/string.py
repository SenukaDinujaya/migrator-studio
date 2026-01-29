from __future__ import annotations

import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists


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
    target_column: str | None = None,
) -> pd.DataFrame:
    """Convert column values to uppercase.

    Args:
        df: Input DataFrame.
        column: Column to convert.
        target_column: Optional column to write the result to.
            If not provided, the original column is modified in-place.

    Returns:
        DataFrame with uppercase values.

    Example:
        df = str_upper(df, "name")
        df = str_upper(df, "name", target_column="name_upper")
    """
    validate_column_exists(df, column, "str_upper")
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.upper()
    return result


@tracked("str_lower", affected_columns=_get_string_target)
def str_lower(
    df: pd.DataFrame,
    column: str,
    *,
    target_column: str | None = None,
) -> pd.DataFrame:
    """Convert column values to lowercase.

    Args:
        df: Input DataFrame.
        column: Column to convert.
        target_column: Optional column to write the result to.
            If not provided, the original column is modified in-place.

    Returns:
        DataFrame with lowercase values.

    Example:
        df = str_lower(df, "name")
        df = str_lower(df, "name", target_column="name_lower")
    """
    validate_column_exists(df, column, "str_lower")
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.lower()
    return result


@tracked("str_strip", affected_columns=_get_string_target)
def str_strip(
    df: pd.DataFrame,
    column: str,
    *,
    target_column: str | None = None,
) -> pd.DataFrame:
    """Strip leading and trailing whitespace from column values.

    Args:
        df: Input DataFrame.
        column: Column to strip.
        target_column: Optional column to write the result to.
            If not provided, the original column is modified in-place.

    Returns:
        DataFrame with stripped values.

    Example:
        df = str_strip(df, "name")
        df = str_strip(df, "name", target_column="name_clean")
    """
    validate_column_exists(df, column, "str_strip")
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
    target_column: str | None = None,
) -> pd.DataFrame:
    """Replace substring in column values.

    Args:
        df: Input DataFrame.
        column: Column to perform replacement on.
        old: Substring to find.
        new: Replacement string.
        target_column: Optional column to write the result to.
            If not provided, the original column is modified in-place.

    Returns:
        DataFrame with replaced values.

    Example:
        df = str_replace(df, "phone", "-", "")
    """
    validate_column_exists(df, column, "str_replace")
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
    target_column: str | None = None,
) -> pd.DataFrame:
    """Replace regex pattern in column values.

    Args:
        df: Input DataFrame.
        column: Column to perform replacement on.
        pattern: Regular expression pattern to match.
        replacement: Replacement string (may include backreferences).
        target_column: Optional column to write the result to.
            If not provided, the original column is modified in-place.

    Returns:
        DataFrame with replaced values.

    Example:
        df = str_regex_replace(df, "phone", r"\\D", "")  # Remove non-digits
    """
    validate_column_exists(df, column, "str_regex_replace")
    result = df.copy()
    target = target_column if target_column else column
    result[target] = result[column].str.replace(pattern, replacement, regex=True)
    return result


__all__ = [
    "str_upper",
    "str_lower",
    "str_strip",
    "str_replace",
    "str_regex_replace",
]
