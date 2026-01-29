from __future__ import annotations

import polars as pl

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
    df: pl.DataFrame,
    column: str,
    *,
    target_column: str | None = None,
) -> pl.DataFrame:
    """Convert column values to uppercase.

    Args:
        df: Input DataFrame.
        column: Column to convert.
        target_column: Optional column to write the result to.

    Example:
        df = str_upper(df, "name")
    """
    validate_column_exists(df, column, "str_upper")
    target = target_column if target_column else column
    return df.with_columns(pl.col(column).str.to_uppercase().alias(target))


@tracked("str_lower", affected_columns=_get_string_target)
def str_lower(
    df: pl.DataFrame,
    column: str,
    *,
    target_column: str | None = None,
) -> pl.DataFrame:
    """Convert column values to lowercase.

    Args:
        df: Input DataFrame.
        column: Column to convert.
        target_column: Optional column to write the result to.

    Example:
        df = str_lower(df, "name")
    """
    validate_column_exists(df, column, "str_lower")
    target = target_column if target_column else column
    return df.with_columns(pl.col(column).str.to_lowercase().alias(target))


@tracked("str_strip", affected_columns=_get_string_target)
def str_strip(
    df: pl.DataFrame,
    column: str,
    *,
    target_column: str | None = None,
) -> pl.DataFrame:
    """Strip leading and trailing whitespace from column values.

    Args:
        df: Input DataFrame.
        column: Column to strip.
        target_column: Optional column to write the result to.

    Example:
        df = str_strip(df, "name")
    """
    validate_column_exists(df, column, "str_strip")
    target = target_column if target_column else column
    return df.with_columns(pl.col(column).str.strip_chars().alias(target))


@tracked("str_replace", affected_columns=_get_string_target)
def str_replace(
    df: pl.DataFrame,
    column: str,
    old: str,
    new: str,
    *,
    target_column: str | None = None,
) -> pl.DataFrame:
    """Replace substring in column values.

    Args:
        df: Input DataFrame.
        column: Column to perform replacement on.
        old: Substring to find.
        new: Replacement string.
        target_column: Optional column to write the result to.

    Example:
        df = str_replace(df, "phone", "-", "")
    """
    validate_column_exists(df, column, "str_replace")
    target = target_column if target_column else column
    return df.with_columns(
        pl.col(column).str.replace_all(old, new, literal=True).alias(target)
    )


@tracked("str_regex_replace", affected_columns=_get_string_target)
def str_regex_replace(
    df: pl.DataFrame,
    column: str,
    pattern: str,
    replacement: str,
    *,
    target_column: str | None = None,
) -> pl.DataFrame:
    """Replace regex pattern in column values.

    Args:
        df: Input DataFrame.
        column: Column to perform replacement on.
        pattern: Regular expression pattern to match.
        replacement: Replacement string.
        target_column: Optional column to write the result to.

    Example:
        df = str_regex_replace(df, "phone", r"\\D", "")
    """
    validate_column_exists(df, column, "str_regex_replace")
    target = target_column if target_column else column
    # Convert Python regex backreferences (\1, \2) to Polars/Rust format ($1, $2)
    import re
    pl_replacement = re.sub(r'\\(\d+)', r'$\1', replacement)
    return df.with_columns(
        pl.col(column).str.replace_all(pattern, pl_replacement).alias(target)
    )


__all__ = [
    "str_upper",
    "str_lower",
    "str_strip",
    "str_replace",
    "str_regex_replace",
]
