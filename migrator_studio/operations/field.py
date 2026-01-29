from __future__ import annotations

from typing import Any

import polars as pl

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


@tracked("copy_column", affected_columns=lambda p: [p.get("target", "")])
def copy_column(
    df: pl.DataFrame,
    source: str,
    target: str,
) -> pl.DataFrame:
    """
    Copy values from one column to another.

    Args:
        df: Input DataFrame
        source: Source column name to copy from
        target: Target column name to copy to

    Example:
        df = copy_column(df, "first_name", "name")
    """
    validate_column_exists(df, source, "copy_column")
    return df.with_columns(pl.col(source).alias(target))


@tracked("set_value", affected_columns=lambda p: [p.get("column", "")])
def set_value(
    df: pl.DataFrame,
    column: str,
    value: Any,
) -> pl.DataFrame:
    """
    Set a constant value for a column.

    Args:
        df: Input DataFrame
        column: Column name to set (creates if doesn't exist)
        value: Value to set for all rows

    Example:
        df = set_value(df, "company", "ArrowCorp")
    """
    return df.with_columns(pl.lit(value).alias(column))


@tracked("concat_columns", affected_columns=lambda p: [p.get("target", "")])
def concat_columns(
    df: pl.DataFrame,
    columns: list[str],
    target: str,
    sep: str = " ",
) -> pl.DataFrame:
    """
    Concatenate multiple columns into one.

    Args:
        df: Input DataFrame
        columns: List of column names to concatenate
        target: Target column name for the result
        sep: Separator between values (default: " ")

    Example:
        df = concat_columns(df, ["first_name", "last_name"], "full_name")
        df = concat_columns(df, ["city", "state", "zip"], "address", sep=", ")
    """
    validate_columns_exist(df, columns, "concat_columns")
    exprs = [pl.col(c).fill_null(pl.lit("")).cast(pl.Utf8) for c in columns]
    return df.with_columns(pl.concat_str(exprs, separator=sep).alias(target))


@tracked("rename_columns", affected_columns=lambda p: list(p.get("mapping", {}).values()))
def rename_columns(
    df: pl.DataFrame,
    mapping: dict[str, str],
) -> pl.DataFrame:
    """
    Rename columns using a mapping dict.

    Args:
        df: Input DataFrame
        mapping: Dict mapping old names to new names

    Example:
        df = rename_columns(df, {"old_name": "new_name", "col1": "column_one"})
    """
    missing = [col for col in mapping if col not in df.columns]
    if missing:
        raise KeyError(
            f"rename_columns failed: Columns {missing} not found in DataFrame. "
            f"Available columns: {list(df.columns)}."
        )
    return df.rename(mapping)


@tracked("drop_columns", affected_columns=lambda p: p.get("columns", []))
def drop_columns(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """
    Drop specified columns.

    Args:
        df: Input DataFrame
        columns: List of column names to drop

    Example:
        df = drop_columns(df, ["temp_col", "unused_col"])
    """
    validate_columns_exist(df, columns, "drop_columns")
    return df.drop(columns)


@tracked("select_columns", affected_columns=lambda p: p.get("columns", []))
def select_columns(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """
    Keep only specified columns (select/project).

    Args:
        df: Input DataFrame
        columns: List of column names to keep

    Example:
        df = select_columns(df, ["id", "name", "status"])
    """
    validate_columns_exist(df, columns, "select_columns")
    return df.select(columns)


__all__ = ["copy_column", "set_value", "concat_columns", "rename_columns", "drop_columns", "select_columns"]
