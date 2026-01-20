from __future__ import annotations

from typing import Any

import pandas as pd

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


@tracked("copy_column", affected_columns=lambda p: [p.get("target", "")])
def copy_column(
    df: pd.DataFrame,
    source: str,
    target: str,
) -> pd.DataFrame:
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
    result = df.copy()
    result[target] = result[source]
    return result


@tracked("set_value", affected_columns=lambda p: [p.get("column", "")])
def set_value(
    df: pd.DataFrame,
    column: str,
    value: Any,
) -> pd.DataFrame:
    """
    Set a constant value for a column.

    Args:
        df: Input DataFrame
        column: Column name to set (creates if doesn't exist)
        value: Value to set for all rows

    Example:
        df = set_value(df, "company", "ArrowCorp")
    """
    result = df.copy()
    result[column] = value
    return result


@tracked("concat_columns", affected_columns=lambda p: [p.get("target", "")])
def concat_columns(
    df: pd.DataFrame,
    columns: list[str],
    target: str,
    sep: str = " ",
) -> pd.DataFrame:
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
    result = df.copy()
    # Fill NA with empty string to avoid "nan" in output
    result[target] = result[columns].fillna("").astype(str).agg(sep.join, axis=1)
    return result


@tracked("rename_columns", affected_columns=lambda p: list(p.get("mapping", {}).values()))
def rename_columns(
    df: pd.DataFrame,
    mapping: dict[str, str],
) -> pd.DataFrame:
    """
    Rename columns using a mapping dict.

    Args:
        df: Input DataFrame
        mapping: Dict mapping old names to new names

    Example:
        df = rename_columns(df, {"old_name": "new_name", "col1": "column_one"})
    """
    # Validate source columns exist
    missing = [col for col in mapping.keys() if col not in df.columns]
    if missing:
        raise KeyError(
            f"rename_columns failed: Columns {missing} not found in DataFrame. "
            f"Available columns: {list(df.columns)}."
        )
    return df.rename(columns=mapping)


@tracked("drop_columns", affected_columns=lambda p: p.get("columns", []))
def drop_columns(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Drop specified columns.

    Args:
        df: Input DataFrame
        columns: List of column names to drop

    Example:
        df = drop_columns(df, ["temp_col", "unused_col"])
    """
    validate_columns_exist(df, columns, "drop_columns")
    return df.drop(columns=columns)


@tracked("select_columns", affected_columns=lambda p: p.get("columns", []))
def select_columns(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Keep only specified columns (select/project).

    Args:
        df: Input DataFrame
        columns: List of column names to keep

    Example:
        df = select_columns(df, ["id", "name", "status"])
    """
    validate_columns_exist(df, columns, "select_columns")
    return df[columns].copy()
