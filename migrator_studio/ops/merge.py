from __future__ import annotations

from typing import Literal, Optional, Union

import pandas as pd

from ._base import tracked
from ._validation import validate_columns_exist


def _prepare_right_df(
    right: pd.DataFrame,
    on: Optional[Union[str, list[str]]],
    right_on: Optional[Union[str, list[str]]],
    select_columns: Optional[list[str]],
) -> pd.DataFrame:
    """Prepare the right DataFrame by selecting only needed columns."""
    if select_columns is None:
        return right

    join_cols = right_on if right_on else on
    if isinstance(join_cols, str):
        join_cols = [join_cols]

    cols_to_keep = list(set(join_cols or []) | set(select_columns))
    validate_columns_exist(right, cols_to_keep, "merge")

    return right[cols_to_keep]


@tracked("merge_left")
def merge_left(
    df: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: Optional[Union[str, list[str]]] = None,
    left_on: Optional[Union[str, list[str]]] = None,
    right_on: Optional[Union[str, list[str]]] = None,
    select_columns: Optional[list[str]] = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """
    Left join two DataFrames.

    Keeps all rows from left, adds matching columns from right.
    Non-matching rows have null values for right columns.

    Args:
        df: Left DataFrame (all rows kept)
        right: Right DataFrame (matching rows joined)
        on: Column(s) to join on (when same name in both)
        left_on: Column(s) from left DataFrame
        right_on: Column(s) from right DataFrame
        select_columns: Only include these columns from right
        suffixes: Suffixes for overlapping column names

    Example:
        df = merge_left(df, regions, on="RegionID")
        df = merge_left(df, lookup, left_on="Code", right_on="legacy_code")
    """
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    return df.merge(
        right_df,
        how="left",
        on=on,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
    )


@tracked("merge_inner")
def merge_inner(
    df: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: Optional[Union[str, list[str]]] = None,
    left_on: Optional[Union[str, list[str]]] = None,
    right_on: Optional[Union[str, list[str]]] = None,
    select_columns: Optional[list[str]] = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """
    Inner join two DataFrames.

    Keeps only rows that have matches in both DataFrames.
    """
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    return df.merge(
        right_df,
        how="inner",
        on=on,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
    )


@tracked("merge_outer")
def merge_outer(
    df: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: Optional[Union[str, list[str]]] = None,
    left_on: Optional[Union[str, list[str]]] = None,
    right_on: Optional[Union[str, list[str]]] = None,
    select_columns: Optional[list[str]] = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """
    Outer join two DataFrames.

    Keeps all rows from both DataFrames, with nulls where no match exists.
    """
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    return df.merge(
        right_df,
        how="outer",
        on=on,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
    )
