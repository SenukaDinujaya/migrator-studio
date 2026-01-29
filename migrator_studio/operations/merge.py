from __future__ import annotations

import polars as pl

from ._base import tracked
from ._polars_compat import ensure_polars
from ._validation import validate_columns_exist


def _prepare_right_df(
    right: pl.DataFrame,
    on: str | list[str] | None,
    right_on: str | list[str] | None,
    select_columns: list[str] | None,
) -> pl.DataFrame:
    """Prepare the right DataFrame by selecting only needed columns."""
    if select_columns is None:
        return right

    join_cols = right_on if right_on else on
    if isinstance(join_cols, str):
        join_cols = [join_cols]

    cols_to_keep = list(set(join_cols or []) | set(select_columns))
    validate_columns_exist(right, cols_to_keep, "merge")

    return right.select(cols_to_keep)


def _get_merge_columns(p: dict) -> list[str]:
    """Get columns added by merge operations."""
    select_columns = p.get("select_columns")
    if select_columns:
        return select_columns
    return []


def _do_join(
    df: pl.DataFrame,
    right: pl.DataFrame,
    *,
    how: str,
    on: str | list[str] | None,
    left_on: str | list[str] | None,
    right_on: str | list[str] | None,
    suffix: str,
) -> pl.DataFrame:
    """Perform a Polars join with common parameter handling."""
    if on is not None:
        return df.join(right, on=on, how=how, suffix=suffix)
    else:
        return df.join(right, left_on=left_on, right_on=right_on, how=how, suffix=suffix)


@tracked("merge_left", affected_columns=_get_merge_columns)
def merge_left(
    df: pl.DataFrame,
    right: pl.DataFrame,
    *,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    select_columns: list[str] | None = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pl.DataFrame:
    """
    Left join two DataFrames.

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
    """
    right = ensure_polars(right)
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    return _do_join(
        df, right_df, how="left", on=on, left_on=left_on, right_on=right_on,
        suffix=suffixes[1],
    )


@tracked("merge_inner", affected_columns=_get_merge_columns)
def merge_inner(
    df: pl.DataFrame,
    right: pl.DataFrame,
    *,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    select_columns: list[str] | None = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pl.DataFrame:
    """Inner join two DataFrames."""
    right = ensure_polars(right)
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    return _do_join(
        df, right_df, how="inner", on=on, left_on=left_on, right_on=right_on,
        suffix=suffixes[1],
    )


@tracked("merge_outer", affected_columns=_get_merge_columns)
def merge_outer(
    df: pl.DataFrame,
    right: pl.DataFrame,
    *,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    select_columns: list[str] | None = None,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pl.DataFrame:
    """Outer join two DataFrames."""
    right = ensure_polars(right)
    right_df = _prepare_right_df(right, on, right_on, select_columns)

    # Polars uses "full" for outer join, and requires coalesce for the on keys
    if on is not None:
        result = _do_join(
            df, right_df, how="full", on=on, left_on=left_on, right_on=right_on,
            suffix=suffixes[1],
        )
        # Coalesce the join key columns (Polars produces both left and right versions)
        on_cols = [on] if isinstance(on, str) else on
        coalesce_exprs = []
        for c in on_cols:
            right_name = f"{c}{suffixes[1]}"
            if right_name in result.columns:
                coalesce_exprs.append(
                    pl.coalesce([pl.col(c), pl.col(right_name)]).alias(c)
                )
        if coalesce_exprs:
            result = result.with_columns(coalesce_exprs)
            drop_cols = [f"{c}{suffixes[1]}" for c in on_cols if f"{c}{suffixes[1]}" in result.columns]
            if drop_cols:
                result = result.drop(drop_cols)
        return result
    else:
        return _do_join(
            df, right_df, how="full", on=on, left_on=left_on, right_on=right_on,
            suffix=suffixes[1],
        )


__all__ = ["merge_left", "merge_inner", "merge_outer"]
