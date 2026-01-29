from __future__ import annotations

import polars as pl

from ._base import tracked
from ._validation import validate_column_exists, validate_columns_exist


def _get_agg_columns(p: dict) -> list[str]:
    """Get columns created by aggregation operations."""
    agg = p.get("agg", {})
    cols = []
    for col, funcs in agg.items():
        if isinstance(funcs, list):
            cols.extend([f"{col}_{f}" for f in funcs])
        else:
            cols.append(col)
    return cols


_AGG_MAP = {
    "sum": lambda c: pl.col(c).sum(),
    "mean": lambda c: pl.col(c).mean(),
    "min": lambda c: pl.col(c).min(),
    "max": lambda c: pl.col(c).max(),
    "count": lambda c: pl.col(c).count(),
    "first": lambda c: pl.col(c).first(),
    "last": lambda c: pl.col(c).last(),
    "std": lambda c: pl.col(c).std(),
    "var": lambda c: pl.col(c).var(),
    "median": lambda c: pl.col(c).median(),
}


@tracked("groupby_agg", affected_columns=_get_agg_columns)
def groupby_agg(
    df: pl.DataFrame,
    by: str | list[str],
    agg: dict[str, str | list[str]],
) -> pl.DataFrame:
    """
    Group by columns and aggregate.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        agg: Aggregation specification — dict mapping column names to
             aggregation function(s).

    Example:
        df = groupby_agg(df, "region", {"amount": "sum", "qty": "sum"})
        df = groupby_agg(df, ["region", "category"], {"amount": ["sum", "mean"]})
    """
    validate_columns_exist(df, by, "groupby_agg")
    for col in agg:
        validate_column_exists(df, col, "groupby_agg")

    if isinstance(by, str):
        by = [by]

    exprs = []
    for col, funcs in agg.items():
        if isinstance(funcs, str):
            funcs = [funcs]
        for func_name in funcs:
            if func_name not in _AGG_MAP:
                raise ValueError(
                    f"groupby_agg: Unknown aggregation function '{func_name}'. "
                    f"Available: {list(_AGG_MAP.keys())}"
                )
            # If multiple funcs for same col, suffix with func name
            alias = f"{col}_{func_name}" if len(funcs) > 1 else col
            exprs.append(_AGG_MAP[func_name](col).alias(alias))

    return df.group_by(by, maintain_order=True).agg(exprs)


@tracked("groupby_concat", affected_columns=lambda p: [p.get("target", "")])
def groupby_concat(
    df: pl.DataFrame,
    by: str | list[str],
    column: str,
    target: str,
    sep: str = " ",
) -> pl.DataFrame:
    """
    Group and concatenate string values.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        column: Column to concatenate values from
        target: Target column name for concatenated result
        sep: Separator between values (default: " ")

    Example:
        df = groupby_concat(df, "order_id", "message", "all_messages", sep=" | ")
    """
    validate_columns_exist(df, by, "groupby_concat")
    validate_column_exists(df, column, "groupby_concat")

    if isinstance(by, str):
        by = [by]

    return df.group_by(by, maintain_order=True).agg(
        pl.col(column).drop_nulls().cast(pl.Utf8).str.join(sep).alias(target)
    )


__all__ = ["groupby_agg", "groupby_concat"]
