from __future__ import annotations

from typing import Union

import pandas as pd

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


@tracked("groupby_agg", affected_columns=_get_agg_columns)
def groupby_agg(
    df: pd.DataFrame,
    by: Union[str, list[str]],
    agg: dict[str, Union[str, list[str]]],
) -> pd.DataFrame:
    """
    Group by columns and aggregate.

    Args:
        df: Input DataFrame
        by: Column(s) to group by
        agg: Aggregation specification - dict mapping column names to
             aggregation function(s). Functions can be: 'sum', 'mean',
             'min', 'max', 'count', 'first', 'last', 'std', 'var', etc.

    Example:
        # Single aggregation per column
        df = groupby_agg(df, "region", {"amount": "sum", "qty": "sum"})

        # Multiple aggregations per column
        df = groupby_agg(df, ["region", "category"], {
            "amount": ["sum", "mean"],
            "qty": "sum"
        })
    """
    validate_columns_exist(df, by, "groupby_agg")

    # Validate agg columns exist
    for col in agg.keys():
        validate_column_exists(df, col, "groupby_agg")

    if isinstance(by, str):
        by = [by]

    result = df.groupby(by, as_index=False).agg(agg)

    # Flatten multi-level column names if present
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = [
            f"{col}_{func}" if func else col
            for col, func in result.columns
        ]

    return result.reset_index(drop=True)


@tracked("groupby_concat", affected_columns=lambda p: [p.get("target", "")])
def groupby_concat(
    df: pd.DataFrame,
    by: Union[str, list[str]],
    column: str,
    target: str,
    sep: str = " ",
) -> pd.DataFrame:
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
        df = groupby_concat(df, ["customer_id", "year"], "item_code", "all_items", sep=", ")
    """
    validate_columns_exist(df, by, "groupby_concat")
    validate_column_exists(df, column, "groupby_concat")

    if isinstance(by, str):
        by = [by]

    # Group and concatenate
    grouped = df.groupby(by, as_index=False).agg(
        **{target: (column, lambda x: sep.join(x.dropna().astype(str)))}
    )

    return grouped.reset_index(drop=True)
