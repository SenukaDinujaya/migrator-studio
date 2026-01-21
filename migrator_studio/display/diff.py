"""DataFrame comparison function for inspecting changes."""
from __future__ import annotations

import pandas as pd


def diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    *,
    sample_changes: int = 5,
) -> pd.DataFrame:
    """
    Compare two DataFrames and show the differences.

    Args:
        before_df: The original DataFrame.
        after_df: The modified DataFrame.
        sample_changes: Number of sample changes to include (for future use).

    Returns:
        DataFrame showing comparison metrics between before and after.
    """
    before_cols = set(before_df.columns)
    after_cols = set(after_df.columns)

    columns_added = sorted(after_cols - before_cols)
    columns_removed = sorted(before_cols - after_cols)

    # Find dtype changes for common columns
    common_cols = before_cols & after_cols
    dtype_changes = []
    for col in sorted(common_cols):
        before_dtype = str(before_df[col].dtype)
        after_dtype = str(after_df[col].dtype)
        if before_dtype != after_dtype:
            dtype_changes.append(f"{col}: {before_dtype} -> {after_dtype}")

    # Build comparison records
    records = [
        {
            "metric": "row_count",
            "before": len(before_df),
            "after": len(after_df),
            "change": len(after_df) - len(before_df),
        },
        {
            "metric": "column_count",
            "before": len(before_df.columns),
            "after": len(after_df.columns),
            "change": len(after_df.columns) - len(before_df.columns),
        },
        {
            "metric": "columns_added",
            "before": "",
            "after": ", ".join(columns_added) if columns_added else "(none)",
            "change": len(columns_added),
        },
        {
            "metric": "columns_removed",
            "before": ", ".join(columns_removed) if columns_removed else "(none)",
            "after": "",
            "change": -len(columns_removed) if columns_removed else 0,
        },
        {
            "metric": "dtype_changes",
            "before": "",
            "after": "; ".join(dtype_changes) if dtype_changes else "(none)",
            "change": len(dtype_changes),
        },
    ]

    return pd.DataFrame(records)
