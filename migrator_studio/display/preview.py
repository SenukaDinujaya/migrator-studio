"""DataFrame preview function for development inspection."""
from __future__ import annotations

import pandas as pd


def preview(
    df: pd.DataFrame,
    *,
    sample_rows: int = 10,
    show_nulls: bool = True,
) -> pd.DataFrame:
    """
    Generate a preview DataFrame showing column information.

    Args:
        df: DataFrame to preview.
        sample_rows: Number of sample values to show per column.
        show_nulls: Whether to include null statistics.

    Returns:
        DataFrame with column metadata including types, nulls, and samples.
    """
    total_rows = len(df)
    total_cols = len(df.columns)

    records = []
    for col in df.columns:
        series = df[col]
        null_count = series.isna().sum()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0

        # Get sample non-null values
        non_null = series.dropna()
        sample_values = non_null.head(sample_rows).tolist()
        sample_str = ", ".join(str(v) for v in sample_values)

        record = {
            "column": col,
            "dtype": str(series.dtype),
            "unique_count": series.nunique(dropna=False),
            "sample_values": sample_str,
        }

        if show_nulls:
            record["null_count"] = null_count
            record["null_pct"] = f"{null_pct:.1f}%"

        records.append(record)

    # Reorder columns for better display
    if show_nulls:
        column_order = ["column", "dtype", "null_count", "null_pct", "unique_count", "sample_values"]
    else:
        column_order = ["column", "dtype", "unique_count", "sample_values"]

    result = pd.DataFrame(records)[column_order]

    # Print summary line
    print(f"Total rows: {total_rows} | Total columns: {total_cols}")

    return result
