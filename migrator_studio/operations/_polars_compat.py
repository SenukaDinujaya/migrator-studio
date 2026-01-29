"""Polars ↔ Pandas conversion utilities using Arrow zero-copy."""
from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl


DataFrameT = Union[pd.DataFrame, pl.DataFrame]
SeriesT = Union[pd.Series, pl.Series]


def to_polars(df: pd.DataFrame) -> pl.DataFrame:
    """Convert pandas DataFrame to Polars via Arrow (zero-copy when possible).

    Handles mixed-type object columns by converting them to string first,
    since Polars doesn't support heterogeneous dtypes.
    """
    try:
        return pl.from_pandas(df)
    except Exception:
        # Fallback: convert problematic object columns to string
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str)
        return pl.from_pandas(df)


def to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert Polars DataFrame to pandas via Arrow (zero-copy when possible).

    Ensures backward-compatible dtypes: StringDtype → object, etc.
    """
    pdf = df.to_pandas()
    # Convert StringDtype columns back to object for backward compat
    for col in pdf.columns:
        if pd.api.types.is_string_dtype(pdf[col]) and pdf[col].dtype != object:
            pdf[col] = pdf[col].astype(object)
    # Replace NaN with None in object columns (preserving original pandas null behavior)
    for col in pdf.columns:
        if pdf[col].dtype == object:
            pdf[col] = pdf[col].where(pdf[col].notna(), None)
    return pdf


def ensure_polars(df: DataFrameT) -> pl.DataFrame:
    """Accept either pandas or Polars DataFrame, return Polars."""
    if isinstance(df, pl.DataFrame):
        return df
    return to_polars(df)


def ensure_polars_series(s: SeriesT) -> pl.Series:
    """Accept either pandas or Polars Series, return Polars."""
    if isinstance(s, pl.Series):
        return s
    return pl.from_pandas(s)


def is_polars(df: DataFrameT) -> bool:
    """Check if a DataFrame is Polars."""
    return isinstance(df, pl.DataFrame)


__all__ = [
    "DataFrameT",
    "SeriesT",
    "to_polars",
    "to_pandas",
    "ensure_polars",
    "ensure_polars_series",
    "is_polars",
]
