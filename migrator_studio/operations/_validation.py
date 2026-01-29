from __future__ import annotations

import warnings
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import polars as pl

from ._polars_compat import DataFrameT


class FilterTypeError(TypeError):
    """Raised when filter value type is incompatible with column dtype."""
    pass


class FilterValueWarning(UserWarning):
    """Warning for filter values that may produce unexpected results."""
    pass


def validate_column_exists(df: DataFrameT, column: str, operation: str) -> None:
    """Validate that a column exists in the DataFrame (works for both pandas and Polars)."""
    if column not in df.columns:
        raise KeyError(
            f"{operation} failed: Column '{column}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}."
        )


def validate_columns_exist(
    df: DataFrameT,
    columns: str | list[str],
    operation: str,
) -> None:
    """Validate that all columns exist in the DataFrame."""
    if isinstance(columns, str):
        columns = [columns]

    cols = list(df.columns)
    missing = [c for c in columns if c not in cols]
    if missing:
        raise KeyError(
            f"{operation} failed: Columns {missing} not found in DataFrame. "
            f"Available columns: {cols}."
        )


def _is_polars_numeric(dtype: pl.DataType) -> bool:
    """Check if a Polars dtype is numeric."""
    return dtype.is_numeric()


def _is_polars_temporal(dtype: pl.DataType) -> bool:
    """Check if a Polars dtype is temporal."""
    return dtype.is_temporal()


def _is_polars_string(dtype: pl.DataType) -> bool:
    """Check if a Polars dtype is string."""
    return dtype == pl.Utf8 or dtype == pl.String


def validate_membership_values(
    df: pl.DataFrame,
    column: str,
    values: Sequence[Any],
    operation: str,
) -> None:
    """Validate values for membership operations (isin, not_isin)."""
    dtype = df.schema[column]

    for val in values:
        if val is None:
            continue
        _validate_polars_value(dtype, val, column, operation)


def validate_comparable(
    df: pl.DataFrame,
    column: str,
    value: Any,
    operation: str,
) -> None:
    """Validate a single value for comparison operations."""
    if value is None:
        return
    dtype = df.schema[column]
    _validate_polars_value(dtype, value, column, operation)


def _validate_polars_value(
    dtype: pl.DataType,
    value: Any,
    column: str,
    operation: str,
) -> None:
    """Validate a single value against a Polars column dtype."""
    # Boolean columns
    if dtype == pl.Boolean:
        if not isinstance(value, bool):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype 'Boolean', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: bool (True/False)."
            )
        return

    # Numeric columns
    if dtype.is_numeric():
        if isinstance(value, bool):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type 'bool'. "
                f"Expected: int or float."
            )
        if not isinstance(value, (int, float)):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: int or float."
            )
        return

    # Duration columns (timedelta)
    if dtype == pl.Duration or (hasattr(dtype, 'base_type') and str(dtype).startswith("Duration")):
        if not isinstance(value, (timedelta, pd.Timedelta)):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: timedelta or pd.Timedelta."
            )
        return

    # Temporal columns (date/datetime)
    if dtype.is_temporal():
        if isinstance(value, (datetime, pd.Timestamp)):
            return
        if isinstance(value, str):
            try:
                pd.to_datetime(value)
                return
            except (ValueError, TypeError):
                raise FilterTypeError(
                    f"{operation} failed: Cannot parse date value '{value}' for column '{column}'. "
                    f"Expected: ISO format string (e.g., '2025-01-01'), datetime, or pd.Timestamp."
                )
        raise FilterTypeError(
            f"{operation} failed: Type mismatch on column '{column}'. "
            f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
            f"Expected: str (ISO format), datetime, or pd.Timestamp."
        )

    # String columns
    if _is_polars_string(dtype):
        if not isinstance(value, str):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: str."
            )
        return

    # Object/unknown — allow anything
    return


def validate_date_value(
    value: Any,
    param_name: str,
    operation: str,
) -> pd.Timestamp:
    """Validate and convert a date value to pd.Timestamp."""
    if value is None:
        raise ValueError(f"{operation}: {param_name} cannot be None")

    if isinstance(value, pd.Timestamp):
        return value

    if isinstance(value, datetime):
        return pd.Timestamp(value)

    if isinstance(value, str):
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            raise FilterTypeError(
                f"{operation} failed: Cannot parse date value '{value}' for parameter '{param_name}'. "
                f"Expected: ISO format string (e.g., '2025-01-01'), datetime, or pd.Timestamp."
            )

    raise FilterTypeError(
        f"{operation} failed: Invalid type for '{param_name}'. "
        f"Received value '{value}' of type '{type(value).__name__}'. "
        f"Expected: str (ISO format), datetime, or pd.Timestamp."
    )
