from __future__ import annotations

import warnings
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

import pandas as pd


class FilterTypeError(TypeError):
    """Raised when filter value type is incompatible with column dtype."""

    pass


class FilterValueWarning(UserWarning):
    """Warning for filter values that may produce unexpected results."""

    pass


def validate_column_exists(df: pd.DataFrame, column: str, operation: str) -> None:
    """Validate that a column exists in the DataFrame."""
    if column not in df.columns:
        raise KeyError(
            f"{operation} failed: Column '{column}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}."
        )


def validate_columns_exist(
    df: pd.DataFrame,
    columns: str | list[str],
    operation: str,
) -> None:
    """Validate that all columns exist in the DataFrame."""
    if isinstance(columns, str):
        columns = [columns]

    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"{operation} failed: Columns {missing} not found in DataFrame. "
            f"Available columns: {list(df.columns)}."
        )


def validate_membership_values(
    series: pd.Series,
    values: Sequence[Any],
    operation: str,
) -> None:
    """Validate values for membership operations (isin, not_isin)."""
    column = series.name
    dtype = series.dtype

    for val in values:
        if val is None or pd.isna(val):
            continue

        _validate_single_value(dtype, val, column, operation)


def validate_comparable(
    series: pd.Series,
    value: Any,
    operation: str,
) -> None:
    """Validate a single value for comparison operations."""
    if value is None:
        return

    column = series.name
    dtype = series.dtype
    _validate_single_value(dtype, value, column, operation)


def _validate_single_value(
    dtype: Any,
    value: Any,
    column: str,
    operation: str,
) -> None:
    """Validate a single value against the column dtype."""
    # Boolean columns - strict bool only
    if pd.api.types.is_bool_dtype(dtype):
        if not isinstance(value, bool):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype 'bool', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: bool (True/False)."
            )
        return

    # Numeric columns (int, float) - no bool, no string
    if pd.api.types.is_numeric_dtype(dtype):
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

    # Datetime columns
    if pd.api.types.is_datetime64_any_dtype(dtype):
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

    # Timedelta columns
    if pd.api.types.is_timedelta64_dtype(dtype):
        if not isinstance(value, (timedelta, pd.Timedelta)):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: timedelta or pd.Timedelta."
            )
        return

    # Categorical columns - warn if value not in categories
    if isinstance(dtype, pd.CategoricalDtype):
        if value not in dtype.categories:
            warnings.warn(
                f"Value '{value}' not found in categories for column '{column}'. "
                f"Valid categories: {list(dtype.categories)}. "
                f"This filter will return 0 rows.",
                FilterValueWarning,
                stacklevel=4,
            )
        return

    # String dtype (StringDtype) - strict str only
    if pd.api.types.is_string_dtype(dtype) and dtype != object:
        if not isinstance(value, str):
            raise FilterTypeError(
                f"{operation} failed: Type mismatch on column '{column}'. "
                f"Column has dtype '{dtype}', but received value '{value}' of type '{type(value).__name__}'. "
                f"Expected: str."
            )
        return

    # Object dtype - sample column to detect homogeneous types
    if dtype == object:
        _validate_object_dtype(dtype, value, column, operation)
        return

    # Unknown dtype - allow anything
    return


def _validate_object_dtype(
    dtype: Any,
    value: Any,
    column: str,
    operation: str,
) -> None:
    """Validate value against an object dtype column with smart type detection."""
    # For object dtype, we're lenient but warn on potential issues
    # Most common case: string columns stored as object
    if isinstance(value, str):
        return  # Strings are always safe for object columns

    # Warn if using non-string with object column (might be intentional mixed types)
    warnings.warn(
        f"Column '{column}' has dtype 'object'. "
        f"Comparing with value '{value}' (type: {type(value).__name__}) may produce unexpected results "
        f"if the column contains strings.",
        FilterValueWarning,
        stacklevel=5,
    )


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
