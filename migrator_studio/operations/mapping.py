from __future__ import annotations

from typing import Any, Optional, Union

import pandas as pd

from ._base import tracked
from ._validation import validate_columns_exist


def _get_lookup_series(
    df: pd.DataFrame,
    column: Union[str, list[str]],
) -> pd.Series:
    """Get a Series to use for lookup (single value or tuple for multi-column)."""
    if isinstance(column, str):
        return df[column]

    # Multi-column: create tuple series
    return pd.Series(list(zip(*[df[col] for col in column])), index=df.index)


def _apply_fallback(
    mapped: pd.Series,
    original: pd.Series,
    fallback: Optional[Any],
    fallback_original: bool,
) -> pd.Series:
    """Apply fallback logic to mapped values."""
    if fallback_original:
        return mapped.fillna(original)
    elif fallback is not None:
        return mapped.fillna(fallback)
    return mapped


def _resolve_target(
    column: Union[str, list[str]],
    target: Optional[str],
    func_name: str,
) -> str:
    """Resolve target column name, requiring it for multi-column keys."""
    if target is not None:
        return target
    if isinstance(column, str):
        return column
    raise ValueError(
        f"{func_name}: 'target' must be specified when using multi-column keys."
    )


def _get_map_target(p: dict) -> list[str]:
    """Get target column for mapping operations."""
    target = p.get("target")
    if target:
        return [target]
    column = p.get("column")
    if isinstance(column, str):
        return [column]
    return []


@tracked("map_dict", affected_columns=_get_map_target)
def map_dict(
    df: pd.DataFrame,
    column: Union[str, list[str]],
    mapping: dict[Any, Any],
    *,
    target: Optional[str] = None,
    fallback: Optional[Any] = None,
    fallback_original: bool = False,
) -> pd.DataFrame:
    """
    Map values in column(s) using a dictionary.

    Args:
        df: Input DataFrame
        column: Source column(s) to map from. Can be a single column name or
            a list of columns for multi-column keys.
        mapping: Dictionary mapping source values to target values.
            For multi-column source, dict keys must be tuples.
        target: Where to store the result. If not specified, replaces
            the source column. Required when using multi-column keys.
        fallback: Value to use for unmapped values (default: NA)
        fallback_original: If True, keep original value for unmapped entries.

    Example:
        # Single column mapping
        df = map_dict(df, "status", {"A": "Active", "P": "Pending"})

        # Multi-column key
        df = map_dict(df, ["branch", "region"],
                      {("B1", "R1"): "Territory A", ("B2", "R2"): "Territory B"},
                      target="territory")

        # Create new column instead of replacing
        df = map_dict(df, "status", {"A": "Active"}, target="status_text")

        # Keep original value for unmapped entries
        df = map_dict(df, "status", {"A": "Active"}, fallback_original=True)
    """
    validate_columns_exist(df, column, "map_dict")

    result = df.copy()
    target_col = _resolve_target(column, target, "map_dict")

    lookup_series = _get_lookup_series(df, column)
    mapped = lookup_series.map(mapping)

    if isinstance(column, str):
        original_values = df[column]
    else:
        original_values = lookup_series

    result[target_col] = _apply_fallback(mapped, original_values, fallback, fallback_original)

    return result


@tracked("map_lookup", affected_columns=_get_map_target)
def map_lookup(
    df: pd.DataFrame,
    column: Union[str, list[str]],
    lookup_df: pd.DataFrame,
    key_column: Union[str, list[str]],
    value_column: str,
    *,
    target: Optional[str] = None,
    fallback: Optional[Any] = None,
    fallback_original: bool = False,
) -> pd.DataFrame:
    """
    Map values in column(s) using a DataFrame lookup table.

    Args:
        df: Input DataFrame
        column: Source column(s) to map from. Can be a single column name or
            a list of columns for multi-column keys.
        lookup_df: DataFrame containing the lookup table.
        key_column: Column(s) in lookup_df to match against. Must match
            the structure of column (single vs list).
        value_column: Column in lookup_df containing the target values.
        target: Where to store the result. If not specified, replaces
            the source column. Required when using multi-column keys.
        fallback: Value to use for unmapped values (default: NA)
        fallback_original: If True, keep original value for unmapped entries.

    Example:
        # Single column lookup
        df = map_lookup(df, "region_code", regions_df,
                        key_column="code", value_column="name")

        # Multi-column key lookup
        df = map_lookup(df, ["branch", "region"], lookup_df,
                        key_column=["branch_code", "region_code"],
                        value_column="territory_name",
                        target="territory")

        # Create new column
        df = map_lookup(df, "code", lookup_df,
                        key_column="code", value_column="name",
                        target="full_name")
    """
    validate_columns_exist(df, column, "map_lookup")
    validate_columns_exist(lookup_df, key_column, "map_lookup")
    validate_columns_exist(lookup_df, value_column, "map_lookup")

    # Validate column count matches
    source_cols = [column] if isinstance(column, str) else column
    key_cols = [key_column] if isinstance(key_column, str) else key_column
    if len(source_cols) != len(key_cols):
        raise ValueError(
            f"map_lookup: Source column count ({len(source_cols)}) must match "
            f"key_column count ({len(key_cols)})."
        )

    # Build mapping dict from lookup DataFrame
    if isinstance(key_column, str):
        mapping_dict = lookup_df.set_index(key_column)[value_column].to_dict()
    else:
        # Multi-column key: create tuple index
        lookup_copy = lookup_df.copy()
        lookup_copy["_tuple_key"] = list(zip(*[lookup_copy[col] for col in key_column]))
        mapping_dict = lookup_copy.set_index("_tuple_key")[value_column].to_dict()

    result = df.copy()
    target_col = _resolve_target(column, target, "map_lookup")

    lookup_series = _get_lookup_series(df, column)
    mapped = lookup_series.map(mapping_dict)

    if isinstance(column, str):
        original_values = df[column]
    else:
        original_values = lookup_series

    result[target_col] = _apply_fallback(mapped, original_values, fallback, fallback_original)

    return result
