from __future__ import annotations

from typing import Any

import polars as pl

from ._base import tracked
from ._polars_compat import ensure_polars
from ._validation import validate_columns_exist


def _resolve_target(
    column: str | list[str],
    target: str | None,
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
    df: pl.DataFrame,
    column: str | list[str],
    mapping: dict[Any, Any],
    *,
    target: str | None = None,
    fallback: Any | None = None,
    fallback_original: bool = False,
) -> pl.DataFrame:
    """
    Map values in column(s) using a dictionary.

    Args:
        df: Input DataFrame
        column: Source column(s) to map from.
        mapping: Dictionary mapping source values to target values.
        target: Where to store the result.
        fallback: Value to use for unmapped values (default: null)
        fallback_original: If True, keep original value for unmapped entries.

    Example:
        df = map_dict(df, "status", {"A": "Active", "P": "Pending"})
    """
    validate_columns_exist(df, column, "map_dict")
    target_col = _resolve_target(column, target, "map_dict")

    if isinstance(column, str):
        # Single column mapping — use Polars replace
        default = None
        if fallback_original:
            # Use replace with default=keep original
            return df.with_columns(
                pl.col(column).replace(mapping, default=pl.col(column)).alias(target_col)
            )
        elif fallback is not None:
            return df.with_columns(
                pl.col(column).replace(mapping, default=fallback).alias(target_col)
            )
        else:
            return df.with_columns(
                pl.col(column).replace(mapping, default=None).alias(target_col)
            )
    else:
        # Multi-column key: build a lookup DataFrame and join
        keys = list(mapping.keys())
        values = list(mapping.values())

        # keys are tuples
        lookup_data = {f"_key_{i}": [k[i] for k in keys] for i in range(len(column))}
        lookup_data["_mapped_value"] = values
        lookup_df = pl.DataFrame(lookup_data)

        left_on = list(column)
        right_on = [f"_key_{i}" for i in range(len(column))]

        result = df.join(lookup_df, left_on=left_on, right_on=right_on, how="left")

        if fallback_original:
            # For multi-column, fallback_original doesn't have a single original col
            result = result.rename({"_mapped_value": target_col})
        elif fallback is not None:
            result = result.with_columns(
                pl.col("_mapped_value").fill_null(fallback).alias(target_col)
            )
            if "_mapped_value" != target_col:
                result = result.drop("_mapped_value")
        else:
            result = result.rename({"_mapped_value": target_col})

        return result


@tracked("map_lookup", affected_columns=_get_map_target)
def map_lookup(
    df: pl.DataFrame,
    column: str | list[str],
    lookup_df: pl.DataFrame,
    key_column: str | list[str],
    value_column: str,
    *,
    target: str | None = None,
    fallback: Any | None = None,
    fallback_original: bool = False,
) -> pl.DataFrame:
    """
    Map values in column(s) using a DataFrame lookup table.

    Args:
        df: Input DataFrame
        column: Source column(s) to map from.
        lookup_df: DataFrame containing the lookup table.
        key_column: Column(s) in lookup_df to match against.
        value_column: Column in lookup_df containing the target values.
        target: Where to store the result.
        fallback: Value for unmapped values (default: null)
        fallback_original: If True, keep original value for unmapped entries.

    Example:
        df = map_lookup(df, "region_code", regions_df, key_column="code", value_column="name")
    """
    validate_columns_exist(df, column, "map_lookup")
    lookup_df = ensure_polars(lookup_df)
    validate_columns_exist(lookup_df, key_column, "map_lookup")
    validate_columns_exist(lookup_df, value_column, "map_lookup")

    source_cols = [column] if isinstance(column, str) else column
    key_cols = [key_column] if isinstance(key_column, str) else key_column
    if len(source_cols) != len(key_cols):
        raise ValueError(
            f"map_lookup: Source column count ({len(source_cols)}) must match "
            f"key_column count ({len(key_cols)})."
        )

    target_col = _resolve_target(column, target, "map_lookup")

    # Select only needed columns from lookup
    lookup_subset = lookup_df.select(list(set(key_cols + [value_column])))

    # Rename value_column in lookup to a temp name to avoid any clashes
    temp_val_col = f"__lookup_val_{value_column}__"
    lookup_subset = lookup_subset.rename({value_column: temp_val_col})

    # Join
    result = df.join(
        lookup_subset,
        left_on=source_cols,
        right_on=key_cols,
        how="left",
    )

    # If target_col already exists (e.g., replacing source column), drop it first
    if target_col in result.columns and target_col != temp_val_col:
        result = result.drop(target_col)

    # Rename temp column to target
    result = result.rename({temp_val_col: target_col})

    # Apply fallback
    if fallback_original and isinstance(column, str):
        result = result.with_columns(
            pl.col(target_col).fill_null(pl.col(column)).alias(target_col)
        )
    elif fallback is not None:
        result = result.with_columns(
            pl.col(target_col).fill_null(fallback).alias(target_col)
        )

    return result


__all__ = ["map_dict", "map_lookup"]
