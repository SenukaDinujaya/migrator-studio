"""Session summary function for operation history inspection."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from ..session import BuildSession


def _format_params(params: dict[str, Any], max_length: int = 50) -> str:
    """Format parameters dict as a truncated string."""
    if not params:
        return ""

    parts = []
    for key, value in params.items():
        if isinstance(value, (list, tuple)):
            if len(value) > 3:
                val_str = f"[{value[0]!r}, {value[1]!r}, ... +{len(value)-2}]"
            else:
                val_str = repr(value)
        elif isinstance(value, str) and len(value) > 20:
            val_str = f"{value[:17]!r}..."
        else:
            val_str = repr(value)
        parts.append(f"{key}={val_str}")

    result = ", ".join(parts)
    if len(result) > max_length:
        result = result[:max_length - 3] + "..."
    return result


def _format_affected_cols(cols: list[str], max_length: int = 30) -> str:
    """Format affected columns list as a truncated string."""
    if not cols:
        return ""

    result = ", ".join(cols)
    if len(result) > max_length:
        result = result[:max_length - 3] + "..."
    return result


def summary(
    session: BuildSession,
    *,
    include_params: bool = True,
) -> pd.DataFrame:
    """
    Generate an enhanced summary DataFrame of session operation history.

    Args:
        session: The BuildSession to summarize.
        include_params: Whether to include formatted parameters.

    Returns:
        DataFrame with operation history including params, row changes, and timing.
    """
    history = session.history

    if not history:
        columns = ["#", "operation", "rows_before", "rows_after", "change", "change_pct", "duration_ms"]
        if include_params:
            columns.insert(2, "params")
            columns.insert(-1, "affected_cols")
        return pd.DataFrame(columns=columns)

    records = []
    for idx, op in enumerate(history, start=1):
        change = op.rows_after - op.rows_before
        if op.rows_before > 0:
            change_pct = (change / op.rows_before) * 100
            change_pct_str = f"{change_pct:+.1f}%"
        else:
            change_pct_str = "N/A"

        # Format change with sign
        change_str = f"{change:+d}" if change != 0 else "0"

        record = {
            "#": idx,
            "operation": op.operation,
            "rows_before": op.rows_before,
            "rows_after": op.rows_after,
            "change": change_str,
            "change_pct": change_pct_str,
            "duration_ms": round(op.duration_ms, 2) if op.duration_ms else None,
        }

        if include_params:
            record["params"] = _format_params(op.params)
            record["affected_cols"] = _format_affected_cols(op.affected_columns)

        records.append(record)

    # Define column order
    if include_params:
        column_order = [
            "#", "operation", "params", "rows_before", "rows_after",
            "change", "change_pct", "affected_cols", "duration_ms"
        ]
    else:
        column_order = [
            "#", "operation", "rows_before", "rows_after",
            "change", "change_pct", "duration_ms"
        ]

    return pd.DataFrame(records)[column_order]
