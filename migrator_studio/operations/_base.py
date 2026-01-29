from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, Union

import pandas as pd
import polars as pl

from ._polars_compat import DataFrameT, ensure_polars, to_pandas
from ._tracking import get_active_session

F = TypeVar("F", bound=Callable[..., Union[pd.DataFrame, pl.DataFrame]])

# Operation registry for discoverability
_OPERATIONS: dict[str, Callable] = {}


def tracked(
    operation_name: str,
    affected_columns: Callable[[dict], list[str]] | None = None,
) -> Callable[[F], F]:
    """Decorator that tracks operation in build mode.

    Handles polymorphic input: if the first arg is a pd.DataFrame, it is
    converted to Polars before calling the (now Polars-native) function body,
    and the result is converted back to pandas. If the first arg is already
    a pl.DataFrame, it passes straight through with zero conversion.
    """
    def decorator(func: F) -> F:
        _OPERATIONS[operation_name] = func

        @wraps(func)
        def wrapper(df: DataFrameT, *args: Any, **kwargs: Any) -> DataFrameT:
            input_is_pandas = isinstance(df, pd.DataFrame)

            # Convert to Polars if needed
            pl_df = ensure_polars(df) if input_is_pandas else df

            session = get_active_session()

            if session is None:
                result = func(pl_df, *args, **kwargs)
                return to_pandas(result) if input_is_pandas else result

            rows_before = len(pl_df)
            start_time = time.perf_counter()

            result = func(pl_df, *args, **kwargs)

            duration_ms = (time.perf_counter() - start_time) * 1000
            rows_after = len(result)
            params = _extract_params(func, args, kwargs)

            cols: list[str] = []
            if affected_columns is not None:
                try:
                    cols = affected_columns(params)
                except Exception:
                    cols = []

            # For session recording, convert to pandas for display compatibility
            result_for_record = to_pandas(result) if isinstance(result, pl.DataFrame) else result

            session.record(
                operation=operation_name,
                params=params,
                rows_before=rows_before,
                rows_after=rows_after,
                duration_ms=duration_ms,
                affected_columns=cols,
                result_df=result_for_record,
            )

            return to_pandas(result) if input_is_pandas else result

        return wrapper  # type: ignore
    return decorator


def _extract_params(func: Callable, args: tuple, kwargs: dict) -> dict[str, Any]:
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())

    result: dict[str, Any] = {}
    for i, value in enumerate(args):
        if i + 1 < len(params):
            name = params[i + 1]
            if not isinstance(value, (pd.DataFrame, pl.DataFrame)):
                result[name] = _safe_repr(value)

    for name, value in kwargs.items():
        if not isinstance(value, (pd.DataFrame, pl.DataFrame)):
            result[name] = _safe_repr(value)

    return result


def _safe_repr(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)) and len(value) <= 10:
        return list(value)
    if isinstance(value, dict) and len(value) <= 10:
        return dict(value)
    return f"<{type(value).__name__}>"


def list_operations() -> list[str]:
    """List all available registered operations."""
    return sorted(_OPERATIONS.keys())


def get_operation(name: str) -> Callable:
    """Get an operation function by name."""
    if name not in _OPERATIONS:
        raise KeyError(f"Operation '{name}' not found. Available: {list_operations()}")
    return _OPERATIONS[name]


__all__ = ["tracked", "list_operations", "get_operation"]
