from __future__ import annotations

import inspect
import time
from functools import wraps
from typing import Any, Callable, TypeVar

import pandas as pd

from .._tracking import get_active_session

F = TypeVar("F", bound=Callable[..., pd.DataFrame])

# Operation registry for discoverability
_OPERATIONS: dict[str, Callable] = {}


def tracked(operation_name: str) -> Callable[[F], F]:
    """Decorator that tracks operation in build mode. No-op in production."""
    def decorator(func: F) -> F:
        # Register the operation
        _OPERATIONS[operation_name] = func

        @wraps(func)
        def wrapper(df: pd.DataFrame, *args: Any, **kwargs: Any) -> pd.DataFrame:
            session = get_active_session()

            if session is None:
                return func(df, *args, **kwargs)

            rows_before = len(df)
            start_time = time.perf_counter()

            result = func(df, *args, **kwargs)

            duration_ms = (time.perf_counter() - start_time) * 1000
            rows_after = len(result)
            params = _extract_params(func, args, kwargs)

            session.record(
                operation=operation_name,
                params=params,
                rows_before=rows_before,
                rows_after=rows_after,
                duration_ms=duration_ms,
            )

            return result

        return wrapper  # type: ignore
    return decorator


def _extract_params(func: Callable, args: tuple, kwargs: dict) -> dict[str, Any]:
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())

    result: dict[str, Any] = {}
    for i, value in enumerate(args):
        if i + 1 < len(params):
            name = params[i + 1]
            if not isinstance(value, pd.DataFrame):
                result[name] = _safe_repr(value)

    for name, value in kwargs.items():
        if not isinstance(value, pd.DataFrame):
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
