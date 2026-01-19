"""Display backends for different environments."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import BaseBackend


def _is_marimo_runtime() -> bool:
    """Check if we're actually running inside a Marimo notebook."""
    try:
        import marimo as mo
        # Check if mo.output.append is callable and works
        if not (hasattr(mo, 'output') and hasattr(mo.output, 'append')):
            return False

        # Try to check if we're in an active Marimo context
        try:
            from marimo._runtime.context import get_context
            ctx = get_context()
            return ctx is not None
        except Exception:
            # Any exception means we're not in a Marimo runtime
            # This includes ContextNotInitializedError
            pass

        # Try alternative check for older marimo versions
        try:
            from marimo._runtime.runtime import get_runtime
            return get_runtime() is not None
        except Exception:
            pass

        return False
    except ImportError:
        return False


def get_backend() -> "BaseBackend":
    """Auto-detect and return the appropriate display backend.

    Detection order:
    1. Marimo (if running in Marimo notebook)
    2. Terminal (Rich fallback)

    Returns:
        An instance of the appropriate backend.
    """
    # Try Marimo first - only if we're actually in a Marimo runtime
    if _is_marimo_runtime():
        from .marimo import MarimoBackend
        return MarimoBackend()

    # Fall back to terminal
    from .terminal import TerminalBackend
    return TerminalBackend()


__all__ = ["get_backend"]
