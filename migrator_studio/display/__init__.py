"""
Display system for live preview of transformer operations.

This module provides infrastructure for displaying operation effects
during transformer development, with support for multiple backends
(Marimo notebooks, terminal).

Example:
    from migrator_studio import BuildSession, load_source
    from migrator_studio.display import DisplayConfig

    with BuildSession(sample=10, preview=True, index_cols=["CustomerID"]):
        df = load_source("DAT-00000001")
        df = filter_isin(df, "Status", ["Active"])
        # Each operation displays its effect automatically
"""
from .config import DisplayConfig
from .models import OperationDisplay
from .renderer import Renderer, create_display_callback
from .backends import get_backend
from .backends.base import BaseBackend
from .backends.terminal import TerminalBackend
from .backends.marimo import MarimoBackend

__all__ = [
    "DisplayConfig",
    "OperationDisplay",
    "Renderer",
    "create_display_callback",
    "get_backend",
    "BaseBackend",
    "TerminalBackend",
    "MarimoBackend",
]
