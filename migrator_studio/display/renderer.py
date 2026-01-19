"""Core rendering orchestration."""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pandas as pd

from .config import DisplayConfig
from .models import OperationDisplay

if TYPE_CHECKING:
    from .backends.base import BaseBackend


class Renderer:
    """Orchestrates rendering of operation displays.

    The Renderer is responsible for:
    1. Filtering columns to show (index_cols + affected_columns)
    2. Limiting rows based on config
    3. Delegating to the appropriate backend for output
    """

    def __init__(self, config: DisplayConfig, backend: "BaseBackend"):
        self.config = config
        self.backend = backend

    def render(self, display: OperationDisplay) -> None:
        """Render an operation display."""
        # Prepare the preview data
        preview = self._prepare_preview(display)

        # Create a display with filtered preview
        filtered_display = OperationDisplay(
            operation_name=display.operation_name,
            data_preview=preview,
            rows_before=display.rows_before,
            rows_after=display.rows_after,
            affected_columns=display.affected_columns,
            duration_ms=display.duration_ms,
            params=display.params,
        )

        # Delegate to backend
        self.backend.render(filtered_display, self.config)

    def _prepare_preview(self, display: OperationDisplay) -> pd.DataFrame:
        """Prepare the DataFrame preview with appropriate columns and row limit."""
        df = display.data_preview

        # Limit rows
        if len(df) > self.config.max_rows:
            df = df.head(self.config.max_rows)

        # Filter columns if show_affected_only is True
        if self.config.show_affected_only and display.affected_columns:
            cols_to_show = self._get_columns_to_show(df, display.affected_columns)
            if cols_to_show:
                # Only filter if we have columns to show
                available_cols = [c for c in cols_to_show if c in df.columns]
                if available_cols:
                    df = df[available_cols]

        return df

    def _get_columns_to_show(
        self, df: pd.DataFrame, affected_columns: list[str]
    ) -> list[str]:
        """Get the list of columns to show in preview."""
        # Start with index columns (always shown)
        cols = list(self.config.index_cols)

        # Add affected columns
        for col in affected_columns:
            if col not in cols:
                cols.append(col)

        # If no columns specified, show all
        if not cols:
            return list(df.columns)

        return cols


def create_display_callback(
    config: DisplayConfig,
    backend: Optional["BaseBackend"] = None,
):
    """Create a display callback function for use with SessionTracker.

    Args:
        config: Display configuration.
        backend: Backend to use. If None, auto-detects.

    Returns:
        Callable that accepts OperationDisplay and renders it.
    """
    if backend is None:
        from .backends import get_backend
        backend = get_backend()

    renderer = Renderer(config, backend)

    def callback(display: OperationDisplay) -> None:
        renderer.render(display)

    return callback
