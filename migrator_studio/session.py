from __future__ import annotations

from typing import Callable, Literal, Optional

import pandas as pd

from ._tracking import (
    OperationRecord,
    SessionTracker,
    set_active_session,
)


class BuildSession:
    """
    Context manager for build/development mode.

    When active:
    - Data loading is sampled to specified row count
    - Operations are tracked with before/after row counts
    - History is available for inspection
    - If preview=True, each operation's effect is displayed

    Example:
        with BuildSession(sample=10) as session:
            df = load_source("DAT-00000001")
            df = filter_rows(df, "Status", in_values=["Active"])
            print(session.summary())

        # With live preview (for Marimo notebooks):
        with BuildSession(sample=10, preview=True, index_cols=["CustomerID"]):
            df = load_source("DAT-00000001")
            df = filter_isin(df, "Status", ["Active"])
            # Each operation displays automatically
    """

    def __init__(
        self,
        sample: int = 10,
        preview: bool = False,
        index_cols: Optional[list[str]] = None,
        verbosity: Literal["minimal", "normal", "detailed"] = "normal",
    ):
        """Initialize BuildSession.

        Args:
            sample: Number of rows to sample when loading data.
            preview: If True, display each operation's effect (requires Marimo
                or terminal with Rich).
            index_cols: Columns to always show in preview (e.g., primary keys).
            verbosity: Level of detail in preview output:
                - "minimal": Table only
                - "normal": Table + row count + time (default)
                - "detailed": Table + stats + dtypes + nulls
        """
        self.sample = sample
        self.preview = preview
        self.index_cols = index_cols or []
        self.verbosity = verbosity
        self._tracker: Optional[SessionTracker] = None
        self._display_callback: Optional[Callable] = None

    def set_display_callback(self, callback: Callable) -> None:
        """Set a custom display callback.

        This allows overriding the auto-detected backend with a custom
        display function. The callback receives an OperationDisplay object.

        Args:
            callback: Function that takes OperationDisplay and renders it.
        """
        self._display_callback = callback

    def __enter__(self) -> BuildSession:
        # Create display callback if preview is enabled
        display_callback = None
        if self.preview:
            if self._display_callback:
                display_callback = self._display_callback
            else:
                from .display import DisplayConfig, create_display_callback
                config = DisplayConfig(
                    verbosity=self.verbosity,
                    index_cols=self.index_cols,
                    max_rows=self.sample,
                )
                display_callback = create_display_callback(config)

        self._tracker = SessionTracker(
            sample_size=self.sample,
            display_callback=display_callback,
        )
        set_active_session(self._tracker)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        set_active_session(None)
        return None

    @property
    def history(self) -> list[OperationRecord]:
        if self._tracker is None:
            return []
        return self._tracker.get_history()

    @property
    def last_operation(self) -> Optional[OperationRecord]:
        history = self.history
        return history[-1] if history else None

    def summary(self) -> pd.DataFrame:
        """Get operation history as a DataFrame for display in Marimo."""
        if not self.history:
            return pd.DataFrame(columns=[
                "operation", "rows_before", "rows_after", "change", "duration_ms"
            ])

        records = []
        for op in self.history:
            records.append({
                "operation": op.operation,
                "rows_before": op.rows_before,
                "rows_after": op.rows_after,
                "change": op.rows_after - op.rows_before,
                "duration_ms": round(op.duration_ms, 2) if op.duration_ms else None,
            })

        return pd.DataFrame(records)
