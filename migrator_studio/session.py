from __future__ import annotations

from typing import Optional

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

    Example:
        with BuildSession(sample=10) as session:
            df = load_source("DAT-00000001")
            df = filter_rows(df, "Status", in_values=["Active"])
            print(session.summary())
    """

    def __init__(self, sample: int = 10):
        self.sample = sample
        self._tracker: Optional[SessionTracker] = None

    def __enter__(self) -> BuildSession:
        self._tracker = SessionTracker(sample_size=self.sample)
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
