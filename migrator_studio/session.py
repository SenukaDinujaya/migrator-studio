from __future__ import annotations

import pandas as pd

from .operations._tracking import (
    OperationRecord,
    SessionTracker,
    set_active_session,
)


def _format_change(before: int, after: int) -> str:
    """Format row change as a string with sign."""
    change = after - before
    if change == 0:
        return "="
    return f"{change:+d}"


def _format_cols(cols: list[str], max_len: int = 30) -> str:
    """Format affected columns list."""
    if not cols:
        return ""
    result = ", ".join(cols)
    if len(result) > max_len:
        return result[:max_len - 3] + "..."
    return result


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

    With live preview:
        with BuildSession(sample=10, live_preview=True) as session:
            # Each operation shows sample rows automatically
            df = load_source("DAT-00000001")
            df = filter_isin(df, "Status", ["Active"])
    """

    def __init__(
        self,
        sample: int = 10,
        live_preview: bool = False,
        preview_rows: int = 5,
    ):
        """Initialize BuildSession.

        Args:
            sample: Number of rows to sample when loading data.
            live_preview: If True, show data preview after each operation.
            preview_rows: Number of rows to show in live preview.
        """
        self.sample = sample
        self.live_preview = live_preview
        self.preview_rows = preview_rows
        self._tracker: SessionTracker | None = None
        self._op_count = 0

    def _on_record(self, record: OperationRecord, result_df: pd.DataFrame) -> None:
        """Callback invoked after each operation is recorded."""
        self._op_count += 1
        change = _format_change(record.rows_before, record.rows_after)
        cols = _format_cols(record.affected_columns)

        # Header line
        header_parts = [
            f"[{self._op_count:2d}]",
            f"{record.operation}",
            f"({record.rows_before} → {record.rows_after} rows, {change})",
        ]
        if cols:
            header_parts.append(f"[{cols}]")

        print(f"\n{'─' * 60}")
        print(" ".join(header_parts))
        print("─" * 60)

        # Show sample rows
        if len(result_df) > 0:
            print(result_df.head(self.preview_rows).to_string(index=False))
            if len(result_df) > self.preview_rows:
                print(f"... ({len(result_df) - self.preview_rows} more rows)")
        else:
            print("(empty DataFrame)")

    def __enter__(self) -> BuildSession:
        callback = self._on_record if self.live_preview else None
        self._tracker = SessionTracker(sample_size=self.sample, on_record=callback)
        self._op_count = 0
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
    def last_operation(self) -> OperationRecord | None:
        history = self.history
        return history[-1] if history else None

    def summary(self) -> pd.DataFrame:
        """Get operation history as a DataFrame for display."""
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
