"""
Phase 0 Prototype 3: Test BuildSession integration

Validates extending BuildSession with preview=True and display callbacks.
This simulates the final integration pattern.

Run: marimo run prototypes/phase0_test3_session.py
"""
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import time
    from dataclasses import dataclass
    from typing import Optional, Callable, Any
    from functools import wraps
    from contextvars import ContextVar
    return mo, pd, time, dataclass, Optional, Callable, Any, wraps, ContextVar


@app.cell
def _(mo, pd, time, dataclass, Optional, Callable, Any, wraps, ContextVar):
    # ==================== Display Models ====================
    @dataclass
    class OperationDisplay:
        operation_name: str
        data_preview: pd.DataFrame
        rows_before: int
        rows_after: int
        affected_columns: list[str]
        duration_ms: float

    # ==================== Session Tracker ====================
    class SessionTracker:
        def __init__(
            self,
            sample_size: int,
            display_callback: Optional[Callable[[OperationDisplay], None]] = None,
        ):
            self.sample_size = sample_size
            self.display_callback = display_callback
            self.operations = []

        def record(
            self,
            operation: str,
            params: dict,
            rows_before: int,
            rows_after: int,
            duration_ms: float,
            result_df: Optional[pd.DataFrame] = None,
            affected_columns: Optional[list[str]] = None,
        ):
            self.operations.append({
                "operation": operation,
                "params": params,
                "rows_before": rows_before,
                "rows_after": rows_after,
                "duration_ms": duration_ms,
            })

            # Trigger display if callback is set
            if self.display_callback and result_df is not None:
                display = OperationDisplay(
                    operation_name=operation,
                    data_preview=result_df.head(self.sample_size),
                    rows_before=rows_before,
                    rows_after=rows_after,
                    affected_columns=affected_columns or [],
                    duration_ms=duration_ms,
                )
                self.display_callback(display)

    # ==================== Context Management ====================
    _active_session: ContextVar[Optional[SessionTracker]] = ContextVar(
        "_active_session", default=None
    )

    def get_active_session() -> Optional[SessionTracker]:
        return _active_session.get()

    def set_active_session(tracker: Optional[SessionTracker]) -> None:
        _active_session.set(tracker)

    # ==================== BuildSession ====================
    class BuildSession:
        def __init__(
            self,
            sample: int = 10,
            preview: bool = False,
            index_cols: Optional[list[str]] = None,
            verbosity: str = "normal",
        ):
            self.sample = sample
            self.preview = preview
            self.index_cols = index_cols or []
            self.verbosity = verbosity
            self._tracker: Optional[SessionTracker] = None
            self._display_callback: Optional[Callable] = None

        def set_display_callback(self, callback: Callable[[OperationDisplay], None]):
            self._display_callback = callback

        def __enter__(self):
            callback = self._display_callback if self.preview else None
            self._tracker = SessionTracker(
                sample_size=self.sample,
                display_callback=callback,
            )
            set_active_session(self._tracker)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            set_active_session(None)
            return None

    # ==================== @tracked decorator ====================
    def tracked(
        operation_name: str,
        affected_columns: Optional[Callable[[dict], list[str]]] = None,
    ):
        def decorator(func):
            @wraps(func)
            def wrapper(df: pd.DataFrame, *args, **kwargs):
                session = get_active_session()

                if session is None:
                    return func(df, *args, **kwargs)

                rows_before = len(df)
                start_time = time.perf_counter()

                result = func(df, *args, **kwargs)

                duration_ms = (time.perf_counter() - start_time) * 1000
                rows_after = len(result)

                # Compute affected columns
                cols = []
                if affected_columns:
                    cols = affected_columns(kwargs)

                session.record(
                    operation=operation_name,
                    params=kwargs,
                    rows_before=rows_before,
                    rows_after=rows_after,
                    duration_ms=duration_ms,
                    result_df=result,
                    affected_columns=cols,
                )

                return result
            return wrapper
        return decorator

    # ==================== Sample Operations ====================
    @tracked("set_value", affected_columns=lambda p: [p.get("column", "")])
    def set_value(df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        result = df.copy()
        result[column] = value
        return result

    @tracked("filter_isin", affected_columns=lambda p: [])
    def filter_isin(df: pd.DataFrame, column: str, values: list) -> pd.DataFrame:
        return df[df[column].isin(values)].reset_index(drop=True)

    return (
        OperationDisplay,
        SessionTracker,
        BuildSession,
        tracked,
        set_value,
        filter_isin,
        get_active_session,
        set_active_session,
    )


@app.cell
def _(mo, pd, BuildSession, OperationDisplay, set_value, filter_isin):
    # Define Marimo display callback
    def marimo_display(display: OperationDisplay):
        affected = f" | Affected: {', '.join(display.affected_columns)}" if display.affected_columns else ""
        mo.output.append(mo.md(f"### [{display.operation_name}]"))
        mo.output.append(mo.ui.table(display.data_preview))
        mo.output.append(mo.md(
            f"Rows: {display.rows_before} → {display.rows_after} | "
            f"Time: {display.duration_ms:.2f}ms{affected}"
        ))
        mo.output.append(mo.md("---"))

    # Create sample data
    df = pd.DataFrame({
        "CustomerID": [1, 2, 3, 4, 5],
        "Name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "Status": ["Active", "Inactive", "Active", "Active", "Inactive"],
        "Region": ["NA", "EU", "NA", "APAC", "EU"],
    })

    # Run with preview mode
    mo.output.append(mo.md("## BuildSession Preview Demo"))
    mo.output.append(mo.md("---"))

    with BuildSession(sample=10, preview=True, index_cols=["CustomerID"]) as session:
        session.set_display_callback(marimo_display)

        # Initial display
        mo.output.append(mo.md("### [initial data]"))
        mo.output.append(mo.ui.table(df))
        mo.output.append(mo.md(f"Rows: 0 → {len(df)}"))
        mo.output.append(mo.md("---"))

        # Operations trigger display automatically
        df = filter_isin(df, column="Status", values=["Active"])
        df = set_value(df, column="Company", value="ACME Corp")

    mo.output.append(mo.md("**Test complete!** BuildSession preview integration works."))

    return df,


if __name__ == "__main__":
    app.run()
