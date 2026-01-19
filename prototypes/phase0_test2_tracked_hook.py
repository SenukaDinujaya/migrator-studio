"""
Phase 0 Prototype 2: Test decorator hook

Validates that display can be triggered from within a decorator wrapper,
simulating how the @tracked decorator will call the display system.

Run: marimo run prototypes/phase0_test2_tracked_hook.py
"""
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from functools import wraps
    from typing import Callable, Any
    return mo, pd, wraps, Callable, Any


@app.cell
def _(mo, pd, wraps, Callable, Any):
    # Global display callback (simulates what BuildSession will set)
    _display_callback = None

    def set_display_callback(callback):
        global _display_callback
        _display_callback = callback

    def get_display_callback():
        return _display_callback

    # Simulated @tracked decorator with display hook
    def tracked_with_display(operation_name: str):
        def decorator(func):
            @wraps(func)
            def wrapper(df: pd.DataFrame, *args, **kwargs):
                rows_before = len(df)
                result = func(df, *args, **kwargs)
                rows_after = len(result)

                # Call display callback if set
                callback = get_display_callback()
                if callback:
                    callback(
                        operation_name=operation_name,
                        data_preview=result.head(5),
                        rows_before=rows_before,
                        rows_after=rows_after,
                    )

                return result
            return wrapper
        return decorator

    # Sample operation using the decorator
    @tracked_with_display("set_value")
    def set_value(df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        result = df.copy()
        result[column] = value
        return result

    @tracked_with_display("filter_isin")
    def filter_isin(df: pd.DataFrame, column: str, values: list) -> pd.DataFrame:
        return df[df[column].isin(values)].reset_index(drop=True)

    return (
        set_display_callback,
        get_display_callback,
        tracked_with_display,
        set_value,
        filter_isin,
    )


@app.cell
def _(mo, pd, set_display_callback, set_value, filter_isin):
    # Define the Marimo display callback
    def marimo_display(operation_name, data_preview, rows_before, rows_after):
        mo.output.append(mo.md(f"### [{operation_name}]"))
        mo.output.append(mo.ui.table(data_preview))
        mo.output.append(mo.md(f"Rows: {rows_before} → {rows_after}"))
        mo.output.append(mo.md("---"))

    # Set the callback
    set_display_callback(marimo_display)

    # Run operations - display should be triggered automatically
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "Name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "Status": ["Active", "Inactive", "Active", "Active", "Inactive"],
    })

    mo.output.append(mo.md("### [initial data]"))
    mo.output.append(mo.ui.table(df))
    mo.output.append(mo.md("---"))

    # These calls should trigger the display callback
    df = filter_isin(df, "Status", ["Active"])
    df = set_value(df, "Company", "ACME Corp")

    mo.output.append(mo.md("**Test complete!** Operations triggered display via decorator."))

    return df,


if __name__ == "__main__":
    app.run()
