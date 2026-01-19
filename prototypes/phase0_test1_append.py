"""
Phase 0 Prototype 1: Test mo.output.append()

Validates that multiple DataFrame tables can be stacked vertically
using mo.output.append() in a Marimo notebook.

Run: marimo run prototypes/phase0_test1_append.py
"""
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    return mo, pd


@app.cell
def _(mo, pd):
    # Create sample data
    df = pd.DataFrame({"ID": [1, 2, 3], "Name": ["Alice", "Bob", "Charlie"]})

    # Display first operation
    mo.output.append(mo.md("### [load_source]"))
    mo.output.append(mo.ui.table(df))
    mo.output.append(mo.md(f"Rows: 0 → {len(df)}"))
    mo.output.append(mo.md("---"))

    # Simulate a filter operation
    df2 = df[df["ID"] > 1]
    mo.output.append(mo.md("### [filter_by_value]"))
    mo.output.append(mo.ui.table(df2))
    mo.output.append(mo.md(f"Rows: {len(df)} → {len(df2)}"))
    mo.output.append(mo.md("---"))

    # Simulate a set_value operation
    df3 = df2.copy()
    df3["Company"] = "ACME"
    mo.output.append(mo.md("### [set_value]"))
    mo.output.append(mo.ui.table(df3))
    mo.output.append(mo.md(f"Rows: {len(df2)} → {len(df3)} | Affected: Company"))

    return df, df2, df3


if __name__ == "__main__":
    app.run()
