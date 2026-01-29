"""Step marker for transformer code generation."""
from __future__ import annotations

from .operations._tracking import get_active_session


def step(name: str, description: str | None = None) -> None:
    """
    Mark the beginning of a transformation step.

    This is a marker function used by the CLI to split code into Marimo cells.
    At runtime, it records the step name in the active session tracker so it
    appears in ``session.summary()``.

    Args:
        name: Short name for the step (becomes the cell title)
        description: Optional longer description

    Example:
        def transform(sources):
            customers = sources["DAT-00000001"]

            step("Sanitize data")
            df = sanitize_data(customers, empty_val=None)

            step("Filter active customers")
            df = filter_isin(df, "Status", ["A", "P"])

            return df
    """
    session = get_active_session()
    if session is not None:
        session.record(
            operation="step",
            params={"name": name, "description": description},
            rows_before=0,
            rows_after=0,
            duration_ms=0.0,
            affected_columns=[],
        )
