"""Step marker for transformer code generation."""
from __future__ import annotations

from typing import Optional
from .operations._tracking import get_active_session


def step(name: str, description: Optional[str] = None) -> None:
    """
    Mark the beginning of a transformation step.

    This is a marker function used by the CLI to split code into Marimo cells.
    At runtime, it optionally prints the step name when in preview mode.

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
        # In build mode, we could log or display step transitions
        # For now, just a no-op marker
        pass
