"""Display data models."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class OperationDisplay:
    """Data for displaying a single operation's effect.

    Attributes:
        operation_name: Name of the operation (e.g., "filter_isin", "set_value").
        data_preview: DataFrame preview showing the result.
        rows_before: Row count before the operation.
        rows_after: Row count after the operation.
        affected_columns: List of columns modified by this operation.
        duration_ms: Time taken in milliseconds.
        params: Operation parameters (for detailed view).
    """

    operation_name: str
    data_preview: pd.DataFrame
    rows_before: int
    rows_after: int
    affected_columns: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    params: Optional[dict] = None

    @property
    def row_change(self) -> int:
        """Net change in row count."""
        return self.rows_after - self.rows_before

    @property
    def row_change_str(self) -> str:
        """Human-readable row change."""
        change = self.row_change
        if change > 0:
            return f"+{change}"
        return str(change)
