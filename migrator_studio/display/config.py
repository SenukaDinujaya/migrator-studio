"""Display configuration settings."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class DisplayConfig:
    """Configuration for operation display.

    Attributes:
        verbosity: Level of detail in display output.
            - "minimal": Table only
            - "normal": Table + row count + time (default)
            - "detailed": Table + stats + dtypes + nulls
        index_cols: Columns to always show in preview (e.g., primary keys).
        max_rows: Maximum rows to show in preview (default: 10).
        show_affected_only: If True, only show index_cols + affected columns.
    """

    verbosity: Literal["minimal", "normal", "detailed"] = "normal"
    index_cols: list[str] = field(default_factory=list)
    max_rows: int = 10
    show_affected_only: bool = True
