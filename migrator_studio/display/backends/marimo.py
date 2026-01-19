"""Marimo backend using mo.output.append()."""
from __future__ import annotations

from typing import TYPE_CHECKING

from .base import BaseBackend

if TYPE_CHECKING:
    from ..config import DisplayConfig
    from ..models import OperationDisplay


class MarimoBackend(BaseBackend):
    """Marimo backend using mo.output.append() for stacked displays."""

    def render(self, display: "OperationDisplay", config: "DisplayConfig") -> None:
        """Render operation display in Marimo notebook."""
        import marimo as mo

        # Header
        mo.output.append(mo.md(f"### [{display.operation_name}]"))

        # Data table
        mo.output.append(mo.ui.table(display.data_preview))

        # Stats based on verbosity
        stats = self._format_stats(display, config)
        if stats:
            mo.output.append(mo.md(stats))

        # Separator
        self.render_separator()

    def _format_stats(self, display: "OperationDisplay", config: "DisplayConfig") -> str:
        """Format statistics line based on verbosity."""
        if config.verbosity == "minimal":
            return ""

        parts = [f"Rows: {display.rows_before} → {display.rows_after}"]

        if config.verbosity in ("normal", "detailed"):
            parts.append(f"Time: {display.duration_ms:.2f}ms")

        if display.affected_columns:
            parts.append(f"Affected: {', '.join(display.affected_columns)}")

        if config.verbosity == "detailed":
            # Show dtypes summary
            dtypes = display.data_preview.dtypes.value_counts().to_dict()
            dtype_str = ", ".join(f"{k}: {v}" for k, v in dtypes.items())
            parts.append(f"Types: {dtype_str}")

            # Show null counts
            nulls = display.data_preview.isnull().sum()
            null_cols = nulls[nulls > 0]
            if len(null_cols) > 0:
                null_str = ", ".join(f"{c}: {n}" for c, n in null_cols.items())
                parts.append(f"Nulls: {null_str}")

        return " | ".join(parts)

    def render_separator(self) -> None:
        """Render a markdown separator."""
        import marimo as mo
        mo.output.append(mo.md("---"))
