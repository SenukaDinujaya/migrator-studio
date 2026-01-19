"""Terminal backend using Rich for formatting."""
from __future__ import annotations

from typing import TYPE_CHECKING

from .base import BaseBackend

if TYPE_CHECKING:
    from ..config import DisplayConfig
    from ..models import OperationDisplay


class TerminalBackend(BaseBackend):
    """Terminal backend using Rich for formatted output.

    Falls back to plain print if Rich is not available.
    """

    def __init__(self):
        self._console = None
        self._has_rich = False
        try:
            from rich.console import Console
            self._console = Console()
            self._has_rich = True
        except ImportError:
            pass

    def render(self, display: "OperationDisplay", config: "DisplayConfig") -> None:
        """Render operation display to terminal."""
        if self._has_rich:
            self._render_rich(display, config)
        else:
            self._render_plain(display, config)

    def _render_rich(self, display: "OperationDisplay", config: "DisplayConfig") -> None:
        """Render with Rich formatting."""
        from rich.panel import Panel
        from rich.table import Table

        # Header
        title = f"[bold blue][{display.operation_name}][/bold blue]"

        # Create table
        table = Table(show_header=True, header_style="bold")
        for col in display.data_preview.columns:
            table.add_column(str(col))

        for _, row in display.data_preview.iterrows():
            table.add_row(*[str(v) for v in row.values])

        # Stats line based on verbosity
        stats = self._format_stats(display, config)

        self._console.print(title)
        self._console.print(table)
        if stats:
            self._console.print(stats)
        self._console.print()

    def _render_plain(self, display: "OperationDisplay", config: "DisplayConfig") -> None:
        """Render with plain print."""
        print(f"\n=== [{display.operation_name}] ===")
        print(display.data_preview.to_string())

        stats = self._format_stats(display, config)
        if stats:
            print(stats)
        print()

    def _format_stats(self, display: "OperationDisplay", config: "DisplayConfig") -> str:
        """Format statistics line based on verbosity."""
        if config.verbosity == "minimal":
            return ""

        parts = [f"Rows: {display.rows_before} → {display.rows_after}"]

        if config.verbosity in ("normal", "detailed"):
            parts.append(f"Time: {display.duration_ms:.2f}ms")

        if display.affected_columns:
            parts.append(f"Affected: {', '.join(display.affected_columns)}")

        if config.verbosity == "detailed" and display.params:
            params_str = ", ".join(f"{k}={v}" for k, v in display.params.items())
            parts.append(f"Params: {params_str}")

        return " | ".join(parts)

    def render_separator(self) -> None:
        """Render a separator line."""
        if self._has_rich:
            self._console.print("─" * 40)
        else:
            print("-" * 40)
