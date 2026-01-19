"""Base backend interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import DisplayConfig
    from ..models import OperationDisplay


class BaseBackend(ABC):
    """Abstract base class for display backends.

    Each backend is responsible for rendering OperationDisplay
    objects in a specific environment (Marimo, terminal, etc.).
    """

    @abstractmethod
    def render(self, display: "OperationDisplay", config: "DisplayConfig") -> None:
        """Render an operation display.

        Args:
            display: The operation display data to render.
            config: Display configuration settings.
        """
        pass

    def render_separator(self) -> None:
        """Render a separator between operations.

        Override in subclasses for environment-specific separators.
        """
        pass
