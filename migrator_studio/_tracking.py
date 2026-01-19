from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class OperationRecord:
    operation: str
    params: dict[str, Any]
    rows_before: int
    rows_after: int
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: Optional[float] = None


class SessionTracker:
    def __init__(self, sample_size: int):
        self.sample_size = sample_size
        self.operations: list[OperationRecord] = []
        self._start_time = datetime.now()

    def record(
        self,
        operation: str,
        params: dict[str, Any],
        rows_before: int,
        rows_after: int,
        duration_ms: float,
    ) -> None:
        self.operations.append(OperationRecord(
            operation=operation,
            params=params,
            rows_before=rows_before,
            rows_after=rows_after,
            duration_ms=duration_ms,
        ))

    def get_history(self) -> list[OperationRecord]:
        return self.operations.copy()


_active_session: ContextVar[Optional[SessionTracker]] = ContextVar(
    "_active_session", default=None
)


def get_active_session() -> Optional[SessionTracker]:
    return _active_session.get()


def set_active_session(tracker: Optional[SessionTracker]) -> None:
    _active_session.set(tracker)


def is_build_mode() -> bool:
    return _active_session.get() is not None


def get_sample_size() -> Optional[int]:
    session = _active_session.get()
    return session.sample_size if session else None
