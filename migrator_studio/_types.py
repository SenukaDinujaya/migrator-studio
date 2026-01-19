from __future__ import annotations

from typing import Any, Literal, TypedDict

CompareOp = Literal["==", "!=", ">", ">=", "<", "<="]
JoinHow = Literal["left", "inner", "outer", "right", "cross"]
DateInclusive = Literal["both", "left", "right", "neither"]


class OperationRecordDict(TypedDict):
    operation: str
    params: dict[str, Any]
    rows_before: int
    rows_after: int
    timestamp: str
    duration_ms: float | None


class ConfigDict(TypedDict):
    data_path: str
