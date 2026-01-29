"""
Display module for inspecting DataFrames and session history during development.

Functions:
    preview: Show column information, types, nulls, and sample values.
    summary: Show enhanced operation history with params and changes.
    diff: Compare two DataFrames and show differences.
"""
from .diff import diff
from .preview import preview
from .summary import summary

__all__ = [
    "preview",
    "summary",
    "diff",
]
