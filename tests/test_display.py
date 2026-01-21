"""Tests for display module."""

import pandas as pd
import pytest

from migrator_studio import BuildSession
from migrator_studio.display import preview, summary, diff


class TestPreview:
    """Tests for preview() function."""

    def test_preview_returns_dataframe(self):
        """Preview should return a DataFrame with column info."""
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
        })
        result = preview(df)

        assert isinstance(result, pd.DataFrame)
        assert "column" in result.columns
        assert "dtype" in result.columns
        assert "null_count" in result.columns
        assert "null_pct" in result.columns
        assert "unique_count" in result.columns
        assert "sample_values" in result.columns

    def test_preview_shows_all_columns(self):
        """Preview should show info for all columns."""
        df = pd.DataFrame({
            "col1": [1, 2],
            "col2": [3, 4],
            "col3": [5, 6],
        })
        result = preview(df)

        assert len(result) == 3
        assert list(result["column"]) == ["col1", "col2", "col3"]

    def test_preview_counts_nulls(self):
        """Preview should correctly count null values."""
        df = pd.DataFrame({
            "a": [1, None, None, 4],
            "b": ["x", "y", "z", "w"],
        })
        result = preview(df)

        a_row = result[result["column"] == "a"].iloc[0]
        assert a_row["null_count"] == 2
        assert a_row["null_pct"] == "50.0%"

        b_row = result[result["column"] == "b"].iloc[0]
        assert b_row["null_count"] == 0
        assert b_row["null_pct"] == "0.0%"

    def test_preview_show_nulls_false(self):
        """Preview should hide null columns when show_nulls=False."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = preview(df, show_nulls=False)

        assert "null_count" not in result.columns
        assert "null_pct" not in result.columns

    def test_preview_empty_dataframe(self):
        """Preview should handle empty DataFrames."""
        df = pd.DataFrame({"a": [], "b": []})
        result = preview(df)

        assert len(result) == 2
        assert result[result["column"] == "a"].iloc[0]["null_pct"] == "0.0%"

    def test_preview_sample_values(self):
        """Preview should show sample values."""
        df = pd.DataFrame({
            "a": [1, 2, 3, 4, 5],
        })
        result = preview(df, sample_rows=3)

        a_row = result[result["column"] == "a"].iloc[0]
        assert "1" in a_row["sample_values"]
        assert "2" in a_row["sample_values"]
        assert "3" in a_row["sample_values"]


class TestSummary:
    """Tests for summary() function."""

    def test_summary_empty_session(self):
        """Summary should return empty DataFrame for empty session."""
        with BuildSession() as session:
            result = summary(session)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            assert "#" in result.columns
            assert "operation" in result.columns

    def test_summary_returns_dataframe(self):
        """Summary should return a DataFrame with operation history."""
        with BuildSession() as session:
            result = summary(session)

            assert isinstance(result, pd.DataFrame)
            expected_cols = [
                "#", "operation", "params", "rows_before", "rows_after",
                "change", "change_pct", "affected_cols", "duration_ms"
            ]
            assert list(result.columns) == expected_cols

    def test_summary_without_params(self):
        """Summary should omit params columns when include_params=False."""
        with BuildSession() as session:
            result = summary(session, include_params=False)

            assert "params" not in result.columns
            assert "affected_cols" not in result.columns


class TestDiff:
    """Tests for diff() function."""

    def test_diff_returns_dataframe(self):
        """Diff should return a DataFrame with comparison metrics."""
        before = pd.DataFrame({"a": [1, 2, 3]})
        after = pd.DataFrame({"a": [1, 2]})

        result = diff(before, after)

        assert isinstance(result, pd.DataFrame)
        assert "metric" in result.columns
        assert "before" in result.columns
        assert "after" in result.columns
        assert "change" in result.columns

    def test_diff_row_count(self):
        """Diff should show row count changes."""
        before = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        after = pd.DataFrame({"a": [1, 2, 3]})

        result = diff(before, after)

        row_count = result[result["metric"] == "row_count"].iloc[0]
        assert row_count["before"] == 5
        assert row_count["after"] == 3
        assert row_count["change"] == -2

    def test_diff_column_count(self):
        """Diff should show column count changes."""
        before = pd.DataFrame({"a": [1], "b": [2]})
        after = pd.DataFrame({"a": [1], "b": [2], "c": [3]})

        result = diff(before, after)

        col_count = result[result["metric"] == "column_count"].iloc[0]
        assert col_count["before"] == 2
        assert col_count["after"] == 3
        assert col_count["change"] == 1

    def test_diff_columns_added(self):
        """Diff should show added columns."""
        before = pd.DataFrame({"a": [1]})
        after = pd.DataFrame({"a": [1], "b": [2], "c": [3]})

        result = diff(before, after)

        added = result[result["metric"] == "columns_added"].iloc[0]
        assert "b" in added["after"]
        assert "c" in added["after"]
        assert added["change"] == 2

    def test_diff_columns_removed(self):
        """Diff should show removed columns."""
        before = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        after = pd.DataFrame({"a": [1]})

        result = diff(before, after)

        removed = result[result["metric"] == "columns_removed"].iloc[0]
        assert "b" in removed["before"]
        assert "c" in removed["before"]
        assert removed["change"] == -2

    def test_diff_dtype_changes(self):
        """Diff should show dtype changes."""
        before = pd.DataFrame({"a": [1, 2, 3]})
        after = pd.DataFrame({"a": ["1", "2", "3"]})

        result = diff(before, after)

        dtype_row = result[result["metric"] == "dtype_changes"].iloc[0]
        assert "a:" in dtype_row["after"]
        assert dtype_row["change"] == 1

    def test_diff_no_changes(self):
        """Diff should handle identical DataFrames."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        result = diff(df, df.copy())

        row_count = result[result["metric"] == "row_count"].iloc[0]
        assert row_count["change"] == 0

        added = result[result["metric"] == "columns_added"].iloc[0]
        assert added["after"] == "(none)"


class TestImports:
    """Tests for module imports."""

    def test_import_from_display_submodule(self):
        """Functions should be importable from display submodule."""
        from migrator_studio.display import preview, summary, diff

        assert callable(preview)
        assert callable(summary)
        assert callable(diff)

    def test_import_from_main_module(self):
        """Functions should be importable from main module."""
        from migrator_studio import preview, summary, diff

        assert callable(preview)
        assert callable(summary)
        assert callable(diff)
