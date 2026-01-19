"""Tests for merge operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    merge_left,
    merge_inner,
    merge_outer,
)


class TestMergeLeft:
    """Tests for merge_left function."""

    def test_merge_left_basic(self, sample_df, lookup_df):
        """merge_left should keep all left rows."""
        result = merge_left(sample_df, lookup_df, on="region")

        # All 5 rows from left should be present
        assert len(result) == 5
        # Should have added columns from right
        assert "region_name" in result.columns
        assert "manager" in result.columns

    def test_merge_left_unmatched_rows_have_nulls(self):
        """merge_left should have nulls for unmatched right values."""
        left = pd.DataFrame({"id": [1, 2, 3], "key": ["A", "B", "C"]})
        right = pd.DataFrame({"key": ["A", "B"], "value": [10, 20]})

        result = merge_left(left, right, on="key")

        assert len(result) == 3
        assert result.loc[result["key"] == "C", "value"].isna().all()

    def test_merge_left_different_column_names(self):
        """merge_left should work with left_on/right_on."""
        left = pd.DataFrame({"id": [1, 2], "code": ["X", "Y"]})
        right = pd.DataFrame({"legacy_code": ["X", "Y"], "name": ["Ex", "Why"]})

        result = merge_left(left, right, left_on="code", right_on="legacy_code")

        assert len(result) == 2
        assert "name" in result.columns

    def test_merge_left_select_columns(self, sample_df, lookup_df):
        """merge_left with select_columns should only include specified columns."""
        result = merge_left(
            sample_df, lookup_df,
            on="region",
            select_columns=["manager"]
        )

        assert "manager" in result.columns
        assert "region_name" not in result.columns

    def test_merge_left_tracking(self, sample_df, lookup_df):
        """merge_left should track operations in build mode."""
        with BuildSession() as session:
            result = merge_left(sample_df, lookup_df, on="region")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "merge_left"
            assert op.rows_before == 5
            assert op.rows_after == 5


class TestMergeInner:
    """Tests for merge_inner function."""

    def test_merge_inner_basic(self):
        """merge_inner should keep only matching rows."""
        left = pd.DataFrame({"id": [1, 2, 3], "key": ["A", "B", "C"]})
        right = pd.DataFrame({"key": ["A", "B", "D"], "value": [10, 20, 40]})

        result = merge_inner(left, right, on="key")

        assert len(result) == 2
        assert set(result["key"]) == {"A", "B"}

    def test_merge_inner_no_matches(self):
        """merge_inner with no matches should return empty DataFrame."""
        left = pd.DataFrame({"id": [1, 2], "key": ["A", "B"]})
        right = pd.DataFrame({"key": ["C", "D"], "value": [30, 40]})

        result = merge_inner(left, right, on="key")

        assert len(result) == 0

    def test_merge_inner_select_columns(self):
        """merge_inner with select_columns should work."""
        left = pd.DataFrame({"id": [1, 2], "key": ["A", "B"]})
        right = pd.DataFrame({
            "key": ["A", "B"],
            "value1": [10, 20],
            "value2": [100, 200]
        })

        result = merge_inner(left, right, on="key", select_columns=["value1"])

        assert "value1" in result.columns
        assert "value2" not in result.columns


class TestMergeOuter:
    """Tests for merge_outer function."""

    def test_merge_outer_basic(self):
        """merge_outer should keep all rows from both sides."""
        left = pd.DataFrame({"id": [1, 2], "key": ["A", "B"]})
        right = pd.DataFrame({"key": ["B", "C"], "value": [20, 30]})

        result = merge_outer(left, right, on="key")

        assert len(result) == 3
        assert set(result["key"]) == {"A", "B", "C"}

    def test_merge_outer_nulls_on_both_sides(self):
        """merge_outer should have nulls for unmatched rows on both sides."""
        left = pd.DataFrame({"id": [1, 2], "key": ["A", "B"]})
        right = pd.DataFrame({"key": ["B", "C"], "value": [20, 30]})

        result = merge_outer(left, right, on="key")

        # Row with key A has no value
        assert result.loc[result["key"] == "A", "value"].isna().all()
        # Row with key C has no id
        assert result.loc[result["key"] == "C", "id"].isna().all()

    def test_merge_outer_select_columns(self):
        """merge_outer with select_columns should only include specified columns."""
        left = pd.DataFrame({"id": [1, 2], "key": ["A", "B"]})
        right = pd.DataFrame({
            "key": ["A", "B", "C"],
            "value1": [10, 20, 30],
            "value2": [100, 200, 300]
        })

        result = merge_outer(left, right, on="key", select_columns=["value1"])

        assert "value1" in result.columns
        assert "value2" not in result.columns


class TestMergeMultiColumn:
    """Tests for multi-column merges."""

    def test_merge_on_multiple_columns(self):
        """Merge should work with multiple join columns."""
        left = pd.DataFrame({
            "region": ["North", "North", "South"],
            "year": [2024, 2025, 2024],
            "sales": [100, 150, 200]
        })
        right = pd.DataFrame({
            "region": ["North", "South"],
            "year": [2025, 2024],
            "target": [140, 180]
        })

        result = merge_left(left, right, on=["region", "year"])

        assert len(result) == 3
        # Only rows matching both region and year should have target
        matched = result[result["target"].notna()]
        assert len(matched) == 2
