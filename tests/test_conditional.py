"""Tests for conditional operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    where,
    case,
    fill_null,
    coalesce,
)


class TestWhere:
    """Tests for where function."""

    def test_basic_where(self, sample_df):
        """where should apply if-else logic."""
        result = where(sample_df, "priority", sample_df["amount"] > 200, "High", "Normal")

        high_rows = result[result["amount"] > 200]
        normal_rows = result[result["amount"] <= 200]

        assert all(high_rows["priority"] == "High")
        assert all(normal_rows["priority"] == "Normal")

    def test_else_none(self, sample_df):
        """where should use None as default else value."""
        result = where(sample_df, "vip", sample_df["amount"] > 200, True)

        assert result[result["amount"] > 200]["vip"].all()
        assert result[result["amount"] <= 200]["vip"].isna().all()

    def test_tracking_in_build_mode(self, sample_df):
        """where should track operations in build mode."""
        with BuildSession() as session:
            where(sample_df, "flag", sample_df["amount"] > 100, 1, 0)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "where"


class TestCase:
    """Tests for case function."""

    def test_basic_case(self, sample_df):
        """case should apply multiple conditions."""
        result = case(sample_df, "priority", [
            (sample_df["amount"] > 250, "High"),
            (sample_df["amount"] > 100, "Medium"),
        ], default="Low")

        assert result[result["amount"] > 250]["priority"].iloc[0] == "High"
        assert result[(result["amount"] > 100) & (result["amount"] <= 250)]["priority"].iloc[0] == "Medium"
        assert result[result["amount"] <= 100]["priority"].iloc[0] == "Low"

    def test_first_match_wins(self):
        """case should use first matching condition."""
        df = pd.DataFrame({"value": [100]})

        result = case(df, "result", [
            (df["value"] > 50, "A"),
            (df["value"] > 10, "B"),
        ], default="C")

        assert result["result"].iloc[0] == "A"

    def test_default_value(self):
        """case should use default when no condition matches."""
        df = pd.DataFrame({"value": [5]})

        result = case(df, "result", [
            (df["value"] > 100, "High"),
            (df["value"] > 50, "Medium"),
        ], default="Low")

        assert result["result"].iloc[0] == "Low"

    def test_none_default(self):
        """case should use None as default."""
        df = pd.DataFrame({"value": [5]})

        result = case(df, "result", [
            (df["value"] > 100, "High"),
        ])

        assert result["result"].iloc[0] is None

    def test_tracking_in_build_mode(self, sample_df):
        """case should track operations in build mode."""
        with BuildSession() as session:
            case(sample_df, "flag", [(sample_df["amount"] > 100, 1)], default=0)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "case"


class TestFillNull:
    """Tests for fill_null function."""

    def test_fills_null_values(self):
        """fill_null should replace null values."""
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})

        result = fill_null(df, "value", 0)

        assert list(result["value"]) == [1.0, 0, 3.0, 0, 5.0]

    def test_fills_with_string(self):
        """fill_null should fill with string value."""
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})

        result = fill_null(df, "name", "Unknown")

        assert list(result["name"]) == ["Alice", "Unknown", "Charlie"]

    def test_target_column(self):
        """fill_null should write to target column when specified."""
        df = pd.DataFrame({"value": [1, None, 3]})

        result = fill_null(df, "value", 0, target="value_filled")

        assert "value_filled" in result.columns
        assert pd.isna(result["value"].iloc[1])  # original unchanged
        assert result["value_filled"].iloc[1] == 0

    def test_missing_column_raises(self):
        """fill_null should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="fill_null failed.*nonexistent.*not found"):
            fill_null(df, "nonexistent", 0)

    def test_tracking_in_build_mode(self):
        """fill_null should track operations in build mode."""
        df = pd.DataFrame({"value": [1, None]})

        with BuildSession() as session:
            fill_null(df, "value", 0)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "fill_null"


class TestCoalesce:
    """Tests for coalesce function."""

    def test_basic_coalesce(self):
        """coalesce should return first non-null value."""
        df = pd.DataFrame({
            "phone1": [None, "555-1234", None],
            "phone2": ["555-0000", None, None],
            "phone3": ["555-9999", "555-9999", "555-9999"],
        })

        result = coalesce(df, ["phone1", "phone2", "phone3"], "phone")

        assert list(result["phone"]) == ["555-0000", "555-1234", "555-9999"]

    def test_all_null(self):
        """coalesce should return null if all values are null."""
        df = pd.DataFrame({
            "a": [None],
            "b": [None],
        })

        result = coalesce(df, ["a", "b"], "result")

        assert pd.isna(result["result"].iloc[0])

    def test_first_column_takes_precedence(self):
        """coalesce should prefer earlier columns."""
        df = pd.DataFrame({
            "a": ["first", None],
            "b": ["second", "second"],
        })

        result = coalesce(df, ["a", "b"], "result")

        assert result["result"].iloc[0] == "first"
        assert result["result"].iloc[1] == "second"

    def test_missing_column_raises(self):
        """coalesce should raise KeyError for missing columns."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="coalesce failed.*nonexistent.*not found"):
            coalesce(df, ["a", "nonexistent"], "result")

    def test_tracking_in_build_mode(self):
        """coalesce should track operations in build mode."""
        df = pd.DataFrame({"a": [1], "b": [2]})

        with BuildSession() as session:
            coalesce(df, ["a", "b"], "result")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "coalesce"
