"""Tests for field operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    copy_column,
    set_value,
    concat_columns,
    rename_columns,
    drop_columns,
    select_columns,
)


class TestCopyColumn:
    """Tests for copy_column function."""

    def test_basic_copy(self, sample_df):
        """copy_column should copy values to new column."""
        result = copy_column(sample_df, "name", "name_copy")

        assert "name_copy" in result.columns
        assert list(result["name_copy"]) == list(result["name"])

    def test_overwrite_existing_column(self, sample_df):
        """copy_column should overwrite existing target column."""
        result = copy_column(sample_df, "name", "status")

        assert list(result["status"]) == list(sample_df["name"])

    def test_missing_source_raises(self):
        """copy_column should raise KeyError for missing source column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="copy_column failed.*nonexistent.*not found"):
            copy_column(df, "nonexistent", "target")

    def test_tracking_in_build_mode(self, sample_df):
        """copy_column should track operations in build mode."""
        with BuildSession() as session:
            copy_column(sample_df, "name", "name_copy")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "copy_column"


class TestSetValue:
    """Tests for set_value function."""

    def test_set_string_value(self, sample_df):
        """set_value should set constant string value."""
        result = set_value(sample_df, "company", "ArrowCorp")

        assert "company" in result.columns
        assert all(result["company"] == "ArrowCorp")

    def test_set_numeric_value(self, sample_df):
        """set_value should set constant numeric value."""
        result = set_value(sample_df, "flag", 1)

        assert all(result["flag"] == 1)

    def test_set_none_value(self, sample_df):
        """set_value should set None value."""
        result = set_value(sample_df, "empty", None)

        assert all(result["empty"].isna())

    def test_overwrite_existing_column(self, sample_df):
        """set_value should overwrite existing column."""
        result = set_value(sample_df, "status", "All Active")

        assert all(result["status"] == "All Active")

    def test_tracking_in_build_mode(self, sample_df):
        """set_value should track operations in build mode."""
        with BuildSession() as session:
            set_value(sample_df, "company", "Test")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "set_value"


class TestConcatColumns:
    """Tests for concat_columns function."""

    def test_basic_concatenation(self):
        """concat_columns should concatenate columns."""
        df = pd.DataFrame({
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
        })

        result = concat_columns(df, ["first", "last"], "full_name")

        assert list(result["full_name"]) == ["John Doe", "Jane Smith"]

    def test_custom_separator(self):
        """concat_columns should use custom separator."""
        df = pd.DataFrame({
            "city": ["New York", "Boston"],
            "state": ["NY", "MA"],
        })

        result = concat_columns(df, ["city", "state"], "location", sep=", ")

        assert list(result["location"]) == ["New York, NY", "Boston, MA"]

    def test_handles_null_values(self):
        """concat_columns should handle null values as empty strings."""
        df = pd.DataFrame({
            "first": ["John", None],
            "last": ["Doe", "Smith"],
        })

        result = concat_columns(df, ["first", "last"], "full_name")

        assert result["full_name"].iloc[0] == "John Doe"
        assert result["full_name"].iloc[1] == " Smith"

    def test_missing_column_raises(self):
        """concat_columns should raise KeyError for missing columns."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="concat_columns failed.*nonexistent.*not found"):
            concat_columns(df, ["a", "nonexistent"], "target")

    def test_tracking_in_build_mode(self):
        """concat_columns should track operations in build mode."""
        df = pd.DataFrame({"a": ["x"], "b": ["y"]})

        with BuildSession() as session:
            concat_columns(df, ["a", "b"], "c")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "concat_columns"


class TestRenameColumns:
    """Tests for rename_columns function."""

    def test_basic_rename(self, sample_df):
        """rename_columns should rename columns."""
        result = rename_columns(sample_df, {"name": "customer_name"})

        assert "customer_name" in result.columns
        assert "name" not in result.columns

    def test_multiple_renames(self, sample_df):
        """rename_columns should rename multiple columns."""
        result = rename_columns(sample_df, {
            "name": "customer_name",
            "status": "customer_status",
        })

        assert "customer_name" in result.columns
        assert "customer_status" in result.columns
        assert "name" not in result.columns
        assert "status" not in result.columns

    def test_missing_column_raises(self):
        """rename_columns should raise KeyError for missing columns."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="rename_columns failed.*nonexistent.*not found"):
            rename_columns(df, {"nonexistent": "new_name"})

    def test_tracking_in_build_mode(self, sample_df):
        """rename_columns should track operations in build mode."""
        with BuildSession() as session:
            rename_columns(sample_df, {"name": "n"})

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "rename_columns"


class TestDropColumns:
    """Tests for drop_columns function."""

    def test_basic_drop(self, sample_df):
        """drop_columns should remove specified columns."""
        result = drop_columns(sample_df, ["amount"])

        assert "amount" not in result.columns
        assert "name" in result.columns

    def test_multiple_drops(self, sample_df):
        """drop_columns should remove multiple columns."""
        result = drop_columns(sample_df, ["amount", "region"])

        assert "amount" not in result.columns
        assert "region" not in result.columns
        assert "name" in result.columns

    def test_missing_column_raises(self):
        """drop_columns should raise KeyError for missing columns."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="drop_columns failed.*nonexistent.*not found"):
            drop_columns(df, ["nonexistent"])

    def test_tracking_in_build_mode(self, sample_df):
        """drop_columns should track operations in build mode."""
        with BuildSession() as session:
            drop_columns(sample_df, ["amount"])

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "drop_columns"


class TestSelectColumns:
    """Tests for select_columns function."""

    def test_basic_select(self, sample_df):
        """select_columns should keep only specified columns."""
        result = select_columns(sample_df, ["id", "name"])

        assert list(result.columns) == ["id", "name"]

    def test_preserves_order(self, sample_df):
        """select_columns should preserve specified column order."""
        result = select_columns(sample_df, ["name", "id", "status"])

        assert list(result.columns) == ["name", "id", "status"]

    def test_missing_column_raises(self):
        """select_columns should raise KeyError for missing columns."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="select_columns failed.*nonexistent.*not found"):
            select_columns(df, ["a", "nonexistent"])

    def test_tracking_in_build_mode(self, sample_df):
        """select_columns should track operations in build mode."""
        with BuildSession() as session:
            select_columns(sample_df, ["id", "name"])

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "select_columns"
