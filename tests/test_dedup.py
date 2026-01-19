"""Tests for deduplication operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    drop_duplicates,
    keep_max,
    keep_min,
)


class TestDropDuplicates:
    """Tests for drop_duplicates function."""

    def test_basic_dedup(self):
        """drop_duplicates should remove duplicate rows."""
        df = pd.DataFrame({
            "id": [1, 2, 1, 3, 2],
            "name": ["A", "B", "A2", "C", "B2"],
        })

        result = drop_duplicates(df, "id")

        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]

    def test_keep_first(self):
        """drop_duplicates with keep='first' should keep first occurrence."""
        df = pd.DataFrame({
            "id": [1, 2, 1],
            "name": ["First", "B", "Second"],
        })

        result = drop_duplicates(df, "id", keep="first")

        first_row = result[result["id"] == 1].iloc[0]
        assert first_row["name"] == "First"

    def test_keep_last(self):
        """drop_duplicates with keep='last' should keep last occurrence."""
        df = pd.DataFrame({
            "id": [1, 2, 1],
            "name": ["First", "B", "Second"],
        })

        result = drop_duplicates(df, "id", keep="last")

        last_row = result[result["id"] == 1].iloc[0]
        assert last_row["name"] == "Second"

    def test_multiple_columns(self):
        """drop_duplicates should work with multiple columns."""
        df = pd.DataFrame({
            "order": [1, 1, 2],
            "branch": ["A", "A", "A"],
            "amount": [100, 200, 300],
        })

        result = drop_duplicates(df, ["order", "branch"])

        assert len(result) == 2

    def test_resets_index(self):
        """drop_duplicates should reset index."""
        df = pd.DataFrame({
            "id": [1, 2, 1],
            "name": ["A", "B", "C"],
        })

        result = drop_duplicates(df, "id")

        assert list(result.index) == [0, 1]

    def test_missing_column_raises(self):
        """drop_duplicates should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="drop_duplicates failed.*nonexistent.*not found"):
            drop_duplicates(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """drop_duplicates should track operations in build mode."""
        df = pd.DataFrame({"id": [1, 1, 2]})

        with BuildSession() as session:
            drop_duplicates(df, "id")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "drop_duplicates"


class TestKeepMax:
    """Tests for keep_max function."""

    def test_basic_keep_max(self):
        """keep_max should keep row with max value in each group."""
        df = pd.DataFrame({
            "customer": ["A", "A", "B", "B"],
            "order_date": ["2025-01-01", "2025-02-01", "2025-01-15", "2025-03-01"],
        })

        result = keep_max(df, "customer", "order_date")

        assert len(result) == 2
        a_row = result[result["customer"] == "A"].iloc[0]
        b_row = result[result["customer"] == "B"].iloc[0]
        assert a_row["order_date"] == "2025-02-01"
        assert b_row["order_date"] == "2025-03-01"

    def test_numeric_values(self):
        """keep_max should work with numeric values."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "amount": [100, 300, 200],
        })

        result = keep_max(df, "region", "amount")

        north_row = result[result["region"] == "North"].iloc[0]
        assert north_row["amount"] == 300

    def test_multiple_group_columns(self):
        """keep_max should work with multiple group columns."""
        df = pd.DataFrame({
            "region": ["A", "A", "A", "A"],
            "category": ["X", "X", "Y", "Y"],
            "value": [1, 2, 3, 4],
        })

        result = keep_max(df, ["region", "category"], "value")

        assert len(result) == 2
        x_row = result[result["category"] == "X"].iloc[0]
        y_row = result[result["category"] == "Y"].iloc[0]
        assert x_row["value"] == 2
        assert y_row["value"] == 4

    def test_missing_by_column_raises(self):
        """keep_max should raise KeyError for missing group column."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

        with pytest.raises(KeyError, match="keep_max failed.*nonexistent.*not found"):
            keep_max(df, "nonexistent", "b")

    def test_missing_value_column_raises(self):
        """keep_max should raise KeyError for missing value column."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

        with pytest.raises(KeyError, match="keep_max failed.*nonexistent.*not found"):
            keep_max(df, "a", "nonexistent")

    def test_tracking_in_build_mode(self):
        """keep_max should track operations in build mode."""
        df = pd.DataFrame({"id": [1, 1], "val": [1, 2]})

        with BuildSession() as session:
            keep_max(df, "id", "val")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "keep_max"


class TestKeepMin:
    """Tests for keep_min function."""

    def test_basic_keep_min(self):
        """keep_min should keep row with min value in each group."""
        df = pd.DataFrame({
            "customer": ["A", "A", "B", "B"],
            "order_date": ["2025-01-01", "2025-02-01", "2025-01-15", "2025-03-01"],
        })

        result = keep_min(df, "customer", "order_date")

        assert len(result) == 2
        a_row = result[result["customer"] == "A"].iloc[0]
        b_row = result[result["customer"] == "B"].iloc[0]
        assert a_row["order_date"] == "2025-01-01"
        assert b_row["order_date"] == "2025-01-15"

    def test_numeric_values(self):
        """keep_min should work with numeric values."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "price": [100, 50, 200],
        })

        result = keep_min(df, "region", "price")

        north_row = result[result["region"] == "North"].iloc[0]
        assert north_row["price"] == 50

    def test_missing_column_raises(self):
        """keep_min should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

        with pytest.raises(KeyError, match="keep_min failed.*nonexistent.*not found"):
            keep_min(df, "nonexistent", "b")

    def test_tracking_in_build_mode(self):
        """keep_min should track operations in build mode."""
        df = pd.DataFrame({"id": [1, 1], "val": [1, 2]})

        with BuildSession() as session:
            keep_min(df, "id", "val")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "keep_min"
