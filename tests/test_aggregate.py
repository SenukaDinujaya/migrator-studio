"""Tests for aggregation operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    groupby_agg,
    groupby_concat,
)


class TestGroupbyAgg:
    """Tests for groupby_agg function."""

    def test_single_agg(self):
        """groupby_agg should perform single aggregation."""
        df = pd.DataFrame({
            "region": ["North", "North", "South", "South"],
            "amount": [100, 200, 150, 250],
        })

        result = groupby_agg(df, "region", {"amount": "sum"})

        assert len(result) == 2
        north_row = result[result["region"] == "North"].iloc[0]
        south_row = result[result["region"] == "South"].iloc[0]
        assert north_row["amount"] == 300
        assert south_row["amount"] == 400

    def test_multiple_aggs_same_column(self):
        """groupby_agg should perform multiple aggregations on same column."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "amount": [100, 200, 150],
        })

        result = groupby_agg(df, "region", {"amount": ["sum", "mean"]})

        assert "amount_sum" in result.columns
        assert "amount_mean" in result.columns

    def test_multiple_columns(self):
        """groupby_agg should aggregate multiple columns."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "amount": [100, 200, 150],
            "qty": [1, 2, 3],
        })

        result = groupby_agg(df, "region", {"amount": "sum", "qty": "sum"})

        assert "amount" in result.columns
        assert "qty" in result.columns

    def test_multiple_group_columns(self):
        """groupby_agg should work with multiple group columns."""
        df = pd.DataFrame({
            "region": ["North", "North", "North", "South"],
            "category": ["A", "A", "B", "A"],
            "amount": [100, 200, 150, 250],
        })

        result = groupby_agg(df, ["region", "category"], {"amount": "sum"})

        assert len(result) == 3

    def test_count_agg(self):
        """groupby_agg should work with count."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "amount": [100, 200, 150],
        })

        result = groupby_agg(df, "region", {"amount": "count"})

        north_row = result[result["region"] == "North"].iloc[0]
        assert north_row["amount"] == 2

    def test_missing_group_column_raises(self):
        """groupby_agg should raise KeyError for missing group column."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

        with pytest.raises(KeyError, match="groupby_agg failed.*nonexistent.*not found"):
            groupby_agg(df, "nonexistent", {"b": "sum"})

    def test_missing_agg_column_raises(self):
        """groupby_agg should raise KeyError for missing agg column."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

        with pytest.raises(KeyError, match="groupby_agg failed.*nonexistent.*not found"):
            groupby_agg(df, "a", {"nonexistent": "sum"})

    def test_tracking_in_build_mode(self):
        """groupby_agg should track operations in build mode."""
        df = pd.DataFrame({"region": ["A", "A"], "amount": [1, 2]})

        with BuildSession() as session:
            groupby_agg(df, "region", {"amount": "sum"})

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "groupby_agg"


class TestGroupbyConcat:
    """Tests for groupby_concat function."""

    def test_basic_concat(self):
        """groupby_concat should concatenate strings in groups."""
        df = pd.DataFrame({
            "order_id": [1, 1, 2],
            "item": ["Apple", "Banana", "Cherry"],
        })

        result = groupby_concat(df, "order_id", "item", "items")

        assert len(result) == 2
        order1 = result[result["order_id"] == 1].iloc[0]
        assert order1["items"] == "Apple Banana"

    def test_custom_separator(self):
        """groupby_concat should use custom separator."""
        df = pd.DataFrame({
            "order_id": [1, 1],
            "item": ["Apple", "Banana"],
        })

        result = groupby_concat(df, "order_id", "item", "items", sep=", ")

        order1 = result[result["order_id"] == 1].iloc[0]
        assert order1["items"] == "Apple, Banana"

    def test_handles_null_values(self):
        """groupby_concat should skip null values."""
        df = pd.DataFrame({
            "order_id": [1, 1, 1],
            "item": ["Apple", None, "Banana"],
        })

        result = groupby_concat(df, "order_id", "item", "items")

        order1 = result[result["order_id"] == 1].iloc[0]
        assert order1["items"] == "Apple Banana"

    def test_multiple_group_columns(self):
        """groupby_concat should work with multiple group columns."""
        df = pd.DataFrame({
            "customer": ["A", "A", "A"],
            "year": [2024, 2024, 2025],
            "item": ["X", "Y", "Z"],
        })

        result = groupby_concat(df, ["customer", "year"], "item", "items")

        assert len(result) == 2

    def test_missing_group_column_raises(self):
        """groupby_concat should raise KeyError for missing group column."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        with pytest.raises(KeyError, match="groupby_concat failed.*nonexistent.*not found"):
            groupby_concat(df, "nonexistent", "b", "result")

    def test_missing_concat_column_raises(self):
        """groupby_concat should raise KeyError for missing concat column."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        with pytest.raises(KeyError, match="groupby_concat failed.*nonexistent.*not found"):
            groupby_concat(df, "a", "nonexistent", "result")

    def test_tracking_in_build_mode(self):
        """groupby_concat should track operations in build mode."""
        df = pd.DataFrame({"id": [1, 1], "msg": ["a", "b"]})

        with BuildSession() as session:
            groupby_concat(df, "id", "msg", "all_msgs")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "groupby_concat"
