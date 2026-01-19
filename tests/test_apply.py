"""Tests for apply operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    apply_row,
    apply_column,
    transform,
)


class TestApplyRow:
    """Tests for apply_row function."""

    def test_basic_apply_row(self):
        """apply_row should apply function to each row."""
        df = pd.DataFrame({
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
        })

        result = apply_row(df, lambda row: f"{row['first']} {row['last']}", "full_name")

        assert list(result["full_name"]) == ["John Doe", "Jane Smith"]

    def test_calculation(self):
        """apply_row should work with calculations."""
        df = pd.DataFrame({
            "price": [10.0, 20.0],
            "qty": [2, 3],
        })

        result = apply_row(df, lambda row: row["price"] * row["qty"], "total")

        assert list(result["total"]) == [20.0, 60.0]

    def test_conditional_logic(self):
        """apply_row should work with conditional logic."""
        df = pd.DataFrame({
            "amount": [100, 500, 1500],
        })

        def get_tier(row):
            if row["amount"] >= 1000:
                return "Gold"
            elif row["amount"] >= 200:
                return "Silver"
            return "Bronze"

        result = apply_row(df, get_tier, "tier")

        assert list(result["tier"]) == ["Bronze", "Silver", "Gold"]

    def test_returns_list(self):
        """apply_row should work with functions returning lists."""
        df = pd.DataFrame({
            "item1": ["A", "C"],
            "item2": ["B", None],
        })

        def build_items(row):
            items = []
            if pd.notna(row["item1"]):
                items.append({"code": row["item1"]})
            if pd.notna(row["item2"]):
                items.append({"code": row["item2"]})
            return items

        result = apply_row(df, build_items, "items")

        assert len(result["items"].iloc[0]) == 2
        assert len(result["items"].iloc[1]) == 1

    def test_tracking_in_build_mode(self):
        """apply_row should track operations in build mode."""
        df = pd.DataFrame({"a": [1], "b": [2]})

        with BuildSession() as session:
            apply_row(df, lambda row: row["a"] + row["b"], "sum")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "apply_row"


class TestApplyColumn:
    """Tests for apply_column function."""

    def test_basic_apply_column(self):
        """apply_column should apply function to each value."""
        df = pd.DataFrame({"name": ["  alice  ", "  bob  "]})

        result = apply_column(df, "name", lambda x: x.strip().title())

        assert list(result["name"]) == ["Alice", "Bob"]

    def test_numeric_transformation(self):
        """apply_column should work with numeric transformations."""
        df = pd.DataFrame({"amount": [100, 200, 300]})

        result = apply_column(df, "amount", lambda x: x * 2)

        assert list(result["amount"]) == [200, 400, 600]

    def test_target_column(self):
        """apply_column should write to target column when specified."""
        df = pd.DataFrame({"email": ["john@example.com"]})

        result = apply_column(
            df, "email",
            lambda x: x.split("@")[1] if "@" in str(x) else None,
            target="domain"
        )

        assert result["domain"].iloc[0] == "example.com"
        assert result["email"].iloc[0] == "john@example.com"  # original unchanged

    def test_handles_null(self):
        """apply_column should handle null values."""
        df = pd.DataFrame({"value": ["abc", None, "def"]})

        result = apply_column(df, "value", lambda x: x.upper() if x else None)

        assert result["value"].iloc[0] == "ABC"
        assert result["value"].iloc[1] is None
        assert result["value"].iloc[2] == "DEF"

    def test_missing_column_raises(self):
        """apply_column should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="apply_column failed.*nonexistent.*not found"):
            apply_column(df, "nonexistent", lambda x: x)

    def test_tracking_in_build_mode(self):
        """apply_column should track operations in build mode."""
        df = pd.DataFrame({"name": ["alice"]})

        with BuildSession() as session:
            apply_column(df, "name", str.upper)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "apply_column"


class TestTransform:
    """Tests for transform function."""

    def test_basic_transform(self):
        """transform should apply function to DataFrame."""
        df = pd.DataFrame({
            "status": ["Active", "Inactive", "Active"],
            "amount": [100, 200, 300],
        })

        def filter_active(df):
            return df[df["status"] == "Active"]

        result = transform(df, filter_active)

        assert len(result) == 2
        assert all(result["status"] == "Active")

    def test_complex_transform(self):
        """transform should work with complex transformations."""
        df = pd.DataFrame({
            "region": ["North", "North", "South"],
            "amount": [100, 200, 150],
        })

        def summarize(df):
            return df.groupby("region")["amount"].sum().reset_index()

        result = transform(df, summarize)

        assert len(result) == 2
        assert "region" in result.columns
        assert "amount" in result.columns

    def test_does_not_modify_original(self):
        """transform should not modify original DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        original_len = len(df)

        def drop_first(df):
            return df.iloc[1:]

        result = transform(df, drop_first)

        assert len(df) == original_len
        assert len(result) == original_len - 1

    def test_pivot_operation(self):
        """transform should work with pivot operations."""
        df = pd.DataFrame({
            "date": ["2025-01", "2025-01", "2025-02"],
            "category": ["A", "B", "A"],
            "value": [10, 20, 30],
        })

        def pivot(df):
            return df.pivot_table(index="date", columns="category", values="value", aggfunc="sum")

        result = transform(df, pivot)

        assert "A" in result.columns
        assert "B" in result.columns

    def test_tracking_in_build_mode(self):
        """transform should track operations in build mode."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with BuildSession() as session:
            transform(df, lambda df: df[df["a"] > 1])

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "transform"
