"""Tests for operation registry."""

import pytest

from migrator_studio import list_operations, get_operation


class TestRegistry:
    """Tests for operation registry functions."""

    def test_list_operations_returns_list(self):
        """list_operations should return a list of operation names."""
        ops = list_operations()

        assert isinstance(ops, list)
        assert len(ops) > 0

    def test_list_operations_includes_all_patterns(self):
        """list_operations should include operations from all patterns."""
        ops = list_operations()

        # Filter operations
        assert "filter_isin" in ops
        assert "filter_not_isin" in ops

        # Merge operations
        assert "merge_left" in ops

        # Mapping operations
        assert "map_dict" in ops
        assert "map_lookup" in ops

        # Field operations
        assert "copy_column" in ops
        assert "set_value" in ops

        # Date operations
        assert "parse_date" in ops

        # Aggregate operations
        assert "groupby_agg" in ops

        # Convert operations
        assert "to_numeric" in ops

        # Conditional operations
        assert "where" in ops
        assert "case" in ops

        # String operations
        assert "str_upper" in ops

        # Dedup operations
        assert "drop_duplicates" in ops

        # Apply operations
        assert "apply_row" in ops
        assert "transform" in ops

    def test_get_operation_returns_function(self):
        """get_operation should return the operation function."""
        op = get_operation("filter_isin")

        assert callable(op)

    def test_get_operation_unknown_raises(self):
        """get_operation should raise KeyError for unknown operation."""
        with pytest.raises(KeyError, match="Operation.*not found"):
            get_operation("nonexistent_operation")

    def test_get_operation_error_includes_available(self):
        """get_operation error should include list of available operations."""
        with pytest.raises(KeyError, match="Available"):
            get_operation("nonexistent_operation")
