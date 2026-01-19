"""Tests for mapping operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    map_dict,
    map_lookup,
)


class TestMapDict:
    """Tests for map_dict function."""

    def test_map_dict_basic(self, sample_df, status_mapping):
        """map_dict should replace values using dictionary."""
        result = map_dict(sample_df, "status", status_mapping)

        assert "Enabled" in result["status"].values
        assert "Under Review" in result["status"].values
        assert "Active" not in result["status"].values

    def test_map_dict_unmapped_become_na(self, sample_df):
        """Unmapped values should become NA by default."""
        partial_map = {"Active": "Enabled"}

        result = map_dict(sample_df, "status", partial_map)

        assert result.loc[result["status"].isna()].shape[0] == 2  # Pending and Inactive

    def test_map_dict_with_fallback(self, sample_df):
        """map_dict with fallback should use fallback for unmapped."""
        partial_map = {"Active": "Enabled"}

        result = map_dict(sample_df, "status", partial_map, fallback="Unknown")

        assert "Unknown" in result["status"].values
        assert result["status"].isna().sum() == 0

    def test_map_dict_fallback_original(self, sample_df):
        """map_dict with fallback_original should keep original for unmapped."""
        partial_map = {"Active": "Enabled"}

        result = map_dict(sample_df, "status", partial_map, fallback_original=True)

        assert "Enabled" in result["status"].values
        assert "Pending" in result["status"].values  # Original kept
        assert "Inactive" in result["status"].values  # Original kept

    def test_map_dict_to_different_column(self, sample_df, status_mapping):
        """map_dict should write to different column when target specified."""
        result = map_dict(
            sample_df, "status", status_mapping,
            target="status_mapped"
        )

        assert "status_mapped" in result.columns
        # Original column unchanged
        assert "Active" in result["status"].values

    def test_map_dict_tracking(self, sample_df, status_mapping):
        """map_dict should track operations in build mode."""
        with BuildSession() as session:
            result = map_dict(sample_df, "status", status_mapping)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "map_dict"
            assert op.rows_before == 5
            assert op.rows_after == 5

    def test_map_dict_multi_column(self):
        """map_dict should work with multi-column dict keys (tuples)."""
        df = pd.DataFrame({
            "branch": ["B1", "B1", "B2"],
            "region": ["R1", "R2", "R1"],
            "value": [10, 20, 30]
        })
        mapping = {
            ("B1", "R1"): "Territory A",
            ("B1", "R2"): "Territory B",
            ("B2", "R1"): "Territory C",
        }

        result = map_dict(
            df, ["branch", "region"], mapping,
            target="territory"
        )

        assert result.loc[0, "territory"] == "Territory A"
        assert result.loc[1, "territory"] == "Territory B"
        assert result.loc[2, "territory"] == "Territory C"

    def test_map_dict_multi_column_requires_target(self):
        """map_dict with multi-column should require target."""
        df = pd.DataFrame({
            "branch": ["B1"],
            "region": ["R1"]
        })
        mapping = {("B1", "R1"): "Territory A"}

        with pytest.raises(ValueError, match="target.*must be specified"):
            map_dict(df, ["branch", "region"], mapping)

    def test_map_dict_missing_column_raises(self):
        """map_dict should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="nonexistent"):
            map_dict(df, "nonexistent", {"x": "y"})


class TestMapLookup:
    """Tests for map_lookup function."""

    def test_map_lookup_basic(self, sample_df, lookup_df):
        """map_lookup should map using lookup DataFrame."""
        result = map_lookup(
            sample_df, "region",
            lookup_df, key_column="region", value_column="region_name"
        )

        assert "Northern Region" in result["region"].values
        assert "Southern Region" in result["region"].values

    def test_map_lookup_to_different_column(self, sample_df, lookup_df):
        """map_lookup should write to different column when specified."""
        result = map_lookup(
            sample_df, "region",
            lookup_df, key_column="region", value_column="region_name",
            target="region_full_name"
        )

        assert "region_full_name" in result.columns
        # Original column unchanged
        assert "North" in result["region"].values

    def test_map_lookup_with_fallback(self):
        """map_lookup with fallback should handle unmapped values."""
        df = pd.DataFrame({"code": ["A", "B", "X"]})
        lookup = pd.DataFrame({
            "code": ["A", "B"],
            "name": ["Alpha", "Beta"]
        })

        result = map_lookup(
            df, "code",
            lookup, key_column="code", value_column="name",
            fallback="Unknown"
        )

        # After mapping, code column contains: Alpha, Beta, Unknown
        assert result.loc[2, "code"] == "Unknown"

    def test_map_lookup_fallback_original(self):
        """map_lookup with fallback_original should keep original."""
        df = pd.DataFrame({"code": ["A", "B", "X"]})
        lookup = pd.DataFrame({
            "code": ["A", "B"],
            "name": ["Alpha", "Beta"]
        })

        result = map_lookup(
            df, "code",
            lookup, key_column="code", value_column="name",
            target="name",
            fallback_original=True
        )

        # X should keep its original value
        assert result.loc[2, "name"] == "X"

    def test_map_lookup_tracking(self, sample_df, lookup_df):
        """map_lookup should track operations in build mode."""
        with BuildSession() as session:
            result = map_lookup(
                sample_df, "region",
                lookup_df, key_column="region", value_column="region_name"
            )

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "map_lookup"

    def test_map_lookup_multi_column(self):
        """map_lookup should work with multi-column DataFrame keys."""
        df = pd.DataFrame({
            "branch": ["B1", "B1", "B2"],
            "region": ["R1", "R2", "R1"],
            "value": [10, 20, 30]
        })
        lookup = pd.DataFrame({
            "branch_code": ["B1", "B1", "B2"],
            "region_code": ["R1", "R2", "R1"],
            "territory_name": ["Territory A", "Territory B", "Territory C"]
        })

        result = map_lookup(
            df, ["branch", "region"],
            lookup, key_column=["branch_code", "region_code"], value_column="territory_name",
            target="territory"
        )

        assert result.loc[0, "territory"] == "Territory A"
        assert result.loc[1, "territory"] == "Territory B"
        assert result.loc[2, "territory"] == "Territory C"

    def test_map_lookup_multi_column_requires_target(self):
        """map_lookup with multi-column should require target."""
        df = pd.DataFrame({
            "branch": ["B1"],
            "region": ["R1"]
        })
        lookup = pd.DataFrame({
            "branch_code": ["B1"],
            "region_code": ["R1"],
            "territory_name": ["Territory A"]
        })

        with pytest.raises(ValueError, match="target.*must be specified"):
            map_lookup(
                df, ["branch", "region"],
                lookup, key_column=["branch_code", "region_code"], value_column="territory_name"
            )

    def test_map_lookup_missing_column_raises(self):
        """map_lookup should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        lookup = pd.DataFrame({"x": [1], "y": ["value"]})

        with pytest.raises(KeyError, match="nonexistent"):
            map_lookup(df, "nonexistent", lookup, key_column="x", value_column="y")

    def test_map_lookup_column_count_mismatch(self):
        """map_lookup should raise error if column counts don't match."""
        df = pd.DataFrame({"a": [1], "b": [2]})
        lookup = pd.DataFrame({"x": [1], "y": ["value"]})

        with pytest.raises(ValueError, match="column count"):
            map_lookup(
                df, ["a", "b"],
                lookup, key_column=["x"], value_column="y",
                target="result"
            )
