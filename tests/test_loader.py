"""Tests for data loading."""

import os

import pandas as pd
import pytest

from migrator_studio import BuildSession, configure, load_source


class TestLoadSource:
    """Tests for load_source function."""

    def test_load_source_full_data(self, temp_data_dir, sample_df):
        """Load source should load full data in production mode."""
        configure(data_path=temp_data_dir)

        df = load_source("DAT-00000001")

        assert len(df) == len(sample_df)
        assert list(df.columns) == list(sample_df.columns)

    def test_load_source_with_explicit_sample(self, temp_data_dir):
        """Load source should respect explicit sample parameter."""
        configure(data_path=temp_data_dir)

        df = load_source("DAT-00000001", sample=2)

        assert len(df) == 2

    def test_load_source_in_build_mode(self, temp_data_dir):
        """Load source should auto-sample in build mode."""
        configure(data_path=temp_data_dir)

        with BuildSession(sample=3) as session:
            df = load_source("DAT-00000001")

            assert len(df) == 3
            # Should be tracked
            assert len(session.history) == 1
            assert session.history[0].operation == "load_source"
            assert session.history[0].rows_after == 3

    def test_load_source_explicit_sample_overrides_session(self, temp_data_dir):
        """Explicit sample should override session sample."""
        configure(data_path=temp_data_dir)

        with BuildSession(sample=3) as session:
            df = load_source("DAT-00000001", sample=2)

            assert len(df) == 2

    def test_load_source_file_not_found(self, temp_data_dir):
        """Load source should raise FileNotFoundError for missing files."""
        configure(data_path=temp_data_dir)

        with pytest.raises(FileNotFoundError, match="Source file not found"):
            load_source("DAT-99999999")

    def test_load_source_no_sample_when_data_smaller(self, temp_data_dir, sample_df):
        """Load source should not truncate when data is smaller than sample."""
        configure(data_path=temp_data_dir)

        with BuildSession(sample=100) as session:
            df = load_source("DAT-00000001")

            # Data has 5 rows, sample is 100, should get all 5
            assert len(df) == len(sample_df)
