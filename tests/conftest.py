"""Shared test fixtures."""

import os
import tempfile

import pandas as pd
import pytest

from migrator_studio.config import reset_config


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "status": ["Active", "Pending", "Active", "Inactive", "Active"],
        "amount": [100.0, 250.0, 50.0, 300.0, 150.0],
        "region": ["North", "South", "North", "East", "West"],
        "date": ["2025-01-01", "2025-02-15", "2025-03-20", "2025-04-10", "2025-05-05"],
    })


@pytest.fixture
def empty_df():
    """Create an empty DataFrame with standard columns."""
    return pd.DataFrame({
        "id": pd.Series([], dtype="int64"),
        "name": pd.Series([], dtype="object"),
        "status": pd.Series([], dtype="object"),
        "amount": pd.Series([], dtype="float64"),
    })


@pytest.fixture
def single_row_df():
    """Create a single-row DataFrame for edge case testing."""
    return pd.DataFrame({
        "id": [1],
        "name": ["Alice"],
        "status": ["Active"],
        "amount": [100.0],
        "region": ["North"],
    })


@pytest.fixture
def lookup_df():
    """Create a lookup DataFrame for testing merges and mappings."""
    return pd.DataFrame({
        "region": ["North", "South", "East", "West"],
        "region_name": ["Northern Region", "Southern Region", "Eastern Region", "Western Region"],
        "manager": ["John", "Jane", "Jack", "Jill"],
    })


@pytest.fixture
def status_mapping():
    """Create a status mapping dictionary."""
    return {
        "Active": "Enabled",
        "Pending": "Under Review",
        "Inactive": "Disabled",
    }


@pytest.fixture
def temp_data_dir(sample_df):
    """Create a temporary directory with a sample feather file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Save sample data as feather
        sample_df.to_feather(os.path.join(tmpdir, "DAT-00000001.feather"))
        yield tmpdir


@pytest.fixture(autouse=True)
def reset_config_fixture():
    """Reset config before each test."""
    reset_config()
    yield
    reset_config()
