"""Tests for CLI commands."""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from migrator_studio.cli.main import cli, find_transformers


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_transformer(temp_dir):
    """Create a sample transformer file."""
    transformer = temp_dir / "TFRM-TEST-001.py"
    transformer.write_text('''"""Test transformer."""
from pandas import DataFrame
from migrator_studio import step, filter_not_null

SOURCES = ["DAT-00000001"]

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    df = sources["DAT-00000001"]

    step("Filter valid rows")
    df = filter_not_null(df, "Name")

    return df
''')
    return transformer


@pytest.fixture
def runnable_transformer(temp_dir):
    """Create a transformer with sample data that can actually run."""
    # Create sample feather data
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", None, "Dave"],
        "Status": ["A", "P", "A", "I"],
    })
    df.to_feather(temp_dir / "DAT-00000001.feather")

    transformer = temp_dir / "TFRM-RUNNABLE-001.py"
    transformer.write_text('''"""Runnable test transformer."""
from pandas import DataFrame
from migrator_studio import step, filter_not_null

SOURCES = ["DAT-00000001"]

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    df = sources["DAT-00000001"]

    step("Filter valid rows")
    df = filter_not_null(df, "Name")

    return df
''')
    return transformer


class TestCLIVersion:
    """Test version command."""

    def test_version(self, runner):
        """Test --version flag."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output or "version" in result.output.lower()


class TestCLIHelp:
    """Test help command."""

    def test_help(self, runner):
        """Test --help flag."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Migrator Studio" in result.output
        assert "dev" in result.output
        assert "sync" in result.output
        assert "run" in result.output

    def test_dev_help(self, runner):
        """Test dev command help."""
        result = runner.invoke(cli, ["dev", "--help"])
        assert result.exit_code == 0
        assert "Generate a Marimo notebook" in result.output
        assert "--sample" in result.output
        assert "--output" in result.output

    def test_sync_help(self, runner):
        """Test sync command help."""
        result = runner.invoke(cli, ["sync", "--help"])
        assert result.exit_code == 0
        assert "Sync changes from notebook" in result.output

    def test_run_help(self, runner):
        """Test run command help."""
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "Run a transformer in production mode" in result.output


class TestFindTransformers:
    """Test transformer discovery."""

    def test_find_transformers_empty(self, temp_dir):
        """Test finding no transformers in empty directory."""
        transformers = find_transformers(temp_dir)
        assert transformers == []

    def test_find_transformers_single(self, temp_dir):
        """Test finding a single transformer."""
        (temp_dir / "TFRM-TEST-001.py").touch()
        transformers = find_transformers(temp_dir)
        assert len(transformers) == 1
        assert transformers[0].name == "TFRM-TEST-001.py"

    def test_find_transformers_multiple(self, temp_dir):
        """Test finding multiple transformers."""
        (temp_dir / "TFRM-TEST-001.py").touch()
        (temp_dir / "TFRM-TEST-002.py").touch()
        (temp_dir / "TFRM-ANOTHER-003.py").touch()
        transformers = find_transformers(temp_dir)
        assert len(transformers) == 3

    def test_find_transformers_ignores_non_tfrm(self, temp_dir):
        """Test that non-TFRM files are ignored."""
        (temp_dir / "TFRM-TEST-001.py").touch()
        (temp_dir / "other_file.py").touch()
        (temp_dir / "NOT-TFRM-002.py").touch()
        transformers = find_transformers(temp_dir)
        assert len(transformers) == 1


class TestDevCommand:
    """Test dev command."""

    def test_dev_no_transformer_found(self, runner, temp_dir):
        """Test dev command when no transformer is found."""
        with runner.isolated_filesystem(temp_dir=str(temp_dir)):
            result = runner.invoke(cli, ["dev"])
            assert result.exit_code == 1
            assert "No transformer files" in result.output

    def test_dev_nonexistent_file(self, runner):
        """Test dev command with nonexistent file."""
        result = runner.invoke(cli, ["dev", "nonexistent.py"])
        assert result.exit_code == 2  # Click's error code for bad path

    def test_dev_generates_notebook(self, runner, sample_transformer):
        """Test dev command generates notebook."""
        output = sample_transformer.parent / "test_notebook.nb.py"
        result = runner.invoke(cli, [
            "dev",
            str(sample_transformer),
            "--output", str(output),
            "--no-run",
        ])
        assert result.exit_code == 0
        assert output.exists()
        assert "Generated notebook" in result.output

    def test_dev_shows_steps(self, runner, sample_transformer):
        """Test dev command shows parsed steps."""
        output = sample_transformer.parent / "test_notebook.nb.py"
        result = runner.invoke(cli, [
            "dev",
            str(sample_transformer),
            "--output", str(output),
            "--no-run",
        ])
        assert result.exit_code == 0
        assert "Found 1 steps" in result.output
        assert "Filter valid rows" in result.output


class TestSyncCommand:
    """Test sync command."""

    def test_sync_no_notebook(self, runner, sample_transformer):
        """Test sync command when notebook doesn't exist."""
        result = runner.invoke(cli, ["sync", str(sample_transformer)])
        assert result.exit_code == 1
        assert "Notebook not found" in result.output

    def test_sync_with_notebook(self, runner, sample_transformer):
        """Test sync command with an existing notebook."""
        # Generate a notebook first
        output = sample_transformer.with_suffix(".nb.py")
        runner.invoke(cli, [
            "dev", str(sample_transformer),
            "--output", str(output),
            "--no-run",
        ])

        result = runner.invoke(cli, ["sync", str(sample_transformer)])
        assert result.exit_code == 0
        assert "Sync complete" in result.output

        # Check backup was created
        assert sample_transformer.with_suffix(".py.bak").exists()


class TestRunCommand:
    """Test run command."""

    def test_run_nonexistent_file(self, runner):
        """Test run command with nonexistent file."""
        result = runner.invoke(cli, ["run", "nonexistent.py"])
        assert result.exit_code == 2  # Click's error code for bad path

    def test_run_invalid_transformer(self, runner, temp_dir):
        """Test run command with file missing transform function."""
        bad_transformer = temp_dir / "TFRM-BAD-001.py"
        bad_transformer.write_text("# no transform function")

        result = runner.invoke(cli, ["run", str(bad_transformer)])
        assert result.exit_code == 1
        assert "must have a 'transform' function" in result.output

    def test_run_no_sources(self, runner, temp_dir):
        """Test run command with transformer missing SOURCES."""
        bad_transformer = temp_dir / "TFRM-NOSRC-001.py"
        bad_transformer.write_text(
            "def transform(sources):\n    return sources\n"
        )

        result = runner.invoke(cli, ["run", str(bad_transformer)])
        assert result.exit_code == 1
        assert "no SOURCES" in result.output

    def test_run_success(self, runner, runnable_transformer):
        """Test successful run of a transformer."""
        result = runner.invoke(cli, ["run", str(runnable_transformer)])
        assert result.exit_code == 0
        assert "Result:" in result.output
        assert "3 rows" in result.output
        assert "Columns:" in result.output

    def test_run_with_csv_output(self, runner, runnable_transformer):
        """Test run with CSV output."""
        output = runnable_transformer.parent / "output.csv"
        result = runner.invoke(cli, [
            "run", str(runnable_transformer),
            "--output", str(output),
        ])
        assert result.exit_code == 0
        assert output.exists()
        assert "Saved to:" in result.output

    def test_run_missing_source_file(self, runner, temp_dir):
        """Test run when source feather file is missing."""
        transformer = temp_dir / "TFRM-MISSING-001.py"
        transformer.write_text('''
from pandas import DataFrame
SOURCES = ["DAT-99999999"]
def transform(sources):
    return sources["DAT-99999999"]
''')
        result = runner.invoke(cli, ["run", str(transformer)])
        assert result.exit_code == 1
        assert "Source not found" in result.output


class TestRunIntegration:
    """Integration test running a full transformer end-to-end."""

    def test_full_pipeline(self, runner, temp_dir):
        """Run a multi-step transformer end to end."""
        # Create source data
        customers = pd.DataFrame({
            "CustomerID": [1, 2, 3],
            "Name": ["  alice  ", "BOB", "  Charlie  "],
            "Status": ["Active", "Active", "Inactive"],
        })
        customers.to_feather(temp_dir / "DAT-00000001.feather")

        transformer = temp_dir / "TFRM-INTEG-001.py"
        transformer.write_text('''"""Integration test transformer."""
from pandas import DataFrame
from migrator_studio import (
    step, filter_isin, str_strip, str_upper, select_columns,
)

SOURCES = ["DAT-00000001"]

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    df = sources["DAT-00000001"]

    step("Filter active")
    df = filter_isin(df, "Status", ["Active"])

    step("Clean names")
    df = str_strip(df, "Name")
    df = str_upper(df, "Name")

    step("Select columns")
    df = select_columns(df, ["CustomerID", "Name"])

    return df
''')
        result = runner.invoke(cli, ["run", str(transformer)])
        assert result.exit_code == 0
        assert "2 rows" in result.output
