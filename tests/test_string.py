"""Tests for string operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    str_upper,
    str_lower,
    str_strip,
    str_replace,
    str_regex_replace,
)


class TestStrUpper:
    """Tests for str_upper function."""

    def test_str_upper_basic(self, sample_df):
        """str_upper should convert to uppercase."""
        result = str_upper(sample_df, "name")

        assert "ALICE" in result["name"].values
        assert "BOB" in result["name"].values
        assert "Alice" not in result["name"].values

    def test_str_upper_to_different_column(self, sample_df):
        """str_upper should write to different column when specified."""
        result = str_upper(sample_df, "name", target_column="name_upper")

        assert "name_upper" in result.columns
        assert "ALICE" in result["name_upper"].values
        # Original unchanged
        assert "Alice" in result["name"].values

    def test_str_upper_tracking(self, sample_df):
        """str_upper should track operations in build mode."""
        with BuildSession() as session:
            result = str_upper(sample_df, "name")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "str_upper"


class TestStrLower:
    """Tests for str_lower function."""

    def test_str_lower_basic(self, sample_df):
        """str_lower should convert to lowercase."""
        result = str_lower(sample_df, "name")

        assert "alice" in result["name"].values
        assert "bob" in result["name"].values

    def test_str_lower_to_different_column(self, sample_df):
        """str_lower should write to different column when specified."""
        result = str_lower(sample_df, "name", target_column="name_lower")

        assert "name_lower" in result.columns
        assert "alice" in result["name_lower"].values


class TestStrStrip:
    """Tests for str_strip function."""

    def test_str_strip_basic(self):
        """str_strip should remove leading/trailing whitespace."""
        df = pd.DataFrame({"value": ["  hello  ", "world  ", "  test"]})

        result = str_strip(df, "value")

        assert list(result["value"]) == ["hello", "world", "test"]

    def test_str_strip_to_different_column(self):
        """str_strip should write to different column when specified."""
        df = pd.DataFrame({"value": ["  hello  "]})

        result = str_strip(df, "value", target_column="value_stripped")

        assert result["value_stripped"].values[0] == "hello"
        assert result["value"].values[0] == "  hello  "


class TestStrReplace:
    """Tests for str_replace function."""

    def test_str_replace_basic(self):
        """str_replace should replace substring."""
        df = pd.DataFrame({"phone": ["555-1234", "555-5678"]})

        result = str_replace(df, "phone", "-", "")

        assert list(result["phone"]) == ["5551234", "5555678"]

    def test_str_replace_multiple_occurrences(self):
        """str_replace should replace all occurrences."""
        df = pd.DataFrame({"text": ["a-b-c"]})

        result = str_replace(df, "text", "-", "_")

        assert result["text"].values[0] == "a_b_c"

    def test_str_replace_to_different_column(self):
        """str_replace should write to different column when specified."""
        df = pd.DataFrame({"phone": ["555-1234"]})

        result = str_replace(df, "phone", "-", "", target_column="phone_clean")

        assert result["phone_clean"].values[0] == "5551234"
        assert result["phone"].values[0] == "555-1234"


class TestStrRegexReplace:
    """Tests for str_regex_replace function."""

    def test_str_regex_replace_basic(self):
        """str_regex_replace should replace regex pattern."""
        df = pd.DataFrame({"phone": ["(555) 123-4567", "555.987.6543"]})

        result = str_regex_replace(df, "phone", r"\D", "")

        assert list(result["phone"]) == ["5551234567", "5559876543"]

    def test_str_regex_replace_capture_group(self):
        """str_regex_replace should work with capture groups."""
        df = pd.DataFrame({"text": ["hello123world"]})

        result = str_regex_replace(df, "text", r"(\d+)", r"[\1]")

        assert result["text"].values[0] == "hello[123]world"

    def test_str_regex_replace_to_different_column(self):
        """str_regex_replace should write to different column when specified."""
        df = pd.DataFrame({"phone": ["555-1234"]})

        result = str_regex_replace(
            df, "phone", r"\D", "",
            target_column="phone_digits"
        )

        assert result["phone_digits"].values[0] == "5551234"
        assert result["phone"].values[0] == "555-1234"

    def test_str_regex_replace_tracking(self):
        """str_regex_replace should track operations in build mode."""
        df = pd.DataFrame({"phone": ["555-1234"]})

        with BuildSession() as session:
            result = str_regex_replace(df, "phone", r"\D", "")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "str_regex_replace"


class TestStringOperationChaining:
    """Tests for chaining multiple string operations."""

    def test_chain_operations(self):
        """Multiple string operations should chain correctly."""
        df = pd.DataFrame({"name": ["  John Doe  "]})

        result = str_strip(df, "name")
        result = str_upper(result, "name")
        result = str_replace(result, "name", " ", "_")

        assert result["name"].values[0] == "JOHN_DOE"

    def test_chain_operations_tracking(self):
        """Chained operations should all be tracked."""
        df = pd.DataFrame({"name": ["  John Doe  "]})

        with BuildSession() as session:
            result = str_strip(df, "name")
            result = str_upper(result, "name")
            result = str_replace(result, "name", " ", "_")

            assert len(session.history) == 3
            assert session.history[0].operation == "str_strip"
            assert session.history[1].operation == "str_upper"
            assert session.history[2].operation == "str_replace"
