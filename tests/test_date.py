"""Tests for date operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    parse_date,
    format_date,
    extract_date_part,
    handle_invalid_dates,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_basic_parse(self):
        """parse_date should parse date strings."""
        df = pd.DataFrame({"date": ["2025-01-15", "2025-02-20", "2025-03-25"]})

        result = parse_date(df, "date")

        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_with_format(self):
        """parse_date should use specified format."""
        df = pd.DataFrame({"date": ["15/01/2025", "20/02/2025"]})

        result = parse_date(df, "date", format="%d/%m/%Y")

        assert result["date"].iloc[0].day == 15
        assert result["date"].iloc[0].month == 1

    def test_target_column(self):
        """parse_date should write to target column when specified."""
        df = pd.DataFrame({"date_str": ["2025-01-15"]})

        result = parse_date(df, "date_str", target="date")

        assert "date" in result.columns
        assert result["date_str"].dtype == object  # original unchanged

    def test_coerces_invalid_dates(self):
        """parse_date should coerce invalid dates to NaT."""
        df = pd.DataFrame({"date": ["2025-01-15", "invalid", "2025-03-25"]})

        result = parse_date(df, "date")

        assert pd.isna(result["date"].iloc[1])

    def test_missing_column_raises(self):
        """parse_date should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="parse_date failed.*nonexistent.*not found"):
            parse_date(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """parse_date should track operations in build mode."""
        df = pd.DataFrame({"date": ["2025-01-01"]})

        with BuildSession() as session:
            parse_date(df, "date")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "parse_date"


class TestFormatDate:
    """Tests for format_date function."""

    def test_basic_format(self):
        """format_date should format datetime to string."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-15", "2025-02-20"])})

        result = format_date(df, "date", "%Y-%m-%d")

        assert result["date"].dtype == object
        assert result["date"].iloc[0] == "2025-01-15"

    def test_custom_format(self):
        """format_date should use custom format."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-15"])})

        result = format_date(df, "date", "%d %b %Y")

        assert result["date"].iloc[0] == "15 Jan 2025"

    def test_target_column(self):
        """format_date should write to target column when specified."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-15"])})

        result = format_date(df, "date", "%Y-%m-%d", target="date_str")

        assert "date_str" in result.columns
        assert result["date_str"].iloc[0] == "2025-01-15"

    def test_missing_column_raises(self):
        """format_date should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="format_date failed.*nonexistent.*not found"):
            format_date(df, "nonexistent", "%Y-%m-%d")

    def test_tracking_in_build_mode(self):
        """format_date should track operations in build mode."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-01"])})

        with BuildSession() as session:
            format_date(df, "date", "%Y-%m-%d")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "format_date"


class TestExtractDatePart:
    """Tests for extract_date_part function."""

    def test_extract_year(self):
        """extract_date_part should extract year."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-06-15"])})

        result = extract_date_part(df, "date", "year", "year")

        assert result["year"].iloc[0] == 2025

    def test_extract_month(self):
        """extract_date_part should extract month."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-06-15"])})

        result = extract_date_part(df, "date", "month", "month")

        assert result["month"].iloc[0] == 6

    def test_extract_day(self):
        """extract_date_part should extract day."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-06-15"])})

        result = extract_date_part(df, "date", "day", "day")

        assert result["day"].iloc[0] == 15

    def test_extract_quarter(self):
        """extract_date_part should extract quarter."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-06-15"])})

        result = extract_date_part(df, "date", "quarter", "quarter")

        assert result["quarter"].iloc[0] == 2

    def test_invalid_part_raises(self):
        """extract_date_part should raise ValueError for invalid part."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-06-15"])})

        with pytest.raises(ValueError, match="Invalid part.*invalid"):
            extract_date_part(df, "date", "invalid", "result")

    def test_missing_column_raises(self):
        """extract_date_part should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="extract_date_part failed.*nonexistent.*not found"):
            extract_date_part(df, "nonexistent", "year", "year")

    def test_tracking_in_build_mode(self):
        """extract_date_part should track operations in build mode."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-01"])})

        with BuildSession() as session:
            extract_date_part(df, "date", "year", "year")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "extract_date_part"


class TestHandleInvalidDates:
    """Tests for handle_invalid_dates function."""

    def test_replaces_9999_dates(self):
        """handle_invalid_dates should replace 9999 dates."""
        df = pd.DataFrame({"date": ["2025-01-15", "9999-12-31", "2025-03-25"]})

        result = handle_invalid_dates(df, "date")

        assert result["date"].iloc[0] == "2025-01-15"
        assert result["date"].iloc[1] == "2099-12-31"
        assert result["date"].iloc[2] == "2025-03-25"

    def test_custom_fallback(self):
        """handle_invalid_dates should use custom fallback."""
        df = pd.DataFrame({"date": ["9999-12-31"]})

        result = handle_invalid_dates(df, "date", fallback="2050-01-01")

        assert result["date"].iloc[0] == "2050-01-01"

    def test_various_9999_formats(self):
        """handle_invalid_dates should handle various 9999 formats."""
        df = pd.DataFrame({"date": ["9999-01-01", "9999-06-15", "9999-12-31"]})

        result = handle_invalid_dates(df, "date")

        assert all(result["date"] == "2099-12-31")

    def test_missing_column_raises(self):
        """handle_invalid_dates should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="handle_invalid_dates failed.*nonexistent.*not found"):
            handle_invalid_dates(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """handle_invalid_dates should track operations in build mode."""
        df = pd.DataFrame({"date": ["9999-12-31"]})

        with BuildSession() as session:
            handle_invalid_dates(df, "date")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "handle_invalid_dates"
