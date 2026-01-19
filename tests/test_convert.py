"""Tests for type conversion operations."""

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    to_numeric,
    to_int,
    to_string,
    to_bool,
)


class TestToNumeric:
    """Tests for to_numeric function."""

    def test_converts_string_to_numeric(self):
        """to_numeric should convert string column to numeric."""
        df = pd.DataFrame({"amount": ["100", "200.5", "300"]})

        result = to_numeric(df, "amount")

        assert result["amount"].dtype in ["float64", "int64"]
        assert result["amount"].iloc[0] == 100
        assert result["amount"].iloc[1] == 200.5

    def test_coerces_invalid_values(self):
        """to_numeric should coerce invalid values to NaN by default."""
        df = pd.DataFrame({"amount": ["100", "invalid", "300"]})

        result = to_numeric(df, "amount")

        assert pd.isna(result["amount"].iloc[1])

    def test_target_column(self):
        """to_numeric should write to target column when specified."""
        df = pd.DataFrame({"amount": ["100", "200"]})

        result = to_numeric(df, "amount", target="amount_num")

        assert "amount_num" in result.columns
        assert result["amount"].dtype == object  # original unchanged

    def test_missing_column_raises(self):
        """to_numeric should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="to_numeric failed.*nonexistent.*not found"):
            to_numeric(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """to_numeric should track operations in build mode."""
        df = pd.DataFrame({"amount": ["100"]})

        with BuildSession() as session:
            to_numeric(df, "amount")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "to_numeric"


class TestToInt:
    """Tests for to_int function."""

    def test_converts_to_int(self):
        """to_int should convert column to integer."""
        df = pd.DataFrame({"qty": [1.5, 2.7, 3.0]})

        result = to_int(df, "qty")

        assert result["qty"].dtype == int
        assert list(result["qty"]) == [1, 2, 3]

    def test_fills_null_with_default(self):
        """to_int should fill null values with default (0)."""
        df = pd.DataFrame({"qty": [1.0, None, 3.0]})

        result = to_int(df, "qty")

        assert result["qty"].iloc[1] == 0

    def test_custom_fill_value(self):
        """to_int should use custom fill value."""
        df = pd.DataFrame({"qty": [1.0, None, 3.0]})

        result = to_int(df, "qty", fill=-1)

        assert result["qty"].iloc[1] == -1

    def test_target_column(self):
        """to_int should write to target column when specified."""
        df = pd.DataFrame({"qty": [1.5, 2.5]})

        result = to_int(df, "qty", target="qty_int")

        assert "qty_int" in result.columns
        assert result["qty"].dtype == float  # original unchanged

    def test_missing_column_raises(self):
        """to_int should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="to_int failed.*nonexistent.*not found"):
            to_int(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """to_int should track operations in build mode."""
        df = pd.DataFrame({"qty": [1.0]})

        with BuildSession() as session:
            to_int(df, "qty")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "to_int"


class TestToString:
    """Tests for to_string function."""

    def test_converts_to_string(self):
        """to_string should convert column to string."""
        df = pd.DataFrame({"code": [1, 2, 3]})

        result = to_string(df, "code")

        assert result["code"].dtype == object
        assert list(result["code"]) == ["1", "2", "3"]

    def test_target_column(self):
        """to_string should write to target column when specified."""
        df = pd.DataFrame({"code": [1, 2]})

        result = to_string(df, "code", target="code_str")

        assert "code_str" in result.columns
        assert result["code"].dtype == int  # original unchanged

    def test_missing_column_raises(self):
        """to_string should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="to_string failed.*nonexistent.*not found"):
            to_string(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """to_string should track operations in build mode."""
        df = pd.DataFrame({"code": [1]})

        with BuildSession() as session:
            to_string(df, "code")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "to_string"


class TestToBool:
    """Tests for to_bool function."""

    def test_converts_default_true_values(self):
        """to_bool should convert default true values to True."""
        df = pd.DataFrame({"active": [1, "Yes", "true", "N", 0]})

        result = to_bool(df, "active")

        assert result["active"].iloc[0] == True
        assert result["active"].iloc[1] == True
        assert result["active"].iloc[2] == True
        assert result["active"].iloc[3] == False
        assert result["active"].iloc[4] == False

    def test_custom_true_values(self):
        """to_bool should use custom true values."""
        df = pd.DataFrame({"flag": ["ON", "OFF", "ON"]})

        result = to_bool(df, "flag", true_values=["ON"])

        assert list(result["flag"]) == [True, False, True]

    def test_target_column(self):
        """to_bool should write to target column when specified."""
        df = pd.DataFrame({"flag": [1, 0]})

        result = to_bool(df, "flag", target="flag_bool")

        assert "flag_bool" in result.columns

    def test_missing_column_raises(self):
        """to_bool should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2]})

        with pytest.raises(KeyError, match="to_bool failed.*nonexistent.*not found"):
            to_bool(df, "nonexistent")

    def test_tracking_in_build_mode(self):
        """to_bool should track operations in build mode."""
        df = pd.DataFrame({"flag": [1]})

        with BuildSession() as session:
            to_bool(df, "flag")

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "to_bool"
