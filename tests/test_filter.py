"""Tests for filter operations."""

import warnings

import pandas as pd
import pytest

from migrator_studio import (
    BuildSession,
    filter_isin,
    filter_not_isin,
    filter_by_value,
    filter_null,
    filter_not_null,
    filter_date,
    sanitize_data,
)
from migrator_studio.operations._validation import FilterTypeError, FilterValueWarning


class TestFilterIsin:
    """Tests for filter_isin function."""

    def test_basic_membership(self, sample_df):
        """filter_isin should keep rows matching values."""
        result = filter_isin(sample_df, "status", ["Active"])

        assert len(result) == 3
        assert all(result["status"] == "Active")

    def test_multiple_values(self, sample_df):
        """filter_isin should work with multiple values."""
        result = filter_isin(sample_df, "status", ["Active", "Pending"])

        assert len(result) == 4
        assert set(result["status"].unique()) == {"Active", "Pending"}

    def test_resets_index(self, sample_df):
        """filter_isin should reset index."""
        result = filter_isin(sample_df, "status", ["Active"])

        assert list(result.index) == [0, 1, 2]

    def test_missing_column_raises(self):
        """filter_isin should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_isin failed.*nonexistent.*not found"):
            filter_isin(df, "nonexistent", [1, 2])

    def test_tracking_in_build_mode(self, sample_df):
        """filter_isin should track operations in build mode."""
        with BuildSession() as session:
            filter_isin(sample_df, "status", ["Active"])

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "filter_isin"
            assert op.rows_before == 5
            assert op.rows_after == 3


class TestFilterNotIsin:
    """Tests for filter_not_isin function."""

    def test_basic_exclusion(self, sample_df):
        """filter_not_isin should exclude rows matching values."""
        result = filter_not_isin(sample_df, "status", ["Inactive"])

        assert len(result) == 4
        assert "Inactive" not in result["status"].values

    def test_multiple_values(self, sample_df):
        """filter_not_isin should exclude multiple values."""
        result = filter_not_isin(sample_df, "status", ["Active", "Inactive"])

        assert len(result) == 1
        assert result["status"].iloc[0] == "Pending"

    def test_missing_column_raises(self):
        """filter_not_isin should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_not_isin failed.*nonexistent.*not found"):
            filter_not_isin(df, "nonexistent", [1, 2])


class TestFilterByValue:
    """Tests for filter_by_value function."""

    def test_eq(self, sample_df):
        """filter_by_value with eq should filter exactly."""
        result = filter_by_value(sample_df, "region", eq="North")

        assert len(result) == 2
        assert all(result["region"] == "North")

    def test_ne(self, sample_df):
        """filter_by_value with ne should exclude matches."""
        result = filter_by_value(sample_df, "region", ne="North")

        assert len(result) == 3
        assert "North" not in result["region"].values

    def test_gt(self, sample_df):
        """filter_by_value with gt should keep greater values."""
        result = filter_by_value(sample_df, "amount", gt=100)

        assert len(result) == 3
        assert all(result["amount"] > 100)

    def test_gte(self, sample_df):
        """filter_by_value with gte should keep >= values."""
        result = filter_by_value(sample_df, "amount", gte=100)

        assert len(result) == 4
        assert all(result["amount"] >= 100)

    def test_lt(self, sample_df):
        """filter_by_value with lt should keep lesser values."""
        result = filter_by_value(sample_df, "amount", lt=150)

        assert len(result) == 2
        assert all(result["amount"] < 150)

    def test_lte(self, sample_df):
        """filter_by_value with lte should keep <= values."""
        result = filter_by_value(sample_df, "amount", lte=150)

        assert len(result) == 3
        assert all(result["amount"] <= 150)

    def test_combined_conditions(self, sample_df):
        """filter_by_value should AND multiple conditions."""
        result = filter_by_value(sample_df, "amount", gte=100, lte=250)

        assert len(result) == 3
        assert all((result["amount"] >= 100) & (result["amount"] <= 250))

    def test_gt_and_ne_combined(self, sample_df):
        """filter_by_value should combine gt and ne."""
        result = filter_by_value(sample_df, "amount", gt=100, ne=250)

        assert len(result) == 2
        assert 250 not in result["amount"].values
        assert all(result["amount"] > 100)

    def test_no_conditions_raises(self, sample_df):
        """filter_by_value with no conditions should raise ValueError."""
        with pytest.raises(ValueError, match="No filter conditions provided"):
            filter_by_value(sample_df, "amount")

    def test_missing_column_raises(self):
        """filter_by_value should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_by_value failed.*nonexistent.*not found"):
            filter_by_value(df, "nonexistent", eq=1)

    def test_tracking_in_build_mode(self, sample_df):
        """filter_by_value should track operations."""
        with BuildSession() as session:
            filter_by_value(sample_df, "amount", gt=100)

            assert len(session.history) == 1
            op = session.history[0]
            assert op.operation == "filter_by_value"


class TestFilterNull:
    """Tests for filter_null function."""

    def test_keeps_null_values(self):
        """filter_null should keep only null values."""
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})

        result = filter_null(df, "value")

        assert len(result) == 2
        assert all(result["value"].isna())

    def test_missing_column_raises(self):
        """filter_null should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_null failed.*nonexistent.*not found"):
            filter_null(df, "nonexistent")


class TestFilterNotNull:
    """Tests for filter_not_null function."""

    def test_keeps_non_null_values(self):
        """filter_not_null should keep only non-null values."""
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})

        result = filter_not_null(df, "value")

        assert len(result) == 3
        assert all(result["value"].notna())

    def test_missing_column_raises(self):
        """filter_not_null should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_not_null failed.*nonexistent.*not found"):
            filter_not_null(df, "nonexistent")


class TestFilterDate:
    """Tests for filter_date function."""

    def test_after_cutoff(self, sample_df):
        """filter_date with after should exclude the boundary."""
        result = filter_date(sample_df, "date", after="2025-03-01")

        assert len(result) == 3  # Mar 20, April 10, May 5

    def test_before_cutoff(self, sample_df):
        """filter_date with before should exclude the boundary."""
        result = filter_date(sample_df, "date", before="2025-02-28")

        assert len(result) == 2  # Jan 1 and Feb 15

    def test_on_or_after_inclusive(self, sample_df):
        """filter_date with on_or_after should include the boundary."""
        result = filter_date(sample_df, "date", on_or_after="2025-03-20")

        assert len(result) == 3  # Mar 20, Apr 10, May 5

    def test_on_or_before_inclusive(self, sample_df):
        """filter_date with on_or_before should include the boundary."""
        result = filter_date(sample_df, "date", on_or_before="2025-02-15")

        assert len(result) == 2  # Jan 1 and Feb 15

    def test_date_range(self, sample_df):
        """filter_date should support date ranges."""
        result = filter_date(
            sample_df, "date",
            on_or_after="2025-02-01",
            on_or_before="2025-04-30",
        )

        assert len(result) == 3  # Feb 15, Mar 20, Apr 10

    def test_exclusive_range(self, sample_df):
        """filter_date with after/before should be exclusive."""
        result = filter_date(
            sample_df, "date",
            after="2025-01-01",
            before="2025-05-05",
        )

        assert len(result) == 3  # Feb 15, Mar 20, Apr 10

    def test_no_conditions_raises(self, sample_df):
        """filter_date with no conditions should raise ValueError."""
        with pytest.raises(ValueError, match="No filter conditions provided"):
            filter_date(sample_df, "date")

    def test_invalid_date_string_raises(self, sample_df):
        """filter_date with invalid date should raise FilterTypeError."""
        with pytest.raises(FilterTypeError, match="Cannot parse date value"):
            filter_date(sample_df, "date", after="not-a-date")

    def test_missing_column_raises(self):
        """filter_date should raise KeyError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(KeyError, match="filter_date failed.*nonexistent.*not found"):
            filter_date(df, "nonexistent", after="2025-01-01")


class TestSanitizeData:
    """Tests for sanitize_data function."""

    def test_strips_whitespace(self):
        """sanitize_data should strip whitespace from string columns."""
        df = pd.DataFrame({"name": ["  hello  ", "world", "  test"]})

        result = sanitize_data(df)

        assert result["name"].tolist() == ["hello", "world", "test"]

    def test_keeps_empty_strings_by_default(self):
        """sanitize_data should keep empty strings by default."""
        df = pd.DataFrame({"name": ["hello", "", "world"]})

        result = sanitize_data(df)

        assert result["name"].tolist() == ["hello", "", "world"]

    def test_converts_empty_to_null_when_specified(self):
        """sanitize_data should convert empty strings to None when empty_val=None."""
        df = pd.DataFrame({"name": ["hello", "", "world"]})

        result = sanitize_data(df, empty_val=None)

        assert result["name"].tolist()[0] == "hello"
        assert pd.isna(result["name"].tolist()[1])
        assert result["name"].tolist()[2] == "world"

    def test_whitespace_becomes_empty_by_default(self):
        """sanitize_data should strip whitespace to empty string by default."""
        df = pd.DataFrame({"name": ["hello", "   ", "world", "\t\n"]})

        result = sanitize_data(df)

        assert result["name"].tolist() == ["hello", "", "world", ""]

    def test_whitespace_becomes_null_when_specified(self):
        """sanitize_data should convert whitespace to None when empty_val=None."""
        df = pd.DataFrame({"name": ["hello", "   ", "world"]})

        result = sanitize_data(df, empty_val=None)

        assert result["name"].tolist()[0] == "hello"
        assert pd.isna(result["name"].tolist()[1])
        assert result["name"].tolist()[2] == "world"

    def test_preserves_null(self):
        """sanitize_data should preserve existing null values."""
        df = pd.DataFrame({"name": ["hello", None, "world"]})

        result = sanitize_data(df)

        assert result["name"].tolist()[0] == "hello"
        assert pd.isna(result["name"].tolist()[1])
        assert result["name"].tolist()[2] == "world"

    def test_ignore_cols(self):
        """sanitize_data should skip columns in ignore_cols."""
        df = pd.DataFrame({"name": ["  hello  "], "raw": ["  data  "]})

        result = sanitize_data(df, ignore_cols=["raw"])

        assert result["name"].tolist() == ["hello"]
        assert result["raw"].tolist() == ["  data  "]

    def test_skips_non_string_columns(self):
        """sanitize_data should only process object/string columns."""
        df = pd.DataFrame({"name": ["  hello  "], "value": [123]})

        result = sanitize_data(df)

        assert result["name"].tolist() == ["hello"]
        assert result["value"].tolist() == [123]

    def test_multiple_columns(self):
        """sanitize_data should clean all string columns."""
        df = pd.DataFrame({
            "name": ["  john  ", "jane"],
            "email": ["test@example.com  ", ""],
            "value": [1, 2],
        })

        result = sanitize_data(df, empty_val=None)

        assert result["name"].tolist() == ["john", "jane"]
        assert result["email"].tolist()[0] == "test@example.com"
        assert pd.isna(result["email"].tolist()[1])
        assert result["value"].tolist() == [1, 2]

    def test_no_strip_keeps_whitespace(self):
        """sanitize_data with strip_whitespace=False keeps whitespace."""
        df = pd.DataFrame({"name": ["  hello  ", ""]})

        result = sanitize_data(df, strip_whitespace=False)

        assert result["name"].tolist() == ["  hello  ", ""]

    def test_custom_empty_val(self):
        """sanitize_data can replace empty strings with custom value."""
        df = pd.DataFrame({"name": ["hello", "", "world"]})

        result = sanitize_data(df, empty_val="N/A")

        assert result["name"].tolist() == ["hello", "N/A", "world"]

    def test_strip_without_empty_conversion(self):
        """sanitize_data strips whitespace but keeps empty strings."""
        df = pd.DataFrame({"name": ["  hello  ", "   ", "world"]})

        result = sanitize_data(df)

        assert result["name"].tolist() == ["hello", "", "world"]


class TestTypeValidation:
    """Tests for type validation in filter operations."""

    def test_numeric_column_with_string_raises(self):
        """Filtering numeric column with string should raise FilterTypeError."""
        df = pd.DataFrame({"amount": [100, 200, 300]})

        with pytest.raises(FilterTypeError, match="Type mismatch.*amount.*int or float"):
            filter_by_value(df, "amount", eq="abc")

    def test_numeric_column_with_bool_raises(self):
        """Filtering numeric column with bool should raise FilterTypeError."""
        df = pd.DataFrame({"amount": [100, 200, 300]})

        with pytest.raises(FilterTypeError, match="Type mismatch.*amount.*type 'bool'"):
            filter_by_value(df, "amount", eq=True)

    def test_boolean_column_with_int_raises(self):
        """Filtering boolean column with int should raise FilterTypeError."""
        df = pd.DataFrame({"flag": [True, False, True]})

        with pytest.raises(FilterTypeError, match="Type mismatch.*flag.*bool.*True/False"):
            filter_by_value(df, "flag", eq=1)

    def test_boolean_column_with_string_raises(self):
        """Filtering boolean column with string should raise FilterTypeError."""
        df = pd.DataFrame({"flag": [True, False, True]})

        with pytest.raises(FilterTypeError, match="Type mismatch.*flag.*bool"):
            filter_by_value(df, "flag", eq="yes")

    def test_datetime_column_with_invalid_string_raises(self):
        """Filtering datetime column with unparseable string should raise."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-01", "2025-02-01"])})

        with pytest.raises(FilterTypeError, match="Cannot parse date value"):
            filter_by_value(df, "date", eq="not-a-date")

    def test_datetime_column_with_valid_string_works(self):
        """Filtering datetime column with valid string should work."""
        df = pd.DataFrame({"date": pd.to_datetime(["2025-01-01", "2025-02-01"])})

        result = filter_by_value(df, "date", eq="2025-01-01")

        assert len(result) == 1

    def test_timedelta_column_with_int_raises(self):
        """Filtering timedelta column with int should raise FilterTypeError."""
        df = pd.DataFrame({"duration": pd.to_timedelta(["1 day", "2 days"])})

        with pytest.raises(FilterTypeError, match="Type mismatch.*duration.*timedelta"):
            filter_by_value(df, "duration", gt=100)

    def test_categorical_value_not_in_categories_warns(self):
        """Filtering categorical with non-existent value should warn."""
        df = pd.DataFrame({"status": pd.Categorical(["A", "B", "A"])})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            filter_isin(df, "status", ["Unknown"])

            assert len(w) == 1
            assert issubclass(w[0].category, FilterValueWarning)
            assert "not found in categories" in str(w[0].message)

    def test_string_dtype_with_int_raises(self):
        """Filtering StringDtype column with int should raise."""
        df = pd.DataFrame({"name": pd.array(["Alice", "Bob"], dtype="string")})

        with pytest.raises(FilterTypeError, match="Type mismatch.*name.*str"):
            filter_by_value(df, "name", eq=123)

    def test_object_dtype_with_non_string_warns(self):
        """Filtering object dtype with non-string should warn."""
        df = pd.DataFrame({"data": ["a", "b", "c"]})  # object dtype

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            filter_by_value(df, "data", eq=123)

            assert len(w) == 1
            assert issubclass(w[0].category, FilterValueWarning)
            assert "may produce unexpected results" in str(w[0].message)

    def test_object_dtype_with_string_no_warning(self):
        """Filtering object dtype with string should not warn."""
        df = pd.DataFrame({"data": ["a", "b", "c"]})  # object dtype

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = filter_by_value(df, "data", eq="a")

            assert len(w) == 0
            assert len(result) == 1


class TestFilterIsinValidation:
    """Tests for type validation in filter_isin."""

    def test_numeric_column_with_string_in_list_raises(self):
        """filter_isin with string in list for numeric column should raise."""
        df = pd.DataFrame({"amount": [100, 200, 300]})

        with pytest.raises(FilterTypeError, match="Type mismatch.*amount"):
            filter_isin(df, "amount", [100, "abc"])

    def test_valid_values_work(self):
        """filter_isin with valid values should work."""
        df = pd.DataFrame({"amount": [100, 200, 300]})

        result = filter_isin(df, "amount", [100, 200])

        assert len(result) == 2
