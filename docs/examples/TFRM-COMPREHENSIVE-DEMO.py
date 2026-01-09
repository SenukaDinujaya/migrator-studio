"""
Comprehensive Transformer Example - Demonstrating All Data Manipulation Techniques

This transformer showcases ALL data manipulation techniques used throughout the project:
- Filtering & Selection
- Merging & Joining
- Mapping & Lookups
- Field Operations (Copy, Set, Concatenate)
- Date Handling
- Aggregation & Grouping
- Type Conversion
- Conditional Logic
- String Operations
- Building Child Tables
- Deduplication
- Complex Transformations

Author: Senuka Dinujaya Withana Arachchige
Date: 2025
"""

import pandas as pd
import numpy as np
from pandas import DataFrame
from typing import Dict, List, Optional
from collections import defaultdict
import json


# ============================================================================
# 1. FILTERING & SELECTION TECHNIQUES
# ============================================================================

def filter_by_status(df: DataFrame, allowed_statuses: List[str]) -> DataFrame:
    """
    TECHNIQUE: Boolean mask filtering with isin()
    Filter records by allowed status values.
    """
    return df[df['Status'].isin(allowed_statuses)].reset_index(drop=True)


def filter_by_date_range(df: DataFrame, date_column: str, start_date: str, end_date: str) -> DataFrame:
    """
    TECHNIQUE: Date filtering with comparison operators
    Filter records within a date range.
    """
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    return df[(df[date_column] >= start) & (df[date_column] <= end)].reset_index(drop=True)


def filter_by_condition(df: DataFrame, column: str, operator: str, value) -> DataFrame:
    """
    TECHNIQUE: Comparison operators (>, <, ==, !=)
    Filter by numeric or string conditions.
    """
    if operator == '>':
        return df[df[column] > value].reset_index(drop=True)
    elif operator == '<':
        return df[df[column] < value].reset_index(drop=True)
    elif operator == '>=':
        return df[df[column] >= value].reset_index(drop=True)
    elif operator == '<=':
        return df[df[column] <= value].reset_index(drop=True)
    elif operator == '==':
        return df[df[column] == value].reset_index(drop=True)
    elif operator == '!=':
        return df[df[column] != value].reset_index(drop=True)


def filter_by_multiple_conditions(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Complex boolean logic with & (AND) and | (OR)
    Filter records matching multiple conditions simultaneously.
    """
    mask = (
        (df['Status'].isin(['Active', 'Pending'])) &
        (df['Amount'] > 100) &
        ((df['Region'] == 'North') | (df['Region'] == 'South'))
    )
    return df[mask].reset_index(drop=True)


def filter_non_null(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Null/NaN handling with notna() and isna()
    Remove rows with null values.
    """
    return df[df[column].notna()].reset_index(drop=True)


def filter_empty_strings(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: String filtering with empty checks
    Remove rows with empty string values.
    """
    return df[df[column].fillna('').str.strip() != ''].reset_index(drop=True)


# ============================================================================
# 2. MERGING & JOINING TECHNIQUES
# ============================================================================

def merge_left_join(left_df: DataFrame, right_df: DataFrame, on_column: str) -> DataFrame:
    """
    TECHNIQUE: LEFT JOIN - Keep all rows from left, match from right
    Merge with left join to preserve all records.
    """
    return left_df.merge(
        right_df[['lookup_id', 'lookup_value']],
        left_on=on_column,
        right_on='lookup_id',
        how='left'
    )


def merge_inner_join(left_df: DataFrame, right_df: DataFrame, on_columns: List[str]) -> DataFrame:
    """
    TECHNIQUE: INNER JOIN - Keep only matching rows
    Merge with inner join to keep only matched records.
    """
    return left_df.merge(
        right_df,
        on=on_columns,
        how='inner'
    )


def merge_multiple_keys(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Multi-column join
    Merge on multiple columns simultaneously.
    """
    return df.merge(
        lookup_df[['Branch', 'TaxCode', 'TaxRate']],
        on=['Branch', 'TaxCode'],
        how='left'
    )


# ============================================================================
# 3. MAPPING & LOOKUP TECHNIQUES
# ============================================================================

def map_with_dictionary(df: DataFrame, column: str, mapping: Dict[str, str]) -> DataFrame:
    """
    TECHNIQUE: Dictionary-based mapping with map()
    Apply a simple lookup table.
    """
    df[f'{column}_mapped'] = df[column].map(mapping)
    return df


def map_with_default_fallback(df: DataFrame, column: str, mapping: Dict) -> DataFrame:
    """
    TECHNIQUE: Mapping with fillna() fallback for unmapped values
    Map values and provide defaults for missing mappings.
    """
    df['status_mapped'] = df[column].map(mapping).fillna('Unknown')
    return df


def map_from_dataframe(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Mapping using set_index() to create lookup dictionary
    Extract mapping from another dataframe.
    """
    lookup_dict = lookup_df.set_index('source_code')['target_name'].to_dict()
    df['target_name'] = df['source_code'].map(lookup_dict)
    return df


# ============================================================================
# 4. FIELD OPERATIONS - Copy, Set, Concatenate
# ============================================================================

def copy_field(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    TECHNIQUE: Simple field copy
    Copy one column to another.
    """
    df[target_col] = df[source_col]
    return df


def set_constant_value(df: DataFrame, column: str, value) -> DataFrame:
    """
    TECHNIQUE: Set constant value for all rows
    Assign same value to all rows in a column.
    """
    df[column] = value
    return df


def concatenate_fields(df: DataFrame, col1: str, col2: str, separator: str = ' ') -> DataFrame:
    """
    TECHNIQUE: Field concatenation with str.cat()
    Combine multiple columns into one.
    """
    df['combined'] = (
        df[col1].fillna('').astype(str) + separator +
        df[col2].fillna('').astype(str)
    ).str.strip()
    return df


def concatenate_with_separator(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    TECHNIQUE: Concatenate multiple columns with newline separator
    Combine multiple fields with line breaks.
    """
    df['address'] = (
        df[columns[0]].fillna('') + '\n' +
        df[columns[1]].fillna('') + '\n' +
        df[columns[2]].fillna('')
    ).str.replace(r'\n+', '\n', regex=True).str.strip()
    return df


def conditional_field_copy(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Conditional field assignment with np.where()
    Copy different source columns based on conditions.
    """
    df['primary_contact'] = np.where(
        df['MailContact'] != '',
        df['MailContact'],
        df['ShipContact']
    )
    return df


# ============================================================================
# 5. DATE HANDLING TECHNIQUES
# ============================================================================

def parse_dates(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Date parsing with pd.to_datetime()
    Convert strings to datetime objects.
    """
    df[column] = pd.to_datetime(df[column], errors='coerce')
    return df


def format_dates(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    TECHNIQUE: Date formatting with strftime()
    Format datetime to specific string format.
    """
    df[target_col] = df[source_col].dt.strftime('%Y-%m-%d')
    return df


def handle_invalid_dates(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    TECHNIQUE: Handle special dates (9999) with apply() and lambda
    Replace invalid dates with reasonable defaults.
    """
    df[target_col] = df[source_col].apply(
        lambda x: '2099-12-31' if str(x).startswith('9999')
        else str(pd.to_datetime(x, errors='coerce').date())
    )
    return df


def extract_year_month(df: DataFrame, date_col: str) -> DataFrame:
    """
    TECHNIQUE: Extract date components with dt.year, dt.month
    Extract year/month/day from dates.
    """
    df['year'] = df[date_col].dt.year
    df['month'] = df[date_col].dt.month
    df['year_month'] = df[date_col].dt.strftime('%Y-%m')
    return df


# ============================================================================
# 6. AGGREGATION & GROUPING TECHNIQUES
# ============================================================================

def aggregate_by_group(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: groupby() with single aggregation function
    Summarize data by groups.
    """
    result = df.groupby('Region')[['Amount', 'Quantity']].agg({
        'Amount': 'sum',
        'Quantity': 'mean'
    }).reset_index()
    return result


def aggregate_multiple_functions(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: groupby() with multiple aggregation functions
    Apply different aggregations to different columns.
    """
    result = df.groupby(['Region', 'Category']).agg({
        'Amount': ['sum', 'mean', 'count'],
        'Quantity': ['sum', 'max'],
        'Name': 'first'
    }).reset_index()
    result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
    return result


def concatenate_strings_in_group(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: groupby().apply() with string concatenation
    Combine strings from multiple rows into one.
    """
    result = (
        df.groupby(['OrderNum', 'Branch'])['Message']
        .apply(lambda msgs: " ".join(msgs.dropna().astype(str)))
        .reset_index()
        .rename(columns={'Message': 'MessageConcat'})
    )
    return result


def group_and_collect_lists(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: groupby().apply() collecting into lists
    Collect multiple rows into list columns.
    """
    result = (
        df.groupby('CustomerId')[['ItemCode', 'Quantity']]
        .apply(lambda x: x.values.tolist())
        .reset_index()
        .rename(columns={0: 'items'})
    )
    return result


# ============================================================================
# 7. TYPE CONVERSION TECHNIQUES
# ============================================================================

def convert_to_numeric(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Convert to numeric with pd.to_numeric()
    Handle string-to-number conversion safely.
    """
    df[column] = pd.to_numeric(df[column], errors='coerce')
    return df


def convert_to_integer(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Convert to integer with astype()
    Convert columns to integer type.
    """
    df[column] = df[column].fillna(0).astype(int)
    return df


def convert_to_string(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Convert to string with astype()
    Force string conversion.
    """
    df[column] = df[column].astype(str)
    return df


def convert_to_boolean(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Convert to boolean with astype(bool)
    Convert to boolean type.
    """
    df[column] = (df[column] != 0).astype(bool)
    return df


# ============================================================================
# 8. CONDITIONAL LOGIC TECHNIQUES
# ============================================================================

def conditional_with_numpy_where(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: np.where() for simple if-else conditions
    Apply conditional logic efficiently.
    """
    df['company'] = np.where(
        df['Branch'] == '01',
        'ArrowCorp',
        'Premier Grain Cleaner'
    )
    return df


def conditional_with_nested_where(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Nested np.where() for multiple conditions
    Handle multiple conditional branches.
    """
    df['discount'] = np.where(
        df['Amount'] > 1000,
        0.15,
        np.where(df['Amount'] > 500, 0.10, 0.05)
    )
    return df


def conditional_with_apply_lambda(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: apply() with lambda for complex conditions
    Apply custom logic row-by-row.
    """
    df['category'] = df.apply(
        lambda row: 'VIP' if (row['Amount'] > 1000 and row['Status'] == 'Active')
        else 'Regular',
        axis=1
    )
    return df


def conditional_with_loc(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: .loc[] with boolean mask for assignment
    Conditionally assign values to specific rows.
    """
    df['processed'] = 0
    df.loc[df['Status'] == 'Completed', 'processed'] = 1
    df.loc[(df['Amount'] > 500) & (df['Status'] == 'Active'), 'processed'] = 2
    return df


# ============================================================================
# 9. STRING OPERATIONS TECHNIQUES
# ============================================================================

def string_upper_lower(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: String case conversion with .str.upper() and .str.lower()
    Convert case of text fields.
    """
    df['name_upper'] = df[column].str.upper()
    df['name_lower'] = df[column].str.lower()
    return df


def string_strip(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: String trimming with .str.strip()
    Remove whitespace from start/end.
    """
    df[column] = df[column].str.strip()
    return df


def string_replace(df: DataFrame, column: str, pattern: str, replacement: str) -> DataFrame:
    """
    TECHNIQUE: String replacement with .str.replace()
    Find and replace patterns in strings.
    """
    df[column] = df[column].str.replace(pattern, replacement)
    return df


def string_regex_replace(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: Regular expression replacement
    Use regex for complex pattern matching.
    """
    df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)  # Keep only digits
    df['email'] = df['email'].str.replace(r'[^a-zA-Z0-9@.]', '', regex=True)  # Clean email
    return df


def string_split(df: DataFrame, column: str, separator: str) -> DataFrame:
    """
    TECHNIQUE: String splitting with .str.split()
    Split strings into parts.
    """
    df[['first_name', 'last_name']] = df[column].str.split(separator, expand=True)
    return df


def string_extract_from_email(df: DataFrame, email_col: str, new_col: str) -> DataFrame:
    """
    TECHNIQUE: Extract name from email with chained .str operations
    Parse email addresses to extract information.
    """
    df[new_col] = df[email_col].str.split('<').str[0]
    df[new_col] = df[new_col].str.split('@').str[0]
    df[new_col] = df[new_col].str.split('.').str[0]
    df[new_col] = df[new_col].str.replace(r'[^a-zA-Z ]', '', regex=True)
    return df


# ============================================================================
# 10. BUILDING CHILD TABLES TECHNIQUES
# ============================================================================
# This is one of the most important patterns in the framework!
# It covers:
# - Simple child table creation
# - Conditional child record generation
# - Complex aggregation from grouped data
# - Advanced multi-step processing (see build_advanced_child_tables)

def build_child_table_simple(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Create list column with dictionary entries
    Build child tables for one-to-many relationships.
    """
    def create_items(row):
        items = []
        if pd.notna(row['Item1']):
            items.append({'item_code': row['Item1'], 'qty': row['Qty1']})
        if pd.notna(row['Item2']):
            items.append({'item_code': row['Item2'], 'qty': row['Qty2']})
        return items

    df['items'] = df.apply(create_items, axis=1)
    return df


def build_child_table_conditional(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Build child tables with conditional logic
    Create child records based on conditions.
    """
    def create_taxes(row):
        taxes = []
        if row['TaxAmount1'] > 0:
            taxes.append({
                'account': 'Tax Account 1',
                'amount': row['TaxAmount1']
            })
        if row['TaxAmount2'] > 0:
            taxes.append({
                'account': 'Tax Account 2',
                'amount': row['TaxAmount2']
            })
        return taxes

    df['taxes'] = df.apply(create_taxes, axis=1)
    return df


def build_child_table_from_grouped_data(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Build child tables from aggregated data
    Create child records from grouped rows.
    """
    def aggregate_taxes(group):
        tax_dict = defaultdict(float)
        for _, row in group.iterrows():
            tax_dict[row['TaxCode']] += row['TaxAmount']

        taxes = [
            {'account_head': code, 'amount': amount}
            for code, amount in tax_dict.items()
        ]
        return taxes

    result = []
    for (order_num, branch), group in df.groupby(['OrderNum', 'Branch']):
        taxes = aggregate_taxes(group)
        result.append({
            'OrderNum': order_num,
            'Branch': branch,
            'taxes': taxes
        })

    return pd.DataFrame(result)


def build_advanced_child_tables(
    main_df: DataFrame,
    order_line_df: DataFrame,
    tax_template_df: DataFrame
) -> DataFrame:
    """
    TECHNIQUE: Advanced child table building with complex aggregation
    Demonstrates sophisticated patterns used in sales transformers:
    - Multi-step grouping and aggregation
    - Tax consolidation with defaultdict
    - Sales team allocation percentages
    - Deduplication using JSON hashing
    - Message concatenation

    This mirrors the build_child_tables() pattern from common_functions_for_sales_history_transformers.py
    """

    # ============ HELPER FUNCTIONS ============

    def fix_date(value):
        """Fix invalid dates (9999 → 2099-12-31)."""
        try:
            value_str = str(value)
            if value_str.startswith("9999"):
                return "2099-12-31"
            return str(pd.to_datetime(value, errors="coerce").date())
        except Exception:
            return "2099-12-31"

    def fix_list_column(val):
        """Ensure column contains valid list values."""
        return val if isinstance(val, list) else []

    def safe_strip(val):
        """Safely strip string values."""
        return str(val).strip() if pd.notna(val) else ""

    # ============ STEP 1: MESSAGE CONCATENATION & GROUPING ============
    # Group messages by OrderNum and concatenate them
    message_groups = (
        order_line_df.sort_values(by="UniqueNo")
        .groupby(["OrderNum", "Branch"])["Message"]
        .apply(lambda msgs: " ".join(msgs.dropna().astype(str)))
        .reset_index()
        .rename(columns={"Message": "MessageConcat"})
    )

    # Merge concatenated messages back
    order_line_df = order_line_df.merge(
        message_groups,
        on=["OrderNum", "Branch"],
        how="left"
    )

    # ============ STEP 2: BUILD ITEMS CHILD TABLE ============
    item_data = []

    for (order_num, branch), group in order_line_df.groupby(["OrderNum", "Branch"]):
        items = []

        for _, row in group.iterrows():
            try:
                # Get conversion rate from main_df
                conversion_rate = main_df.loc[
                    (main_df["OrderNum"] == order_num) &
                    (main_df["Branch"] == branch),
                    "conversion_rate"
                ]
                conversion_rate_val = float(conversion_rate.iloc[0]) if not conversion_rate.empty else 1.0

                if conversion_rate_val == 0:
                    conversion_rate_val = 1.0

                # Build item dictionary
                item_dict = {
                    "item_code": row.get("ItemCode"),
                    "qty": float(row.get("Quantity", 0)),
                    "rate": float(row.get("UnitPrice", 0)),
                    "description": safe_strip(row.get("Description", "")),
                    "is_free_item": 1 if float(row.get("UnitPrice", 0)) == 0 else 0,
                    "discount_percentage": float(row.get("DiscountFactor", 0)),
                    "conversion_rate": conversion_rate_val
                }

                if item_dict['qty'] != 0:
                    items.append(item_dict)

            except Exception as e:
                print(f"Error processing item for OrderNum={order_num}: {e}")

        item_data.append({
            "OrderNum": order_num,
            "Branch": branch,
            "items": fix_list_column(items)
        })

    # ============ STEP 3: BUILD TAXES CHILD TABLE WITH CONSOLIDATION ============
    tax_data = []

    # Sort taxes by account head
    sorted_taxes = (
        tax_template_df.sort_values(by="account_head", ascending=True)
        .groupby("parent")
    )

    for (order_num, branch), group in order_line_df.groupby(["OrderNum", "Branch"]):
        taxes = []
        account_head_totals = defaultdict(float)
        account_head_code = defaultdict(str)

        for _, row in group.iterrows():
            # Aggregate taxes from multiple tax code columns
            template_name = row.get("TaxTemplate", "")

            if template_name in sorted_taxes.groups:
                tax_rows = sorted_taxes.get_group(template_name)
                for i, (_, tax_row) in enumerate(tax_rows.iterrows(), 1):
                    account_head = tax_row["account_head"]
                    tax_amount = float(row.get(f"TaxAmt{i}", 0) or 0)
                    account_head_totals[account_head] += tax_amount
                    account_head_code[account_head] = row.get(f"TaxCode{i}", "")

        # Build tax entries from aggregated totals
        for account_head, total_amount in account_head_totals.items():
            taxes.append({
                "charge_type": "Actual",
                "account_head": account_head,
                "tax_amount": total_amount,
                "description": account_head_code[account_head]
            })

        tax_data.append({
            "OrderNum": order_num,
            "Branch": branch,
            "taxes": fix_list_column(taxes)
        })

    # ============ STEP 4: MERGE CHILD TABLES BACK TO MAIN ============
    tax_df = pd.DataFrame(tax_data).drop_duplicates(subset=["OrderNum", "Branch"])
    item_df = pd.DataFrame(item_data).drop_duplicates(subset=["OrderNum", "Branch"])

    result_df = main_df.merge(tax_df, on=["OrderNum", "Branch"], how="left")
    result_df = result_df.merge(item_df, on=["OrderNum", "Branch"], how="left")

    result_df["items"] = result_df["items"].apply(fix_list_column)
    result_df["taxes"] = result_df["taxes"].apply(fix_list_column)

    # ============ STEP 5: REMOVE DUPLICATE DICTS IN LISTS ============
    def drop_duplicate_dicts_in_lists(df: DataFrame, column: str) -> DataFrame:
        """
        TECHNIQUE: Remove duplicate dictionaries from list columns using JSON serialization
        Handles non-serializable types and sorts items by order_line_number.
        """
        def serialize_for_json(obj):
            # Convert non-serializable types
            from decimal import Decimal
            if isinstance(obj, Decimal):
                return float(obj)
            return str(obj) if not isinstance(obj, (int, float, str, bool, type(None))) else obj

        def remove_duplicates(row):
            lst = row[column]
            if not isinstance(lst, list):
                return lst

            seen = set()
            unique = []

            for d in lst:
                if not isinstance(d, dict):
                    continue

                # Convert dict to JSON string with serializable values for hashing
                key = json.dumps(d, sort_keys=True, default=serialize_for_json)
                if key not in seen:
                    seen.add(key)
                    unique.append(d)

            # Sort items by order_line_number if applicable
            if column == "items":
                unique = sorted(unique, key=lambda x: int(x.get('order_line_number', 0)))

            return unique

        df[column] = df.apply(remove_duplicates, axis=1)
        return df

    result_df = drop_duplicate_dicts_in_lists(result_df, "items")
    result_df = drop_duplicate_dicts_in_lists(result_df, "taxes")

    # ============ STEP 6: CONSOLIDATE DICTS IN LIST BY GROUPING & SUMMING ============
    def consolidate_dicts_in_list(df: DataFrame, column: str, group_keys: List[str], sum_key: str) -> DataFrame:
        """
        TECHNIQUE: Group dictionaries in list columns and sum numeric values
        Consolidates duplicate tax entries by summing amounts.
        """
        def consolidate(lst):
            if not isinstance(lst, list):
                return lst

            grouped = defaultdict(lambda: 0)
            extra_data = {}

            for d in lst:
                key = tuple(d.get(k) for k in group_keys)
                grouped[key] += d.get(sum_key, 0)
                # Keep copy of latest non-sum data
                extra_data[key] = {k: d.get(k) for k in group_keys}

            # Rebuild list with consolidated values
            result = []
            for key, total in grouped.items():
                new_entry = extra_data[key].copy()
                new_entry[sum_key] = round(total, 2)
                result.append(new_entry)

            return result

        df[column] = df[column].apply(consolidate)
        return df

    result_df = consolidate_dicts_in_list(
        result_df, "taxes",
        ['account_head', 'charge_type', 'description'],
        'tax_amount'
    )

    # ============ STEP 7: BUILD SALES TEAM WITH ALLOCATION ============
    def extract_unique_sales_team(row):
        """
        TECHNIQUE: Extract unique values from multiple columns and allocate percentages
        Builds sales team list with equal percentage allocation.
        """
        sales_people = list(
            {row.get("SalesPerson1"), row.get("SalesPerson2"), row.get("SalesPerson3")}
            - {None, "", "NA"}
        )

        total_people = len(sales_people)
        if total_people == 0:
            return []

        allocated = round(100 / total_people, 2)
        remainder = 100 - (allocated * total_people)
        sales_team = []

        for i, person in enumerate(sales_people):
            percentage = allocated + remainder if i == 0 else allocated
            sales_team.append({
                "sales_person": person,
                "allocated_percentage": percentage
            })
            if i == 0:
                remainder = 0

        return sales_team

    result_df["sales_team"] = result_df.apply(extract_unique_sales_team, axis=1)

    return result_df


# ============================================================================
# 11. DEDUPLICATION TECHNIQUES
# ============================================================================

def remove_duplicates_single_column(df: DataFrame, column: str) -> DataFrame:
    """
    TECHNIQUE: drop_duplicates() with single column
    Keep first occurrence of each unique value.
    """
    return df.drop_duplicates(subset=[column], keep='first').reset_index(drop=True)


def remove_duplicates_multiple_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    TECHNIQUE: drop_duplicates() with multiple columns
    Keep first occurrence of each unique combination.
    """
    return df.drop_duplicates(subset=columns, keep='last').reset_index(drop=True)


def remove_duplicates_by_group(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Remove duplicates using groupby() and transform()
    Keep rows with highest value in group.
    """
    df['rank'] = df.groupby(['OrderNum', 'Branch'])['Amount'].rank(method='dense', ascending=False)
    df = df[df['rank'] == 1].drop(columns=['rank'])
    return df


def remove_duplicate_dicts_in_list(list_of_dicts: List[Dict]) -> List[Dict]:
    """
    TECHNIQUE: Deduplicate list of dictionaries using set and JSON
    Remove duplicate dictionaries from list columns.
    """
    seen = set()
    unique = []

    for d in list_of_dicts:
        # Convert dict to JSON string for hashing
        key = json.dumps(d, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            unique.append(d)

    return unique


# ============================================================================
# 12. COMPLEX TRANSFORMATION TECHNIQUES
# ============================================================================

def complex_multi_step_transformation(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Chained transformations combining multiple techniques
    Complex real-world transformation pipeline.
    """
    # Step 1: Filter active records
    df = df[df['Status'] == 'Active'].copy()

    # Step 2: Parse and validate dates
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df = df[df['OrderDate'].notna()]

    # Step 3: Merge with lookup data
    df = df.merge(lookup_df[['Code', 'Description']], left_on='Region', right_on='Code', how='left')

    # Step 4: Apply conditional logic
    df['Priority'] = np.where(df['Amount'] > 1000, 'High', 'Normal')

    # Step 5: String operations
    df['customer_name'] = df['customer_name'].str.upper().str.strip()

    # Step 6: Type conversions
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)

    # Step 7: Remove duplicates
    df = df.drop_duplicates(subset=['OrderNum'], keep='first')

    return df


def transformation_with_error_handling(df: DataFrame) -> DataFrame:
    """
    TECHNIQUE: Apply transformations with error handling
    Safely handle potential errors during transformation.
    """
    try:
        # Try to convert amount to float
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

        # Calculate discount safely
        df['Discount'] = df['Amount'].apply(
            lambda x: x * 0.1 if pd.notna(x) and x > 100 else 0
        )
    except Exception as e:
        print(f"Error during transformation: {e}")
        df['Discount'] = 0

    return df


# ============================================================================
# MAIN TRANSFORM FUNCTION - DEMONSTRATING ALL TECHNIQUES
# ============================================================================

def transform(sources: Dict[str, DataFrame]) -> DataFrame:
    """
    Main transformation function demonstrating all techniques combined.

    Args:
        sources: Dictionary of source dataframes

    Returns:
        Transformed DataFrame with all demonstrated techniques
    """

    # Get sample data
    orders_df = sources.get('orders', pd.DataFrame())
    customers_df = sources.get('customers', pd.DataFrame())
    items_df = sources.get('items', pd.DataFrame())
    taxes_df = sources.get('taxes', pd.DataFrame())

    print("=== COMPREHENSIVE DATA TRANSFORMATION ===\n")

    # 1. FILTERING
    print("1. Applying filters...")
    orders_df = filter_by_status(orders_df, ['Active', 'Pending'])
    orders_df = filter_by_date_range(orders_df, 'OrderDate', '2025-01-01', '2025-12-31')

    # 2. MERGING
    print("2. Merging data...")
    orders_df = orders_df.merge(
        customers_df[['CustomerId', 'CustomerName', 'Region']],
        on='CustomerId',
        how='left'
    )

    # 3. MAPPING
    print("3. Applying mappings...")
    status_map = {'A': 'Active', 'P': 'Pending', 'C': 'Completed'}
    orders_df['StatusText'] = orders_df['Status'].map(status_map).fillna('Unknown')

    # 4. FIELD OPERATIONS
    print("4. Performing field operations...")
    orders_df['CustomerName'] = orders_df['CustomerName'].str.upper().str.strip()
    orders_df['FullInfo'] = (
        orders_df['CustomerName'].fillna('') + ' - ' +
        orders_df['Region'].fillna('')
    )

    # 5. DATE HANDLING
    print("5. Handling dates...")
    orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'], errors='coerce')
    orders_df['OrderDateStr'] = orders_df['OrderDate'].dt.strftime('%Y-%m-%d')

    # 6. CONDITIONAL LOGIC
    print("6. Applying conditional logic...")
    orders_df['Priority'] = np.where(
        orders_df['Amount'] > 1000,
        'High',
        np.where(orders_df['Amount'] > 500, 'Medium', 'Low')
    )

    # 7. AGGREGATION (grouped data)
    print("7. Aggregating data...")
    agg_by_region = orders_df.groupby('Region')[['Amount']].agg({
        'Amount': ['sum', 'mean', 'count']
    }).reset_index()

    # 8. TYPE CONVERSION
    print("8. Converting types...")
    orders_df['Quantity'] = pd.to_numeric(orders_df['Quantity'], errors='coerce').fillna(0)
    orders_df['OrderId'] = orders_df['OrderId'].astype(str)

    # 9. STRING OPERATIONS
    print("9. Performing string operations...")
    orders_df['NormalizedPhone'] = (
        orders_df['Phone'].str.replace(r'\D', '', regex=True)
    )

    # 10. BUILD CHILD TABLES
    print("10. Building child tables...")
    def build_order_items(row):
        return [
            {
                'item_code': row['ItemCode1'],
                'quantity': row['Qty1'],
                'price': row['Price1']
            }
        ] if pd.notna(row['ItemCode1']) else []

    orders_df['items'] = orders_df.apply(build_order_items, axis=1)

    # 11. DEDUPLICATION
    print("11. Removing duplicates...")
    orders_df = orders_df.drop_duplicates(subset=['OrderId'], keep='first')

    # 12. FINAL FILTERING & CLEANUP
    print("12. Final filtering...")
    orders_df = orders_df[orders_df['Amount'] > 0].reset_index(drop=True)
    orders_df = orders_df[orders_df['CustomerName'].fillna('').str.strip() != '']

    print(f"\n=== TRANSFORMATION COMPLETE ===")
    print(f"Total records: {len(orders_df)}")
    print(f"Columns: {list(orders_df.columns)}")

    return orders_df


# ============================================================================
# ADVANCED CHILD TABLE BUILDING REFERENCE
# ============================================================================
"""
The build_advanced_child_tables() function demonstrates the sophisticated
patterns used in real transformers like common_functions_for_sales_history_transformers.py

Key Advanced Techniques Used:
1. **Message Concatenation & Grouping**
   - Group messages by multiple keys
   - Concatenate strings with specific order (sort by UniqueNo)
   - Merge concatenated results back to original data

2. **Complex Item Building with Conversion Rates**
   - Look up conversion rates from main dataframe
   - Validate conversion rates (handle zero values)
   - Build items with calculated fields
   - Filter items by quantity (skip qty=0)

3. **Tax Aggregation with defaultdict**
   - Accumulate tax amounts by account head
   - Handle multiple tax code columns (TaxCode1, TaxCode2, etc.)
   - Group taxes by parent template
   - Consolidate duplicate taxes by summing amounts

4. **Deduplication Using JSON Serialization**
   - Convert dictionaries to JSON strings for hashing
   - Use set to track seen items
   - Handle non-serializable types (Decimal, datetime, etc.)
   - Maintain order (sort by order_line_number)

5. **Consolidation & Grouping in Lists**
   - Group dictionaries by multiple keys
   - Sum numeric values within groups
   - Rebuild list with consolidated entries
   - Preserve metadata from latest entries

6. **Sales Team Allocation**
   - Extract unique values from multiple columns
   - Remove null/empty/"NA" values
   - Calculate equal percentage allocation
   - Handle remainder distribution

7. **Deduplication at Merge Points**
   - Use drop_duplicates() on subset of columns
   - Keep 'first' or 'last' based on priority
   - Reset index after filtering

8. **Date Handling in Child Tables**
   - Handle '9999' dates (future placeholders)
   - Convert to consistent format ('2099-12-31')
   - Format dates for display
"""

if __name__ == "__main__":
    # Example usage with sample data
    sample_sources = {
        'orders': pd.DataFrame({
            'OrderId': [1, 2, 3, 4, 5],
            'CustomerId': [101, 102, 101, 103, 102],
            'Amount': [1500, 450, 2000, 800, 350],
            'Quantity': [10, 5, 15, 8, 3],
            'Status': ['A', 'A', 'P', 'A', 'C'],
            'OrderDate': ['2025-03-01', '2025-04-15', '2025-05-20', '2025-06-10', '2025-07-05'],
            'Phone': ['555-1234', '555-5678', '555-9012', '555-3456', '555-7890'],
            'ItemCode1': ['ITEM001', 'ITEM002', 'ITEM001', None, 'ITEM003'],
            'Qty1': [10, 5, 15, 0, 3],
            'Price1': [150, 90, 150, 0, 250]
        }),
        'customers': pd.DataFrame({
            'CustomerId': [101, 102, 103],
            'CustomerName': ['acme corp', 'beta inc', 'gamma ltd'],
            'Region': ['North', 'South', 'East']
        }),
        'items': pd.DataFrame(),
        'taxes': pd.DataFrame()
    }

    result = transform(sample_sources)
    print("\nSample output:")
    print(result[['OrderId', 'CustomerName', 'Amount', 'Priority', 'items']].to_string())
