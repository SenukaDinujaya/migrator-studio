# Comprehensive Transformer Example - Advanced Techniques Summary

## Overview

The `TFRM-COMPREHENSIVE-DEMO.py` file demonstrates **ALL 12 categories** of data manipulation techniques used throughout the transformer framework, including **advanced child table building patterns** from `common_functions_for_sales_history_transformers.py`.

---

## What Was Added

### ✅ Basic Child Table Building (Section 10)
Three foundational patterns:

1. **Simple Child Tables** - Create list columns with dictionaries
   ```python
   def create_items(row):
       items = []
       if pd.notna(row['Item1']):
           items.append({'item_code': row['Item1'], 'qty': row['Qty1']})
       return items
   ```

2. **Conditional Child Records** - Generate entries based on conditions
   ```python
   def create_taxes(row):
       taxes = []
       if row['TaxAmount1'] > 0:
           taxes.append({'account': 'Tax Account 1', 'amount': row['TaxAmount1']})
       return taxes
   ```

3. **Grouped Aggregation** - Aggregate data from multiple rows
   ```python
   for (order_num, branch), group in df.groupby(['OrderNum', 'Branch']):
       taxes = aggregate_taxes(group)
   ```

---

### 🔥 ADVANCED Child Table Building (`build_advanced_child_tables()`)

This function demonstrates **8 sophisticated techniques** used in real transformers:

#### 1. **Message Concatenation & Grouping**
```python
message_groups = (
    order_line_df.sort_values(by="UniqueNo")
    .groupby(["OrderNum", "Branch"])["Message"]
    .apply(lambda msgs: " ".join(msgs.dropna().astype(str)))
    .reset_index()
    .rename(columns={"Message": "MessageConcat"})
)
```
- Groups messages by multiple keys (OrderNum, Branch)
- Sorts messages by UniqueNo before concatenation
- Concatenates strings with space separator
- Merges results back to original dataframe

#### 2. **Complex Item Building with Conversion Rates**
```python
# Look up conversion rate from main_df
conversion_rate = main_df.loc[
    (main_df["OrderNum"] == order_num) &
    (main_df["Branch"] == branch),
    "conversion_rate"
]
conversion_rate_val = float(conversion_rate.iloc[0]) if not conversion_rate.empty else 1.0

if conversion_rate_val == 0:
    conversion_rate_val = 1.0

# Build item with calculated fields
item_dict = {
    "item_code": row.get("ItemCode"),
    "qty": float(row.get("Quantity", 0)),
    "rate": float(row.get("UnitPrice", 0)),
    "is_free_item": 1 if float(row.get("UnitPrice", 0)) == 0 else 0,
    "conversion_rate": conversion_rate_val
}
```
- Cross-references main dataframe for conversion rates
- Validates conversion rates (handles zero values)
- Builds items with calculated fields
- Filters items by quantity (skips qty=0)

#### 3. **Tax Aggregation with defaultdict**
```python
account_head_totals = defaultdict(float)
account_head_code = defaultdict(str)

if template_name in sorted_taxes.groups:
    tax_rows = sorted_taxes.get_group(template_name)
    for i, (_, tax_row) in enumerate(tax_rows.iterrows(), 1):
        account_head = tax_row["account_head"]
        tax_amount = float(row.get(f"TaxAmt{i}", 0) or 0)
        account_head_totals[account_head] += tax_amount
        account_head_code[account_head] = row.get(f"TaxCode{i}", "")
```
- Accumulates tax amounts by account head using defaultdict
- Handles multiple tax code columns (TaxCode1, TaxCode2, TaxCode3, etc.)
- Groups taxes by parent template
- Tracks both amounts and codes

#### 4. **Deduplication Using JSON Serialization**
```python
def drop_duplicate_dicts_in_lists(df: DataFrame, column: str) -> DataFrame:
    def serialize_for_json(obj):
        from decimal import Decimal
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj) if not isinstance(obj, (int, float, str, bool, type(None))) else obj

    def remove_duplicates(row):
        seen = set()
        unique = []
        for d in lst:
            key = json.dumps(d, sort_keys=True, default=serialize_for_json)
            if key not in seen:
                seen.add(key)
                unique.append(d)
        return unique

    df[column] = df.apply(remove_duplicates, axis=1)
    return df
```
- Converts dictionaries to JSON strings for hashing
- Handles non-serializable types (Decimal, datetime, etc.)
- Maintains order and sorts by order_line_number
- Removes duplicate dictionary entries from list columns

#### 5. **Consolidation & Grouping in Lists**
```python
def consolidate_dicts_in_list(df: DataFrame, column: str, group_keys: List[str], sum_key: str):
    def consolidate(lst):
        grouped = defaultdict(lambda: 0)
        extra_data = {}

        for d in lst:
            key = tuple(d.get(k) for k in group_keys)
            grouped[key] += d.get(sum_key, 0)
            extra_data[key] = {k: d.get(k) for k in group_keys}

        result = []
        for key, total in grouped.items():
            new_entry = extra_data[key].copy()
            new_entry[sum_key] = round(total, 2)
            result.append(new_entry)

        return result

    df[column] = df[column].apply(consolidate)
    return df
```
- Groups dictionaries in list columns by specific keys
- Sums numeric values within each group
- Preserves metadata from latest entries
- Returns consolidated list with combined amounts

#### 6. **Sales Team Allocation**
```python
def extract_unique_sales_team(row):
    sales_people = list(
        {row.get("SalesPerson1"), row.get("SalesPerson2"), row.get("SalesPerson3")}
        - {None, "", "NA"}
    )

    total_people = len(sales_people)
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
```
- Extracts unique values from multiple columns
- Removes null/empty/"NA" values using set difference
- Calculates equal percentage allocation
- Handles remainder distribution to first entry

#### 7. **Deduplication at Merge Points**
```python
tax_df = pd.DataFrame(tax_data).drop_duplicates(subset=["OrderNum", "Branch"])
item_df = pd.DataFrame(item_data).drop_duplicates(subset=["OrderNum", "Branch"])

result_df = main_df.merge(tax_df, on=["OrderNum", "Branch"], how="left")
result_df = result_df.merge(item_df, on=["OrderNum", "Branch"], how="left")
```
- Uses drop_duplicates() on subset of columns
- Applies at merge points to prevent duplication
- Maintains data integrity across joins

#### 8. **Date Handling in Child Tables**
```python
def fix_date(value):
    try:
        value_str = str(value)
        if value_str.startswith("9999"):
            return "2099-12-31"
        return str(pd.to_datetime(value, errors="coerce").date())
    except Exception:
        return "2099-12-31"
```
- Handles special '9999' dates (future placeholders)
- Converts to consistent format
- Includes error handling with fallback defaults

---

## All 12 Technique Categories Covered

| # | Category | Location | Example |
|---|----------|----------|---------|
| 1 | **Filtering & Selection** | Lines 34-79 | `filter_by_status()`, `filter_by_condition()`, `filter_non_null()` |
| 2 | **Merging & Joining** | Lines 83-111 | `merge_left_join()`, `merge_inner_join()`, `merge_multiple_keys()` |
| 3 | **Mapping & Lookups** | Lines 115-143 | `map_with_dictionary()`, `map_from_dataframe()` |
| 4 | **Field Operations** | Lines 147-196 | `copy_field()`, `concatenate_fields()`, `conditional_field_copy()` |
| 5 | **Date Handling** | Lines 200-249 | `parse_dates()`, `format_dates()`, `handle_invalid_dates()` |
| 6 | **Aggregation & Grouping** | Lines 253-298 | `aggregate_by_group()`, `concatenate_strings_in_group()` |
| 7 | **Type Conversion** | Lines 302-337 | `convert_to_numeric()`, `convert_to_integer()`, `convert_to_boolean()` |
| 8 | **Conditional Logic** | Lines 341-405 | `conditional_with_numpy_where()`, `conditional_with_apply_lambda()` |
| 9 | **String Operations** | Lines 409-486 | `string_upper_lower()`, `string_regex_replace()`, `string_extract_from_email()` |
| 10 | **Basic Child Tables** | Lines 490-576 | `build_child_table_simple()`, `build_child_table_conditional()` |
| 11 | **Advanced Child Tables** | Lines 581-824 | `build_advanced_child_tables()` with 8 sub-techniques |
| 12 | **Complex Transformations** | Lines 828-875 | `complex_multi_step_transformation()` |

---

## How to Use This File

### 1. **Learning Reference**
Read through each function to understand specific techniques. Each function has:
- Clear docstring explaining the technique
- Practical code example
- Real-world use case

### 2. **Quick Copy-Paste**
Find the technique you need and copy the relevant function into your transformer.

### 3. **Advanced Patterns**
For complex transformations, study `build_advanced_child_tables()` which shows how to:
- Handle multi-step processing
- Manage error conditions
- Integrate multiple techniques

### 4. **Testing**
Run the file directly to see example output:
```bash
python TFRM-COMPREHENSIVE-DEMO.py
```

---

## Key Takeaways

### Most Important Patterns from Your Project

1. **Child Table Building** - The most complex and important pattern
   - Used heavily in sales transformers
   - Requires careful handling of aggregations and deduplication
   - Must validate data at multiple steps

2. **Deduplication in Lists** - JSON serialization approach
   - Handles non-standard types
   - Maintains order and structure
   - Prevents duplicate records

3. **Tax/Fee Consolidation** - defaultdict aggregation
   - Groups by multiple keys simultaneously
   - Sums numeric values safely
   - Preserves associated metadata

4. **Message Concatenation** - String grouping pattern
   - Sort before grouping (order matters)
   - Handle nulls with fillna/dropna
   - Merge results back carefully

5. **Sales Team Allocation** - Percentage distribution
   - Handle sets and unique values
   - Distribute remainders fairly
   - Build structured child records

---

## Related Files

- **`common_functions_for_sales_history_transformers.py`** - Source of advanced patterns
- **`TFRM-00000001.py`** - Customer transformer example
- **`TFRM-00000004.py`** - Contact transformer example
- **`TFRM-00000029.py`** - Item price transformer example

---

## Notes

- All examples use pandas DataFrames
- Functions are designed to be chainable/composable
- Error handling is included where appropriate
- Comments explain the "why" not just the "what"
