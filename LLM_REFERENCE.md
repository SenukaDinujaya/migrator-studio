# Migrator Studio - LLM Reference

## CLI Commands

```bash
pip install -e ".[dev]"                      # Install
migrator dev transformer.py                  # Generate notebook + open Marimo
migrator dev transformer.py --no-run         # Generate only
migrator dev transformer.py -s 50            # Sample 50 rows
migrator run transformer.py                  # Production run (WIP)
```

---

## Transformer File Structure

```python
"""TFRM-XXX-001: Description of what this transforms."""
from migrator_studio import step, load_source
from migrator_studio.operations import filter_isin, merge_left, map_dict  # etc.

SOURCES = ["DAT-00000001", "DAT-00000002"]  # Required data sources

def transform(sources: dict[str, "DataFrame"]) -> "DataFrame":
    customers = sources["DAT-00000001"]
    regions = sources["DAT-00000002"]

    step("Filter active customers")
    df = filter_isin(customers, "Status", ["A", "P"])

    step("Join regions")
    df = merge_left(df, regions, left_on="RegionID", right_on="ID",
                    select_columns=["RegionName"])

    step("Map status codes")
    df = map_dict(df, "Status", {"A": "Active", "P": "Pending"})

    return df
```

**Rules:**
- `step("name")` marks cell boundaries for notebook generation
- `SOURCES` list declares required `.feather` files
- `transform()` receives dict of DataFrames, returns single DataFrame

---

## Operations Quick Reference

### Filter
```python
filter_isin(df, column, values)                    # Keep rows IN list
filter_not_isin(df, column, values)                # Keep rows NOT IN list
filter_by_value(df, col, eq=, ne=, gt=, lt=)       # Comparison filter
filter_null(df, column)                            # Keep nulls only
filter_not_null(df, column)                        # Drop nulls
filter_date(df, col, after=, before=, on_or_after=, on_or_before=)
```

### Merge
```python
merge_left(df, right, on=, left_on=, right_on=, select_columns=)
merge_inner(df, right, ...)
merge_outer(df, right, ...)
```

### Mapping
```python
map_dict(df, column, {"old": "new"}, target=None, fallback=None)
map_lookup(df, column, lookup_df, key_col, value_col, target=None)
```

### Field
```python
copy_column(df, source, target)
set_value(df, column, value)                       # Constant value
concat_columns(df, ["A", "B"], target, sep=" ")
rename_columns(df, {"old": "new"})
drop_columns(df, ["col1", "col2"])
select_columns(df, ["keep1", "keep2"])
```

### String
```python
str_upper(df, column, target_column=None)
str_lower(df, column, target_column=None)
str_strip(df, column, target_column=None)
str_replace(df, column, old, new, target_column=None)
str_regex_replace(df, column, pattern, repl, target_column=None)
```

### Date
```python
parse_date(df, column, target=None, format=None)
format_date(df, column, format="%Y-%m-%d", target=None)
extract_date_part(df, column, part, target)        # year/month/day/quarter/week
handle_invalid_dates(df, column, fallback="2099-12-31")
```

### Convert
```python
to_numeric(df, column, target=None, errors="coerce")
to_int(df, column, target=None, fill=0)
to_string(df, column, target=None)
to_bool(df, column, target=None, true_values=["Y", "1"])
```

### Conditional
```python
where(df, column, df["X"] > 100, then_value, else_value=None)
case(df, column, [(df["X"] > 100, "High"), (df["X"] > 50, "Med")], default="Low")
fill_null(df, column, value, target=None)
coalesce(df, ["col1", "col2", "col3"], target)     # First non-null
```

### Aggregate
```python
groupby_agg(df, by=["col"], agg={"amount": "sum", "qty": ["sum", "mean"]})
groupby_concat(df, by=["col"], column="values", target="combined", sep=",")
```

### Dedup
```python
drop_duplicates(df, columns, keep="first")         # or "last"
keep_max(df, by=["group_col"], value_column="amount")
keep_min(df, by=["group_col"], value_column="amount")
```

### Apply (escape hatch)
```python
apply_row(df, lambda row: row["A"] + row["B"], target="C")
apply_column(df, "col", lambda x: x * 2, target=None)
transform(df, lambda df: df.query("X > 0"))        # Arbitrary pandas
```

---

## Generated Notebook Structure

When running `migrator dev`, produces:

```python
import marimo
app = marimo.App()

@app.cell
def __():
    from migrator_studio import configure, BuildSession, load_source
    from migrator_studio.operations import filter_isin, merge_left, ...
    configure(data_path="./path")
    session = BuildSession(sample=10)
    session.__enter__()
    return (session, filter_isin, ...)

@app.cell
def __(load_source):
    sources = {"DAT-00000001": load_source("DAT-00000001"), ...}
    customers = sources["DAT-00000001"]
    return (sources, customers)

@app.cell
def __(customers, filter_isin):
    # Step: Filter active customers
    df_1 = filter_isin(customers, "Status", ["A", "P"])
    df_1
    return df_1,

@app.cell
def __(df_1, merge_left, regions):
    # Step: Join regions
    df_2 = merge_left(df_1, regions, left_on="RegionID", ...)
    df_2
    return df_2,
```

Each `step()` becomes a cell. Variables renamed (df_1, df_2) for Marimo reactivity.

---

## Data Files

- Location: `{data_path}/{source_id}.feather`
- Example: `./data/DAT-00000001.feather`
- Configure: `MIGRATOR_STUDIO_DATA_PATH` env var or `configure(data_path="...")`
