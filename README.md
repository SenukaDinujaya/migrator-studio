# Migrator Studio

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A framework for building ERP data migration transformers faster. Write declarative transformations with live preview, automatic tracking, and interactive development mode.

## Features

- **40+ Pre-built Operations** - Filter, merge, map, string, date, and more
- **Interactive Development** - Live preview with [Marimo](https://marimo.io) notebooks
- **Automatic Tracking** - Row counts, timing, and affected columns tracked automatically
- **Step-by-Step Debugging** - Mark transformation stages with `step()` for easy debugging
- **Type Safety** - Full type hints and pandas DataFrame operations

## Installation

```bash
pip install migrator-studio
```

Or install from source:

```bash
git clone https://github.com/SenukaDinujaya/migrator-studio.git
cd migrator-studio
pip install -e ".[dev]"
```

## Quick Start

### 1. Create a Transformer

Create a file `TFRM-CUSTOMERS-001.py`:

```python
from pandas import DataFrame
from migrator_studio import (
    step,
    filter_isin,
    filter_not_null,
    merge_left,
    map_dict,
    str_upper,
    fill_null,
    select_columns,
)

SOURCES = ["DAT-00000001", "DAT-00000005"]

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    customers = sources["DAT-00000001"]
    regions = sources["DAT-00000005"]

    step("Filter active customers")
    df = filter_isin(customers, "Status", ["A", "P"])
    df = filter_not_null(df, "Name")

    step("Join with regions")
    df = merge_left(df, regions, left_on="Region", right_on="RegionCode")

    step("Clean and format")
    df = str_upper(df, "Name")
    df = fill_null(df, "Email", "unknown@example.com")

    step("Select output columns")
    df = select_columns(df, ["Name", "Email", "RegionName"])

    return df
```

### 2. Run in Development Mode

```bash
# Auto-discover transformer in current directory
migrator dev

# Or specify a transformer explicitly
migrator dev TFRM-CUSTOMERS-001.py

# With custom sample size
migrator dev TFRM-CUSTOMERS-001.py --sample 50
```

This opens an interactive Marimo notebook where you can:
- See data at each step
- Preview row counts and changes
- Iterate quickly on your transformation logic

### 3. Run in Production

```python
from migrator_studio import load_source
from your_transformer import transform, SOURCES

sources = {name: load_source(name) for name in SOURCES}
result = transform(sources)
result.to_csv("output.csv", index=False)
```

## Operations Reference

### Filter Operations

| Operation | Description |
|-----------|-------------|
| `filter_isin(df, col, values)` | Keep rows where column is in values |
| `filter_not_isin(df, col, values)` | Remove rows where column is in values |
| `filter_by_value(df, col, op, value)` | Filter by comparison (==, !=, <, >, etc.) |
| `filter_null(df, col)` | Keep rows where column is null |
| `filter_not_null(df, col)` | Remove rows where column is null |
| `filter_date(df, col, start, end)` | Filter by date range |
| `sanitize_data(df, empty_val)` | Replace empty strings with value |

### Merge Operations

| Operation | Description |
|-----------|-------------|
| `merge_left(df, right, left_on, right_on)` | Left join with optional column selection |
| `merge_inner(df, right, left_on, right_on)` | Inner join |
| `merge_outer(df, right, left_on, right_on)` | Full outer join |

### Field Operations

| Operation | Description |
|-----------|-------------|
| `copy_column(df, source, target)` | Copy column to new name |
| `set_value(df, col, value)` | Set constant value for column |
| `concat_columns(df, cols, target, sep)` | Concatenate columns |
| `rename_columns(df, mapping)` | Rename columns |
| `drop_columns(df, cols)` | Remove columns |
| `select_columns(df, cols)` | Keep only specified columns |

### String Operations

| Operation | Description |
|-----------|-------------|
| `str_upper(df, col)` | Convert to uppercase |
| `str_lower(df, col)` | Convert to lowercase |
| `str_strip(df, col)` | Remove whitespace |
| `str_replace(df, col, old, new)` | Replace substring |
| `str_regex_replace(df, col, pattern, repl)` | Regex replace |

### Date Operations

| Operation | Description |
|-----------|-------------|
| `parse_date(df, col, format)` | Parse string to date |
| `format_date(df, col, format)` | Format date to string |
| `extract_date_part(df, col, part, target)` | Extract year/month/day |
| `handle_invalid_dates(df, col, default)` | Replace invalid dates |

### Value Operations

| Operation | Description |
|-----------|-------------|
| `map_dict(df, col, mapping, target)` | Map values via dictionary |
| `map_lookup(df, col, lookup_df, key, value)` | Map via lookup table |
| `where(df, col, condition, then, else)` | Conditional value |
| `case(df, col, conditions, values, default)` | Multiple conditions |
| `fill_null(df, col, value)` | Replace nulls |
| `coalesce(df, cols, target)` | First non-null value |

### Aggregate Operations

| Operation | Description |
|-----------|-------------|
| `groupby_agg(df, by, aggs)` | Group and aggregate |
| `groupby_concat(df, by, col, sep)` | Group and concatenate strings |
| `drop_duplicates(df, cols, keep)` | Remove duplicates |
| `keep_max(df, by, col)` | Keep row with max value |
| `keep_min(df, by, col)` | Keep row with min value |

### Type Conversion

| Operation | Description |
|-----------|-------------|
| `to_numeric(df, col)` | Convert to numeric |
| `to_int(df, col)` | Convert to integer |
| `to_string(df, col)` | Convert to string |
| `to_bool(df, col)` | Convert to boolean |

## CLI Commands

```bash
# Interactive development mode (auto-discovers TFRM-*.py files)
migrator dev [TRANSFORMER] [--sample N] [--output PATH] [--no-run]

# Sync notebook changes back to transformer
migrator sync TRANSFORMER

# Run transformer in production mode
migrator run TRANSFORMER [--output PATH]
```

## Examples

See the [examples/](examples/) directory for complete working examples including sample data.

```bash
# Run the example transformer
cd examples/
migrator dev TFRM-EXAMPLE-001.py
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=migrator_studio --cov-report=html

# Type checking
mypy migrator_studio

# Linting
ruff check migrator_studio
```

## License

MIT - see [LICENSE](LICENSE) for details.
