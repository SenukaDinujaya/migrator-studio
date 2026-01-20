# Migrator Studio

A framework for building ERP data migration transformers faster.

## Installation

```bash
pip install migrator_studio
```

Or install from source:

```bash
git clone https://github.com/SenukaDinujaya/migrator-studio.git
cd migrator-studio
pip install -e .
```

## Quick Start

```python
from migrator_studio import BuildSession, load_source
from migrator_studio.ops import filter_isin, map_dict, set_value

# Development mode with live preview
with BuildSession(sample=10) as session:
    df = load_source("DAT-00000001")

    # Filter active records
    df = filter_isin(df, "Status", ["A", "P"])

    # Map values
    df = map_dict(df, "Type", {"O": "Order", "Q": "Quote"})

    # Set constant value
    df = set_value(df, "Processed", True)

    # View operation history
    print(session.summary())
```

## Features

- **Configuration-driven transformations** - Pre-built operations for common data transformation patterns
- **Live preview mode** - Step-by-step execution with sample data during development
- **Operation tracking** - Automatic tracking of row counts, timing, and affected columns
- **Standalone ops module** - Use operations independently with `from migrator_studio.ops import ...`

## Supported Operations

| Category | Operations |
|----------|------------|
| Filter | `filter_isin`, `filter_not_isin`, `filter_by_value`, `filter_null`, `filter_not_null`, `filter_date`, `sanitize_data` |
| Merge | `merge_left`, `merge_inner`, `merge_outer` |
| Mapping | `map_dict`, `map_lookup` |
| Field | `copy_column`, `set_value`, `concat_columns`, `rename_columns`, `drop_columns`, `select_columns` |
| String | `str_upper`, `str_lower`, `str_strip`, `str_replace`, `str_regex_replace` |
| Date | `parse_date`, `format_date`, `extract_date_part`, `handle_invalid_dates` |
| Convert | `to_numeric`, `to_int`, `to_string`, `to_bool` |
| Conditional | `where`, `case`, `fill_null`, `coalesce` |
| Dedup | `drop_duplicates`, `keep_max`, `keep_min` |
| Aggregate | `groupby_agg`, `groupby_concat` |
| Apply | `apply_row`, `apply_column`, `transform` |

## Documentation

See [SPEC.md](SPEC.md) for detailed specification and design documentation.

## License

MIT
