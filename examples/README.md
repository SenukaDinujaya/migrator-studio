# Examples

This directory contains example transformers and sample data for learning migrator-studio.

## Sample Data

The feather files (`DAT-*.feather`) contain synthetic ERP data with intentional data quality issues for demonstration:

| File | Description | Rows |
|------|-------------|------|
| `DAT-00000001.feather` | Customers | 100 |
| `DAT-00000002.feather` | Products | 50 |
| `DAT-00000003.feather` | Sales Orders | 100 |
| `DAT-00000004.feather` | Order Lines | ~250 |
| `DAT-00000005.feather` | Region Lookup | 10 |
| `DAT-00000006.feather` | Status Lookup | 6 |

### Data Quality Issues (Intentional)

- Null values in various columns
- Empty strings and whitespace-only values
- Invalid dates (9999-12-31, 1900-01-01)
- Invalid codes not in lookup tables
- Negative and zero values where unexpected

## Example Transformers

### TFRM-EXAMPLE-001.py - Customer Transformer

Demonstrates common transformation patterns:

1. **Sanitize data** - Clean empty strings and whitespace
2. **Filter rows** - Keep only active customers with names
3. **Join tables** - Merge with region lookup
4. **Map values** - Convert status codes to descriptions
5. **String operations** - Strip and uppercase names
6. **Handle nulls** - Fill missing values with defaults
7. **Add columns** - Copy and set constant values
8. **Conditional logic** - Create priority flag based on region
9. **Deduplicate** - Remove duplicate records
10. **Select columns** - Choose and rename final output fields

## Running Examples

```bash
# Generate fresh sample data (optional - already included)
python examples/generate_sample_data.py

# Run the transformer in development mode
migrator dev --data-path examples/

# Or specify a transformer directly
migrator dev examples/TFRM-EXAMPLE-001.py --data-path examples/
```

## Creating Your Own Transformer

1. Create a new file following the naming pattern `TFRM-*.py`
2. Define `SOURCES` list with the data files you need
3. Implement `transform(sources: dict[str, DataFrame]) -> DataFrame`
4. Use `step()` to mark transformation stages

```python
from pandas import DataFrame
from migrator_studio import step, filter_not_null, select_columns

SOURCES = ["DAT-00000001"]

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    df = sources["DAT-00000001"]

    step("Filter valid records")
    df = filter_not_null(df, "Name")

    step("Select output columns")
    df = select_columns(df, ["CustomerID", "Name", "Email"])

    return df
```
