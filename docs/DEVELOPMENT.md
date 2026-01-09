# Development Notes

**For: Future development when building the framework**

---

## Context for Development

This document contains technical notes and decisions for when we start building the framework.

---

## Architecture Overview (Planned)

### High-Level Components

```
┌─────────────────────────────────────────────────────────────┐
│                    TRANSFORMER FRAMEWORK                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   CONFIG    │───▶│ TRANSFORMER  │───▶│   EXECUTOR   │  │
│  │   PARSER    │    │   ENGINE     │    │              │  │
│  └─────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                    │          │
│         │                   │                    ▼          │
│         ▼                   ▼            ┌──────────────┐  │
│  ┌─────────────┐    ┌──────────────┐    │    DISPLAY   │  │
│  │  VALIDATOR  │    │   PATTERNS   │    │    LAYER     │  │
│  │             │    │   LIBRARY    │    │ (Live View)  │  │
│  └─────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Module Breakdown

1. **Config Parser** (`framework/config/`)
   - Parse YAML/Python DSL config files
   - Validate configuration syntax
   - Build transformation AST (Abstract Syntax Tree)

2. **Transformer Engine** (`framework/engine/`)
   - Execute transformation steps
   - Handle data flow between steps
   - Manage state and context

3. **Patterns Library** (`framework/patterns/`)
   - Pre-built transformation components
   - 12 categories from TFRM-COMPREHENSIVE-DEMO.py
   - Extensible plugin system

4. **Executor** (`framework/executor/`)
   - Development mode: Interpret config, run on sample data
   - Production mode: Generate optimized Python code, run on full dataset

5. **Display Layer** (`framework/display/`)
   - CLI output with `rich` formatting
   - Sample data preview (10 rows)
   - Progress indicators
   - Error display

6. **Validator** (`framework/validator/`)
   - Pre-execution validation
   - Config schema checking
   - Data availability checks

---

## Key Design Patterns

### Pattern 1: Strategy Pattern for Transformations

Each transformation type implements a common interface:

```python
class TransformationStep(ABC):
    @abstractmethod
    def execute(self, df: DataFrame, config: dict) -> DataFrame:
        """Execute transformation on dataframe"""
        pass

    @abstractmethod
    def validate(self, config: dict) -> ValidationResult:
        """Validate configuration before execution"""
        pass

    @abstractmethod
    def preview_description(self, config: dict) -> str:
        """Return human-readable description for preview"""
        pass
```

### Pattern 2: Pipeline/Chain for Step Execution

```python
class TransformationPipeline:
    def __init__(self, steps: List[TransformationStep]):
        self.steps = steps

    def execute(self, df: DataFrame, mode: str = "preview"):
        result = df
        for step in self.steps:
            result = step.execute(result)
            if mode == "preview":
                self.display_preview(step, result)
        return result
```

### Pattern 3: Factory for Creating Transformation Steps

```python
class TransformationFactory:
    _registry = {}

    @classmethod
    def register(cls, name: str, step_class: Type[TransformationStep]):
        cls._registry[name] = step_class

    @classmethod
    def create(cls, config: dict) -> TransformationStep:
        step_type = config.get("type")
        step_class = cls._registry.get(step_type)
        return step_class(config)
```

---

## Configuration Schema (Draft)

### Example YAML Config

```yaml
# TFRM-00000999.yaml - Example transformer configuration

name: "Customer Contact Transformer"
version: "1.0"
sources:
  customers: "DAT-00000001"
  vendors: "DAT-00000004"

transformations:
  # 1. Load and select columns
  - type: select_source
    source: customers
    columns:
      - CustomerAcct
      - MailContact
      - ShipContact
      - MailEmail
      - MailPhone

  # 2. Clean data
  - type: clean_data
    apply_defaults: true

  # 3. String operations
  - type: string_operations
    operations:
      - field: MailPhone
        operation: regex_replace
        pattern: '\D'
        replacement: ''
        output: CleanMailPhone
      - field: ShipPhone
        operation: regex_replace
        pattern: '\D'
        replacement: ''
        output: CleanShipPhone

  # 4. Build child table
  - type: build_child_table
    output_column: phone_nos
    logic:
      - condition: "row['MailPhone'] != ''"
        record:
          phone: "row['MailPhone']"
          is_primary_phone: 1
      - condition: "row['MailCellPhone'] != '' and row['MailCellPhone'] != row['MailPhone']"
        record:
          phone: "row['MailCellPhone']"
          is_primary_mobile_no: 1

  # 5. Merge with lookup
  - type: merge
    source: vendors
    left_on: CustomerAcct
    right_on: VendorAcct
    how: left

  # 6. Filter records
  - type: filter
    conditions:
      - field: Status
        operator: in
        values: ["Active", "Pending"]
      - field: Amount
        operator: ">"
        value: 0

  # 7. Select final columns
  - type: select_columns
    columns:
      - first_name
      - email_ids
      - phone_nos
      - links

output:
  doctype: "Contact"
  mode: "insert"
```

---

## Implementation Priority

### Phase 1: Core Infrastructure
1. Config parser (YAML loader)
2. Basic validation
3. Pipeline executor
4. Simple CLI display

### Phase 2: Top 5 Patterns
1. **Filter** - Most common, easiest to implement
2. **Select columns** - Simple, frequently used
3. **Merge/Join** - Critical for lookups
4. **String operations** - High-frequency pattern
5. **Simple child tables** - Core ERP pattern

### Phase 3: Advanced Patterns
6. Aggregation & Grouping
7. Type conversions
8. Conditional logic
9. Advanced child tables
10. Date handling
11. Mapping & lookups
12. Deduplication

---

## Testing Strategy

### Unit Tests
- Each transformation pattern has dedicated tests
- Test with sample data from TFRM-COMPREHENSIVE-DEMO.py
- Edge cases: null values, empty strings, special characters

### Integration Tests
- Full transformer execution
- Config → Generated code → Output
- Compare manual transformer vs framework-generated transformer output

### Regression Tests
- Recreate existing transformers (TFRM-00000004.py) using framework
- Verify output matches byte-for-byte

### Performance Benchmarks
- Measure execution time on datasets of varying sizes
- Ensure framework overhead is < 10% vs manual code

---

## Code Generation Strategy

### Development Mode: Interpret Config
```python
# Direct execution from config
config = load_config("TFRM-00000999.yaml")
pipeline = build_pipeline(config)
result = pipeline.execute(source_data, mode="preview")
```

### Production Mode: Generate Python Code
```python
# Generated Python transformer
def transform(sources: dict[str, DataFrame]) -> DataFrame:
    # Auto-generated from TFRM-00000999.yaml
    # Generated on: 2026-01-15 14:30:00

    df = sources["DAT-00000001"]
    df = df[["CustomerAcct", "MailContact", ...]]
    df = clean_data(df)
    df["CleanMailPhone"] = df["MailPhone"].str.replace(r'\D', '', regex=True)
    # ... rest of generated code
    return df
```

---

## Error Handling Philosophy

### Fail Fast, Fail Clearly

**Bad Error**:
```
KeyError: 'CustomerAcct'
```

**Good Error**:
```
❌ Configuration Error in step 3 (select_columns):
   Field 'CustomerAcct' not found in source 'DAT-00000001'

   Available fields:
   - customer_account
   - mail_contact
   - ship_contact

   Did you mean 'customer_account'?
```

### Error Categories
1. **Config errors**: Invalid YAML, unknown transformation type
2. **Data errors**: Missing columns, type mismatches
3. **Logic errors**: Division by zero, invalid regex
4. **Runtime errors**: Out of memory, file not found

---

## Dependencies (Planned)

### Core
- `pandas >= 2.0.0` - Data manipulation
- `pyyaml >= 6.0` - Config parsing
- `rich >= 13.0.0` - Terminal formatting
- `click >= 8.0.0` - CLI framework

### Development
- `pytest >= 7.0.0` - Testing
- `black >= 23.0.0` - Code formatting
- `mypy >= 1.0.0` - Type checking
- `ruff >= 0.1.0` - Linting

### Optional
- `polars >= 0.20.0` - Future performance optimization
- `textual >= 0.50.0` - Future TUI upgrade

---

## File Structure (When Building)

```
transformer_framework/
├── README.md
├── project_document.md
├── pyproject.toml                 # Package configuration
├── setup.py                       # Installation
├── src/
│   └── transformer_framework/
│       ├── __init__.py
│       ├── cli.py                 # CLI entry point
│       ├── config/
│       │   ├── __init__.py
│       │   ├── parser.py          # YAML parser
│       │   └── validator.py       # Config validation
│       ├── engine/
│       │   ├── __init__.py
│       │   ├── pipeline.py        # Pipeline executor
│       │   └── context.py         # Execution context
│       ├── patterns/
│       │   ├── __init__.py
│       │   ├── base.py            # Base transformation class
│       │   ├── filter.py          # Filter pattern
│       │   ├── merge.py           # Merge pattern
│       │   ├── string_ops.py      # String operations
│       │   ├── child_table.py     # Child table builder
│       │   └── ...                # Other patterns
│       ├── executor/
│       │   ├── __init__.py
│       │   ├── interpreter.py     # Config interpreter
│       │   └── codegen.py         # Code generator
│       └── display/
│           ├── __init__.py
│           └── cli.py             # CLI display logic
├── tests/
│   ├── conftest.py
│   ├── test_parser.py
│   ├── test_patterns.py
│   └── test_integration.py
├── docs/
│   ├── DEVELOPMENT.md             # This file
│   ├── examples/
│   │   ├── TFRM-COMPREHENSIVE-DEMO.py
│   │   └── TFRM-00000004.py
│   └── guides/
│       ├── quickstart.md
│       ├── pattern_reference.md
│       └── migration_guide.md
└── archive/                       # Historical docs
```

---

## Open Technical Questions

### Q1: How to handle custom Python logic in YAML?

**Option A**: Inline Python strings
```yaml
- type: custom
  code: |
    df['total'] = df['price'] * df['quantity']
```

**Option B**: Reference external functions
```yaml
- type: custom
  function: "custom_functions.calculate_total"
```

**Option C**: Hybrid - allow both
```yaml
- type: custom
  function: "custom_functions.complex_logic"  # Recommended
  fallback_code: |                            # For simple cases
    df['total'] = df['price'] * df['quantity']
```

### Q2: How to handle sampling for preview?

**Option A**: Always use first 10 rows
```python
preview_df = df.head(10)
```

**Option B**: Smart sampling based on config
```yaml
preview:
  method: "stratified"  # or "random", "head", "tail"
  size: 10
  filter: "Status == 'Active'"  # Preview only active records
```

### Q3: How to generate child tables declaratively?

**Challenge**: Child table logic can be very complex (see TFRM-COMPREHENSIVE-DEMO.py:572)

**Solution**: Tiered approach
1. **Simple child tables**: Full YAML support
2. **Medium complexity**: YAML with helper functions
3. **High complexity**: Python function required

---

## Performance Considerations

### Memory Management
- Don't load entire dataset in preview mode
- Use chunking for large datasets in production mode
- Stream data where possible

### Optimization Opportunities
1. **Lazy evaluation**: Build execution plan, optimize before running
2. **Column pruning**: Only load needed columns from sources
3. **Predicate pushdown**: Apply filters early to reduce data size
4. **Caching**: Cache intermediate results during development

---

## Next Actions (When Development Starts)

1. Set up project structure with `pyproject.toml`
2. Implement basic YAML parser
3. Create filter pattern (simplest)
4. Build pipeline executor
5. Add CLI with `rich` preview
6. Test with simple example
7. Iterate and expand

---

## Reference Implementation Locations

When implementing patterns, reference these functions in example code:

### Filtering (TFRM-COMPREHENSIVE-DEMO.py)
- `filter_by_status()` - Line 34
- `filter_by_date_range()` - Line 42
- `filter_by_condition()` - Line 53
- `filter_by_multiple_conditions()` - Line 72

### Merging (TFRM-COMPREHENSIVE-DEMO.py)
- `merge_left_join()` - Line 105
- `merge_inner_join()` - Line 118
- `merge_multiple_keys()` - Line 130

### String Operations (TFRM-COMPREHENSIVE-DEMO.py)
- `string_upper_lower()` - Line 435
- `string_strip()` - Line 445
- `string_replace()` - Line 454
- `string_regex_replace()` - Line 463

### Child Tables (TFRM-COMPREHENSIVE-DEMO.py)
- `build_child_table_simple()` - Line 504
- `build_child_table_conditional()` - Line 521
- `build_advanced_child_tables()` - Line 572 ⭐ Complex pattern

### Real Production Example (TFRM-00000004.py)
- `get_mailcontacts()` - Line 78
- `get_mailcontact_phone_child_table()` - Line 110
- `replace_contactname()` - Line 11
- Full `transform()` - Line 331

---

**Last Updated**: 2026-01-09
**Status**: Pre-development planning phase
