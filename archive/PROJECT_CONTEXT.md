# Transformer Framework Project - Context & Overview

**Last Updated:** 2026-01-07
**Status:** Framework Design & Development Phase

---

## Project Purpose

Build a **Python framework** that simplifies writing data transformation scripts (transformers) for migrating ANY legacy ERP system to ERPNext. The framework should reduce boilerplate, standardize patterns, and make transformers easier to write, test, and maintain.

---

## Background: The Migrator Platform

### What is Migrator?

Migrator is a Frappe-based application that orchestrates ERP data migrations. It handles three main functions:

1. **Data Extraction** - Pull data from legacy systems (SQL databases, Excel files, APIs, etc.)
2. **Data Storage** - Store extracted data as feather files (intermediate format)
3. **Data Transformation** - Run Python transformers to prepare data for ERPNext import

### How Migrator Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    MIGRATOR PLATFORM                             │
└─────────────────────────────────────────────────────────────────┘

1. DATA EXTRACTION
   ┌──────────┐      ┌──────────────┐      ┌────────────┐
   │ Legacy   │ ───> │ Data         │ ───> │ DataTable  │
   │ System   │      │ Extractor    │      │ (feather)  │
   │ (Source) │      │              │      │ DAT-00001  │
   └──────────┘      └──────────────┘      └────────────┘
   Examples:                                Multiple tables
   - SQL Server                             extracted per
   - SharePoint                             client
   - Excel files

2. DATA TRANSFORMATION
   ┌────────────┐      ┌──────────────┐      ┌────────────┐
   │ DataTable  │      │              │      │ Transformed│
   │ DAT-00001  │ ───> │ Transformer  │ ───> │ DataFrame  │
   │ DAT-00002  │      │ (Python)     │      │            │
   │ DAT-00003  │      │              │      │            │
   └────────────┘      └──────────────┘      └────────────┘
   Multiple inputs      transform()          Single output
   (sources dict)       function             (DataFrame)

3. DATA IMPORT TO ERPNEXT
   ┌────────────┐      ┌──────────────┐      ┌────────────┐
   │ Transformed│      │ Data         │      │  ERPNext   │
   │ DataFrame  │ ───> │ Importer     │ ───> │ (Target)   │
   │            │      │              │      │            │
   └────────────┘      └──────────────┘      └────────────┘
```

### Key Entities in Migrator

- **Client:** Top-level org (the company being migrated)
- **Site:** Connection to legacy system (credentials, connection strings)
- **DataTable:** A stored dataset (feather file) with metadata
- **Data Extractor:** Pulls data from Site → creates DataTable
- **Transformer:** Python script that processes DataTables
- **Transformer Source:** Links Transformer to input DataTables (many-to-one)
- **Data Importer:** Imports transformed data into ERPNext

---

## What is a Transformer?

### Definition

A **Transformer** is a Python module containing a `transform()` function that:
- **Input:** Multiple source DataTables (as dictionary of DataFrames)
- **Output:** Single DataFrame ready for ERPNext import
- **Purpose:** Data manipulation, cleaning, mapping, enrichment

### Transformer Template

```python
from pandas import DataFrame

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Transforms data from one format to another.

    Args:
        sources (dict[str, DataFrame]): A dictionary of dataframes,
                                        where the key is the name of the
                                        source datatable (e.g., 'DAT-00001').

    Returns:
        DataFrame: A pandas DataFrame containing the transformed data.
    """

    # All transformation logic goes here
    pass
```

### Example Usage

```python
def transform(sources: dict[str, DataFrame]) -> DataFrame:
    # Get source data
    customers_df = sources['DAT-00001']  # Legacy customer table
    addresses_df = sources['DAT-00002']  # Legacy address table
    terms_df = sources['DAT-00003']      # Payment terms lookup

    # Transform to ERPNext format
    # ... 200+ lines of pandas code ...

    return erpnext_customers_df  # Ready for import
```

---

## The Problem We're Solving

### Current State (Without Framework)

Each transformer is **hundreds of lines of repetitive code**:
- Manual field mapping (source fields → ERPNext fields)
- Repeated filtering logic (status, dates, branches)
- Copy-pasted merge operations
- Hardcoded business rules
- Duplicate validation logic
- Inconsistent error handling
- No standardization across transformers

**Example:** A customer transformer might be 400 lines, an order transformer 600 lines, with 70% of the code being similar patterns.

### Challenges

1. **Variable Source Systems** - Every client has different legacy ERP:
   - Quantus, E2, Pacific, Sage, or unknown systems
   - Different schemas, table structures, field names
   - Different data quality and completeness
   - Different business rules embedded in data

2. **Constant Target** - All migrations go to ERPNext:
   - Fixed schema and field names
   - Known validation rules
   - Standard document structures (parent + child tables)
   - Consistent requirements

3. **Repetitive Patterns** - Same transformation types every time:
   - Filtering records by status, date, organization
   - Mapping field names (source → target)
   - Merging multiple tables
   - Building child tables (items, taxes, contacts)
   - Handling dates, currencies, codes
   - Data validation and cleaning

---

## The Solution: Transformer Framework

### Vision

A framework that makes writing transformers **80% faster** by:
- Providing reusable components for common patterns
- Declarative configuration where possible (YAML/JSON configs)
- Source-agnostic abstractions (work with any legacy system)
- ERPNext-aware validation (catch errors before import)
- Composable pipeline pattern (chain transformations)

### Example: Before vs. After Framework

**Before (Imperative, ~400 lines):**
```python
def transform(sources):
    df = sources['DAT-00001']

    # Manual filtering
    df = df[df['Status'] == 'A']
    df = df[df['Branch'].isin(['01', 'PG'])]
    df = df[df['dtCreated'] > '2018-01-01']

    # Manual field mapping
    df['customer_name'] = df['Name1'].str.upper()
    df['legacy_id'] = df['CustomerAcct']
    df['tax_id'] = df['TaxNum1']

    # Manual merging
    terms_df = sources['DAT-00003']
    df = pd.merge(df, terms_df, on=['DueType', 'DueDays'], how='left')

    # ... 300+ more lines ...

    return df
```

**After (Declarative, ~50 lines):**
```python
def transform(sources):
    config = {
        'source': 'DAT-00001',
        'filters': [
            {'field': 'Status', 'op': 'eq', 'value': 'A'},
            {'field': 'Branch', 'op': 'in', 'value': ['01', 'PG']},
            {'field': 'dtCreated', 'op': 'gt', 'value': '2018-01-01'},
        ],
        'fields': {
            'customer_name': {'source': 'Name1', 'transform': 'uppercase'},
            'legacy_id': {'source': 'CustomerAcct'},
            'tax_id': {'source': 'TaxNum1'},
        },
        'merges': [
            {'source': 'DAT-00003', 'on': ['DueType', 'DueDays']}
        ]
    }

    pipeline = TransformationPipeline.from_config(config)
    return pipeline.execute(sources)
```

---

## Key Constraints & Requirements

### Must Be Source-Agnostic

- **NO assumptions** about source system structure
- **NO hardcoded** table names, field names, business rules
- Configuration-driven transformations (per client)
- Runtime schema discovery (inspect source, don't assume)

**Why:** We migrate different clients with different legacy systems. Framework must handle ANY source → ERPNext migration.

### Must Be ERPNext-Aware

- Validate against ERPNext schema and business rules
- Pre-import validation (catch errors early)
- Field-level validation (data types, formats, constraints)
- Document-level validation (required fields, child tables)

**Why:** ERPNext is the constant destination. Framework should ensure output always meets ERPNext requirements.

### Must Be Composable

- Small, reusable transformation components
- Pipeline pattern (chain operations)
- Plugin architecture (custom transformations)
- Testable units

**Why:** Complex transformations are easier to build from small pieces.

---

## Framework Architecture (Proposed)

### Core Components

```
┌───────────────────────────────────────────────────────────┐
│              TRANSFORMER FRAMEWORK                         │
├───────────────────────────────────────────────────────────┤
│                                                            │
│  1. Configuration Layer                                    │
│     - YAML/JSON configs                                    │
│     - Field mappings, filters, rules                       │
│     - Client-specific configurations                       │
│                                                            │
│  2. Transformation Engine                                  │
│     - FilterStep (declarative filtering)                   │
│     - MapFieldsStep (field mapping with transforms)        │
│     - MergeStep (join/merge multiple sources)              │
│     - CalculateStep (derived fields, formulas)             │
│     - ChildTableStep (build nested structures)             │
│     - ValidateStep (data quality checks)                   │
│                                                            │
│  3. Pipeline Orchestrator                                  │
│     - Chain transformation steps                           │
│     - Handle errors gracefully                             │
│     - Progress tracking and logging                        │
│     - Dry-run mode                                         │
│                                                            │
│  4. ERPNext Validator                                      │
│     - Schema introspection                                 │
│     - Field validation (types, formats)                    │
│     - Required field checks                                │
│     - Business rule validation                             │
│                                                            │
│  5. Utility Library                                        │
│     - Date normalization                                   │
│     - Currency conversion                                  │
│     - Code mapping (status codes, currencies, etc.)        │
│     - Text processing (clean, extract, format)             │
│                                                            │
└───────────────────────────────────────────────────────────┘
```

### Pipeline Pattern

```python
# How transformers will be written with the framework
pipeline = Pipeline([
    # 1. Filter source data
    FilterStep(
        filters=[
            Filter('Status', 'in', ['A', 'O']),
            Filter('Branch', 'in', ['01', 'PG']),
        ]
    ),

    # 2. Clean data
    CleanStep(),

    # 3. Map fields
    MapFieldsStep(
        mapping={
            'customer_name': FieldMap('Name1', transform='uppercase'),
            'legacy_id': FieldMap('CustomerAcct'),
        }
    ),

    # 4. Merge reference data
    MergeStep(
        source='DAT-00003',
        on=['DueType', 'DueDays'],
        how='left'
    ),

    # 5. Calculate derived fields
    CalculateStep(
        calculations={
            'grand_total': lambda df: df['base_total'] / df['conversion_rate']
        }
    ),

    # 6. Build child tables
    ChildTableStep(
        name='items',
        source='DAT-00005',
        group_by=['OrderNum', 'Branch'],
        fields={'item_code': 'ItemNum', 'qty': 'OrderQty'}
    ),

    # 7. Validate output
    ValidateStep(
        rules=[
            ValidateNotEmpty('customer_name'),
            ValidateNotEmpty('items'),
        ]
    ),
])

result = pipeline.execute(sources)
```

---

## Universal Transformation Patterns

These 20 patterns appear in **every** migration regardless of source system:

| Pattern | Description | Example |
|---------|-------------|---------|
| **Filtering** | Select relevant records | Status = 'Active', Date > 2018 |
| **Field Mapping** | Source field → ERPNext field | CustomerAcct → legacy_id |
| **Enrichment/Merging** | Join multiple tables | Orders + Customers + Items |
| **Conditional Logic** | If-then rules | IF Branch='01' THEN currency='CAD' |
| **Code Mapping** | Translate codes | Status 'A' → 'Active' |
| **Date Handling** | Format & validate dates | '9999-12-31' → '2099-12-31' |
| **Child Tables** | Build nested lists | Order → Items[], Taxes[] |
| **Aggregation** | Group & summarize | Sum amounts, concat messages |
| **Calculations** | Derive new fields | total / rate = foreign_total |
| **Text Processing** | Clean & format text | Trim, uppercase, extract |
| **Data Quality** | Validate & fix | Remove nulls, type coercion |
| **Deduplication** | Remove duplicates | Keep first/last by priority |
| **Conditional Population** | Context-aware values | Different logic per doc type |
| **Data Flattening** | Denormalize | Join 5 tables → 1 flat structure |
| **Reference Resolution** | ID → Name lookup | CustomerID → customer_name |
| **Document Naming** | Generate unique IDs | HSO-AC-12345 |
| **Nested Transforms** | Multi-stage processing | Stage1 → Stage2 → Stage3 |
| **Multi-Source Reconciliation** | Combine sources | Union, prioritize, fallback |
| **Conditional Pipelines** | Route by type | SO vs. DN vs. SI |
| **Defaults** | Set constant values | order_type = 'Sales' |

**See:** `ERP_Migration_Transformation_Patterns.md` for detailed pattern analysis.

---

## Design Principles

### 1. Declarative Over Imperative
Prefer configuration over code for common patterns.

### 2. Source-Agnostic
No assumptions about source system structure.

### 3. ERPNext-Aware
Validate against ERPNext schema and rules.

### 4. Composable & Extensible
Build from small, reusable components.

### 5. Fail Fast with Clear Errors
Detect problems early, provide actionable messages.

### 6. Observable & Debuggable
Detailed logging, intermediate outputs, lineage tracking.

### 7. Performance-Conscious
Handle millions of records efficiently.

### 8. Self-Documenting
Transformations should be readable as documentation.

---

## Technical Context

### Technology Stack
- **Language:** Python 3.x
- **Data Processing:** pandas, numpy
- **Storage:** Feather files (Apache Arrow format)
- **Target Platform:** ERPNext (Frappe framework)
- **Orchestration:** Frappe-based Migrator app

### Data Flow
```
Legacy System → Extractor → DataTable (feather) →
Transformer (transform() function) → DataFrame →
Importer → ERPNext DocType
```

### Transformer Execution
1. Migrator loads transformer module
2. Reads source DataTables (feather) into DataFrames
3. Calls `transform(sources)` function
4. Validates output DataFrame
5. Imports to ERPNext or saves for review

---

## Project Goals

### Primary Goal
Build a framework that reduces transformer development time by **80%+** while ensuring:
- Consistent, high-quality ERPNext data
- Maintainable, readable transformers
- Reusable components across client projects
- Easy to test and debug

### Success Metrics
- ✅ Transformer LOC reduced from ~400 to ~50-100
- ✅ Transformation logic is declarative and self-documenting
- ✅ New transformers can be created in hours, not days
- ✅ Non-developers can configure basic transformations
- ✅ Framework handles any legacy system → ERPNext migration

---

## Current Status

**Phase:** Design & Development

**Completed:**
- ✅ Analyzed existing transformers (Quantus examples)
- ✅ Identified 20 universal transformation patterns
- ✅ Documented patterns in `ERP_Migration_Transformation_Patterns.md`
- ✅ Defined framework architecture and principles

**Next Steps:**
1. Design core framework API (Pipeline, Steps, Config)
2. Implement basic transformation steps (Filter, Map, Merge)
3. Build configuration schema (YAML/JSON format)
4. Develop ERPNext validator
5. Create example transformers using framework
6. Build testing framework
7. Documentation and best practices guide

---

## Key Files in This Project

### Essential Reading (Start Here)
- **`PROJECT_CONTEXT.md`** (this file) - Complete project overview
- **`transformer-template.py`** - Empty transformer boilerplate
- **`ERP_Migration_Transformation_Patterns.md`** - Detailed pattern analysis

### Reference (As Needed)
- **`diagram.txt`** - Migrator platform architecture
- **`examples/`** - Sample transformers (Quantus-specific, not ideal code)
- **`common_functions_for_sales_history_transformers.py`** - Example patterns

### Future Framework Files
- `framework/` - Core framework code (to be built)
- `configs/` - Client transformation configs
- `tests/` - Framework test suite

---

## Important Notes

### What Transformers Are NOT
- ❌ Not generic data pipelines (specific purpose: legacy ERP → ERPNext)
- ❌ Not ETL tools (no complex scheduling, just transformation logic)
- ❌ Not data warehousing (intermediate format, not long-term storage)

### What Transformers ARE
- ✅ Data manipulation scripts with specific input/output
- ✅ Single-purpose: transform source data to ERPNext format
- ✅ Stateless functions (idempotent, repeatable)
- ✅ Pandas-based data wrangling

### Framework Scope
**In Scope:**
- Transformation logic simplification
- Pattern reusability
- ERPNext validation
- Testing utilities

**Out of Scope:**
- Data extraction (handled by Extractors)
- Data import (handled by Importers)
- Scheduling/orchestration (handled by Migrator)
- UI/frontend (Frappe provides this)

---

## Quick Start (For New Sessions)

To understand this project in a new session, read in order:
1. **This file** (`PROJECT_CONTEXT.md`) - Full context
2. **`transformer-template.py`** - See the boilerplate
3. **`ERP_Migration_Transformation_Patterns.md`** - Understand patterns

That's it! You now understand:
- What Migrator is and how it works
- What transformers are and their purpose
- What framework we're building and why
- What patterns exist across all migrations
- What the design principles and constraints are

---

## Questions to Ask When Continuing Work

1. **What component are we building?** (Pipeline? Steps? Config?)
2. **What patterns does it support?** (Filtering? Mapping? Merging?)
3. **How is it configured?** (YAML? Python API? Both?)
4. **How does it validate?** (Against ERPNext schema? Custom rules?)
5. **How is it tested?** (Unit tests? Integration tests? Sample data?)

---

**Remember:** The framework must work with **any** legacy system (Quantus, E2, Pacific, Sage, unknown future systems) and always output ERPNext-compatible data. The patterns are universal; the details change per client.
