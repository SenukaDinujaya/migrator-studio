# ERP Migration Data Transformation Patterns
## Reference Guide for Building a Generic Transformer Framework

**Version:** 2.0
**Date:** 2026-01-07
**Purpose:** Catalog universal data manipulation patterns for migrating ANY legacy ERP system to ERPNext

---

## Table of Contents

1. [Introduction](#introduction)
2. [Core Transformation Categories](#core-transformation-categories)
3. [Detailed Pattern Analysis](#detailed-pattern-analysis)
4. [Cross-Cutting Concerns](#cross-cutting-concerns)
5. [Framework Design Principles](#framework-design-principles)

---

## Introduction

This document analyzes transformer scripts used in ERPNext migration projects across **multiple different legacy ERP systems**. These patterns represent universal data manipulation techniques that apply regardless of source system.

### Critical Context

**Source Systems (Variable):**
- Different per client: Quantus, E2, Pacific, Sage, and unknown future systems
- Each has unique schemas, table structures, field names, and business rules
- Source data format, quality, and completeness varies widely
- **No assumptions can be made about source system structure**

**Target System (Constant):**
- **ERPNext** - the only fixed destination
- Known schema, validation rules, and document structures
- Consistent field names and data requirements
- Standard child table patterns

**The Challenge:**
- Build a framework flexible enough to handle ANY source → ERPNext migration
- Patterns must be **source-agnostic** and **destination-aware**
- Reusable transformation logic that adapts to different client scenarios

---

## Core Transformation Categories

These categories represent **types of data manipulation**, not specific implementations. The actual fields, tables, and business rules will differ per client, but the **patterns of transformation remain consistent**.

---

### 1. Data Selection & Filtering

**Purpose:** Reduce source datasets to relevant records based on business criteria that vary per client.

**Pattern Intent:**
Filter records based on:
- Status/state values (active vs. inactive, open vs. closed)
- Business entity types (transactions, master data categories)
- Organizational units (branches, companies, departments)
- Temporal boundaries (date ranges, cutoff dates)
- Data quality gates (completeness, validity)

**Generic Pattern:**
```python
# Pattern: Selective filtering with configurable criteria
filtered_data = source_data[
    (source_data[status_field].isin(valid_statuses)) &
    (source_data[org_unit_field].isin(included_units)) &
    (source_data[date_field] > cutoff_date) &
    (source_data[required_field].notna())
]
```

**Why it's universal:**
- Every legacy system has status/state concepts (different names, same purpose)
- All ERPs have organizational hierarchies (different structures, same need to filter)
- Business always wants recent/relevant data (date filtering is universal)
- Data quality issues exist in every source system

**Framework Implication:**
- Provide declarative filtering DSL
- Support multiple filter conditions with AND/OR logic
- Allow dynamic filter criteria based on client configuration

---

### 2. Field Mapping & Renaming

**Purpose:** Translate source system field names to ERPNext field names.

**Pattern Intent:**
Map fields from source schema to target schema:
- Direct one-to-one mappings
- Copy single source to multiple targets
- Preserve legacy identifiers for traceability
- Handle missing source fields gracefully

**Generic Pattern:**
```python
# Pattern: Schema translation (source names → ERPNext names)
target_df['erpnext_field_name'] = source_df['legacy_field_name']
target_df['legacy_id'] = source_df['old_system_identifier']
```

**Why it's universal:**
- Every system has different field naming conventions
- ERPNext has fixed, required field names (customer, transaction_date, etc.)
- Field mapping is required for EVERY migration regardless of source
- Need bidirectional traceability (new system ← → old system)

**Framework Implication:**
- Field mappings should be externalized (config file, not hardcoded)
- Support expression-based mapping (not just direct copies)
- Validate target fields against ERPNext schema
- Auto-generate mapping templates from source schema inspection

---

### 3. Data Enrichment & Merging

**Purpose:** Combine data from multiple source tables to create complete ERPNext documents.

**Pattern Intent:**
Join/merge operations:
- Reference data lookups (master data enrichment)
- Multi-table aggregation (denormalization)
- Cross-entity relationship resolution
- Hierarchical data flattening

**Generic Pattern:**
```python
# Pattern: Multi-source data assembly
enriched_df = base_df.merge(
    reference_df[['key', 'descriptive_fields']],
    on='key',
    how='left'
)
```

**Why it's universal:**
- Source systems store data in normalized tables (relational databases)
- ERPNext documents need denormalized/flattened structure
- Lookups required to resolve codes → descriptions, IDs → names
- Every migration involves joining multiple source tables

**Framework Implication:**
- Declarative join/merge configuration
- Support multiple join types (left, inner, outer)
- Handle missing reference data gracefully
- Optimize merge performance for large datasets

---

### 4. Conditional Logic & Business Rules

**Purpose:** Apply client-specific business logic to determine field values.

**Pattern Intent:**
Decision trees and conditional assignments:
- Multi-condition evaluation (if-then-else chains)
- Context-dependent value determination
- Default value resolution when data missing
- Business rule enforcement during transformation

**Generic Pattern:**
```python
# Pattern: Conditional value assignment
def determine_value(row, rules):
    for condition, result in rules:
        if condition(row):
            return result
    return default_value
```

**Why it's universal:**
- Every business has unique rules embedded in legacy data
- Data is often incomplete, requiring intelligent defaults
- Multi-company/multi-branch scenarios need context-aware logic
- Source data inconsistencies require resolution rules

**Framework Implication:**
- Rule engine for business logic (not hardcoded conditionals)
- Support complex condition evaluation (nested AND/OR)
- Priority-based rule evaluation (first match, best match)
- Externalize business rules from transformation code

---

### 5. Code Mapping & Translation

**Purpose:** Translate legacy system codes/abbreviations to ERPNext standard values.

**Pattern Intent:**
Value translation via lookup:
- Status/state code mapping
- Currency/unit code standardization
- Category/classification mapping
- Geographic/territory code translation

**Generic Pattern:**
```python
# Pattern: Code translation via mapping table
mapping = {'OLD_CODE_1': 'New Value 1', 'OLD_CODE_2': 'New Value 2'}
target_df['erpnext_field'] = source_df['legacy_code'].map(mapping)
```

**Why it's universal:**
- Every system has proprietary codes/abbreviations
- ERPNext has standardized value sets for dropdowns
- Code meanings differ across systems (same code, different meaning)
- Mapping required for data integrity in target system

**Framework Implication:**
- Mapping tables stored externally (CSV, database, config)
- Support one-to-one and many-to-one mappings
- Handle unmapped codes (validation warnings, defaults)
- Version-controlled mapping configurations per client

---

### 6. Date & Time Transformations

**Purpose:** Normalize date/time data to ERPNext format, handle edge cases.

**Pattern Intent:**
Date manipulation:
- Format conversion (various formats → ERPNext standard)
- Invalid date handling (nulls, placeholder dates, future dates)
- Timezone normalization
- Date arithmetic (age, duration, relative dates)

**Generic Pattern:**
```python
# Pattern: Date normalization with error handling
def normalize_date(value, invalid_replacement='2099-12-31'):
    if is_placeholder(value):  # 9999-12-31, 0000-00-00, etc.
        return invalid_replacement
    return format_as_erpnext_date(value)
```

**Why it's universal:**
- Every system stores dates (but in different formats)
- Legacy systems use placeholder dates for "never expires" / "unknown"
- Date format inconsistencies are common (MM/DD/YYYY vs DD/MM/YYYY)
- ERPNext has strict date format requirements

**Framework Implication:**
- Robust date parsing (handle multiple input formats)
- Configurable invalid date handling strategies
- Timezone-aware transformations
- Date validation against business rules

---

### 7. Child Table Construction

**Purpose:** Build ERPNext child table structures (one-to-many relationships) from source data.

**Pattern Intent:**
Aggregate related records into nested lists:
- Line items within transactions (order lines, invoice lines)
- Contact details within entities (multiple phones, emails)
- Financial breakdowns (tax components, payment allocations)
- Hierarchical relationships (team members, allocations)

**Generic Pattern:**
```python
# Pattern: Group related records into child lists
child_records = source_lines.groupby(parent_key).apply(
    lambda group: [
        {'field1': row['col1'], 'field2': row['col2']}
        for _, row in group.iterrows()
    ]
).to_dict()
```

**Why it's universal:**
- ERPNext documents use child tables for one-to-many relationships
- Source systems store related data in separate tables (normalized)
- Every document type has child tables (items, taxes, contacts, etc.)
- Aggregation pattern is consistent regardless of data type

**Framework Implication:**
- Child table builder abstraction
- Declarative child field mapping
- Support nested transformations within child records
- Handle empty child tables gracefully

---

### 8. Aggregation & Grouping

**Purpose:** Consolidate multiple source records into summary or grouped forms.

**Pattern Intent:**
Reduce granularity through aggregation:
- Summing amounts, quantities, totals
- Concatenating text fields (notes, comments, instructions)
- Collecting distinct values (tags, categories)
- Rolling up hierarchical data

**Generic Pattern:**
```python
# Pattern: Aggregate multiple records into one
aggregated = source_df.groupby(grouping_keys).agg({
    'amount_field': 'sum',
    'text_field': lambda x: ' '.join(x),
    'category_field': lambda x: list(set(x))
})
```

**Why it's universal:**
- Source systems often have line-by-line granular data
- Target may need summarized/rolled-up view
- Text consolidation common (multiple memo/comment records)
- Performance optimization (reduce record count)

**Framework Implication:**
- Declarative aggregation rules
- Support multiple aggregation functions (sum, concat, collect, first, last)
- Custom aggregation function support
- Preserve detail when needed (don't force aggregation)

---

### 9. Calculations & Derived Fields

**Purpose:** Compute new values from existing fields using formulas.

**Pattern Intent:**
Mathematical and logical computations:
- Currency conversions
- Quantity calculations (remaining, fulfilled, pending)
- Percentage calculations (allocations, discounts)
- Rate determination (exchange rates, unit prices)
- Balance calculations (total - paid = due)

**Generic Pattern:**
```python
# Pattern: Derive new field from existing fields
target_df['derived_field'] = target_df['field1'] / target_df['field2']
target_df['derived_field'].fillna(default_value, inplace=True)
```

**Why it's universal:**
- Derived values ensure data consistency in target system
- Source systems may store different calculations
- ERPNext formulas may differ from legacy system
- Need to recalculate for integrity

**Framework Implication:**
- Formula engine (define calculations declaratively)
- Safe math (handle division by zero, nulls)
- Support conditional calculations
- Validate calculation results against business rules

---

### 10. Text & String Processing

**Purpose:** Clean, format, extract, or construct text data.

**Pattern Intent:**
String manipulation:
- Case normalization (upper, lower, title case)
- Whitespace trimming and cleanup
- Concatenation (build composite strings)
- Extraction (parse structured data from text)
- Character replacement (remove invalid chars)

**Generic Pattern:**
```python
# Pattern: Text manipulation and cleanup
target_df['cleaned_field'] = (
    source_df['raw_field']
    .str.strip()
    .str.upper()
    .str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
)
```

**Why it's universal:**
- Text data always needs cleaning (whitespace, casing, special chars)
- Source systems have inconsistent formatting
- ERPNext may have length/format constraints
- Need standardized display format

**Framework Implication:**
- Text processing utility library
- Configurable string transformations
- Regex support for complex patterns
- Encoding/decoding support

---

### 11. Data Quality & Validation

**Purpose:** Ensure data meets quality standards, fix issues, enforce constraints.

**Pattern Intent:**
Quality gates and corrections:
- Null/empty value handling
- Data type validation and coercion
- Range/boundary validation
- Mandatory field checks
- Anomaly detection and correction

**Generic Pattern:**
```python
# Pattern: Data quality enforcement
# Handle nulls
df['field'].fillna(default_value, inplace=True)

# Validate ranges
df = df[df['numeric_field'] > 0]

# Type coercion
df['numeric_field'] = pd.to_numeric(df['numeric_field'], errors='coerce')
```

**Why it's universal:**
- Every legacy system has data quality issues
- Manual data entry causes inconsistencies
- ERPNext import will fail on invalid data
- Quality checks prevent import errors

**Framework Implication:**
- Validation rule engine
- Pre-import quality reports
- Automated data correction where possible
- Clear error messages for manual review

---

### 12. Deduplication

**Purpose:** Remove duplicate records or consolidate redundant data.

**Pattern Intent:**
Eliminate duplicates:
- Primary key deduplication
- Composite key deduplication
- Priority-based selection (keep best record)
- Fuzzy matching (near-duplicates)

**Generic Pattern:**
```python
# Pattern: Remove duplicates with priority
df = df.sort_values(priority_column, ascending=False)
df = df.drop_duplicates(subset=unique_keys, keep='first')
```

**Why it's universal:**
- Legacy systems often have duplicate records
- Multiple data sources create overlaps
- Need single source of truth per entity
- Prevent duplicate imports into ERPNext

**Framework Implication:**
- Declarative deduplication rules
- Support multiple deduplication strategies
- Fuzzy matching for near-duplicates
- Audit trail of removed duplicates

---

### 13. Conditional Field Population

**Purpose:** Populate fields differently based on context or document type.

**Pattern Intent:**
Context-aware field assignment:
- Document-type-specific values
- Entity-type-specific logic
- State-dependent field values
- Optional vs. mandatory field handling

**Generic Pattern:**
```python
# Pattern: Context-dependent field population
if document_type == 'TypeA':
    df['field'] = logic_for_type_a(df)
elif document_type == 'TypeB':
    df['field'] = logic_for_type_b(df)
```

**Why it's universal:**
- Same source data feeds multiple ERPNext document types
- Different business rules per context
- Reusable logic with parameterization
- Reduce code duplication

**Framework Implication:**
- Parameterized transformation templates
- Document-type-aware processing
- Conditional logic based on context variables
- Inheritance/composition of transformation rules

---

### 14. Hierarchical Data Flattening

**Purpose:** Convert normalized relational data into flat/denormalized ERPNext documents.

**Pattern Intent:**
Denormalization:
- Multi-table joins to create flat structure
- Embed related data instead of foreign keys
- Collapse hierarchies into single records
- Preserve relationships in document format

**Generic Pattern:**
```python
# Pattern: Flatten relational structure
flat_df = (
    table1
    .merge(table2, on='key1')
    .merge(table3, on='key2')
    .merge(table4, on='key3')
)
```

**Why it's universal:**
- Source systems are relational (normalized tables)
- ERPNext documents are semi-structured (embedded data)
- Flattening improves read performance in ERPNext
- Self-contained documents for workflows

**Framework Implication:**
- Multi-stage join orchestration
- Schema mapping (relational → document)
- Preserve referential integrity
- Optimize join performance

---

### 15. Reference Resolution & Foreign Key Mapping

**Purpose:** Resolve relationships, convert IDs to names/references.

**Pattern Intent:**
Link resolution:
- ID to name conversion
- Legacy ID to ERPNext name mapping
- Cross-entity relationship preservation
- Dynamic link construction

**Generic Pattern:**
```python
# Pattern: Resolve foreign keys to names
lookup_map = reference_df.set_index('id')['name'].to_dict()
target_df['name_field'] = source_df['id_field'].map(lookup_map)
```

**Why it's universal:**
- Source systems use numeric IDs/codes
- ERPNext primarily uses names (strings) as keys
- Need to resolve relationships during migration
- Enable lookups in target system

**Framework Implication:**
- Reference data management
- Lazy vs. eager resolution strategies
- Handle missing references gracefully
- Cache lookup tables for performance

---

### 16. Document Naming & Identification

**Purpose:** Generate unique, meaningful identifiers for ERPNext documents.

**Pattern Intent:**
Name generation:
- Construct compound identifiers
- Apply naming conventions (prefixes, suffixes)
- Ensure uniqueness
- Maintain traceability to source

**Generic Pattern:**
```python
# Pattern: Generate document names
df['__newname'] = prefix + '-' + df['identifier1'] + '-' + df['identifier2']
```

**Why it's universal:**
- ERPNext requires unique document names
- Names should be human-readable
- Distinguish migrated vs. new documents
- Maintain link to legacy system identifiers

**Framework Implication:**
- Naming convention configurator
- Uniqueness validation
- Collision detection/resolution
- Pattern-based name generation

---

### 17. Complex Nested Transformations

**Purpose:** Build intricate multi-level structures with dependencies.

**Pattern Intent:**
Multi-pass processing:
- Sequential transformation stages
- Context-dependent child record building
- Grouped batch processing
- Layered enrichment

**Generic Pattern:**
```python
# Pattern: Multi-stage transformation pipeline
stage1_df = transform_stage1(source_df)
stage2_df = transform_stage2(stage1_df, context)
stage3_df = transform_stage3(stage2_df, additional_data)
```

**Why it's universal:**
- Real-world data has complex interdependencies
- Some transformations require prior transformations
- Parent context needed for child record building
- Maintainability (break complex logic into stages)

**Framework Implication:**
- Pipeline orchestration
- Stage dependency management
- Context passing between stages
- Incremental processing support

---

### 18. Multi-Source Data Reconciliation

**Purpose:** Combine or choose between conflicting data from multiple sources.

**Pattern Intent:**
Source prioritization:
- Union multiple sources
- Priority-based selection (prefer source A over B)
- Fallback chains (try A, then B, then C)
- Conflict resolution

**Generic Pattern:**
```python
# Pattern: Multi-source reconciliation
combined = pd.concat([source1, source2, source3])
deduplicated = combined.drop_duplicates(subset='key', keep='first')
```

**Why it's universal:**
- Migrations often have multiple data sources
- Data quality varies across sources
- Need comprehensive coverage
- Resolve conflicts systematically

**Framework Implication:**
- Multi-source configuration
- Priority rules for conflict resolution
- Source tracking (provenance)
- Validation across sources

---

### 19. Conditional Transformation Pipelines

**Purpose:** Route data through different transformation sequences based on criteria.

**Pattern Intent:**
Routing logic:
- Type-based routing (different transforms per type)
- Parameterized pipelines (same pipeline, different parameters)
- Conditional step execution
- Branch and merge patterns

**Generic Pattern:**
```python
# Pattern: Conditional pipeline routing
if record_type == 'A':
    result = pipeline_A.transform(data)
elif record_type == 'B':
    result = pipeline_B.transform(data)
```

**Why it's universal:**
- Different record types need different handling
- Avoid monolithic transformation logic
- Reuse common steps, vary specific steps
- Maintainable modular design

**Framework Implication:**
- Pipeline routing engine
- Parameterized pipeline templates
- Conditional step inclusion/exclusion
- Pipeline composition/inheritance

---

### 20. Default Value & Static Field Assignment

**Purpose:** Set constant values or defaults for fields that don't exist in source.

**Pattern Intent:**
Constant assignment:
- Required field defaults (ERPNext mandatories)
- Type/category classification
- Flag initialization
- Placeholder values

**Generic Pattern:**
```python
# Pattern: Static field assignment
df['erpnext_required_field'] = constant_value
df['category'] = 'Default Category'
```

**Why it's universal:**
- ERPNext has required fields not in legacy systems
- Need sensible defaults for missing data
- Standardize categorization
- Initialize system fields

**Framework Implication:**
- Default value configuration
- Required field validation against ERPNext schema
- Conditional defaults (context-aware)
- Override mechanism for client-specific defaults

---

## Cross-Cutting Concerns

These concerns apply across all transformation patterns regardless of source system.

### Error Handling & Logging

**Universal Need:**
- **Graceful degradation:** Don't fail entire migration on single record error
- **Progress tracking:** Log progress through large datasets
- **Error logging:** Capture transformation failures with context
- **Validation warnings:** Alert on suspicious data

**Framework Implication:**
- Robust exception handling at record level
- Structured logging (not print statements)
- Progress indicators for long-running operations
- Error report generation (records that failed, why)

---

### Source-Agnostic Data Cleaning

**Universal Need:**
- **Type normalization:** Ensure consistent data types
- **Encoding handling:** Deal with character encoding issues
- **Whitespace cleanup:** Strip, normalize spaces
- **Null standardization:** Consistent null handling

**Framework Implication:**
- Pre-processing pipeline before transformations
- Configurable cleaning rules
- Automatic type detection and coercion
- Encoding detection and conversion

---

### Performance Optimization

**Universal Need:**
- **Vectorization:** Use pandas/numpy operations (avoid row-by-row)
- **Batch processing:** Process in chunks for memory efficiency
- **Early filtering:** Reduce dataset size before expensive operations
- **Index optimization:** Leverage indexing for joins/lookups

**Framework Implication:**
- Optimize for large datasets (millions of records)
- Memory-efficient processing strategies
- Parallel processing where applicable
- Performance profiling tools

---

### Testing & Validation

**Universal Need:**
- **Unit tests:** Test individual transformation functions
- **Integration tests:** Test full transformation pipelines
- **Data validation:** Ensure output meets ERPNext requirements
- **Sample testing:** Run on sample data before full migration

**Framework Implication:**
- Testing framework for transformers
- Sample data generation for testing
- Assertion library for data validation
- Automated testing during development

---

## Framework Design Principles

### 1. Source-Agnostic Architecture

**Principle:** Framework must NOT assume source system structure.

**Design Implications:**
- No hardcoded table names, field names, or business rules
- Configuration-driven transformations
- Client-specific config files (mapping tables, rules)
- Runtime schema discovery (inspect source, don't assume)

**Example:**
```python
# BAD: Hardcoded assumptions
df['customer'] = source_df['CustomerAcct']  # Assumes field exists

# GOOD: Configurable mapping
customer_field = config.get_source_field('customer')
df['customer'] = source_df[customer_field] if customer_field in source_df else None
```

---

### 2. ERPNext-Aware Validation

**Principle:** Framework must validate against ERPNext schema and business rules.

**Design Implications:**
- ERPNext schema introspection
- Pre-import validation (catch errors before import)
- Field-level validation (data types, formats, constraints)
- Document-level validation (required fields, business rules)

**Example:**
```python
# Validate against ERPNext schema
validator = ERPNextValidator(doctype='Customer')
errors = validator.validate(transformed_df)
if errors:
    report_errors(errors)
```

---

### 3. Declarative Over Imperative

**Principle:** Prefer configuration/declaration over code for common patterns.

**Design Implications:**
- DSL for field mapping, filtering, transformations
- YAML/JSON configuration files
- Code generation from configurations
- Reduce boilerplate in transformer scripts

**Example:**
```yaml
# Declarative transformation config
fields:
  customer_name:
    source: Name1
    transform: uppercase
  legacy_id:
    source: CustomerAcct
  transaction_date:
    source: OrderDate
    transform: format_date
```

---

### 4. Composable & Extensible

**Principle:** Build small, reusable components that compose into complex transformations.

**Design Implications:**
- Transformation functions as composable units
- Pipeline pattern (chain transformations)
- Plugin architecture (custom transformations)
- Inheritance/mixins for common logic

**Example:**
```python
# Composable pipeline
pipeline = Pipeline([
    FilterStep(config.filters),
    CleanStep(),
    MapFieldsStep(config.field_mapping),
    MergeStep(reference_data),
    CalculateStep(config.calculations),
    ValidateStep(erpnext_schema),
])
result = pipeline.execute(source_data)
```

---

### 5. Fail Fast with Clear Errors

**Principle:** Detect problems early, provide actionable error messages.

**Design Implications:**
- Validation at config load time (not runtime)
- Type checking and schema validation
- Clear error messages (not stack traces)
- Suggest fixes in error messages

**Example:**
```python
# Clear, actionable errors
raise ConfigurationError(
    f"Field '{field_name}' specified in mapping does not exist in source data. "
    f"Available fields: {list(source_df.columns)}"
)
```

---

### 6. Observable & Debuggable

**Principle:** Provide visibility into transformation process for debugging.

**Design Implications:**
- Detailed logging at each stage
- Intermediate outputs for inspection
- Data lineage tracking (trace transformations)
- Dry-run mode (preview without saving)

**Example:**
```python
# Observable transformations
logger.info(f"Filtering: {len(df)} → {len(filtered_df)} records")
logger.debug(f"Sample output: {filtered_df.head()}")
save_intermediate_output('after_filtering.feather', filtered_df)
```

---

### 7. Performance-Conscious

**Principle:** Handle large datasets efficiently (millions of records).

**Design Implications:**
- Streaming/chunked processing
- Lazy evaluation where possible
- Memory profiling and optimization
- Parallel processing for independent operations

**Example:**
```python
# Process in chunks for memory efficiency
for chunk in read_feather_chunked(source_file, chunk_size=100000):
    transformed_chunk = transform(chunk)
    append_to_output(transformed_chunk)
```

---

### 8. Documentation as Code

**Principle:** Transformations should be self-documenting.

**Design Implications:**
- Descriptive naming (functions, variables, configs)
- Inline documentation in configs
- Auto-generated transformation reports
- Visual pipeline diagrams

**Example:**
```yaml
# Self-documenting configuration
transformations:
  - name: "Normalize customer names to uppercase"
    field: customer_name
    operation: uppercase
    reason: "ERPNext requires uppercase for consistency"
```

---

## Suggested Framework Architecture

### High-Level Components

```
┌─────────────────────────────────────────────────────┐
│         Transformation Framework                     │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌────────────────┐  ┌──────────────────┐           │
│  │ Configuration  │  │  ERPNext Schema  │           │
│  │   Loader       │  │    Validator     │           │
│  └────────┬───────┘  └────────┬─────────┘           │
│           │                   │                      │
│           ▼                   ▼                      │
│  ┌─────────────────────────────────────┐            │
│  │      Transformation Engine           │            │
│  │  - Field Mapping                     │            │
│  │  - Filtering                         │            │
│  │  - Calculations                      │            │
│  │  - Child Table Building              │            │
│  │  - Validation                        │            │
│  └─────────────────┬───────────────────┘            │
│                    │                                 │
│                    ▼                                 │
│  ┌─────────────────────────────────────┐            │
│  │    Execution Pipeline                │            │
│  │  - Stage Orchestration               │            │
│  │  - Error Handling                    │            │
│  │  - Progress Tracking                 │            │
│  └─────────────────┬───────────────────┘            │
│                    │                                 │
│                    ▼                                 │
│  ┌─────────────────────────────────────┐            │
│  │     Validation & Output              │            │
│  │  - ERPNext Validation                │            │
│  │  - Error Reporting                   │            │
│  │  - Output Generation                 │            │
│  └─────────────────────────────────────┘            │
│                                                       │
└─────────────────────────────────────────────────────┘
```

### Usage Pattern

```python
# Client-specific configuration (external file)
config = TransformerConfig.load('client_xyz_customers.yaml')

# Source data (any legacy system)
source_data = read_feather('legacy_customer_data.feather')

# Framework execution
transformer = Transformer(config)
result = transformer.transform(source_data)

# Validation against ERPNext
validator = ERPNextValidator(doctype='Customer')
errors = validator.validate(result)

if errors:
    print(f"Validation errors: {errors}")
else:
    result.to_feather('erpnext_customers.feather')
```

---

## Summary of Universal Patterns

| Pattern | Frequency | Source-Agnostic? | ERPNext-Specific? | Priority |
|---------|-----------|------------------|-------------------|----------|
| Field Mapping | Very High | ✅ Yes | ✅ Yes | **Critical** |
| Filtering | Very High | ✅ Yes | ❌ No | **Critical** |
| Merging | Very High | ✅ Yes | ❌ No | **Critical** |
| Code Mapping | High | ✅ Yes | ✅ Yes | **High** |
| Child Tables | High | ✅ Yes | ✅ Yes | **Critical** |
| Conditional Logic | High | ✅ Yes | ❌ No | **High** |
| Date Handling | High | ✅ Yes | ✅ Yes | **High** |
| Calculations | Medium | ✅ Yes | ❌ No | **Medium** |
| Aggregation | Medium | ✅ Yes | ❌ No | **High** |
| Text Processing | Medium | ✅ Yes | ❌ No | **Medium** |
| Deduplication | Medium | ✅ Yes | ❌ No | **High** |
| Data Quality | High | ✅ Yes | ❌ No | **High** |
| Doc Naming | Medium | ✅ Yes | ✅ Yes | **Medium** |
| Reference Resolution | High | ✅ Yes | ✅ Yes | **High** |
| Defaults | High | ✅ Yes | ✅ Yes | **Medium** |

**Key:**
- ✅ **Source-Agnostic:** Pattern works regardless of legacy system
- ✅ **ERPNext-Specific:** Pattern tied to ERPNext requirements
- **Priority:** Critical = Framework MVP, High = Early feature, Medium = Later enhancement

---

## Conclusion

This framework must be built with the understanding that:

1. **Source systems vary wildly** - no assumptions about schema, structure, or quality
2. **ERPNext is the only constant** - all transformations converge on ERPNext schema
3. **Patterns are universal** - the HOW is consistent, the WHAT changes per client
4. **Configuration over code** - client-specific logic externalized, not hardcoded
5. **Flexibility is paramount** - handle unknown future source systems gracefully

A successful framework will:
- ✅ Reduce transformer development time by 80%+
- ✅ Eliminate redundant code across client projects
- ✅ Ensure consistent, high-quality ERPNext data
- ✅ Enable non-developers to configure transformations
- ✅ Scale to handle any legacy system → ERPNext migration

---

## Next Steps

1. **Define Core Framework API** - Transformer interface, Pipeline pattern
2. **Build Configuration Schema** - YAML/JSON format for transformation rules
3. **Implement Pattern Libraries** - Reusable functions for each pattern
4. **Create ERPNext Validator** - Schema-aware validation engine
5. **Develop CLI Tool** - Generate transformer templates, run transformations
6. **Build Test Framework** - Unit/integration testing for transformers
7. **Document Best Practices** - Migration playbook using framework

---

*This framework is designed to handle migrations from **any** legacy ERP system (Quantus, E2, Pacific, Sage, or unknown future systems) to ERPNext. The patterns identified are universal data manipulation techniques that apply regardless of source system specifics.*
