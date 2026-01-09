# Project Requirements Document

## Project Purpose

Build a developer-friendly framework to simplify and accelerate the creation of data transformation scripts (transformers) for ERP migration projects. This framework will eliminate repetitive coding patterns, provide interactive development feedback, and maintain flexibility for custom business logic.

## Overview

Data migration transformers currently require extensive manual coding for repetitive patterns (filtering, mapping, child table building, etc.). This project creates:

1. **A Development Tool**: Interactive interface to build transformations via configuration
2. **A Runtime Framework**: Execute configured transformations with high performance
3. **Live Feedback System**: Real-time preview during transformer development

**Target Users**: Data migration developers building transformers for client ERP migrations

**Current Transformer Pattern**:
```python
def transform(sources: dict[str, DataFrame]) -> DataFrame:
    # Manual coding of filters, merges, mappings, child tables, etc.
    return result_df
```

**Goal**: Make building these transformers 10x faster while maintaining code quality and flexibility.

## Developer Workflow

### Current Workflow (Manual Coding)
1. Create new transformer file (TFRM-XXXXXXXX.py) 
2. Write `transform(sources)` function manually
3. Code 50-200 lines of repetitive pandas operations (filters, merges, mappings, child tables)
4. Test by running full transformer on complete dataset
5. Debug by adding print statements and re-running
6. Repeat steps 4-5 many times
7. **Time**: 2-8 hours per transformer

### Proposed Workflow (Framework-Assisted)
1. Create new transformer configuration
2. Define transformation steps via config/DSL:
   - Select source tables
   - Add filters, mappings, merges
   - Configure child table rules
   - Set field transformations
3. **Live preview**: See results after each step (10 sample rows)
4. Iterate quickly with immediate feedback
5. Export final configuration or generated Python code
6. Run production execution on full dataset
7. **Time**: 20-60 minutes per transformer

## Key Features

### 1. Configuration-Based Transformation Building

Developers define transformations using a declarative configuration format instead of writing repetitive code.

**Configuration Format Options** (decision pending):
- **Option A**: YAML-based configuration for common patterns
- **Option B**: Python-based DSL (Domain Specific Language) with helper functions
- **Hybrid Approach**: YAML for simple patterns + Python for complex custom logic

**Example Use Case** (from TFRM-00000004.py):
```python
# CURRENT: Manual code (20+ lines)
customer_df['CleanMailPhone'] = customer_df['MailPhone'].str.replace(r'\D', '', regex=True)
customer_df['CleanShipPhone'] = customer_df['ShipPhone'].str.replace(r'\D', '', regex=True)
# ... repeat for each field

# DESIRED: Configuration-driven
string_operations:
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
```

### 2. Live Display Mode (Interactive Development)

When building transformers, developers get **real-time feedback** as they add transformation steps.

**Core Capabilities**:
- **Step-by-step execution**: Run transformations incrementally, see results after each step
- **Data preview**: Display ~10 sample rows after each transformation
- **Smart sampling**: Configure which rows to preview (filters, sampling logic)
- **Error feedback**: Show exceptions immediately with context
- **Visual progress**: Terminal UI with current step, progress indicators, scrollable history
- **Large dataset handling**: Efficient preview without loading entire datasets into memory

**Example Workflow**:
1. Developer adds a filter step in config
2. Framework executes filter on sample data
3. Terminal shows: "✓ Filter applied: Status in ['Active', 'Pending']"
4. Preview displays 10 matching rows
5. Developer adds next step, sees immediate feedback
6. Repeat until transformation is complete

**Benefits**:
- Catch errors early (before processing full dataset)
- Understand data shape at each step
- Iterate quickly without waiting for full execution
- Visual confirmation of transformations working correctly

### 3. Production Execution Mode

Run transformers on full datasets for actual migrations (no interactive preview).

**Characteristics**:
- **No live display**: Headless execution for production environments
- **Standard logging**: Row counts, processing steps, timing metrics, data types
- **Full dataset processing**: Execute on complete data (millions of rows)
- **Performance optimized**: Batch processing, memory management
- **Error handling**: Comprehensive error logging and rollback capabilities

**Use Case**: Final migration execution after transformer is built and tested

### 4. Parent-Child Table Support

A **critical pattern** in ERP migrations: building child records (line items, contacts, addresses, etc.) from parent records.

**Example from TFRM-00000004.py**:
```python
# Building phone_nos child table (one-to-many)
def create_mailcontact_phone(row):
    phone_data = []
    if row["MailPhone"] != "":
        phone_data.append({"phone": row["MailPhone"], "is_primary_phone": 1})
    if row["MailCellPhone"] != "" and row["MailCellPhone"] != row["MailPhone"]:
        phone_data.append({"phone": row["MailCellPhone"], "is_primary_mobile_no": 1})
    return phone_data

df["phone_nos"] = df.apply(create_mailcontact_phone, axis=1)
```

**Framework Requirements**:
- **Declarative child table building**: Define child structure via configuration
- **Conditional child records**: Generate child rows based on conditions
- **Aggregation support**: Group parent rows to build child aggregations (from TFRM-COMPREHENSIVE-DEMO.py:build_advanced_child_tables)
- **Display mode filtering**: Show only relevant child samples in preview
- **Production mode**: Process all child relationships at full scale

**Common Child Table Patterns to Support**:
1. Simple child lists (items, contacts, addresses)
2. Conditional child records (only if amount > 0, only if email exists)
3. Aggregated child tables (group tax amounts by account_head)
4. Deduplication in child lists (remove duplicate phone numbers)
5. Multi-step child building with lookups

### 5. Data Library Foundation
- **Primary**: pandas (current codebase standard, maximum flexibility)
- **Future consideration**: Polars for performance (10-100x faster on large datasets)
- **Initial scope**: pandas-based implementation
- **Design**: Abstract data operations to allow future library swapping

---

# Required Data Manipulation Capabilities

The framework must support all common data transformation patterns used in ERP migrations. These patterns are documented in `TFRM-COMPREHENSIVE-DEMO.py` and used extensively in production transformers like `TFRM-00000004.py`.

**Implementation Strategy**: Each technique below should be:
1. Callable via configuration (declarative)
2. Available as a helper function (for custom logic)
3. Documented with examples
4. Tested with real migration data

**Priority**: Implement high-frequency patterns first (filtering, mapping, child tables), then expand to cover all 12 categories.

## 1. Filtering & Selection Techniques
- Boolean mask filtering with isin()
- Date filtering with comparison operators  
- Comparison operators (>, <, ==, !=)
- Complex boolean logic with & (AND) and | (OR)
- Null/NaN handling with notna() and isna()
- String filtering with empty checks

## 2. Merging & Joining Techniques
- LEFT JOIN - Keep all rows from left, match from right
- INNER JOIN - Keep only matching rows
- Multi-column join with multiple keys simultaneously

## 3. Mapping & Lookup Techniques
- Dictionary-based mapping with map()
- Mapping with fillna() fallback for unmapped values
- Mapping using set_index() to create lookup dictionary

## 4. Field Operations
- Simple field copy
- Set constant value for all rows
- Field concatenation with str.cat()
- Concatenate multiple columns with separators
- Conditional field assignment with np.where()

## 5. Date Handling Techniques
- Date parsing with pd.to_datetime()
- Date formatting with strftime()
- Handle special dates (9999) with apply() and lambda
- Extract date components with dt.year, dt.month

## 6. Aggregation & Grouping Techniques
- groupby() with single aggregation function
- groupby() with multiple aggregation functions
- groupby().apply() with string concatenation
- groupby().apply() collecting into lists

## 7. Type Conversion Techniques
- Convert to numeric with pd.to_numeric()
- Convert to integer with astype()
- Convert to string with astype()
- Convert to boolean with astype(bool)

## 8. Conditional Logic Techniques
- np.where() for simple if-else conditions
- Nested np.where() for multiple conditions
- apply() with lambda for complex conditions
- .loc[] with boolean mask for assignment

## 9. String Operations Techniques
- String case conversion with .str.upper() and .str.lower()
- String trimming with .str.strip()
- String replacement with .str.replace()
- Regular expression replacement
- String splitting with .str.split()
- Extract name from email with chained .str operations

## 10. Building Child Tables Techniques
- Create list column with dictionary entries
- Build child tables with conditional logic
- Build child tables from aggregated data
- Advanced child table building with complex aggregation including:
  - Multi-step grouping and aggregation
  - Tax consolidation with defaultdict
  - Sales team allocation percentages
  - Deduplication using JSON hashing
  - Message concatenation

## 11. Deduplication Techniques
- drop_duplicates() with single column
- drop_duplicates() with multiple columns
- Remove duplicates using groupby() and transform()
- Deduplicate list of dictionaries using set and JSON

## 12. Complex Transformation Techniques
- Chained transformations combining multiple techniques
- Apply transformations with error handling

The framework should support all these techniques with both the live display mode for building and the normal execution mode for full data processing.

---

# Real-World Examples from Production Code

## Example 1: Contact Building (TFRM-00000004.py)

**Business Logic**: Combine customer mail contacts and ship contacts into unified contact records

**Current Implementation** (manual, ~100 lines):
- Extract and clean phone numbers (regex_replace)
- Build phone_nos child table (list of dicts)
- Build email_ids child table
- Build links child table
- Group and aggregate contacts
- Deduplicate phone numbers
- Determine primary contact

**Framework Goal**: Define this transformation in 20-30 lines of config

## Example 2: Advanced Child Table Building (TFRM-COMPREHENSIVE-DEMO.py:572)

**Business Logic**: Build complex child tables with multi-step aggregation

**Patterns demonstrated**:
- Message concatenation with groupby
- Item building with conversion rate lookups
- Tax consolidation using defaultdict
- Deduplication via JSON hashing
- Sales team allocation with percentage distribution

**Framework Goal**: Provide reusable components for each pattern

---

# Design Decisions & Open Questions

## Decision 1: Configuration Format

**Options**:
1. **YAML**: Declarative, easy to read, limited flexibility
2. **Python DSL**: Full power of Python, steeper learning curve
3. **Hybrid**: YAML for common patterns + Python escape hatch for complex logic

**Recommendation**: Start with **Hybrid approach**
- 80% of transformations via YAML
- 20% custom logic via Python functions
- Best balance of simplicity and power

**Next Step**: Create POC with 2-3 transformation types in YAML

## Decision 2: Live Display Implementation

**Options**:
1. **Terminal UI** (rich, textual): Beautiful, interactive, complex to build
2. **Simple CLI**: Line-by-line output, easy to implement, less visual
3. **Web UI**: Most flexible, requires separate server, more complex

**Recommendation**: Start with **Simple CLI** (Phase 1), upgrade to **Terminal UI** (Phase 2)

**Libraries to consider**:
- `rich`: Beautiful terminal formatting
- `textual`: Full TUI framework (if needed later)

## Decision 3: Code Generation vs Runtime Execution

**Options**:
1. **Generate Python code**: Config → Python file → Execute
2. **Runtime interpreter**: Config → Direct execution
3. **Hybrid**: Generate code for production, interpret for development

**Recommendation**: **Hybrid approach**
- Development mode: Interpret config directly (faster iteration)
- Production mode: Generate optimized Python code (better performance, debugging)

## Open Questions

1. **Integration with existing codebase**: How does this fit with current sga_migrator framework?
2. **Version control**: How to version transformers - config files, generated code, or both?
3. **Testing strategy**: How to test generated transformers? Unit tests for each transformation type?
4. **Backward compatibility**: Must support existing manually-written transformers
5. **Child table complexity limit**: When is custom Python required vs config?
6. **Performance benchmarks**: What's acceptable performance overhead for config-driven approach?

---

# Implementation Considerations

## Technical Requirements

1. **Modularity**:
   - Separate concerns: config parsing, transformation engine, display layer
   - Plugin architecture for new transformation types
   - Extensible without modifying core code

2. **Performance**:
   - Development mode: Optimize for fast feedback (<1 second per step on sample data)
   - Production mode: Optimize for throughput (millions of rows)
   - Memory efficiency: Stream large datasets, avoid loading everything in memory

3. **Error Handling**:
   - Validation: Catch config errors before execution
   - User-friendly messages: "Field 'CustomerName' not found in source DAT-00001" (not KeyError)
   - Debugging support: Show which config step caused the error
   - Graceful degradation: Partial failure shouldn't crash entire transformer

4. **Testing**:
   - Unit tests for each transformation technique
   - Integration tests with real migration data samples
   - Regression tests: Ensure generated code matches manual code output
   - Performance benchmarks

5. **Documentation**:
   - Quick start guide with simple example
   - Reference guide for all transformation types
   - Migration guide: Manual code → Config conversion examples
   - Best practices: When to use config vs custom code

## Non-Functional Requirements

- **Maintainability**: Code should be clear and well-documented for team members
- **Onboarding**: New developers should build their first transformer in <30 minutes with framework
- **Flexibility**: Don't lock developers into framework - allow escape hatches
- **Backward Compatibility**: Existing transformers continue working unchanged
- **Gradual Adoption**: Team can adopt framework incrementally, one transformer at a time

---

# Success Criteria

## MVP (Minimum Viable Product)

The framework is considered "usable" when:

1. ✅ Developer can build a simple transformer (filter + merge + map) via config
2. ✅ Live preview works: Shows 10 sample rows after each step
3. ✅ Supports top 5 patterns: Filter, Merge, Map, String ops, Simple child tables
4. ✅ Generates executable Python code from config
5. ✅ At least 1 production transformer migrated to new framework
6. ✅ Documentation: Quick start guide + 3 examples
7. ✅ 50% time reduction vs manual coding (measured on real transformers)



# Next Steps (Prioritized)

## Phase 1: Research & POC (Week 1)
1. ✅ Document requirements (this document)
2. ⬜ Review existing transformers, identify top 10 most common patterns
3. ⬜ Create POC: Simple config format (YAML) for filter + merge + map
4. ⬜ Build basic config parser and transformer generator
5. ⬜ Test POC with one real transformer (TFRM-00000004.py subset)
6. ⬜ Decision: Validate config format choice with team

## Phase 2: Core Development (Week 2)
1. ⬜ Implement top 5 transformation types with config support
2. ⬜ Build live preview system (simple CLI version)
3. ⬜ Create code generator (config → Python code)
4. ⬜ Add error handling and validation
5. ⬜ Write documentation: Quick start guide + examples
6. ⬜ Migrate 1 production transformer to framework

## Phase 3: Validation & Iteration (Ongoing)
1. ⬜ Team testing: 2-3 developers build transformers with framework
2. ⬜ Gather feedback, identify pain points
3. ⬜ Iterate on UX and add missing features
4. ⬜ Measure time savings vs manual approach
5. ⬜ Expand transformation library based on real usage

---

# Appendix

## Reference Files
- `TFRM-COMPREHENSIVE-DEMO.py`: Complete reference of all transformation patterns
- `TFRM-00000004.py`: Production transformer example (contacts)
- `jira_task.md`: Business context and requirements (MAIN-377)

## Glossary
- **Transformer**: Python script that transforms source data to target format
- **Source**: Input DataFrame from legacy system (DAT-XXXXXXXX)
- **Child Table**: One-to-many relationship data stored as list of dicts in DataFrame column
- **Live Preview**: Interactive development mode showing sample data after each transformation
- **Config**: Declarative definition of transformation steps (YAML or Python DSL)