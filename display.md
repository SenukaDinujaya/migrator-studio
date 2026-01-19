# Migrator Studio: Live Preview & Display System

## Overview

This document specifies the implementation of a **live preview and display system** for Migrator Studio. The goal is to provide developers with real-time visual feedback as they build transformer scripts, showing how each operation affects the data.

---

## Problem Statement

Currently, when developing transformer scripts:
1. Developers write OP function calls blindly without seeing intermediate results
2. There's no visual feedback showing which columns were affected
3. Debugging requires manual print statements or stepping through code
4. The feedback loop between writing code and seeing results is slow

**Desired State:** A live preview system where each OP function automatically displays its effect on the data, showing the relevant columns with proper context.

---

## Architecture Requirements

### 1. Build Mode with Sample Limiting

The system should support two modes:

| Mode | Behavior |
|------|----------|
| **Build Mode** | Process only `sample_size` rows (default: 10) for fast iteration |
| **Production Mode** | Process the entire dataset |

**Current Implementation:**
- `BuildSession(sample=N)` already exists in `session.py`
- `load_source()` respects sample size in build mode
- Operations are tracked via `@tracked` decorator

**Required Enhancement:**
- Ensure sample limiting is consistently applied throughout the pipeline
- Add a `SAMPLE_SIZE` configuration constant that can be set globally
- Support deferred sample application (apply sampling only when explicitly requested)

```python
# Example API
from migrator_studio import configure

configure(
    sample_size=10,           # Default sample size for build mode
    defer_sampling=False      # If True, don't sample until explicitly called
)
```

---

### 2. Automatic Column Display After Operations

When an OP function executes in build mode, it should automatically display:

1. **Index columns** - Defined by `INDEX_COLS` constant or DataFrame's current index
2. **Affected columns** - The column(s) that the operation modified/created

**Display Rules:**

| Operation Type | Columns to Display |
|----------------|-------------------|
| `set_value(df, "company", "X")` | INDEX_COLS + `company` |
| `copy_column(df, "A", "B")` | INDEX_COLS + `B` |
| `concat_columns(df, ["A", "B"], "C")` | INDEX_COLS + `C` |
| `rename_columns(df, {"A": "B"})` | INDEX_COLS + `B` |
| `map_dict(df, "status", {...}, target="status_text")` | INDEX_COLS + `status_text` |
| `filter_*` operations | INDEX_COLS + filtered column + row count change |
| `merge_*` operations | INDEX_COLS + all new columns from join |

**Configuration:**

```python
# At the top of transformer file
INDEX_COLS = ["CustomerID", "OrderID"]  # Columns to always show for context

# If not defined, use DataFrame's current index
```

**Display Format:**
```
[set_value] company = "Default Company"
┌──────────────┬─────────────────┐
│ CustomerID   │ company         │
├──────────────┼─────────────────┤
│ CUST-001     │ Default Company │
│ CUST-002     │ Default Company │
│ CUST-003     │ Default Company │
│ ...          │ ...             │
└──────────────┴─────────────────┘
Rows: 10 | Time: 2ms
```

---

### 3. Marimo Integration

The display system **must use [Marimo](https://marimo.io/)** as the rendering backend.

**Requirements:**

1. **Single Entry Point** - The transformer `.py` file should be runnable with Marimo directly:
   ```bash
   marimo run transformer.py
   ```

2. **Minimal Boilerplate** - The transformer file should require minimal additional code to enable preview. Ideally a single decorator or import.

3. **Automatic Cell Detection** - Each OP function call should render as a separate visual output in Marimo.

**Proposed API Options:**

**Option A: Decorator-based**
```python
from migrator_studio import transformer, load_source, set_value

@transformer(preview=True)  # Enables Marimo preview
def transform(sources):
    df = load_source("DAT-00000001")
    df = set_value(df, "company", "ACME")  # Auto-displays in Marimo
    return df
```

**Option B: Context Manager**
```python
from migrator_studio import BuildSession, load_source, set_value

with BuildSession(preview=True) as session:  # Enables Marimo preview
    df = load_source("DAT-00000001")
    df = set_value(df, "company", "ACME")  # Auto-displays in Marimo
```

**Option C: Module-level Configuration**
```python
import migrator_studio
migrator_studio.enable_preview()  # One line to enable everything

# Rest of transformer code works normally
```

**Implementation Considerations:**
- Marimo uses reactive execution - consider how this affects stateful operations
- Need to handle Marimo's cell-based execution model
- Preview should be opt-in to avoid affecting production runs
- Consider using `mo.output()` or similar Marimo APIs for display

---

### 4. Display Content Specifications

Each operation display should include:

#### Required Elements
- **Operation name** - Which function was called
- **Parameters** - Key parameters (column names, values)
- **Data preview** - Table showing INDEX_COLS + affected columns
- **Row count** - Current row count, and delta if rows changed

#### Optional Elements (Configurable)
- **Execution time** - Milliseconds taken
- **Memory usage** - DataFrame memory footprint
- **Data types** - Column dtypes for affected columns
- **Null counts** - Number of nulls in affected columns
- **Value distribution** - For categorical columns, show value counts

**Display Verbosity Levels:**

```python
configure(display_verbosity="minimal")   # Just table preview
configure(display_verbosity="normal")    # Table + row count + time
configure(display_verbosity="detailed")  # All metrics
```

---

### 5. Operation-Specific Display Logic

Each OP function category should implement appropriate display behavior:

#### Filter Operations
Show: Rows removed, filter condition, sample of removed rows (optional)
```
[filter_isin] Status IN ['Active', 'Pending']
Rows: 100 → 75 (removed 25)
```

#### Merge Operations
Show: Join keys, rows before/after, new columns added
```
[merge_left] ON CustomerID
Rows: 100 → 100 | New columns: RegionName, RegionCode
```

#### Mapping Operations
Show: Mapping applied, unmapped values (if any)
```
[map_dict] status → status_text
Unmapped values: ['Unknown'] (5 rows) → using fallback 'Other'
```

#### Field Operations
Show: Column created/modified with sample values

#### Conditional Operations
Show: Condition breakdown (how many rows matched each condition)
```
[case] priority
  amount > 1000: 15 rows → "High"
  amount > 500:  30 rows → "Medium"
  default:       55 rows → "Low"
```

---

## Implementation Plan

### Phase 0: Proof of Concept (CRITICAL - DO THIS FIRST)

Before building anything, we must validate that our approach works with Marimo's execution model.

**Goals:**
- Understand how Marimo renders output from non-notebook Python files
- Validate that we can display output mid-execution (not just at cell end)
- Determine which API approach (decorator/context manager/module-level) is feasible
- Identify any Marimo limitations that affect our design

**Tasks:**
1. **Research Marimo's architecture**
   - How does `marimo run` execute a plain `.py` file?
   - How does Marimo's reactive model work?
   - What are the output primitives (`mo.output()`, `mo.md()`, `mo.ui.table()`, etc.)?
   - Can we emit multiple outputs from a single execution context?

2. **Build minimal prototype**
   ```python
   # prototype.py - Test if this approach works
   import marimo as mo

   def fake_op(df, msg):
       # Can we render output here, mid-function?
       mo.output(mo.ui.table(df.head()))
       return df

   df = load_data()
   df = fake_op(df, "step 1")  # Does this display?
   df = fake_op(df, "step 2")  # Does this display separately?
   ```

3. **Test with `marimo run prototype.py`**
   - Does each `fake_op` call render separately?
   - Or does Marimo batch all output to the end?
   - How do we get the "cell-per-operation" behavior we want?

4. **Document findings and decide on approach**
   - If mid-execution output works → proceed with original plan
   - If not → explore alternatives:
     - AST transformation to split code into cells
     - Code generation to create a Marimo notebook from transformer
     - Different display backend (e.g., Rich for terminal, Panel for web)

**Deliverables:**
- Working prototype demonstrating one OP function with Marimo display
- Decision document: which API approach to use
- List of Marimo constraints/limitations discovered

**Exit Criteria:**
- [ ] Can display a DataFrame table in Marimo from a plain `.py` file
- [ ] Can display multiple separate outputs (one per operation)
- [ ] Understand how to structure transformer files for Marimo compatibility
- [ ] API approach selected and validated

---

### Phase 1: Core Display Infrastructure

Only begin this phase after Phase 0 validates the approach.

**Goals:**
- Build the display rendering system independent of Marimo
- Add metadata to operations for "affected columns" tracking
- Create formatters for each operation type

**Tasks:**
1. **Extend `@tracked` decorator with display metadata**
   ```python
   @tracked(
       operation_name="set_value",
       affected_columns=lambda kwargs: [kwargs.get("column")]
   )
   def set_value(df, column, value):
       ...
   ```

2. **Create `display/` module structure**
   ```
   migrator_studio/display/
   ├── __init__.py
   ├── config.py        # INDEX_COLS, verbosity settings
   ├── renderer.py      # Core rendering logic
   ├── formatters/
   │   ├── __init__.py
   │   ├── base.py      # Base formatter class
   │   ├── filter.py    # Filter operation formatters
   │   ├── merge.py     # Merge operation formatters
   │   ├── field.py     # Field operation formatters
   │   └── ...          # One per operation category
   └── backends/
       ├── __init__.py
       ├── terminal.py  # Rich-based terminal output (fallback)
       └── marimo.py    # Marimo output (primary)
   ```

3. **Implement `INDEX_COLS` configuration**
   - Global config via `configure(index_cols=[...])`
   - Per-transformer override via module-level constant
   - Fallback to DataFrame's existing index

4. **Add affected columns metadata to all 30+ OP functions**
   - Filter ops: the filtered column
   - Field ops: the target column(s)
   - Merge ops: new columns from right table
   - Mapping ops: target column
   - etc.

5. **Create base formatter with standard output structure**
   ```python
   class OperationDisplay:
       operation_name: str
       parameters: dict
       affected_columns: list[str]
       index_columns: list[str]
       data_preview: DataFrame  # subset of rows/cols to display
       row_count_before: int
       row_count_after: int
       execution_time_ms: float
   ```

**Deliverables:**
- `display/` module with working formatters
- All OP functions annotated with affected columns metadata
- Terminal backend working (can test without Marimo)

---

### Phase 2: Marimo Integration

**Goals:**
- Connect display system to Marimo output
- Implement chosen API approach from Phase 0
- Make `marimo run transformer.py` work

**Tasks:**
1. **Implement Marimo backend** (`display/backends/marimo.py`)
   - Convert `OperationDisplay` to Marimo UI elements
   - Use `mo.ui.table()` for DataFrame preview
   - Use `mo.md()` for operation headers and stats
   - Handle Marimo's output batching if needed

2. **Implement chosen API approach**
   - If decorator: create `@transformer(preview=True)`
   - If context manager: extend `BuildSession(preview=True)`
   - If module-level: create `enable_preview()` function

3. **Auto-detect Marimo environment**
   ```python
   def is_marimo_environment():
       """Check if running inside Marimo."""
       try:
           import marimo as mo
           return mo.running_in_notebook()
       except ImportError:
           return False
   ```

4. **Wire display hooks into `@tracked` decorator**
   - After operation completes, call display renderer
   - Only display in build mode + preview enabled
   - No-op in production mode

5. **Create example transformer for testing**
   ```python
   # sample/TFRM-PREVIEW-DEMO.py
   from migrator_studio import transformer, load_source, set_value, filter_isin

   @transformer(preview=True)
   def transform(sources):
       df = load_source("DAT-00000001")
       df = filter_isin(df, "Status", ["Active"])
       df = set_value(df, "company", "ACME")
       return df
   ```

**Deliverables:**
- Working Marimo integration
- `marimo run sample/TFRM-PREVIEW-DEMO.py` displays step-by-step preview
- Auto-detection of Marimo vs terminal environment

---

### Phase 3: Polish & Configuration

**Goals:**
- Add configurability
- Enhance operation-specific displays
- Documentation and examples

**Tasks:**
1. **Implement verbosity levels**
   ```python
   configure(display_verbosity="minimal")   # Table only
   configure(display_verbosity="normal")    # Table + stats (default)
   configure(display_verbosity="detailed")  # Table + stats + dtypes + nulls
   ```

2. **Enhanced operation-specific displays**
   - Filter: show removed row count, optionally sample of removed rows
   - Merge: show join cardinality, unmatched rows warning
   - Mapping: show unmapped values warning
   - Conditional: show breakdown of rows per condition branch

3. **Add display suppression controls**
   ```python
   # Suppress display for specific operations
   df = set_value(df, "internal_flag", True, _display=False)

   # Suppress all display temporarily
   with suppress_display():
       df = intermediate_operation(df)
   ```

4. **Error and warning display styling**
   - Validation errors: red border, error icon
   - Warnings (e.g., unmapped values): yellow border, warning icon
   - Success: green checkmark

5. **History/summary view**
   - Visual version of `session.summary()`
   - Show all operations in collapsible accordion
   - Highlight operations that changed row count

6. **Documentation**
   - Update README with preview feature
   - Add examples to `sample/` directory
   - Document all configuration options

**Deliverables:**
- Full configuration system
- Enhanced formatters for all operation types
- Complete documentation
- Multiple example transformers demonstrating features

---

## Technical Constraints

1. **No changes to production behavior** - Display system must be completely inactive outside build mode
2. **Minimal transformer file changes** - Existing transformers should work with minimal modification
3. **Performance** - Display overhead should be negligible (<10ms per operation)
4. **Thread safety** - Must work correctly with context variables already in use

---

## File Structure (Proposed)

```
migrator_studio/
├── display/
│   ├── __init__.py          # Public exports
│   ├── config.py            # INDEX_COLS, verbosity, display settings
│   ├── renderer.py          # Core rendering orchestration
│   ├── models.py            # OperationDisplay dataclass
│   ├── formatters/
│   │   ├── __init__.py
│   │   ├── base.py          # BaseFormatter abstract class
│   │   ├── filter.py        # filter_*, sanitize_data
│   │   ├── merge.py         # merge_left, merge_inner, merge_outer
│   │   ├── mapping.py       # map_dict, map_lookup
│   │   ├── field.py         # set_value, copy_column, concat_columns, etc.
│   │   ├── date.py          # parse_date, format_date, extract_date_part
│   │   ├── string.py        # str_upper, str_lower, str_replace, etc.
│   │   ├── conditional.py   # where, case, fill_null, coalesce
│   │   ├── convert.py       # to_numeric, to_int, to_string, to_bool
│   │   ├── aggregate.py     # groupby_agg, groupby_concat
│   │   ├── dedup.py         # drop_duplicates, keep_max, keep_min
│   │   └── apply.py         # apply_row, apply_column, transform
│   └── backends/
│       ├── __init__.py      # Backend auto-detection
│       ├── base.py          # BaseBackend abstract class
│       ├── terminal.py      # Rich-based terminal output
│       └── marimo.py        # Marimo notebook output
```

---

## Success Criteria

1. Running `marimo run transformer.py` shows live preview of each operation
2. Each OP function displays relevant columns automatically
3. INDEX_COLS configuration works as specified
4. Display is suppressed in production mode
5. Existing transformers work without modification (preview is opt-in)

---

## Open Questions

### To Be Answered in Phase 0

1. **Marimo Execution Model:** How does `marimo run` execute a plain `.py` file? Can we emit output mid-execution, or only at the end?

2. **Multiple Outputs:** Can we render multiple separate display blocks from a single execution context? Or does Marimo batch everything?

3. **File Structure Requirements:** Does the transformer file need special Marimo markers/decorators to work with `marimo run`?

4. **API Feasibility:** Which of the three proposed APIs (decorator, context manager, module-level) is actually possible given Marimo's constraints?

### To Be Decided During Implementation

5. **Terminal Fallback:** Should display work in plain terminal (via Rich) when not in Marimo? Or is it Marimo-only?

6. **Large DataFrames:** For `display_verbosity="detailed"`, should we warn or truncate when DataFrames exceed a threshold?

7. **Error Display:** How should validation errors and warnings be styled differently from success states?

8. **History View:** Should there be a visual summary view (accordion of all operations) in addition to per-operation display?

9. **Performance Threshold:** What's acceptable display overhead? Current target is <10ms per operation - is this achievable with Marimo rendering?

---

## References

- Current session tracking: `migrator_studio/session.py`
- Operation decorator: `migrator_studio/ops/_base.py`
- Marimo documentation: https://docs.marimo.io/
- Existing transformer example: `sample/TFRM-EXAMPLE-001.py`
