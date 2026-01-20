# Marimo Implementation Archive

This document archives the marimo implementation that was removed from the Migrator Studio project.

---

## Architecture Overview

The marimo implementation consisted of two main systems:

1. **CLI-Based Notebook Generation** - Transform Python transformers into Marimo notebooks
2. **Runtime Display System** - Auto-detect environment and display operation results during execution

### File Structure

```
migrator_studio/
├── cli/
│   ├── main.py         # CLI entry point (preview/export commands)
│   ├── parser.py       # Transformer file parser
│   ├── generator.py    # Marimo notebook generator
│   ├── exporter.py     # Convert notebooks back to transformers
│   └── __init__.py
├── display/
│   ├── __init__.py     # Public display API
│   ├── config.py       # DisplayConfig dataclass
│   ├── models.py       # OperationDisplay dataclass
│   ├── renderer.py     # Core rendering orchestration
│   └── backends/
│       ├── __init__.py # Backend auto-detection
│       ├── base.py     # BaseBackend abstract class
│       ├── marimo.py   # MarimoBackend implementation
│       └── terminal.py # TerminalBackend implementation
├── session.py          # BuildSession with preview support
└── _tracking.py        # SessionTracker with display_callback
```

---

## Integration Points

### 1. session.py (lines 77-87)
```python
if self.preview:
    if self._display_callback:
        display_callback = self._display_callback
    else:
        from .display import DisplayConfig, create_display_callback
        config = DisplayConfig(
            verbosity=self.verbosity,
            index_cols=self.index_cols,
            max_rows=self.sample,
        )
        display_callback = create_display_callback(config)
```

### 2. _tracking.py (lines 56-68)
```python
if self.display_callback and result_df is not None and not suppress_display:
    from .display.models import OperationDisplay
    display = OperationDisplay(
        operation_name=operation,
        data_preview=result_df.head(self.sample_size),
        rows_before=rows_before,
        rows_after=rows_after,
        affected_columns=cols,
        duration_ms=duration_ms,
        params=params,
    )
    self.display_callback(display)
```

### 3. pyproject.toml (lines 24-27)
```toml
preview = [
    "marimo>=0.8.0",
    "rich>=13.0.0",
]
```

---

## CLI Commands

```bash
# Generate Marimo notebook from transformer
migrator preview transformers/customer.py
# Output: .marimo/customer.marimo.py

# Export notebook back to transformer
migrator export .marimo/customer.marimo.py
```

---

## CLI Module Details

### main.py
- `MARIMO_DIR = ".marimo"` - Default subdirectory for notebooks
- `cmd_preview(args)` - Generate marimo notebook from transformer
- `cmd_export(args)` - Export notebook back to transformer
- `main()` - CLI entry point with subparsers for preview/export

### parser.py
- `Step` dataclass - Represents a single step (name, code, line_start, line_end, description)
- `TransformerInfo` dataclass - Parsed transformer info (imports, sources_setup, steps, final_code, transform_args, file_docstring, helper_functions)
- `parse_transformer(file_path)` - Parse transformer and extract steps using AST
- `dedent_code(code)` - Remove common leading indentation

### generator.py
- `generate_marimo_notebook(info, output_path, data_path)` - Main generation function
- `_generate_imports_cell(info)` - Create imports cell with marimo
- `_generate_config_cell(info, data_path)` - Create config/source loading cell
- `_generate_step_cell(step, index, info)` - Create cell for each step
- `_generate_result_cell(info)` - Create final result display cell
- `_extract_imported_names(imports)` - Parse import statements
- `_extract_source_ids(setup_code)` - Find source loading patterns
- `_detect_dependencies(code)` - Identify variable dependencies
- `_extract_op_functions(code, imports)` - Find migrator_studio operations used
- `_assemble_notebook(info, cells)` - Combine cells into complete notebook

### exporter.py
- `CellInfo` dataclass - Information about a Marimo cell
- `export_to_transformer(notebook_path)` - Main export function
- `_extract_cells(source)` - Parse @app.cell decorated functions
- `_extract_imports(cell_code)` - Extract imports, filtering marimo
- `_extract_setup_code(cell_code)` - Convert load_source() to sources[]
- `_extract_step_code(cell_code)` - Extract step logic, remove mo.output
- `_extract_step_name(cell_code)` - Extract step name from cell
- `_extract_source_loads(cell_code)` - Extract source IDs

---

## Display Module Details

### config.py
```python
@dataclass
class DisplayConfig:
    verbosity: Literal["minimal", "normal", "detailed"] = "normal"
    index_cols: list[str] = field(default_factory=list)
    max_rows: int = 10
    show_affected_only: bool = True
```

### models.py
```python
@dataclass
class OperationDisplay:
    operation_name: str
    data_preview: pd.DataFrame
    rows_before: int
    rows_after: int
    affected_columns: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    params: Optional[dict] = None
```

### renderer.py
- `Renderer` class - Orchestrates display rendering
  - `render(display)` - Prepare preview and delegate to backend
  - `_prepare_preview(display)` - Filter columns/rows based on config
  - `_get_columns_to_show(df, affected_columns)` - Determine columns to display
- `create_display_callback(config, backend)` - Factory function for SessionTracker

### backends/__init__.py
- `_is_marimo_runtime()` - Check if running inside Marimo notebook
- `get_backend()` - Auto-detect and return appropriate backend

### backends/base.py
```python
class BaseBackend:
    def render(self, display, config) -> None: ...
    def render_separator(self) -> None: ...
```

### backends/marimo.py
- Uses `mo.output.append()` for stacked displays
- Uses `mo.md()` for headers and stats
- Uses `mo.ui.table()` for DataFrame preview
- `_format_stats()` - Format statistics based on verbosity

### backends/terminal.py
- Falls back to plain print if Rich unavailable
- `_render_rich()` - Rich formatting (Panel, Table)
- `_render_plain()` - Plain text fallback
- `render_separator()` - Draw line separator

---

## Generated Notebook Structure

A generated .marimo.py notebook contains:

1. **Imports cell** - All original imports + marimo + Path
2. **Config cell** - configure() + load_source() calls
3. **Step cells** (one per step) - mo.output displays + step code + table preview
4. **Result cell** - Final result display

---

## Sample Notebook Location

The sample notebook was located at: `sample/.marimo/TFRM-EXAMPLE-001.marimo.py`

---

*Archived on: 2026-01-20*
