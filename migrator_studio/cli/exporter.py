"""Exporter for converting Marimo notebooks back to transformer files."""
from __future__ import annotations

import ast
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class CellInfo:
    """Information about a Marimo cell."""
    name: str
    code: str
    is_step: bool = False
    step_name: Optional[str] = None


def export_to_transformer(notebook_path: Path) -> str:
    """
    Export a Marimo notebook back to a transformer Python file.

    Args:
        notebook_path: Path to the Marimo notebook

    Returns:
        Transformer Python source code
    """
    source = notebook_path.read_text()

    # Extract file docstring
    tree = ast.parse(source)
    file_docstring = ast.get_docstring(tree)

    # Clean up docstring (remove marimo-specific lines)
    if file_docstring:
        lines = file_docstring.splitlines()
        lines = [l for l in lines if "marimo edit" not in l.lower()]
        file_docstring = "\n".join(lines).strip()

    # Find all cell functions
    cells = _extract_cells(source)

    # Separate cells by type
    imports_cell = None
    config_cell = None
    step_cells = []

    for cell in cells:
        if "import marimo as mo" in cell.code:
            imports_cell = cell
        elif "configure(data_path=" in cell.code:
            config_cell = cell
        elif "## Final Result" in cell.code or "Final Result:" in cell.code:
            pass  # Skip result cell
        elif cell.is_step or ("mo.output.append" in cell.code and "### Step" in cell.code):
            step_cells.append(cell)

    # Build the transformer file
    output_lines = []

    # Add docstring
    if file_docstring:
        output_lines.append(f'"""\n{file_docstring}\n"""')

    # Add imports (extract from imports cell)
    if imports_cell:
        imports = _extract_imports(imports_cell.code)
        output_lines.append(imports)

    output_lines.append("")
    output_lines.append("")

    # Build transform function
    output_lines.append("def transform(sources: dict[str, DataFrame]) -> DataFrame:")
    output_lines.append('    """')
    output_lines.append("    Transforms data from source format to target format.")
    output_lines.append("")
    output_lines.append("    Args:")
    output_lines.append("        sources: Dictionary of source DataFrames keyed by DAT ID.")
    output_lines.append("")
    output_lines.append("    Returns:")
    output_lines.append("        Transformed DataFrame.")
    output_lines.append('    """')

    # Add sources setup from config cell (convert load_source to sources[])
    if config_cell:
        setup_code = _extract_setup_code(config_cell.code)
        if setup_code:
            output_lines.append("    # Get source dataframes")
            for line in setup_code.splitlines():
                if line.strip():
                    output_lines.append(f"    {line}")
            output_lines.append("")

    # Add each step
    for cell in step_cells:
        step_name = _extract_step_name(cell.code)
        step_code = _extract_step_code(cell.code)

        if step_name:
            output_lines.append(f'    step("{step_name}")')

        for line in step_code.splitlines():
            output_lines.append(f"    {line}")
        output_lines.append("")

    # Add return statement
    output_lines.append("    return df")
    output_lines.append("")
    output_lines.append("")

    # Add main block
    output_lines.append("# " + "=" * 77)
    output_lines.append("# DEVELOPMENT MODE - Run with: python <filename>")
    output_lines.append("# " + "=" * 77)
    output_lines.append('if __name__ == "__main__":')
    output_lines.append("    from pathlib import Path")
    output_lines.append("")
    output_lines.append("    configure(data_path=str(Path(__file__).parent))")
    output_lines.append("")
    output_lines.append("    with BuildSession(sample=10, preview=True):")
    output_lines.append("        sources = {")

    # Extract source loading from config cell
    if config_cell:
        source_loads = _extract_source_loads(config_cell.code)
        for src_id in source_loads:
            output_lines.append(f'            "{src_id}": load_source("{src_id}"),')

    output_lines.append("        }")
    output_lines.append("        result = transform(sources)")
    output_lines.append("")
    output_lines.append('    print(f"\\nTotal rows: {len(result)}")')
    output_lines.append("    print(result.to_string())")
    output_lines.append("")

    return "\n".join(output_lines)


def _extract_cells(source: str) -> list[CellInfo]:
    """Extract all cell functions from the notebook source."""
    cells = []

    # Find all @app.cell decorated functions
    pattern = r"@app\.cell\s*\ndef\s+(\w+)\([^)]*\):\s*\n((?:(?!@app\.cell)(?!if __name__|def \w+\()[\s\S])*)"

    for match in re.finditer(pattern, source):
        func_name = match.group(1)
        func_body = match.group(2)

        # Check if this is a step cell
        is_step = "### Step" in func_body

        # Extract step name if present
        step_name = None
        step_match = re.search(r'### Step \d+: ([^"]+)"', func_body)
        if step_match:
            step_name = step_match.group(1).strip()

        cells.append(CellInfo(
            name=func_name,
            code=func_body,
            is_step=is_step,
            step_name=step_name,
        ))

    return cells


def _extract_imports(cell_code: str) -> str:
    """Extract import statements from a cell, preserving indentation."""
    lines = []
    in_import = False
    base_indent = None

    for line in cell_code.splitlines():
        stripped = line.strip()

        # Start of an import statement
        if stripped.startswith("import ") or stripped.startswith("from "):
            if "marimo" in stripped:  # Skip marimo imports
                continue
            in_import = True
            # Detect base indentation
            if base_indent is None:
                base_indent = len(line) - len(line.lstrip())
            lines.append(line[base_indent:] if base_indent else line.lstrip())

        # Continuation of multi-line import
        elif in_import:
            if stripped.startswith(")"):
                lines.append(line[base_indent:] if base_indent else line.lstrip())
                in_import = False
            elif stripped.endswith(",") or stripped.endswith("("):
                lines.append(line[base_indent:] if base_indent else line.lstrip())
            elif stripped and not stripped.startswith("return"):
                lines.append(line[base_indent:] if base_indent else line.lstrip())
            else:
                in_import = False

    return "\n".join(lines)


def _extract_setup_code(cell_code: str) -> str:
    """Extract source setup code from config cell, converting load_source to sources[]."""
    lines = []

    for line in cell_code.splitlines():
        stripped = line.strip()

        # Skip configure, mo.output, and return lines
        if "configure(" in stripped or "mo.output" in stripped:
            continue
        if stripped.startswith("return "):
            continue

        # Look for load_source calls and convert to sources[]
        if "load_source" in stripped:
            # Pattern: var = load_source("DAT-00000001")
            match = re.match(r'(\w+)\s*=\s*load_source\(["\']([^"\']+)["\']\)', stripped)
            if match:
                var_name = match.group(1)
                src_id = match.group(2)
                lines.append(f'{var_name} = sources["{src_id}"]')

    return "\n".join(lines)


def _extract_step_name(cell_code: str) -> Optional[str]:
    """Extract step name from cell code."""
    match = re.search(r'### Step \d+: ([^"]+)"', cell_code)
    if match:
        # Remove any trailing description after colon
        name = match.group(1).strip()
        if ":" in name:
            name = name.split(":")[0].strip()
        return name
    return None


def _extract_step_code(cell_code: str) -> str:
    """Extract the actual step code, preserving indentation for multi-line statements."""
    lines = []
    base_indent = None
    started = False

    for line in cell_code.splitlines():
        stripped = line.strip()

        # Skip mo.output lines
        if "mo.output" in stripped:
            continue

        # Skip return statement
        if stripped.startswith("return "):
            continue

        # Skip empty lines at the beginning
        if not started and not stripped:
            continue

        # Found actual code
        if stripped:
            started = True
            # Detect base indentation from first code line
            if base_indent is None:
                base_indent = len(line) - len(line.lstrip())

            # Remove base indentation to normalize
            if base_indent and len(line) >= base_indent:
                lines.append(line[base_indent:])
            else:
                lines.append(line.lstrip())
        elif started:
            # Preserve empty lines within code blocks
            lines.append("")

    # Remove trailing empty lines
    while lines and not lines[-1].strip():
        lines.pop()

    return "\n".join(lines)


def _extract_source_loads(cell_code: str) -> list[str]:
    """Extract source IDs from load_source calls."""
    source_ids = []
    pattern = r'load_source\(["\']([^"\']+)["\']\)'

    for match in re.finditer(pattern, cell_code):
        source_ids.append(match.group(1))

    return source_ids
