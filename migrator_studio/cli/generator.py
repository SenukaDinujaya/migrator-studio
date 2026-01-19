"""Generator for Marimo notebooks from parsed transformer info."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .parser import TransformerInfo, Step


def generate_marimo_notebook(
    info: TransformerInfo,
    output_path: Path,
    data_path: Optional[str] = None,
) -> str:
    """
    Generate a Marimo notebook from parsed transformer info.

    Args:
        info: Parsed transformer information
        output_path: Path where the notebook will be saved
        data_path: Path to data directory (for configure())

    Returns:
        Generated notebook source code
    """
    cells = []

    # Cell 0: Imports
    imports_cell = _generate_imports_cell(info)
    cells.append(imports_cell)

    # Cell 1: Configuration and source loading
    config_cell = _generate_config_cell(info, data_path)
    cells.append(config_cell)

    # Cell 2+: Each step as a separate cell
    for i, step in enumerate(info.steps):
        step_cell = _generate_step_cell(step, i, info)
        cells.append(step_cell)

    # Final cell: Result display
    result_cell = _generate_result_cell(info)
    cells.append(result_cell)

    # Assemble the notebook
    notebook = _assemble_notebook(info, cells)

    return notebook


def _generate_imports_cell(info: TransformerInfo) -> str:
    """Generate the imports cell."""
    imports = info.imports

    cell = '''@app.cell
def _():
    import marimo as mo
    from pathlib import Path
'''

    # Add the original imports (indented)
    for line in imports.splitlines():
        if line.strip():
            cell += f"    {line}\n"

    # Extract all imported names
    imported_names = _extract_imported_names(imports)
    imported_names.update(["mo", "Path"])

    # Return all imported names so they're available in other cells
    return_names = ", ".join(sorted(imported_names))
    cell += f"    return {return_names}\n"

    return cell


def _extract_imported_names(imports: str) -> set[str]:
    """Extract names that are imported from import statements."""
    names = set()
    in_multiline = False

    for line in imports.splitlines():
        line = line.strip()

        # Handle multiline imports
        if in_multiline:
            if ")" in line:
                in_multiline = False
            # Extract names from this line
            for part in line.replace(")", "").replace(",", " ").split():
                if part and not part.startswith("#"):
                    names.add(part)
            continue

        if line.startswith("from ") and " import " in line:
            import_part = line.split(" import ")[1]

            if "(" in import_part and ")" not in import_part:
                in_multiline = True
                import_part = import_part.replace("(", "")

            import_part = import_part.replace("(", "").replace(")", "").replace(",", " ")
            for name in import_part.split():
                name = name.strip()
                if " as " in name:
                    name = name.split(" as ")[1].strip()
                if name and not name.startswith("#"):
                    names.add(name)

        elif line.startswith("import "):
            import_part = line[7:]
            for name in import_part.split(","):
                name = name.strip()
                if " as " in name:
                    name = name.split(" as ")[1].strip()
                else:
                    name = name.split(".")[0]
                if name:
                    names.add(name)

    return names


def _generate_config_cell(info: TransformerInfo, data_path: Optional[str]) -> str:
    """Generate the configuration and source loading cell."""
    data_path_str = data_path or 'str(Path(__file__).parent)'

    # Extract source IDs from the setup code
    source_ids = _extract_source_ids(info.sources_setup)

    cell = f'''@app.cell
def _(configure, load_source, mo, Path):
    # Configure data path
    configure(data_path={data_path_str})

    mo.output.append(mo.md("## Loading Sources"))

    # Load source dataframes
'''

    # Add load_source calls
    var_names = []
    for src_id, var_name in source_ids:
        cell += f'    {var_name} = load_source("{src_id}")\n'
        var_names.append(var_name)

    # If we found source assignments but no load_source calls, add them
    if not source_ids and info.sources_setup:
        # Parse the setup to find variable names
        for line in info.sources_setup.splitlines():
            if "sources[" in line and "=" in line:
                # Extract variable name
                var_name = line.split("=")[0].strip()
                # Extract source ID
                import re
                match = re.search(r'sources\["([^"]+)"\]', line)
                if match:
                    src_id = match.group(1)
                    cell += f'    {var_name} = load_source("{src_id}")\n'
                    var_names.append(var_name)

    cell += "\n"

    # Show loaded sources
    for var in var_names:
        cell += f'    mo.output.append(mo.md(f"**{var}**: {{len({var})}} rows"))\n'

    cell += "\n"

    # Return variables
    return_names = ", ".join(["mo"] + var_names)
    cell += f"    return {return_names}\n"

    return cell


def _extract_source_ids(setup_code: str) -> list[tuple[str, str]]:
    """Extract (source_id, variable_name) pairs from setup code."""
    import re
    results = []

    for line in setup_code.splitlines():
        # Pattern: var = sources["DAT-00000001"]
        match = re.match(r'\s*(\w+)\s*=\s*sources\["([^"]+)"\]', line)
        if match:
            var_name = match.group(1)
            src_id = match.group(2)
            results.append((src_id, var_name))

    return results


def _generate_step_cell(step: Step, index: int, info: TransformerInfo) -> str:
    """Generate a cell for a single step."""
    # Determine dependencies for this cell
    deps = ["mo"]

    # First step needs source dataframes used in the code
    if index == 0:
        source_ids = _extract_source_ids(info.sources_setup)
        for _, var_name in source_ids:
            # Only add if actually used in this step's code
            if var_name in step.code:
                deps.append(var_name)
    else:
        # Other steps need df from previous cell
        deps.append("df")

    # Check for additional dependencies in the code (lookup tables, etc.)
    additional_deps = _detect_dependencies(step.code)
    for dep in additional_deps:
        if dep not in deps:
            deps.append(dep)

    # Also need the OP functions used in this cell
    op_funcs = _extract_op_functions(step.code, info.imports)
    deps.extend(op_funcs)

    # Build cell header
    deps_str = ", ".join(sorted(set(deps)))
    cell = f"@app.cell\ndef _({deps_str}):\n"

    # Add step header
    description = f": {step.description}" if step.description else ""
    cell += f'    mo.output.append(mo.md("### Step {index + 1}: {step.name}{description}"))\n\n'

    # Add the step code
    for line in step.code.splitlines():
        if line.strip():
            cell += f"    {line}\n"
        else:
            cell += "\n"

    # Display the df after this step
    cell += "\n    mo.output.append(mo.ui.table(df.head(10)))\n"
    cell += '    mo.output.append(mo.md(f"Rows: {len(df)}"))\n'

    # Return df
    cell += "    return df,\n"

    return cell


def _detect_dependencies(code: str) -> list[str]:
    """Detect variable dependencies in code."""
    import re
    deps = []

    # Common dataframes that might be used
    common_vars = ["customers", "regions", "statuses", "lookup", "sources",
                   "status_map", "lookup_df"]
    for var_name in common_vars:
        # Check if variable is used but not assigned
        if re.search(rf"\b{var_name}\b", code):
            # Check if it's not on the left side of an assignment
            if not re.search(rf"^\s*{var_name}\s*=", code, re.MULTILINE):
                deps.append(var_name)

    return deps


def _extract_op_functions(code: str, imports: str) -> list[str]:
    """Extract OP function names used in the code."""
    # Get all imported function names from migrator_studio
    imported_funcs = set()
    in_migrator_import = False

    for line in imports.splitlines():
        if "from migrator_studio import" in line:
            in_migrator_import = True
        if in_migrator_import:
            # Extract function names
            for part in line.replace("(", "").replace(")", "").replace(",", " ").split():
                if part and part not in ["from", "migrator_studio", "import"]:
                    imported_funcs.add(part)
            if ")" in line or (not line.strip().endswith(",") and "(" not in line):
                in_migrator_import = False

    # Find which ones are used in this code
    used = []
    for func in imported_funcs:
        if f"{func}(" in code:
            used.append(func)

    return used


def _generate_result_cell(info: TransformerInfo) -> str:
    """Generate the final result cell."""
    cell = '''@app.cell
def _(df, mo):
    mo.output.append(mo.md("---"))
    mo.output.append(mo.md(f"## Final Result: {len(df)} rows"))
    mo.output.append(mo.ui.table(df))
    return df,
'''
    return cell


def _assemble_notebook(info: TransformerInfo, cells: list[str]) -> str:
    """Assemble the complete notebook."""
    # File header
    header = '"""'
    if info.file_docstring:
        header += f"\n{info.file_docstring}\n"
    else:
        header += "\nGenerated Marimo notebook for transformer preview.\n"
    header += '\nRun with: marimo edit <filename>\n"""\n'

    notebook = header
    notebook += "import marimo\n\n"
    notebook += "app = marimo.App()\n\n"

    # Add all cells
    for cell in cells:
        notebook += cell + "\n\n"

    # Add main block
    notebook += 'if __name__ == "__main__":\n'
    notebook += "    app.run()\n"

    return notebook
