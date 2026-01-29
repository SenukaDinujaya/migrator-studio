"""Sync notebook changes back to transformer file."""
from __future__ import annotations

import re
import shutil
from pathlib import Path


def _extract_cells(notebook_source: str) -> list[dict[str, str]]:
    """Extract cell source code from a Marimo notebook."""
    cells = []
    # Match @app.cell followed by def __(...): and the indented body
    pattern = re.compile(
        r'@app\.cell\s*\n'
        r'def __\(([^)]*)\):\s*\n'
        r'((?:    .+\n?)+)',
    )
    for match in pattern.finditer(notebook_source):
        deps = match.group(1).strip()
        body = match.group(2)
        # Dedent body by 4 spaces
        lines = []
        for line in body.splitlines():
            if line.startswith("    "):
                lines.append(line[4:])
            else:
                lines.append(line)
        cells.append({
            "deps": deps,
            "body": "\n".join(lines).rstrip(),
        })
    return cells


def _is_step_cell(cell: dict[str, str]) -> tuple[bool, str]:
    """Check if a cell is a step cell. Returns (is_step, step_name)."""
    match = re.match(r'^# Step \d+: (.+)$', cell["body"], re.MULTILINE)
    if match:
        return True, match.group(1)
    return False, ""


def _reverse_variable_rename(code: str, cell_index: int) -> str:
    """Reverse the df_N variable renaming back to df."""
    # Replace df_N = ... with df = ...
    code = re.sub(rf'\bdf_{cell_index}\b', 'df', code)
    return code


def _extract_step_code(cell: dict[str, str], cell_index: int) -> str:
    """Extract transformation code from a step cell."""
    lines = cell["body"].splitlines()
    code_lines = []
    for line in lines:
        # Skip the step comment header
        if re.match(r'^# Step \d+:', line):
            continue
        # Skip bare variable references (display lines like `df_3`)
        if re.match(r'^df_\d+$', line.strip()):
            continue
        # Skip return statements
        if re.match(r'^return\s', line.strip()):
            continue
        code_lines.append(line)

    code = "\n".join(code_lines).strip()
    return _reverse_variable_rename(code, cell_index)


def sync_notebook(notebook_path: Path, transformer_path: Path) -> None:
    """
    Sync changes from a Marimo notebook back to the transformer file.

    Reads the notebook, extracts step cells, and rebuilds the transformer's
    transform() function while preserving imports and SOURCES.

    Args:
        notebook_path: Path to the .nb.py Marimo notebook.
        transformer_path: Path to the transformer .py file to update.
    """
    notebook_source = notebook_path.read_text()
    transformer_source = transformer_path.read_text()

    cells = _extract_cells(notebook_source)
    if not cells:
        raise ValueError("No cells found in notebook.")

    # Extract step cells and their code
    steps: list[tuple[str, str]] = []
    step_cell_start = None
    for i, cell in enumerate(cells):
        is_step, step_name = _is_step_cell(cell)
        if is_step:
            if step_cell_start is None:
                step_cell_start = i
            step_code = _extract_step_code(cell, i - (step_cell_start or 0) + 1)
            steps.append((step_name, step_code))

    if not steps:
        raise ValueError("No step cells found in notebook.")

    # Rebuild the transform function body
    # Extract everything before 'def transform' from the original
    transform_match = re.search(
        r'^(def transform\([^)]*\)[^:]*:)\s*\n',
        transformer_source,
        re.MULTILINE,
    )
    if not transform_match:
        raise ValueError("Could not find transform() function in transformer.")

    pre_transform = transformer_source[:transform_match.start()]
    transform_sig = transform_match.group(1)

    # Extract the original docstring if present
    post_sig = transformer_source[transform_match.end():]
    docstring = ""
    ds_match = re.match(r'(\s+""".*?""")\s*\n', post_sig, re.DOTALL)
    if ds_match:
        docstring = ds_match.group(1) + "\n"

    # Extract setup code (source assignments before first step)
    setup_match = re.search(
        r'def transform\([^)]*\)[^:]*:\s*\n(?:\s+""".*?"""\s*\n)?(.*?)(?=\s+step\()',
        transformer_source,
        re.DOTALL,
    )
    setup_code = ""
    if setup_match:
        setup_code = setup_match.group(1).rstrip() + "\n"

    # Build new transform body
    body_lines = []
    if docstring:
        body_lines.append(docstring)
    if setup_code:
        body_lines.append(setup_code)

    for step_name, step_code in steps:
        body_lines.append(f'\n    step("{step_name}")')
        for line in step_code.splitlines():
            body_lines.append(f"    {line}" if line.strip() else "")

    body_lines.append("\n    return df\n")

    new_source = pre_transform + transform_sig + "\n" + "\n".join(body_lines)

    # Backup original
    backup_path = transformer_path.with_suffix(".py.bak")
    shutil.copy2(transformer_path, backup_path)

    transformer_path.write_text(new_source)
