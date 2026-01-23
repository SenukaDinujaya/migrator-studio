"""Generate Marimo notebooks from parsed transformer."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from .parser import TransformerAST, Step


# Common imports to include in notebooks (project_root placeholder will be replaced)
NOTEBOOK_IMPORTS_TEMPLATE = """import sys
from pathlib import Path

# Add project root to path for migrator_studio imports
_project_root = Path("{project_root}")
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from migrator_studio import configure, BuildSession, load_source
from migrator_studio import (
    step,
    sanitize_data, filter_isin, filter_not_null, filter_null, filter_by_value, filter_date,
    merge_left, merge_inner, merge_outer,
    map_dict, map_lookup,
    copy_column, set_value, concat_columns, rename_columns, drop_columns, select_columns,
    str_strip, str_upper, str_lower, str_replace, str_regex_replace,
    fill_null, where, case, coalesce,
    drop_duplicates, keep_max, keep_min,
    to_numeric, to_int, to_string, to_bool,
    parse_date, format_date, extract_date_part, handle_invalid_dates,
    groupby_agg, groupby_concat,
    apply_row, apply_column, transform,
)"""


def _find_used_variables(code: str, available: set[str]) -> list[str]:
    """Find which available variables are used in the code."""
    used = []
    for var in available:
        # Simple check - look for the variable name as a word
        if re.search(rf'\b{var}\b', code):
            used.append(var)
    return sorted(used)


def _find_project_root(start_path: Path) -> Path:
    """Find the project root by looking for pyproject.toml."""
    current = start_path.resolve()
    while current.parent != current:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return start_path.resolve()


def generate_notebook(
    transformer_ast: TransformerAST,
    output_path: Optional[Path] = None,
    sample_size: int = 10,
    data_path: Optional[Path] = None,
) -> str:
    """
    Generate a Marimo notebook from a parsed transformer.

    Args:
        transformer_ast: Parsed transformer AST
        output_path: Optional path to write notebook (defaults to same dir as transformer)
        sample_size: Number of rows to sample in build mode
        data_path: Optional path to data files. If not provided, uses transformer's directory.

    Returns:
        Generated notebook source code
    """
    # Determine data path for the notebook
    if data_path is None:
        data_path = transformer_ast.filepath.parent

    # Calculate relative path from notebook to data directory
    if output_path is None:
        output_path = transformer_ast.filepath.with_suffix('.nb.py')

    # Use absolute path for data since Marimo runs from /tmp
    data_path_resolved = Path(data_path).resolve()

    # Find project root for sys.path
    project_root = _find_project_root(output_path)
    # Marimo notebook header
    lines = [
        'import marimo',
        '',
        f'__generated_with = "0.19.0"',
        '',
        'app = marimo.App(width="medium")',
        '',
    ]

    # Add docstring as comments (handling multi-line)
    docstring = transformer_ast.module_docstring or "Transformer Notebook"
    for doc_line in docstring.splitlines():
        lines.append(f'# {doc_line}' if doc_line.strip() else '#')
    lines.append(f'# Generated from: {transformer_ast.filepath.name}')
    lines.append('')

    # Track variables that cells export
    exported_vars: set[str] = set()

    # Cell 1: Setup (imports and configuration)
    lines.extend([
        '@app.cell',
        'def __():',
    ])
    # Add imports inside the cell (with project root for sys.path)
    notebook_imports = NOTEBOOK_IMPORTS_TEMPLATE.format(project_root=project_root)
    for import_line in notebook_imports.splitlines():
        lines.append(f'    {import_line}')
    lines.extend([
        f'    ',
        f'    # Configure data path (absolute path since Marimo runs from /tmp)',
        f'    configure(data_path="{data_path_resolved}")',
        f'    ',
        f'    # Start build session',
        f'    session = BuildSession(sample={sample_size})',
        f'    session.__enter__()',
        f'    ',
        f'    # Return imports and session for other cells',
        f'    return (',
        f'        session,',
        f'        sanitize_data, filter_isin, filter_not_null, filter_null,',
        f'        merge_left, merge_inner, merge_outer,',
        f'        map_dict, map_lookup,',
        f'        copy_column, set_value, concat_columns, rename_columns, drop_columns, select_columns,',
        f'        str_strip, str_upper, str_lower, str_replace,',
        f'        fill_null, where, case, coalesce,',
        f'        drop_duplicates, keep_max, keep_min,',
        f'        to_numeric, to_int, to_string, to_bool,',
        f'        load_source,',
        f'    )',
        '',
    ])
    exported_vars.add('session')
    # Add all the function names as exported
    exported_vars.update([
        'sanitize_data', 'filter_isin', 'filter_not_null', 'filter_null',
        'merge_left', 'merge_inner', 'merge_outer',
        'map_dict', 'map_lookup',
        'copy_column', 'set_value', 'concat_columns', 'rename_columns', 'drop_columns', 'select_columns',
        'str_strip', 'str_upper', 'str_lower', 'str_replace',
        'fill_null', 'where', 'case', 'coalesce',
        'drop_duplicates', 'keep_max', 'keep_min',
        'to_numeric', 'to_int', 'to_string', 'to_bool',
        'load_source',
    ])

    # Cell 2: Load sources
    if transformer_ast.sources:
        lines.extend([
            '@app.cell',
            'def __(load_source):',
            '    # Load source data',
            '    sources = {',
        ])
        for src in transformer_ast.sources:
            lines.append(f'        "{src}": load_source("{src}"),')
        lines.extend([
            '    }',
        ])

        # Add setup code (extracting individual sources)
        if transformer_ast.setup_code:
            lines.append('    ')
            for setup_line in transformer_ast.setup_code.splitlines():
                lines.append(f'    {setup_line}')

        # Determine what variables are created in setup
        setup_vars = ['sources']
        if transformer_ast.setup_code:
            # Extract variable assignments from setup code
            for match in re.finditer(r'^(\w+)\s*=', transformer_ast.setup_code, re.MULTILINE):
                setup_vars.append(match.group(1))

        lines.append(f'    return {", ".join(setup_vars)},')
        lines.append('')
        exported_vars.update(setup_vars)

    # Cells for each step - use unique variable names to avoid Marimo conflicts
    prev_df_var = None
    for i, step in enumerate(transformer_ast.steps):
        step_num = i + 1
        current_df_var = f"df_{step_num}"

        # Replace 'df' references in code with the appropriate variable names
        step_code = step.code
        input_var = prev_df_var if prev_df_var else current_df_var

        # Count how many assignments there are in this step
        # Each 'df =' at the start of a line (ignoring indentation) is an assignment
        assignment_pattern = r'^(\s*)df(\s*=)'
        num_assignments = len(re.findall(assignment_pattern, step_code, re.MULTILINE))

        if num_assignments == 0:
            # No assignments, just replace all df with input_var
            step_code = re.sub(r'\bdf\b', input_var, step_code)
        elif num_assignments == 1:
            # Single assignment - most common case
            # Replace 'df =' with 'current_df_var ='
            step_code = re.sub(r'^(\s*)df(\s*=)', rf'\g<1>{current_df_var}\g<2>', step_code, count=1, flags=re.MULTILINE)
            # Replace all remaining 'df' with input_var
            step_code = re.sub(r'\bdf\b', input_var, step_code)
        else:
            # Multiple assignments (chained operations)
            # Split by assignment pattern and process each segment
            lines_list = step_code.splitlines()
            result_lines = []
            current_input = input_var

            i = 0
            while i < len(lines_list):
                line = lines_list[i]

                # Check if this line starts an assignment
                match = re.match(r'^(\s*)df(\s*=)', line)
                if match:
                    # Replace df = with current_df_var =
                    line = re.sub(r'^(\s*)df(\s*=)', rf'\g<1>{current_df_var}\g<2>', line, count=1)
                    # Replace remaining df in this line with current_input
                    line = re.sub(r'\bdf\b', current_input, line)
                    result_lines.append(line)

                    # Check if assignment continues on next lines (multi-line function call)
                    # by looking for unbalanced parentheses
                    paren_count = line.count('(') - line.count(')')
                    while paren_count > 0 and i + 1 < len(lines_list):
                        i += 1
                        line = lines_list[i]
                        line = re.sub(r'\bdf\b', current_input, line)
                        result_lines.append(line)
                        paren_count += line.count('(') - line.count(')')

                    # After this assignment, current_df_var becomes the input
                    current_input = current_df_var
                else:
                    # Not an assignment, replace df with current input
                    line = re.sub(r'\bdf\b', current_input, line)
                    result_lines.append(line)

                i += 1

            step_code = '\n'.join(result_lines)

        # Determine dependencies for this cell (after replacements)
        all_deps = set()
        # Add the previous df variable as a dependency
        if prev_df_var:
            all_deps.add(prev_df_var)
        # Add any other dependencies from exported_vars
        for var in exported_vars:
            if var != prev_df_var and re.search(rf'\b{var}\b', step_code):
                all_deps.add(var)

        deps_str = ', '.join(sorted(all_deps)) if all_deps else ''

        lines.extend([
            '@app.cell',
            f'def __({deps_str}):',
            f'    # Step {step_num}: {step.name}',
        ])

        for code_line in step_code.splitlines():
            lines.append(f'    {code_line}')

        # Show the dataframe
        lines.append(f'    {current_df_var}')
        lines.append(f'    return {current_df_var},')
        lines.append('')

        # Track this variable for next step
        exported_vars.add(current_df_var)
        prev_df_var = current_df_var

    # Final cell: Summary - use the last step's variable
    final_df_var = prev_df_var or 'df'
    lines.extend([
        '@app.cell',
        f'def __({final_df_var}):',
        '    # Final Result',
        f'    print(f"Total rows: {{len({final_df_var})}}")',
        f'    print(f"Columns: {{list({final_df_var}.columns)}}")',
        f'    {final_df_var}',
        '    return',
        '',
    ])

    # Main block
    lines.extend([
        '',
        'if __name__ == "__main__":',
        '    app.run()',
    ])

    notebook_source = '\n'.join(lines)

    # Write to file
    if output_path is None:
        output_path = transformer_ast.filepath.with_suffix('.nb.py')

    output_path.write_text(notebook_source)

    return notebook_source
