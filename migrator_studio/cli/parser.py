"""Parser for transformer files - extracts steps for Marimo generation."""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Step:
    """Represents a single step in the transformer."""
    name: str
    code: str
    line_start: int
    line_end: int
    description: Optional[str] = None


@dataclass
class TransformerInfo:
    """Parsed information from a transformer file."""
    imports: str
    sources_setup: str  # Code before first step (getting sources)
    steps: list[Step]
    final_code: str  # Code after last step (return statement, etc.)
    transform_args: str  # Arguments to transform function
    file_docstring: Optional[str] = None
    helper_functions: str = ""  # Any functions defined outside transform


def parse_transformer(file_path: Path) -> TransformerInfo:
    """
    Parse a transformer Python file and extract steps.

    Args:
        file_path: Path to the transformer .py file

    Returns:
        TransformerInfo with parsed components
    """
    source = file_path.read_text()
    lines = source.splitlines()
    tree = ast.parse(source)

    # Extract file docstring
    file_docstring = ast.get_docstring(tree)

    # Find imports (everything before first function def)
    imports_lines = []
    helper_functions_lines = []
    transform_func = None
    transform_start_line = None
    transform_end_line = None

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports_lines.append(ast.get_source_segment(source, node))
        elif isinstance(node, ast.FunctionDef):
            if node.name == "transform":
                transform_func = node
                transform_start_line = node.lineno
                transform_end_line = node.end_lineno
            else:
                # Helper function
                helper_functions_lines.append(
                    "\n".join(lines[node.lineno - 1:node.end_lineno])
                )

    if transform_func is None:
        raise ValueError(f"No 'transform' function found in {file_path}")

    # Get transform function arguments
    args = transform_func.args
    transform_args = ast.get_source_segment(source, args) or "sources"

    # Parse the body of transform function to find step() calls
    transform_body_lines = lines[transform_start_line:transform_end_line]
    transform_body = "\n".join(transform_body_lines)

    # Find all step() calls and their positions
    step_positions = []
    for node in ast.walk(transform_func):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Name) and call.func.id == "step":
                # Extract step name
                if call.args:
                    if isinstance(call.args[0], ast.Constant):
                        step_name = call.args[0].value
                    else:
                        step_name = "Unnamed Step"
                else:
                    step_name = "Unnamed Step"

                # Extract description if provided
                description = None
                if len(call.args) > 1 and isinstance(call.args[1], ast.Constant):
                    description = call.args[1].value

                step_positions.append({
                    "name": step_name,
                    "description": description,
                    "lineno": node.lineno,
                })

    # Now split the transform body into sections
    # Section before first step = sources setup
    # Each step() to next step() = step code
    # After last step = final code

    # Get the body lines (excluding def line)
    body_start = transform_start_line  # Line after 'def transform(...):'
    body_lines = lines[body_start:transform_end_line]

    # Find the actual body start (skip docstring if present)
    func_body_start_offset = 0
    for i, line in enumerate(body_lines):
        stripped = line.strip()
        if stripped and not stripped.startswith('"""') and not stripped.startswith("'''"):
            if not (stripped.startswith('"') or stripped.startswith("'")):
                func_body_start_offset = i
                break
        elif '"""' in stripped or "'''" in stripped:
            # Skip docstring lines
            continue

    # Adjust for docstring
    if transform_func.body and isinstance(transform_func.body[0], ast.Expr):
        if isinstance(transform_func.body[0].value, ast.Constant):
            # Has docstring, skip it
            docstring_end = transform_func.body[0].end_lineno
            func_body_start_offset = docstring_end - transform_start_line

    steps = []
    if not step_positions:
        # No steps defined, treat entire body as one step
        body_code = "\n".join(body_lines[func_body_start_offset:])
        # Remove leading indentation
        body_code = dedent_code(body_code)
        return TransformerInfo(
            imports="\n".join(imports_lines),
            sources_setup="",
            steps=[Step(name="Transform", code=body_code, line_start=body_start, line_end=transform_end_line)],
            final_code="",
            transform_args=transform_args,
            file_docstring=file_docstring,
            helper_functions="\n\n".join(helper_functions_lines),
        )

    # Convert line numbers to relative positions within transform body
    relative_positions = []
    for sp in step_positions:
        rel_line = sp["lineno"] - transform_start_line - 1
        relative_positions.append({
            **sp,
            "rel_lineno": rel_line,
        })

    # Extract sources setup (before first step)
    first_step_rel = relative_positions[0]["rel_lineno"]
    sources_setup_lines = body_lines[func_body_start_offset:first_step_rel]
    sources_setup = dedent_code("\n".join(sources_setup_lines))

    # Extract each step's code
    for i, sp in enumerate(relative_positions):
        step_start = sp["rel_lineno"] + 1  # Line after step() call

        if i + 1 < len(relative_positions):
            step_end = relative_positions[i + 1]["rel_lineno"]
        else:
            # Last step - find where the return statement or end is
            step_end = len(body_lines)
            # Look for return statement
            for j in range(sp["rel_lineno"] + 1, len(body_lines)):
                if body_lines[j].strip().startswith("return "):
                    step_end = j
                    break

        step_code = "\n".join(body_lines[step_start:step_end])
        step_code = dedent_code(step_code)

        steps.append(Step(
            name=sp["name"],
            code=step_code.strip(),
            line_start=transform_start_line + step_start,
            line_end=transform_start_line + step_end,
            description=sp.get("description"),
        ))

    # Extract final code (return statement, etc.)
    last_step_end = relative_positions[-1]["rel_lineno"] + 1
    # Find where last step's code ends
    for i, step in enumerate(steps):
        if i == len(steps) - 1:
            last_step_end = step.line_end - transform_start_line

    final_code_lines = body_lines[last_step_end:]
    final_code = dedent_code("\n".join(final_code_lines))

    return TransformerInfo(
        imports="\n".join(imports_lines),
        sources_setup=sources_setup.strip(),
        steps=steps,
        final_code=final_code.strip(),
        transform_args=transform_args,
        file_docstring=file_docstring,
        helper_functions="\n\n".join(helper_functions_lines),
    )


def dedent_code(code: str) -> str:
    """Remove common leading indentation from code."""
    lines = code.splitlines()
    if not lines:
        return code

    # Find minimum indentation (ignoring empty lines)
    min_indent = float("inf")
    for line in lines:
        if line.strip():
            indent = len(line) - len(line.lstrip())
            min_indent = min(min_indent, indent)

    if min_indent == float("inf"):
        return code

    # Remove that indentation from all lines
    dedented = []
    for line in lines:
        if line.strip():
            dedented.append(line[int(min_indent):])
        else:
            dedented.append("")

    return "\n".join(dedented)
