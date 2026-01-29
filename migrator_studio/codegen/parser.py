"""Parse transformer files and extract step structure."""
from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Step:
    """A single step in the transformer."""
    name: str
    code: str
    lineno: int


@dataclass
class TransformerAST:
    """Parsed transformer file structure."""
    filepath: Path
    module_docstring: str | None
    imports: str
    sources: list[str]
    setup_code: str  # Code before first step() in transform
    steps: list[Step]
    return_var: str  # Variable name returned from transform


def _get_source_segment(source_lines: list[str], node: ast.AST) -> str:
    """Extract source code for an AST node."""
    start = node.lineno - 1
    end = node.end_lineno
    lines = source_lines[start:end]

    if lines:
        # Handle indentation of first line
        first_indent = len(lines[0]) - len(lines[0].lstrip())
        lines = [line[first_indent:] if len(line) >= first_indent else line for line in lines]

    return "\n".join(lines)


def _extract_sources_constant(tree: ast.Module) -> list[str]:
    """Extract SOURCES = [...] constant if present."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "SOURCES":
                    if isinstance(node.value, ast.List):
                        sources = []
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant):
                                sources.append(elt.value)
                        return sources
    return []


def _find_transform_function(tree: ast.Module) -> ast.FunctionDef | None:
    """Find the transform() function definition."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "transform":
            return node
    return None


def _is_step_call(node: ast.stmt) -> str | None:
    """Check if statement is a step() call, return step name if so."""
    if isinstance(node, ast.Expr):
        if isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Name) and func.id == "step":
                if node.value.args:
                    arg = node.value.args[0]
                    if isinstance(arg, ast.Constant):
                        return arg.value
    return None


def _get_return_var(func: ast.FunctionDef) -> str:
    """Get the variable name from the return statement."""
    for node in reversed(func.body):
        if isinstance(node, ast.Return):
            if isinstance(node.value, ast.Name):
                return node.value.id
    return "df"


def parse_transformer(filepath: Path) -> TransformerAST:
    """
    Parse a transformer file and extract its structure.

    Args:
        filepath: Path to the transformer .py file

    Returns:
        TransformerAST with parsed structure
    """
    source = filepath.read_text()
    source_lines = source.splitlines()
    tree = ast.parse(source)

    # Extract module docstring
    module_docstring = ast.get_docstring(tree)

    # Extract imports (all Import and ImportFrom nodes at module level)
    import_lines = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            import_lines.append(_get_source_segment(source_lines, node))
    imports = "\n".join(import_lines)

    # Extract SOURCES constant
    sources = _extract_sources_constant(tree)

    # Find transform function
    transform_func = _find_transform_function(tree)
    if transform_func is None:
        raise ValueError(f"No transform() function found in {filepath}")

    # Get return variable
    return_var = _get_return_var(transform_func)

    # Split function body into setup and steps
    setup_lines = []
    steps: list[Step] = []
    current_step_name: str | None = None
    current_step_lines: list[str] = []
    current_step_lineno: int = 0

    for node in transform_func.body:
        # Skip docstring
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str) and not current_step_name:
                continue

        # Skip return statement
        if isinstance(node, ast.Return):
            continue

        step_name = _is_step_call(node)

        if step_name:
            # Save previous step if exists
            if current_step_name:
                steps.append(Step(
                    name=current_step_name,
                    code="\n".join(current_step_lines).strip(),
                    lineno=current_step_lineno,
                ))

            # Start new step
            current_step_name = step_name
            current_step_lines = []
            current_step_lineno = node.lineno
        else:
            code = _get_source_segment(source_lines, node)

            if current_step_name is None:
                # Before any step() call - this is setup
                setup_lines.append(code)
            else:
                current_step_lines.append(code)

    # Save last step
    if current_step_name:
        steps.append(Step(
            name=current_step_name,
            code="\n".join(current_step_lines).strip(),
            lineno=current_step_lineno,
        ))

    setup_code = "\n".join(setup_lines).strip()

    return TransformerAST(
        filepath=filepath,
        module_docstring=module_docstring,
        imports=imports,
        sources=sources,
        setup_code=setup_code,
        steps=steps,
        return_var=return_var,
    )
