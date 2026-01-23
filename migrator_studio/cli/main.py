"""Main CLI entry point for migrator_studio."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import click

from ..config import configure, get_config
from ..codegen import parse_transformer, generate_notebook


@click.group()
@click.version_option()
def cli():
    """Migrator Studio - Build ERP data migration transformers faster."""
    pass


def find_transformers(directory: Path = Path(".")) -> list[Path]:
    """Find transformer files (TFRM-*.py) in the given directory."""
    return sorted(directory.glob("TFRM-*.py"))


@cli.command()
@click.argument("transformer", type=click.Path(exists=True, path_type=Path), required=False)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output path for the notebook. Defaults to <transformer>.nb.py",
)
@click.option(
    "--sample", "-s",
    default=10,
    help="Number of rows to sample in development mode.",
)
@click.option(
    "--no-run",
    is_flag=True,
    help="Generate notebook without opening Marimo.",
)
def dev(transformer: Path | None, output: Path, sample: int, no_run: bool):
    """
    Generate a Marimo notebook for interactive development.

    TRANSFORMER is the path to the transformer .py file.
    If not provided, auto-discovers TFRM-*.py files in the current directory.

    Examples:
        migrator dev
        migrator dev sample/TFRM-EXAMPLE-001.py
        migrator dev sample/TFRM-EXAMPLE-001.py -o notebooks/dev.nb.py
    """
    # Auto-discover transformer if not provided
    if transformer is None:
        transformers = find_transformers()
        if not transformers:
            click.echo("No transformer files (TFRM-*.py) found in current directory.", err=True)
            click.echo("Provide a path: migrator dev <path/to/transformer.py>")
            sys.exit(1)
        elif len(transformers) == 1:
            transformer = transformers[0]
            click.echo(f"Auto-detected transformer: {transformer}")
        else:
            click.echo("Multiple transformers found:")
            for i, t in enumerate(transformers, 1):
                click.echo(f"  {i}. {t.name}")

            # Prompt user to select
            choice = click.prompt(
                "\nSelect transformer",
                type=click.IntRange(1, len(transformers)),
                default=1,
            )
            transformer = transformers[choice - 1]
            click.echo(f"Selected: {transformer.name}")

    click.echo(f"Parsing transformer: {transformer}")

    try:
        ast = parse_transformer(transformer)
    except Exception as e:
        click.echo(f"Error parsing transformer: {e}", err=True)
        sys.exit(1)

    click.echo(f"  Found {len(ast.steps)} steps")
    for step in ast.steps:
        click.echo(f"    - {step.name}")

    # Determine notebook output path
    if output:
        notebook_path = output
    else:
        # Use configured notebook_dir (defaults to .tmp inside transformer's directory)
        configure(data_path=str(transformer.parent))
        config = get_config()
        notebook_dir = Path(config.notebook_dir)

        # Create notebook directory if it doesn't exist
        notebook_dir.mkdir(parents=True, exist_ok=True)

        # Notebook filename is transformer name with .nb.py extension
        notebook_path = notebook_dir / f"{transformer.stem}.nb.py"

    generate_notebook(ast, notebook_path, sample_size=sample, data_path=transformer.parent)
    click.echo(f"Generated notebook: {notebook_path}")

    if not no_run:
        click.echo("Starting Marimo...")
        try:
            # Use uv run to ensure migrator_studio is available in the environment
            subprocess.run(["uv", "run", "marimo", "edit", str(notebook_path)], check=True)
        except FileNotFoundError:
            click.echo(
                "uv or marimo not found. Install marimo with: uv add marimo",
                err=True,
            )
            click.echo(f"Notebook saved at: {notebook_path}")
        except subprocess.CalledProcessError as e:
            click.echo(f"Marimo exited with error: {e}", err=True)
            sys.exit(1)


@cli.command()
@click.argument("transformer", type=click.Path(exists=True, path_type=Path))
def sync(transformer: Path):
    """
    Sync changes from notebook back to transformer.

    TRANSFORMER is the path to the transformer .py file.
    The corresponding .nb.py notebook will be read and merged.

    Example:
        migrator sync sample/TFRM-EXAMPLE-001.py
    """
    notebook_path = transformer.with_suffix(".nb.py")

    if not notebook_path.exists():
        click.echo(f"Notebook not found: {notebook_path}", err=True)
        click.echo("Run 'migrator dev' first to generate the notebook.")
        sys.exit(1)

    click.echo(f"Syncing from: {notebook_path}")
    click.echo(f"To transformer: {transformer}")

    # TODO: Implement sync logic
    # This requires parsing the notebook and extracting step code
    click.echo("Sync not yet implemented - coming soon!")


@cli.command()
@click.argument("transformer", type=click.Path(exists=True, path_type=Path))
def run(transformer: Path):
    """
    Run a transformer in production mode.

    TRANSFORMER is the path to the transformer .py file.

    Example:
        migrator run sample/TFRM-EXAMPLE-001.py
    """
    click.echo(f"Running transformer: {transformer}")

    # Import and run the transformer
    import importlib.util

    spec = importlib.util.spec_from_file_location("transformer", transformer)
    if spec is None or spec.loader is None:
        click.echo(f"Could not load transformer: {transformer}", err=True)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "transform"):
        click.echo("Transformer must have a 'transform' function", err=True)
        sys.exit(1)

    # TODO: Load sources and run transform
    click.echo("Production run not yet implemented - coming soon!")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
