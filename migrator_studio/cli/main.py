"""Main CLI entry point for migrator_studio."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import click

from ..codegen import generate_notebook, parse_transformer
from ..config import configure, get_config


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
    from ..codegen.sync import sync_notebook

    notebook_path = transformer.with_suffix(".nb.py")

    # Also check in the configured notebook directory
    if not notebook_path.exists():
        try:
            configure(data_path=str(transformer.parent))
            config = get_config()
            notebook_dir = Path(config.notebook_dir)
            alt_path = notebook_dir / f"{transformer.stem}.nb.py"
            if alt_path.exists():
                notebook_path = alt_path
        except Exception:
            pass

    if not notebook_path.exists():
        click.echo(f"Notebook not found: {notebook_path}", err=True)
        click.echo("Run 'migrator dev' first to generate the notebook.")
        sys.exit(1)

    click.echo(f"Syncing from: {notebook_path}")
    click.echo(f"To transformer: {transformer}")

    try:
        sync_notebook(notebook_path, transformer)
        click.echo("Sync complete.")
    except Exception as e:
        click.echo(f"Sync failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("transformer", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Save result to file. Supports .csv and .feather formats.",
)
def run(transformer: Path, output: Path | None):
    """
    Run a transformer in production mode.

    Loads all sources defined in SOURCES, runs the transform() function,
    and prints a summary of the result.

    TRANSFORMER is the path to the transformer .py file.

    Example:
        migrator run sample/TFRM-EXAMPLE-001.py
        migrator run sample/TFRM-EXAMPLE-001.py -o output.csv
    """
    click.echo(f"Running transformer: {transformer}")

    spec = importlib.util.spec_from_file_location("transformer", transformer)
    if spec is None or spec.loader is None:
        click.echo(f"Could not load transformer: {transformer}", err=True)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        click.echo(f"Error loading transformer: {e}", err=True)
        sys.exit(1)

    if not hasattr(module, "transform"):
        click.echo("Transformer must have a 'transform' function", err=True)
        sys.exit(1)

    # Load sources
    sources_list = getattr(module, "SOURCES", [])
    if not sources_list:
        click.echo("Transformer has no SOURCES list defined.", err=True)
        sys.exit(1)

    # Configure data path relative to transformer
    configure(data_path=str(transformer.parent.resolve()))

    from ..loader import load_source

    click.echo(f"Loading {len(sources_list)} sources...")
    try:
        sources = {src: load_source(src) for src in sources_list}
    except FileNotFoundError as e:
        click.echo(f"Source not found: {e}", err=True)
        sys.exit(1)

    for src_id, src_df in sources.items():
        click.echo(f"  {src_id}: {len(src_df)} rows")

    # Run transform
    click.echo("Running transform...")
    try:
        result = module.transform(sources)
    except Exception as e:
        click.echo(f"Transform failed: {e}", err=True)
        sys.exit(1)

    # Print summary
    click.echo(f"\nResult: {len(result)} rows, {len(result.columns)} columns")
    click.echo(f"Columns: {list(result.columns)}")
    click.echo(f"Dtypes:\n{result.dtypes.to_string()}")

    # Optionally save output
    if output:
        if output.suffix == ".feather":
            result.to_feather(output)
        elif output.suffix == ".csv":
            result.to_csv(output, index=False)
        else:
            click.echo(f"Unsupported output format: {output.suffix}. Use .csv or .feather.", err=True)
            sys.exit(1)
        click.echo(f"Saved to: {output}")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
