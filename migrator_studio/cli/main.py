"""CLI entry point for migrator_studio."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Default subdirectory for Marimo notebooks
MARIMO_DIR = ".marimo"


def cmd_preview(args):
    """Generate a Marimo notebook from a transformer file."""
    from .parser import parse_transformer
    from .generator import generate_marimo_notebook

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Determine output path (in .marimo subdirectory)
    if args.output:
        output_path = Path(args.output)
    else:
        marimo_dir = input_path.parent / MARIMO_DIR
        marimo_dir.mkdir(exist_ok=True)
        output_path = marimo_dir / f"{input_path.stem}.marimo.py"

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Parsing: {input_path}")
    try:
        info = parse_transformer(input_path)
    except Exception as e:
        print(f"Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(info.steps)} steps:")
    for i, step in enumerate(info.steps, 1):
        print(f"  {i}. {step.name}")

    print(f"\nGenerating: {output_path}")

    # Data path relative to the notebook location (which is in .marimo/)
    data_path = args.data_path or 'str(Path(__file__).parent.parent)'
    notebook = generate_marimo_notebook(info, output_path, data_path)

    output_path.write_text(notebook)
    print(f"\nDone! Run with:")
    print(f"  marimo edit {output_path}")


def cmd_export(args):
    """Export a Marimo notebook back to a transformer file."""
    from .exporter import export_to_transformer

    input_path = Path(args.input)

    # If input doesn't exist, check if it's a transformer name and look in .marimo/
    if not input_path.exists():
        # Try to find it in .marimo/ subdirectory
        if not str(input_path).endswith(".marimo.py"):
            marimo_path = input_path.parent / MARIMO_DIR / f"{input_path.stem}.marimo.py"
            if marimo_path.exists():
                input_path = marimo_path
            else:
                print(f"Error: File not found: {input_path}", file=sys.stderr)
                print(f"       Also checked: {marimo_path}", file=sys.stderr)
                sys.exit(1)
        else:
            print(f"Error: File not found: {input_path}", file=sys.stderr)
            sys.exit(1)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        # If in .marimo/ directory, output to parent directory
        name = input_path.stem
        if name.endswith(".marimo"):
            name = name[:-7]  # Remove .marimo suffix

        if input_path.parent.name == MARIMO_DIR:
            # Output to parent of .marimo/
            output_path = input_path.parent.parent / f"{name}.py"
        else:
            output_path = input_path.parent / f"{name}.py"

    print(f"Exporting: {input_path}")
    print(f"Output: {output_path}")

    try:
        transformer_code = export_to_transformer(input_path)
        output_path.write_text(transformer_code)
        print("\nDone!")
    except Exception as e:
        print(f"Error exporting: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="migrator",
        description="Migrator Studio CLI tools",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Preview command
    preview_parser = subparsers.add_parser(
        "preview",
        help="Generate a Marimo notebook from a transformer file",
    )
    preview_parser.add_argument(
        "input",
        help="Path to the transformer Python file",
    )
    preview_parser.add_argument(
        "-o", "--output",
        help=f"Output path for the Marimo notebook (default: {MARIMO_DIR}/<name>.marimo.py)",
    )
    preview_parser.add_argument(
        "--data-path",
        help="Data path to configure (default: auto-detect)",
    )
    preview_parser.set_defaults(func=cmd_preview)

    # Export command
    export_parser = subparsers.add_parser(
        "export",
        help="Export a Marimo notebook back to a transformer file",
    )
    export_parser.add_argument(
        "input",
        help="Path to the Marimo notebook file (or transformer name)",
    )
    export_parser.add_argument(
        "-o", "--output",
        help="Output path for the transformer file",
    )
    export_parser.set_defaults(func=cmd_export)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
