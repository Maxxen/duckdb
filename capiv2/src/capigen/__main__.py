"""CLI entry point for capigen."""

import argparse
import importlib
import sys
from pathlib import Path

from .loader import load_metadata, load_modules
from .validate import validate_semantics

_DEFAULT_SPEC_DIR = Path("api_spec")


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB C API v2 header generator")
    parser.add_argument("adapter", help="Language adapter name (e.g. 'c')")
    parser.add_argument("--output", "-o", required=True, help="Output file path")
    parser.add_argument(
        "--spec-dir",
        default=str(_DEFAULT_SPEC_DIR),
        help=f"Path to API spec directory (default: {_DEFAULT_SPEC_DIR})",
    )
    args = parser.parse_args()

    spec_dir = Path(args.spec_dir)
    if not spec_dir.is_dir():
        print(f"Error: API spec directory not found: {spec_dir}", file=sys.stderr)
        sys.exit(1)

    # 1-2. Load and validate
    metadata = load_metadata(spec_dir)
    modules = load_modules(spec_dir)

    # 3. Semantic validation
    errors = validate_semantics(modules, metadata)
    if errors:
        print("\n--- SEMANTIC VALIDATION ERRORS ---", file=sys.stderr)
        for err in errors:
            print(err, file=sys.stderr)
        print("\nGeneration aborted.", file=sys.stderr)
        sys.exit(1)

    # 4. Import adapter: try as built-in first, then as external module path
    try:
        adapter = importlib.import_module(f"capigen.adapters.{args.adapter}")
    except ModuleNotFoundError:
        try:
            adapter = importlib.import_module(args.adapter)
        except ModuleNotFoundError:
            print(f"Error: Cannot import adapter '{args.adapter}'", file=sys.stderr)
            sys.exit(1)

    output_path = Path(args.output)
    adapter.generate(modules, metadata, output_path)


if __name__ == "__main__":
    main()
