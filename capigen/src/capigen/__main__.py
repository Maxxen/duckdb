"""CLI entry point for capigen."""

import argparse
import importlib
import inspect
import sys
from pathlib import Path

from . import SCHEMA_VERSION, __version__
from .loader import SchemaVersionError, load_metadata, load_modules
from .validate import validate_semantics

_DEFAULT_SPEC_DIR = Path("api_spec/v2")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Declarative C API generator from YAML specs"
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--schema-version",
        action="version",
        version=SCHEMA_VERSION,
        help="Print the supported spec schema version and exit",
    )
    parser.add_argument("adapter", help="Language adapter name (e.g. 'c')")
    parser.add_argument("--output", "-o", required=True, help="Output file path")
    parser.add_argument(
        "--spec-dir",
        default=str(_DEFAULT_SPEC_DIR),
        help=f"Path to spec directory containing metadata.yaml and module YAMLs (default: {_DEFAULT_SPEC_DIR})",
    )
    parser.add_argument(
        "--scan-dir",
        default=None,
        help="Directory to scan for already-implemented functions (bridge adapter only)",
    )
    parser.add_argument(
        "--template",
        default=None,
        help="Frozen template header to verify and append (extension_header adapter only)",
    )
    parser.add_argument(
        "--internal-out",
        default=None,
        dest="internal_out",
        help="Path for the derived engine-side header (extension_header adapter only)",
    )
    parser.add_argument(
        "--emit-deprecated",
        choices=["true", "false", "strict"],
        default=None,
        dest="emit_deprecated",
        help="Override deprecated_encoding from metadata: true=guarded (default), false=omit entirely, strict=guarded + the configured deprecated macro attribute",
    )
    args = parser.parse_args()

    spec_dir = Path(args.spec_dir)
    if not spec_dir.is_dir():
        print(f"Error: API spec directory not found: {spec_dir}", file=sys.stderr)
        sys.exit(1)

    # 1-2. Load and validate
    try:
        metadata = load_metadata(spec_dir)
    except SchemaVersionError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    modules = load_modules(spec_dir)

    if args.emit_deprecated is not None:
        _encoding_map = {"true": "guarded", "false": "none", "strict": "strict"}
        metadata.setdefault("options", {}).setdefault("c", {})[
            "deprecated_encoding"
        ] = _encoding_map[args.emit_deprecated]

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
    params = inspect.signature(adapter.generate).parameters
    extra_kwargs: dict = {}
    if args.scan_dir is not None:
        extra_kwargs["scan_dir"] = Path(args.scan_dir)
    if args.template is not None and "template" in params:
        extra_kwargs["template"] = Path(args.template)
    if args.internal_out is not None and "internal_out" in params:
        extra_kwargs["internal_out"] = Path(args.internal_out)
    if "invocation" in params:
        extra_kwargs["invocation"] = "capigen " + " ".join(sys.argv[1:])
    adapter.generate(modules, metadata, output_path, **extra_kwargs)


if __name__ == "__main__":
    main()
