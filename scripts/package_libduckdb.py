#!/usr/bin/env python3
"""Assemble a libduckdb-<platform>.zip from a build directory.

The payload of the binary distribution channel: the shared library plus
duckdb.h and duckdb_v2.h, flat, matching the layout of upstream's libduckdb
release zips. Hosting is a separate concern; this script only produces the
artifact.

Usage:
    python3 scripts/package_libduckdb.py --build-dir build/reldebug \
        --output libduckdb-osx-universal.zip
"""

import argparse
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SHARED_LIB_NAMES = ["libduckdb.dylib", "libduckdb.so", "duckdb.dll", "duckdb.lib"]
HEADERS = ["duckdb.h", "duckdb_v2.h"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", required=True, help="build directory, e.g. build/reldebug")
    parser.add_argument("--output", required=True, help="output zip path")
    args = parser.parse_args()

    lib_dir = Path(args.build_dir) / "src"
    libs = [lib_dir / name for name in SHARED_LIB_NAMES if (lib_dir / name).is_file()]
    if not libs:
        sys.exit(f"error: no duckdb shared library in {lib_dir} (is the build complete?)")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for lib in libs:
            zf.write(lib, lib.name)
        for header in HEADERS:
            src = REPO_ROOT / "src" / "include" / header
            if not src.is_file():
                sys.exit(f"error: missing header {src}")
            zf.write(src, header)

    contents = ", ".join(p.name for p in libs) + ", " + ", ".join(HEADERS)
    print(f"{out} ({contents})")


if __name__ == "__main__":
    main()
