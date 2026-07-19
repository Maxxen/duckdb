#!/usr/bin/env python3
"""Assemble the duckdb-cpp package artifact.

Collects the C++ API sources, a snapshot of the generated duckdb_v2.h, and the
consumer CMake glue into a self-contained package directory. The snapshot is
taken here, at assembly time: the in-tree build always compiles against the
live header, so there is no vendored copy to drift.

Usage:
    python3 scripts/package_cpp_api.py --output <dir> --package-version 0.1.0 \
        [--duckdb-version 1.5.4] [--zip <file.zip>]

--duckdb-version is the libduckdb floor stamped into the package; it defaults
to the version of this checkout (git describe).
"""

import argparse
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PACKAGE_FILES = [
    ("cpp_api/duckdb_cpp.hpp", "duckdb_cpp.hpp"),
    ("cpp_api/duckdb_cpp.cpp", "duckdb_cpp.cpp"),
    ("src/include/duckdb_v2.h", "duckdb_v2.h"),
    ("cpp_api/package/CMakeLists.txt", "CMakeLists.txt"),
    ("cpp_api/cmake/DuckDBCppApi.cmake", "cmake/DuckDBCppApi.cmake"),
]

VERSION_TEMPLATE = REPO_ROOT / "cpp_api" / "package" / "duckdb_cpp_version.cmake.in"


def default_duckdb_version():
    described = subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"], cwd=REPO_ROOT, text=True).strip()
    return described.lstrip("v")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="package output directory")
    parser.add_argument("--package-version", required=True, help="duckdb-cpp package version")
    parser.add_argument("--duckdb-version", help="libduckdb floor version (default: git describe)")
    parser.add_argument("--zip", help="also write the package as this zip file")
    args = parser.parse_args()

    floor = args.duckdb_version or default_duckdb_version()
    out = Path(args.output)
    if out.exists() and any(out.iterdir()):
        sys.exit(f"error: output directory {out} exists and is not empty")

    for src_rel, dst_rel in PACKAGE_FILES:
        src = REPO_ROOT / src_rel
        if not src.is_file():
            sys.exit(f"error: missing source file {src}")
        dst = out / dst_rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    stamped = VERSION_TEMPLATE.read_text()
    stamped = stamped.replace("@DUCKDB_CPP_VERSION@", args.package_version)
    stamped = stamped.replace("@DUCKDB_CPP_FLOOR@", floor)
    (out / "cmake" / "duckdb_cpp_version.cmake").write_text(stamped)

    if args.zip:
        with zipfile.ZipFile(args.zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(out.rglob("*")):
                if path.is_file():
                    zf.write(path, path.relative_to(out))

    print(f"duckdb-cpp {args.package_version} (requires libduckdb >= {floor}) -> {out}")


if __name__ == "__main__":
    main()
