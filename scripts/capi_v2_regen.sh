#!/usr/bin/env bash
# Regenerates the V2 C API header and bridge stubs from api_spec/, then
# formats the regenerated files. Invoked manually after editing YAML and
# automatically by the matching pre-commit hook.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

uv run capigen c \
    --spec-dir api_spec \
    -o src/include/duckdb_v2.h

uv run capigen bridge \
    --spec-dir api_spec \
    --scan-dir src/main/capi_v2 \
    -o src/main/capi_v2/capi_v2_stubs.cpp

uv run --group dev python scripts/format.py HEAD --fix --noconfirm --silent
