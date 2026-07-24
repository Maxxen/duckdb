#!/usr/bin/env bash
# Regenerates the V1 C API header from api_spec/v1/, then formats the
# regenerated file. Invoked manually after editing YAML.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

uv run --project api_spec capigen c \
	--spec-dir api_spec/v1 \
	-o src/include/duckdb_v1.h

uv run --project api_spec capigen extension_header \
	--spec-dir api_spec/v1 \
	--template api_spec/v1/extension/duckdb_extension.h.in \
	--internal-out src/include/duckdb/main/capi/extension_api.hpp \
	-o src/include/duckdb_extension.h

uv run --project api_spec --group dev python scripts/format.py HEAD --fix --noconfirm --silent
