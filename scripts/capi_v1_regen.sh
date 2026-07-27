#!/usr/bin/env bash
# Regenerates the V1 C API headers from api_spec/v1/, then formats the outputs.
# Invoked manually after editing YAML, or via make generate-files.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

uv run --project api_spec --group generate capigen c \
	--spec-dir api_spec/v1 \
	-o src/include/duckdb_v1.h

uv run --project api_spec --group generate capigen extension_header \
	--spec-dir api_spec/v1 \
	--template api_spec/v1/extension/duckdb_extension.h.in \
	--internal-out src/include/duckdb/main/capi/extension_api.hpp \
	-o src/include/duckdb_extension.h

uv run --project api_spec --group dev python scripts/format.py HEAD --fix --noconfirm --silent