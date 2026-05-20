#!/usr/bin/env bash
# Regenerates the V1 C API header from api_spec/v1/, then formats the
# regenerated file. Invoked manually after editing YAML.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

uv run capigen c \
	--spec-dir api_spec/v1 \
	-o src/include/duckdb_v1.h

uv run --group dev python scripts/format.py HEAD --fix --noconfirm --silent
