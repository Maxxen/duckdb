#!/usr/bin/env bash
# Regenerates tools/juliapkg/src/api.jl from api_spec/v1 via the capigen Julia
# adapter. No JuliaFormatter: the raw adapter output is authored to be formatter
# clean, and the pre-commit and CI environments have no Julia toolchain. Invoked
# by the pre-commit drift hook and by tools/juliapkg/update_api.sh.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

PYTHONPATH=tools/juliapkg/scripts uv run capigen julia_adapter \
	--spec-dir api_spec/v1 \
	-o tools/juliapkg/src/api.jl
