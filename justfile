run:
  uv run capigen c --spec-dir ../api_spec -o ../src/include/duckdb_v2.h

stubs:
  uv run capigen bridge --spec-dir ../api_spec --scan-dir ../src/main/capi_v2 -o ../src/main/capi_v2/capi_v2_stubs.cpp

watch:
  watchexec --restart --exts yaml,py,j2,json --clear --watch src --watch ../api_spec -- uv run capigen c --spec-dir ../api_spec -o ../src/include/duckdb_v2.h

lint:
  uvx ruff check

typecheck:
  uvx ty check

test *args:
  uv run --group dev pytest {{args}}

check:
  just lint typecheck

format:
  uvx ruff format
