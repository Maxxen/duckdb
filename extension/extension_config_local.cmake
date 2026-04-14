################################################################################
# Local extension configuration for V2 C API development
################################################################################

duckdb_extension_load(json_v2
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/extension_v2/json
    INCLUDE_DIR ${CMAKE_SOURCE_DIR}/extension_v2/json/include
    LOAD_TESTS
    TEST_DIR ${CMAKE_SOURCE_DIR}/test/sql/json_v2
)

duckdb_extension_load(sqlite_v2
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/extension_v2/sqlite
    INCLUDE_DIR ${CMAKE_SOURCE_DIR}/extension_v2/sqlite/include
    LOAD_TESTS
    TEST_DIR ${CMAKE_SOURCE_DIR}/test/sql/sqlite_v2
)
