cmake_minimum_required(VERSION 2.8.12...3.29)
project(SqliteV2Extension)

include_directories(include)
include_directories(third_party)

add_subdirectory(storage)

set(SQLITE_V2_EXTENSION_FILES
    sqlite_db.cpp
    sqlite_extension.cpp
    sqlite_scanner.cpp
    sqlite_stmt.cpp
    sqlite_storage.cpp
    sqlite_utils.cpp
    third_party/sqlite3.c
    ${SQLITE_V2_STORAGE_FILES})

build_static_extension(sqlite_v2 ${SQLITE_V2_EXTENSION_FILES})
build_loadable_extension(sqlite_v2 "" ${SQLITE_V2_EXTENSION_FILES})

# SQLite3 compile definitions
target_compile_definitions(sqlite_v2_extension PRIVATE
    SQLITE_ENABLE_FTS5 SQLITE_ENABLE_FTS4 SQLITE_ENABLE_FTS3_PARENTHESIS SQLITE_ENABLE_RTREE)
target_compile_definitions(sqlite_v2_loadable_extension PRIVATE
    SQLITE_ENABLE_FTS5 SQLITE_ENABLE_FTS4 SQLITE_ENABLE_FTS3_PARENTHESIS SQLITE_ENABLE_RTREE)

install(
  TARGETS sqlite_v2_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
