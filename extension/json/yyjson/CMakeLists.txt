if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(duckdb_extension_yyjson STATIC yyjson_ext.cpp)

target_include_directories(
        duckdb_extension_yyjson
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

install(TARGETS duckdb_extension_yyjson
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_extension_yyjson)
