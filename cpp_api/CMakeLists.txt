# Stable C++ API. Compiled into consumers, never into libduckdb: the V2 C ABI is
# the boundary. Hidden visibility keeps duckdb_api symbols out of the export
# surface of anything this archive ends up linked into (e.g. via a static
# extension that reaches the shared library).
add_library(duckdb_cpp_api STATIC duckdb_cpp.cpp)
target_include_directories(duckdb_cpp_api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(duckdb_cpp_api PROPERTIES CXX_VISIBILITY_PRESET hidden
                                                VISIBILITY_INLINES_HIDDEN ON)
