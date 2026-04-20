add_library_unity(duckdb_main_capi_v2 OBJECT capi_v2_stubs.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_capi_v2>
    PARENT_SCOPE)
