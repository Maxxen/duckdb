add_library_unity(duckdb_function_geometry OBJECT geometry_functions.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function_geometry>
    PARENT_SCOPE)
