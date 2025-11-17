add_library_unity(duckdb_service OBJECT service_container.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_service>
    PARENT_SCOPE)
