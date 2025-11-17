add_subdirectory(http)

add_library_unity(
  duckdb_services OBJECT service_container.cpp crypto/encryption_functions.cpp
  crypto/encryption_key_manager.cpp crypto/encryption_state.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_services>
    PARENT_SCOPE)
