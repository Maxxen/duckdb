add_library_unity(
  duckdb_main_capi_v2
  OBJECT
  capi_v2_stubs.cpp
  connection-v2.cpp
  database-v2.cpp
  environment-v2.cpp
  error-v2.cpp
  option-v2.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_capi_v2>
    PARENT_SCOPE)
