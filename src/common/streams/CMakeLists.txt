add_library_unity(
  duckdb_streams
  OBJECT
  memory_stream.cpp
  file_stream.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_streams>
    PARENT_SCOPE)
