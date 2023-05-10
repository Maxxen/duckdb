add_library_unity(
  duckdb_rtree_index_execution
  OBJECT
  rtree.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_rtree_index_execution>
    PARENT_SCOPE)
