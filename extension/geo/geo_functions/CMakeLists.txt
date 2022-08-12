add_library_unity(
  duckdb_geo_functions
  OBJECT
  st_point.cpp
  st_make_line.cpp
  st_geohash.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_geo_functions>
    PARENT_SCOPE)
