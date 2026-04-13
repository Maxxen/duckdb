include_directories(${CMAKE_SOURCE_DIR}/src/main/capi_v2)

add_library_unity(
  test_sql_capi_v2
  OBJECT
  test_capi_v2.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi_v2>
    PARENT_SCOPE)
