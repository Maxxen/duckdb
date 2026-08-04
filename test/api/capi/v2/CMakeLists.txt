include_directories(${CMAKE_SOURCE_DIR}/src/main/capi/v2)

add_library_unity(
  test_sql_capi_v2
  OBJECT
  test_capi_v2.cpp
  test_capi_v2_basic.cpp
  test_capi_v2_arena.cpp
  test_capi_v2_bytes.cpp
  test_capi_v2_data_chunk.cpp
  test_capi_v2_error_info.cpp
  test_capi_v2_logical_type.cpp
  test_capi_v2_option.cpp
  test_capi_v2_query.cpp
  test_capi_v2_result.cpp
  test_capi_v2_statement.cpp
  test_capi_v2_value.cpp
  test_capi_v2_vector_write.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi_v2>
    PARENT_SCOPE)
