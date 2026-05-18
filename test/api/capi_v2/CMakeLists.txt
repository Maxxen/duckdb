include_directories(${CMAKE_SOURCE_DIR}/src/main/capi_v2)

add_library_unity(test_sql_capi_v2 OBJECT test_capi_v2.cpp
                  test_capi_v2_logical_type.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi_v2>
    PARENT_SCOPE)
