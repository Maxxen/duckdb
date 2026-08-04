add_library_unity(test_sql_capi_v2 OBJECT test_capi_v2.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi_v2>
    PARENT_SCOPE)
