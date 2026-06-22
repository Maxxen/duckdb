add_extension_definitions()

add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}"
                -DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

# Opt-in: route the sqllogictest runner's SQL execution through the stable C++
# API (duckdb_api) instead of the internal ClientContext::Query path. Enable
# with -DSQLLOGIC_CPP_API_EXECUTION=1 at configure time.
if(SQLLOGIC_CPP_API_EXECUTION)
  add_definitions(-DSQLLOGIC_CPP_API_EXECUTION)
endif()

set(SQLITE_TEST_RUNNER_SOURCES
    result_helper.cpp sqllogic_command.cpp sqllogic_test_runner.cpp
    sqllogic_parser.cpp test_sqllogictest.cpp sqllogic_test_logger.cpp)

add_library_unity(test_sqlite OBJECT ${SQLITE_TEST_RUNNER_SOURCES})
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlite>
    PARENT_SCOPE)
