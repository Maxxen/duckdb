cmake_minimum_required(VERSION 2.8.12...3.29)

project(demo_v2_extension)

include_directories(include)
# Configure extension: the minimum CAPI version where the extension still
# compiles should be chosen here
set(EXTENSION_NAME demo_v2)
set(EXTENSION_FILES demo_v2_extension.cpp)

build_static_extension_capi_v2_unstable(${EXTENSION_NAME} ${EXTENSION_FILES})
target_link_libraries(${EXTENSION_NAME}_extension duckdb_cpp_api)

build_loadable_extension_capi_v2_unstable(${EXTENSION_NAME} ${EXTENSION_FILES})
target_link_libraries(${EXTENSION_NAME}_loadable_extension
                      duckdb_cpp_api_loadable)
