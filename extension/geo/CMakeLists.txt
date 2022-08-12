cmake_minimum_required(VERSION 2.8.12)

project(GeoExtension)

include_directories(include)
include_directories(third_party/geos/include)
add_subdirectory(third_party/geos)

add_subdirectory(geo_functions)

set(GEO_EXTENSION_FILES
    geo-extension.cpp
    geo_common.cpp
    geo_types.cpp)

file(GLOB GEO_EXTENSION_FUNCTION_FILES geo_functions/*.cpp)

add_library(geo_extension STATIC ${GEO_EXTENSION_FILES} ${GEO_EXTENSION_FUNCTION_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(geo geo-extension.cpp)

install(
  TARGETS geo_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
