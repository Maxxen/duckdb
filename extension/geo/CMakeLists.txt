cmake_minimum_required(VERSION 2.8.12)

project(GeoExtension)

include_directories(include)

add_library(geo_extension STATIC geo-extension.cpp)
set(INGORE_WARNINGS OFF)
build_loadable_extension(geo geo-extension.cpp)

install(
  TARGETS geo_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
