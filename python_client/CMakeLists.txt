cmake_minimum_required(VERSION 3.30)

if (NOT DEFINED SKBUILD_PROJECT_NAME)
    message (FATAL_ERROR "Must configure through scikit-build-core, please run uv sync / pip install")
endif ()

project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

message (STATUS "Building ${SKBUILD_PROJECT_NAME} ${SKBUILD_PROJECT_VERSION_FULL} (${SKBUILD_STATE})")

# We'll create a private package
set (PYPACKAGE_NAME "_${SKBUILD_PROJECT_NAME}")
# By default assume we're building for the Stable ABI
set (NANOBIND_SABI STABLE_ABI)

# Default to Release builds
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()
message (STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Set flags for Debug builds
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    message (STATUS "Enabling full debug info")
    # set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -fsanitize=address" CACHE STRING "" FORCE)
    set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0" CACHE STRING "" FORCE)
    # set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -fsanitize=address" CACHE STRING "" FORCE)
    set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g" CACHE STRING "" FORCE)
    set (CMAKE_MACOSX_RPATH 1)
    set (CMAKE_OSX_DEPLOYMENT_TARGET "" CACHE STRING "" FORCE) # avoid stripping
    set (CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)
    set (CMAKE_STRIP FALSE)
    # Don't do LTO
    set (NANOBIND_LTO "")
endif ()

# Find Python
find_package(Python 3.12 COMPONENTS Interpreter REQUIRED)
if (Python_SOABI MATCHES "td-" OR Python_SOABI MATCHES "t-")
    # Free-threading Python detected (has 't' in SOABI like "cpython-313t-darwin")
    message (STATUS "Detected free-threading Python ${Python3_VERSION} - building without Limited API")
    find_package (Python 3.12 REQUIRED COMPONENTS Development.Module)
    # Don't build for the Stable ABI
    set (NANOBIND_SABI "")
else ()
    # Regular Python - always use Limited API
    message (STATUS "Detected regular Python ${Python3_VERSION} - building with Limited API")
    find_package(Python 3.12 REQUIRED COMPONENTS Development.Module Development.SABIModule)
endif ()

# Detect the installed nanobind package and import it into CMake
execute_process(
        COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

# Create module
nanobind_add_module(
        ${PYPACKAGE_NAME}
        NB_STATIC FREE_THREADED LTO ${NANOBIND_SABI}
        src/duckdb.cpp
)

# If we're not on Python FT we enable the Stable ABI for Python >= 3.12
if (NOT "${NANOBIND_SABI}" STREQUAL "")
    target_compile_definitions (${PYPACKAGE_NAME} PRIVATE Py_LIMITED_API=0x030C0000)
endif ()

# Make sure we create the correct extension on all platforms
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set_property (TARGET ${PYPACKAGE_NAME} PROPERTY SUFFIX ".${SKBUILD_SOABI}.pyd")
else()
    set_property (TARGET ${PYPACKAGE_NAME} PROPERTY SUFFIX ".${SKBUILD_SOABI}${CMAKE_SHARED_MODULE_SUFFIX}")
endif()

# Install
install (TARGETS ${PYPACKAGE_NAME} DESTINATION ${SKBUILD_PROJECT_NAME})
