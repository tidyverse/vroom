cmake_minimum_required(VERSION 3.15)
project(vroom_csv_python LANGUAGES CXX)

# =============================================================================
# Python bindings for libvroom using pybind11
# =============================================================================

# Find Python and pybind11
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build position-independent code for shared libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# SIMD dispatch option - default to dynamic for runtime CPU detection
# This allows wheels to work across different CPU generations
option(HWY_COMPILE_ONLY_STATIC "Disable dynamic SIMD dispatch" OFF)
if(HWY_COMPILE_ONLY_STATIC)
    message(STATUS "Static SIMD dispatch enabled for Python bindings")
    add_compile_definitions(HWY_COMPILE_ONLY_STATIC)
else()
    message(STATUS "Dynamic SIMD dispatch enabled for Python bindings")
endif()

# =============================================================================
# Build libvroom as a static library (embedded in the Python extension)
# =============================================================================

# Fetch Google Highway (required by libvroom)
include(FetchContent)
FetchContent_Declare(
    highway
    GIT_REPOSITORY https://github.com/google/highway.git
    GIT_TAG 1.3.0
)
set(HWY_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_CONTRIB OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS_SAVED ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(highway)
set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_SAVED} CACHE BOOL "" FORCE)

# libvroom source files (relative to parent directory)
set(LIBVROOM_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")
set(VROOM_SOURCES
    ${LIBVROOM_ROOT}/src/io_util.cpp
    ${LIBVROOM_ROOT}/src/error.cpp
    ${LIBVROOM_ROOT}/src/dialect.cpp
    ${LIBVROOM_ROOT}/src/encoding.cpp
    ${LIBVROOM_ROOT}/src/index_cache.cpp
    ${LIBVROOM_ROOT}/src/libvroom_c.cpp
    ${LIBVROOM_ROOT}/src/mmap_util.cpp
    ${LIBVROOM_ROOT}/src/value_extraction.cpp
    ${LIBVROOM_ROOT}/src/streaming.cpp
    ${LIBVROOM_ROOT}/src/two_pass.cpp
    ${LIBVROOM_ROOT}/src/branchless_state_machine.cpp
    ${LIBVROOM_ROOT}/src/simd_number_parsing.cpp
    ${LIBVROOM_ROOT}/src/utf8.cpp
    ${LIBVROOM_ROOT}/src/libvroom_types.cpp
)

# Add SIMD dispatch source for dynamic dispatch (when HWY_COMPILE_ONLY_STATIC is OFF)
if(NOT HWY_COMPILE_ONLY_STATIC)
    message(STATUS "Dynamic SIMD dispatch enabled for Python bindings")
    list(APPEND VROOM_SOURCES ${LIBVROOM_ROOT}/src/simd_dispatch.cpp)
endif()

# Build libvroom as a static library for embedding
add_library(vroom_static STATIC ${VROOM_SOURCES})
target_include_directories(vroom_static PUBLIC ${LIBVROOM_ROOT}/include)
# Add source directory for Highway's foreach_target.h self-inclusion pattern
target_include_directories(vroom_static PRIVATE ${LIBVROOM_ROOT})
target_link_libraries(vroom_static PUBLIC hwy)
set_target_properties(vroom_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =============================================================================
# Python extension module
# =============================================================================

pybind11_add_module(_core MODULE
    src/bindings.cpp
)

target_link_libraries(_core PRIVATE vroom_static)
target_include_directories(_core PRIVATE ${LIBVROOM_ROOT}/include)

# Compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Install the extension module into vroom_csv package directory
install(TARGETS _core LIBRARY DESTINATION vroom_csv COMPONENT python)
