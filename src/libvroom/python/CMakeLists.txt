cmake_minimum_required(VERSION 3.15)
project(vroom_csv_python LANGUAGES CXX)

# =============================================================================
# Python bindings for libvroom using pybind11
# =============================================================================

# LLVM source-based coverage (for bindings.cpp coverage)
# Must be set BEFORE find_package(pybind11) to ensure flags are properly inherited
option(ENABLE_LLVM_COVERAGE "Enable LLVM source-based code coverage (requires Clang)" OFF)
if(ENABLE_LLVM_COVERAGE)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "LLVM source-based coverage requires Clang compiler")
    endif()
    message(STATUS "LLVM source-based coverage enabled for Python bindings")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-instr-generate")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fprofile-instr-generate")
endif()

# Find Python and pybind11
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# Set C++ standard (C++20 required for libvroom2)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build position-independent code for shared libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# =============================================================================
# SIMD compiler flags for Highway
# =============================================================================
include(CheckCXXCompilerFlag)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
    message(STATUS "Python bindings: Detected x86-64 architecture, enabling AVX2/SIMD flags")
    set(SIMD_FLAGS "")

    check_cxx_compiler_flag("-mavx2" HAVE_AVX2)
    if(HAVE_AVX2)
        list(APPEND SIMD_FLAGS "-mavx2")
    endif()

    check_cxx_compiler_flag("-mfma" HAVE_FMA)
    if(HAVE_FMA)
        list(APPEND SIMD_FLAGS "-mfma")
    endif()

    check_cxx_compiler_flag("-mbmi" HAVE_BMI)
    if(HAVE_BMI)
        list(APPEND SIMD_FLAGS "-mbmi")
    endif()

    check_cxx_compiler_flag("-mbmi2" HAVE_BMI2)
    if(HAVE_BMI2)
        list(APPEND SIMD_FLAGS "-mbmi2")
    endif()

    check_cxx_compiler_flag("-mf16c" HAVE_F16C)
    if(HAVE_F16C)
        list(APPEND SIMD_FLAGS "-mf16c")
    endif()

    check_cxx_compiler_flag("-maes" HAVE_AES)
    if(HAVE_AES)
        list(APPEND SIMD_FLAGS "-maes")
    endif()

    check_cxx_compiler_flag("-mpclmul" HAVE_PCLMUL)
    if(HAVE_PCLMUL)
        list(APPEND SIMD_FLAGS "-mpclmul")
    endif()

    if(SIMD_FLAGS)
        message(STATUS "Python bindings: Enabling SIMD flags: ${SIMD_FLAGS}")
        add_compile_options(${SIMD_FLAGS})
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    message(STATUS "Python bindings: Detected ARM64 architecture, NEON is enabled by default")
endif()

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

# =============================================================================
# Dependencies (fetched via FetchContent)
# =============================================================================
include(FetchContent)

# Google Highway for portable SIMD
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

# fast_float - SIMD float parsing
FetchContent_Declare(
    fast_float
    GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
    GIT_TAG v6.1.1
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(fast_float)

# simdutf - SIMD UTF-8 validation
FetchContent_Declare(
    simdutf
    GIT_REPOSITORY https://github.com/simdutf/simdutf.git
    GIT_TAG v5.5.0
    GIT_SHALLOW TRUE
)
set(SIMDUTF_TESTS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_TOOLS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_BENCHMARKS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(simdutf)

# BS::thread_pool - Lightweight thread pool
FetchContent_Declare(
    thread_pool
    GIT_REPOSITORY https://github.com/bshoshany/thread-pool.git
    GIT_TAG v4.1.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(thread_pool)

# System compression libraries
find_package(ZLIB REQUIRED)

# On macOS, add Homebrew paths for finding compression libraries
if(APPLE)
    # Homebrew on Apple Silicon
    if(EXISTS "/opt/homebrew")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/homebrew/lib")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/homebrew/include")
    endif()
    # Homebrew on Intel Macs
    if(EXISTS "/usr/local/Cellar")
        list(APPEND CMAKE_PREFIX_PATH "/usr/local")
        list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib")
        list(APPEND CMAKE_INCLUDE_PATH "/usr/local/include")
    endif()
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD libzstd)
    pkg_check_modules(LZ4 liblz4)
    pkg_check_modules(SNAPPY snappy)
endif()
if(NOT ZSTD_FOUND)
    find_library(ZSTD_LIBRARIES zstd)
    find_path(ZSTD_INCLUDE_DIRS zstd.h)
    if(ZSTD_LIBRARIES AND ZSTD_INCLUDE_DIRS)
        set(ZSTD_FOUND TRUE)
    endif()
endif()
if(NOT LZ4_FOUND)
    find_library(LZ4_LIBRARIES lz4)
    find_path(LZ4_INCLUDE_DIRS lz4.h)
    if(LZ4_LIBRARIES AND LZ4_INCLUDE_DIRS)
        set(LZ4_FOUND TRUE)
    endif()
endif()
if(NOT SNAPPY_FOUND)
    find_library(SNAPPY_LIBRARIES snappy)
    find_path(SNAPPY_INCLUDE_DIRS snappy.h)
    if(SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIRS)
        set(SNAPPY_FOUND TRUE)
    endif()
endif()

# =============================================================================
# libvroom source files (libvroom2 architecture)
# =============================================================================
set(LIBVROOM_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/..")
set(VROOM_SOURCES
    ${LIBVROOM_ROOT}/src/encoding.cpp
    ${LIBVROOM_ROOT}/src/error.cpp
    ${LIBVROOM_ROOT}/src/io_util.cpp
    ${LIBVROOM_ROOT}/src/dialect.cpp
    ${LIBVROOM_ROOT}/src/convert.cpp
    ${LIBVROOM_ROOT}/src/table.cpp
    ${LIBVROOM_ROOT}/src/reader/mmap_source.cpp
    ${LIBVROOM_ROOT}/src/reader/csv_reader.cpp
    ${LIBVROOM_ROOT}/src/parser/chunk_finder.cpp
    ${LIBVROOM_ROOT}/src/parser/simd_chunk_finder.cpp
    ${LIBVROOM_ROOT}/src/parser/line_parser.cpp
    ${LIBVROOM_ROOT}/src/parser/split_fields.cpp
    ${LIBVROOM_ROOT}/src/parser/split_fields_iter.cpp
    ${LIBVROOM_ROOT}/src/parser/quote_parity.cpp
    ${LIBVROOM_ROOT}/src/parser/simd_atoi.cpp
    ${LIBVROOM_ROOT}/src/schema/type_inference.cpp
    ${LIBVROOM_ROOT}/src/schema/type_parsers.cpp
    ${LIBVROOM_ROOT}/src/columns/column_builder.cpp
    ${LIBVROOM_ROOT}/src/encoding/plain.cpp
    ${LIBVROOM_ROOT}/src/encoding/rle.cpp
    ${LIBVROOM_ROOT}/src/encoding/delta_bitpacked.cpp
    ${LIBVROOM_ROOT}/src/encoding/delta_length.cpp
    ${LIBVROOM_ROOT}/src/encoding/hybrid_rle.cpp
    ${LIBVROOM_ROOT}/src/writer/thrift_compact.cpp
    ${LIBVROOM_ROOT}/src/writer/parquet_types.cpp
    ${LIBVROOM_ROOT}/src/writer/parquet_file.cpp
    ${LIBVROOM_ROOT}/src/writer/row_group.cpp
    ${LIBVROOM_ROOT}/src/writer/column_writer.cpp
    ${LIBVROOM_ROOT}/src/writer/page_writer.cpp
    ${LIBVROOM_ROOT}/src/writer/compression.cpp
    ${LIBVROOM_ROOT}/src/writer/statistics.cpp
    ${LIBVROOM_ROOT}/src/writer/dictionary.cpp
    ${LIBVROOM_ROOT}/src/simd/statistics_simd.cpp
    ${LIBVROOM_ROOT}/src/writer/arrow_ipc_writer.cpp
    ${LIBVROOM_ROOT}/src/cache/index_cache.cpp
)

# Build libvroom as a static library for embedding
add_library(vroom_static STATIC ${VROOM_SOURCES})
target_include_directories(vroom_static PUBLIC
    ${LIBVROOM_ROOT}/include
    ${LIBVROOM_ROOT}/include/vroom
)
# Add source directory for Highway's foreach_target.h self-inclusion pattern
# The -inl.h files use paths like "parser/simd_atoi-inl.h" relative to src/
target_include_directories(vroom_static PRIVATE
    ${LIBVROOM_ROOT}
    ${LIBVROOM_ROOT}/src
    ${thread_pool_SOURCE_DIR}/include
)
target_link_libraries(vroom_static PUBLIC
    hwy
    fast_float
    simdutf
    ZLIB::ZLIB
)
# Optional compression libraries
if(ZSTD_FOUND)
    if(ZSTD_LIBRARY_DIRS)
        target_link_directories(vroom_static PUBLIC ${ZSTD_LIBRARY_DIRS})
    endif()
    target_link_libraries(vroom_static PUBLIC ${ZSTD_LIBRARIES})
    target_include_directories(vroom_static PRIVATE ${ZSTD_INCLUDE_DIRS})
    target_compile_definitions(vroom_static PUBLIC VROOM_HAVE_ZSTD)
endif()
if(LZ4_FOUND)
    if(LZ4_LIBRARY_DIRS)
        target_link_directories(vroom_static PUBLIC ${LZ4_LIBRARY_DIRS})
    endif()
    target_link_libraries(vroom_static PUBLIC ${LZ4_LIBRARIES})
    target_include_directories(vroom_static PRIVATE ${LZ4_INCLUDE_DIRS})
    target_compile_definitions(vroom_static PRIVATE VROOM_HAVE_LZ4)
endif()
if(SNAPPY_FOUND)
    if(SNAPPY_LIBRARY_DIRS)
        target_link_directories(vroom_static PUBLIC ${SNAPPY_LIBRARY_DIRS})
    endif()
    target_link_libraries(vroom_static PUBLIC ${SNAPPY_LIBRARIES})
    target_include_directories(vroom_static PRIVATE ${SNAPPY_INCLUDE_DIRS})
    target_compile_definitions(vroom_static PRIVATE VROOM_HAVE_SNAPPY)
endif()
set_target_properties(vroom_static PROPERTIES POSITION_INDEPENDENT_CODE ON)

# =============================================================================
# Python extension module
# =============================================================================

pybind11_add_module(_core MODULE
    src/bindings.cpp
)

target_link_libraries(_core PRIVATE vroom_static)
target_include_directories(_core PRIVATE
    ${LIBVROOM_ROOT}/include
    ${LIBVROOM_ROOT}/include/vroom
)

# Compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

# LLVM coverage: explicitly add flags to _core target when coverage is requested
# This ensures flags are applied even when CMAKE_CXX_FLAGS is set via toolchain file
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if(ENABLE_LLVM_COVERAGE OR CMAKE_CXX_FLAGS MATCHES "fprofile-instr-generate")
        message(STATUS "Adding LLVM coverage flags to _core target")
        target_compile_options(_core PRIVATE -fprofile-instr-generate -fcoverage-mapping)
        target_link_options(_core PRIVATE -fprofile-instr-generate)
        # Also add to vroom_static since _core links against it
        target_compile_options(vroom_static PRIVATE -fprofile-instr-generate -fcoverage-mapping)
    endif()
endif()

# Install the extension module into vroom_csv package directory
install(TARGETS _core LIBRARY DESTINATION vroom_csv COMPONENT python)
