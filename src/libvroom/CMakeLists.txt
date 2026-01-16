cmake_minimum_required(VERSION 3.14)
project(libvroom VERSION 0.1.0 LANGUAGES CXX C)

# =============================================================================
# Compiler cache (ccache) for faster rebuilds
# =============================================================================
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    message(STATUS "Found ccache: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
else()
    message(STATUS "ccache not found - install with: sudo apt install ccache (or brew install ccache)")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(BUILD_TESTING "Build test executables" ON)
option(BUILD_BENCHMARKS "Build benchmark executables" ON)
option(BUILD_SHARED_LIBS "Build shared library instead of static" OFF)

# Code coverage options
option(ENABLE_COVERAGE "Enable code coverage reporting (gcov-compatible)" OFF)
option(ENABLE_LLVM_COVERAGE "Enable LLVM source-based code coverage (requires Clang)" OFF)

# Sanitizer options
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# SIMD dispatch option (issue #455)
option(HWY_COMPILE_ONLY_STATIC "Disable dynamic SIMD dispatch, compile for build machine only" OFF)
if(HWY_COMPILE_ONLY_STATIC)
    message(STATUS "Static SIMD dispatch enabled (compile for build machine only)")
    add_compile_definitions(HWY_COMPILE_ONLY_STATIC)
else()
    message(STATUS "Dynamic SIMD dispatch enabled (runtime CPU detection)")
endif()

# Compiler flags
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g)
else()
    add_compile_options(-O3)
endif()

# =============================================================================
# SIMD compiler flags for Highway (issue #443)
# =============================================================================
# Highway requires specific compiler flags to detect and use advanced SIMD
# instructions. Without these flags, Highway falls back to SSE2 which is
# ~10x slower than AVX2/AVX-512.
#
# Required flags for x86-64:
# - -mavx2: AVX2 instructions (256-bit vectors)
# - -mfma: Fused multiply-add (required for many AVX2 operations)
# - -mbmi -mbmi2: Bit manipulation instructions (used by Highway)
# - -mf16c: Half-precision float conversion
# - -maes -mpclmul: AES/carry-less multiply (required for Highway baseline detection)
#
# See: https://github.com/google/highway/blob/master/g3doc/quick_reference.md
# =============================================================================
include(CheckCXXCompilerFlag)

# Detect target architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
    message(STATUS "Detected x86-64 architecture, enabling AVX2/SIMD flags for Highway")

    # Check which flags are supported by the compiler
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

    # These are critical for Highway SIMD baseline detection
    check_cxx_compiler_flag("-maes" HAVE_AES)
    if(HAVE_AES)
        list(APPEND SIMD_FLAGS "-maes")
    endif()

    check_cxx_compiler_flag("-mpclmul" HAVE_PCLMUL)
    if(HAVE_PCLMUL)
        list(APPEND SIMD_FLAGS "-mpclmul")
    endif()

    if(SIMD_FLAGS)
        message(STATUS "Enabling SIMD flags: ${SIMD_FLAGS}")
        add_compile_options(${SIMD_FLAGS})
    else()
        message(WARNING "No SIMD flags supported - Highway may fall back to SSE2 (slow)")
    endif()

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    message(STATUS "Detected ARM64 architecture, NEON is enabled by default")
    # ARM NEON is typically enabled by default on aarch64
else()
    message(STATUS "Unknown architecture: ${CMAKE_SYSTEM_PROCESSOR}, relying on Highway defaults")
endif()

# Code coverage flags
if(ENABLE_COVERAGE)
    message(STATUS "Code coverage enabled (gcov-compatible)")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # Use atomic profile updates for thread-safe coverage in multi-threaded code
        add_compile_options(--coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic)
        add_link_options(--coverage)
    else()
        message(WARNING "Code coverage not supported for compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif()
endif()

# LLVM source-based coverage (provides better header file coverage attribution)
# See: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html
if(ENABLE_LLVM_COVERAGE)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "LLVM source-based coverage requires Clang compiler")
    endif()
    message(STATUS "LLVM source-based coverage enabled")
    # -fprofile-instr-generate: Enables execution count collection
    # -fcoverage-mapping: Enables mapping information in generated code
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate)
endif()

# Sanitizer flags
if(ENABLE_ASAN OR ENABLE_UBSAN OR ENABLE_TSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # Common sanitizer flags
        add_compile_options(-fno-omit-frame-pointer -g)

        if(ENABLE_ASAN)
            message(STATUS "AddressSanitizer enabled")
            add_compile_options(-fsanitize=address)
            add_link_options(-fsanitize=address)
        endif()

        if(ENABLE_UBSAN)
            message(STATUS "UndefinedBehaviorSanitizer enabled")
            add_compile_options(-fsanitize=undefined)
            add_link_options(-fsanitize=undefined)
        endif()

        if(ENABLE_TSAN)
            message(STATUS "ThreadSanitizer enabled")
            add_compile_options(-fsanitize=thread)
            add_link_options(-fsanitize=thread)
        endif()
    else()
        message(WARNING "Sanitizers not supported for compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif()
endif()

# Dependencies
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
FetchContent_MakeAvailable(highway)

# Google Benchmark for performance testing (only if benchmarks enabled)
if(BUILD_BENCHMARKS)
    FetchContent_Declare(
      benchmark
      GIT_REPOSITORY https://github.com/google/benchmark.git
      GIT_TAG v1.8.3
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_DOXYGEN OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)
endif()

# External CSV parser benchmarks (optional, only relevant if benchmarks enabled)
if(BUILD_BENCHMARKS)
    option(ENABLE_DUCKDB_BENCHMARK "Enable DuckDB CSV parser benchmarks" OFF)
    option(ENABLE_ZSV_BENCHMARK "Enable zsv CSV parser benchmarks" OFF)
    option(ENABLE_ARROW_BENCHMARK "Enable Apache Arrow CSV parser benchmarks" OFF)
endif()

# Apache Arrow output integration (optional)
option(LIBVROOM_ENABLE_ARROW "Enable Apache Arrow output integration" OFF)

# Type detection module (optional, but ON by default for backward compatibility)
option(LIBVROOM_ENABLE_TYPE_DETECTION "Enable type detection module (libvroom_types.h)" ON)

if(LIBVROOM_ENABLE_TYPE_DETECTION)
    message(STATUS "Type detection module enabled")
else()
    message(STATUS "Type detection module disabled (libvroom_types.h excluded)")
endif()

# External benchmark parsers (only built if benchmarks enabled)
if(BUILD_BENCHMARKS)
    # zsv integration (lightweight, SIMD CSV parser)
    # Note: zsv uses Makefile, so we download source only and build it as part of our project
    if(ENABLE_ZSV_BENCHMARK)
      message(STATUS "Enabling zsv CSV parser benchmarks")

      # Download zsv source - use FetchContent_Populate manually to avoid CMake configure
      set(ZSV_SOURCE_DIR "${CMAKE_BINARY_DIR}/_deps/zsv-src")
      set(ZSV_DOWNLOAD_DIR "${CMAKE_BINARY_DIR}/_deps/zsv-subbuild")

      if(NOT EXISTS "${ZSV_SOURCE_DIR}/src/zsv.c")
        message(STATUS "Downloading zsv...")
        file(MAKE_DIRECTORY ${ZSV_DOWNLOAD_DIR})
        execute_process(
          COMMAND ${CMAKE_COMMAND} -E env GIT_TERMINAL_PROMPT=0
                  git clone --depth 1 --branch v1.3.0
                  https://github.com/liquidaty/zsv.git
                  ${ZSV_SOURCE_DIR}
          RESULT_VARIABLE GIT_RESULT
        )
        if(NOT GIT_RESULT EQUAL 0)
          message(FATAL_ERROR "Failed to clone zsv repository")
        endif()
      endif()

      # Generate zsv.h from zsv.h.in (zsv uses sed to do this in their Makefile)
      if(NOT EXISTS "${ZSV_SOURCE_DIR}/include/zsv.h")
        message(STATUS "Generating zsv.h...")
        file(READ "${ZSV_SOURCE_DIR}/include/zsv.h.in" ZSV_H_IN)
        # Remove the __ZSV_EXTRAS__DEFINE__ placeholder (we don't need ZSV_EXTRAS)
        string(REPLACE "__ZSV_EXTRAS__DEFINE__" "" ZSV_H_OUT "${ZSV_H_IN}")
        file(WRITE "${ZSV_SOURCE_DIR}/include/zsv.h" "${ZSV_H_OUT}")
      endif()

      # Build zsv as a static library from source
      add_library(zsv_lib STATIC
        ${ZSV_SOURCE_DIR}/src/zsv.c
      )
      target_include_directories(zsv_lib PUBLIC
        ${ZSV_SOURCE_DIR}/include
      )
      # zsv source files use relative includes, need to add include dir
      target_include_directories(zsv_lib PRIVATE
        ${ZSV_SOURCE_DIR}/include/zsv
      )
      # zsv uses C11/GNU11
      set_target_properties(zsv_lib PROPERTIES
        C_STANDARD 11
        C_STANDARD_REQUIRED ON
      )
      # Add compiler flags matching zsv's Makefile
      target_compile_options(zsv_lib PRIVATE
        -O3
        -fsigned-char
        -DNDEBUG
        -fPIC
      )
      # Suppress some warnings from zsv code
      target_compile_options(zsv_lib PRIVATE
        -Wno-missing-braces
        -Wno-gnu-statement-expression
      )

      set(HAVE_ZSV TRUE)
      set(ZSV_INCLUDE_DIR ${ZSV_SOURCE_DIR}/include)
    endif()

    # DuckDB integration (full database with CSV reader)
    if(ENABLE_DUCKDB_BENCHMARK)
      message(STATUS "Enabling DuckDB CSV parser benchmarks (this may take a while to build)")
      FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG v1.1.3
        GIT_SHALLOW TRUE
      )
      set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
      set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
      set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
      set(BUILD_PARQUET_EXTENSION OFF CACHE BOOL "" FORCE)
      set(BUILD_HTTPFS_EXTENSION OFF CACHE BOOL "" FORCE)
      set(BUILD_JSON_EXTENSION OFF CACHE BOOL "" FORCE)
      FetchContent_MakeAvailable(duckdb)
      set(HAVE_DUCKDB TRUE)
    endif()

    # Apache Arrow integration (columnar CSV reader)
    if(ENABLE_ARROW_BENCHMARK)
      message(STATUS "Enabling Apache Arrow CSV parser benchmarks")
      find_package(Arrow REQUIRED)
      set(HAVE_ARROW TRUE)
    endif()
endif()

# Library target (SHARED or STATIC based on BUILD_SHARED_LIBS)
set(VROOM_SOURCES
    src/io_util.cpp
    src/error.cpp
    src/dialect.cpp
    src/encoding.cpp
    src/index_cache.cpp
    src/libvroom_c.cpp
    src/mmap_util.cpp
    src/value_extraction.cpp
    src/streaming.cpp
    src/two_pass.cpp
    src/branchless_state_machine.cpp
    src/simd_number_parsing.cpp
    src/utf8.cpp
)

# Add SIMD dispatch source for dynamic dispatch (runtime CPU detection)
if(NOT HWY_COMPILE_ONLY_STATIC)
    list(APPEND VROOM_SOURCES src/simd_dispatch.cpp)
endif()

# Add type detection source if enabled
if(LIBVROOM_ENABLE_TYPE_DETECTION)
    list(APPEND VROOM_SOURCES src/libvroom_types.cpp)
endif()

add_library(vroom ${VROOM_SOURCES})

target_include_directories(vroom PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Add source directory for Highway's foreach_target.h self-inclusion pattern
target_include_directories(vroom PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(vroom PUBLIC
    hwy
)

# Optional Apache Arrow output integration
include(cmake/ArrowIntegration.cmake)

# CLI utility (user-friendly CSV tool)
add_executable(vroom-cli
    src/cli.cpp
)

# Set output binary name to 'vroom' (target name is 'vroom-cli' to avoid conflict with library)
set_target_properties(vroom-cli PROPERTIES OUTPUT_NAME vroom)

target_link_libraries(vroom-cli PRIVATE
    vroom
    pthread
)

# =============================================================================
# Testing (optional, controlled by BUILD_TESTING)
# =============================================================================
if(BUILD_TESTING)
    enable_testing()

    # Download and configure Google Test
    FetchContent_Declare(
      googletest
      GIT_REPOSITORY https://github.com/google/googletest.git
      GIT_TAG v1.14.0
    )

    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    # =============================================================================
    # Test data copying - single target to avoid race conditions during parallel builds
    # All test targets depend on this target instead of each copying the data themselves.
    # See: https://github.com/jimhester/libvroom/issues/372
    # =============================================================================
    add_custom_target(copy_test_data ALL
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/test/data
        ${CMAKE_CURRENT_BINARY_DIR}/test/data
        COMMENT "Copying test data to build directory"
    )

# Test executable
add_executable(libvroom_test
    test/csv_parser_test.cpp
)

target_link_libraries(libvroom_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(libvroom_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(libvroom_test copy_test_data)

# Error handling test executable
add_executable(error_handling_test
    test/error_handling_test.cpp
)

target_link_libraries(error_handling_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(error_handling_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(error_handling_test copy_test_data)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(libvroom_test)
gtest_discover_tests(error_handling_test)

# CSV parsing test executable (portable SIMD via Highway)
add_executable(csv_parsing_test
    test/csv_parsing_test.cpp
)

target_link_libraries(csv_parsing_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(csv_parsing_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(csv_parsing_test copy_test_data)

# Register parsing tests with CTest
gtest_discover_tests(csv_parsing_test)

# CSV parser error detection test executable
add_executable(csv_parser_errors_test
    test/csv_parser_errors_test.cpp
)

target_link_libraries(csv_parser_errors_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(csv_parser_errors_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(csv_parser_errors_test copy_test_data)

# Register parser error tests with CTest
gtest_discover_tests(csv_parser_errors_test)

# Extended CSV test executable (additional coverage from zsv/duckdb)
add_executable(csv_extended_test
    test/csv_extended_test.cpp
)

target_link_libraries(csv_extended_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(csv_extended_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(csv_extended_test copy_test_data)

# Register extended tests with CTest
gtest_discover_tests(csv_extended_test)

# Dialect detection test executable
add_executable(dialect_detection_test
    test/dialect_detection_test.cpp
)

target_link_libraries(dialect_detection_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(dialect_detection_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(dialect_detection_test copy_test_data)

# Register dialect detection tests with CTest
gtest_discover_tests(dialect_detection_test)

# Type detection test executable (only if type detection is enabled)
if(LIBVROOM_ENABLE_TYPE_DETECTION)
    add_executable(type_detection_test
        test/type_detection_test.cpp
    )

    target_link_libraries(type_detection_test PRIVATE
        vroom
        GTest::gtest_main
        pthread
    )

    target_include_directories(type_detection_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )

    add_dependencies(type_detection_test copy_test_data)

    # Register type detection tests with CTest
    gtest_discover_tests(type_detection_test)
endif()

# C API test executable
add_executable(c_api_test
    test/c_api_test.cpp
)

target_link_libraries(c_api_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(c_api_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(c_api_test copy_test_data)

# Register C API tests with CTest
gtest_discover_tests(c_api_test)

# Debug mode test executable
add_executable(debug_test
    test/debug_test.cpp
)

target_link_libraries(debug_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(debug_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Register debug tests with CTest
gtest_discover_tests(debug_test)

# Simplified API test executable
add_executable(api_test
    test/api_test.cpp
)

target_link_libraries(api_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(api_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(api_test copy_test_data)

# Register API tests with CTest
gtest_discover_tests(api_test)

# Quote mask unit tests (verifies PCLMULQDQ/PMULL implementation)
add_executable(quote_mask_test
    test/quote_mask_test.cpp
)

target_link_libraries(quote_mask_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(quote_mask_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

gtest_discover_tests(quote_mask_test)

# Value extraction test executable
add_executable(value_extraction_test
    test/value_extraction_test.cpp
)

target_link_libraries(value_extraction_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(value_extraction_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(value_extraction_test copy_test_data)

# Register value extraction tests with CTest
gtest_discover_tests(value_extraction_test)

# Streaming parser test executable
add_executable(streaming_test
    test/streaming_test.cpp
)

target_link_libraries(streaming_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(streaming_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(streaming_test copy_test_data)

# Register streaming tests with CTest
gtest_discover_tests(streaming_test)

# Branchless state machine test executable
add_executable(branchless_test
    test/branchless_test.cpp
)

target_link_libraries(branchless_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(branchless_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(branchless_test copy_test_data)

# Register branchless tests with CTest
gtest_discover_tests(branchless_test)

# SIMD error detection test executable (edge cases from issue #383)
add_executable(simd_error_detection_test
    test/simd_error_detection_test.cpp
)

target_link_libraries(simd_error_detection_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(simd_error_detection_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(simd_error_detection_test copy_test_data)

# Register SIMD error detection tests with CTest
gtest_discover_tests(simd_error_detection_test)

# Size limits test executable (security feature from issue #270)
add_executable(size_limits_test
    test/size_limits_test.cpp
)

target_link_libraries(size_limits_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(size_limits_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Register size limits tests with CTest
gtest_discover_tests(size_limits_test)

# SIMD number parsing test executable
add_executable(simd_number_parsing_test
    test/simd_number_parsing_test.cpp
)

target_link_libraries(simd_number_parsing_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(simd_number_parsing_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(simd_number_parsing_test copy_test_data)

# Register SIMD number parsing tests with CTest
gtest_discover_tests(simd_number_parsing_test)

# IO utility test executable
add_executable(io_util_test
    test/io_util_test.cpp
)

target_link_libraries(io_util_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(io_util_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(io_util_test copy_test_data)

# Register IO utility tests with CTest
gtest_discover_tests(io_util_test)

# CLI integration test executable
add_executable(cli_test
    test/cli_test.cpp
)

target_link_libraries(cli_test PRIVATE
    GTest::gtest_main
    pthread
)

target_include_directories(cli_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# CLI test depends on vroom executable being built and test data
add_dependencies(cli_test vroom-cli copy_test_data)

# Register CLI tests with CTest
gtest_discover_tests(cli_test)

# Two-pass coverage test executable (comprehensive tests for two_pass.h)
add_executable(two_pass_coverage_test
    test/two_pass_coverage_test.cpp
)

target_link_libraries(two_pass_coverage_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(two_pass_coverage_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(two_pass_coverage_test copy_test_data)

# Register two-pass coverage tests with CTest
gtest_discover_tests(two_pass_coverage_test)

# Encoding detection and transcoding test executable
add_executable(encoding_test
    test/encoding_test.cpp
)

target_link_libraries(encoding_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(encoding_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(encoding_test copy_test_data)

# Register encoding tests with CTest
gtest_discover_tests(encoding_test)

# UTF-8 utilities test executable
add_executable(utf8_test
    test/utf8_test.cpp
)

target_link_libraries(utf8_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(utf8_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Register UTF-8 tests with CTest
gtest_discover_tests(utf8_test)

# Bounds validation test executable (tests assertions from PR #123)
add_executable(bounds_validation_test
    test/bounds_validation_test.cpp
)

target_link_libraries(bounds_validation_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(bounds_validation_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Pass type detection enable flag to bounds validation test
if(LIBVROOM_ENABLE_TYPE_DETECTION)
    target_compile_definitions(bounds_validation_test PRIVATE LIBVROOM_ENABLE_TYPE_DETECTION)
endif()

add_dependencies(bounds_validation_test copy_test_data)

# Register bounds validation tests with CTest
gtest_discover_tests(bounds_validation_test)

# End-to-end integration tests
add_executable(integration_test
    test/integration_test.cpp
)

target_link_libraries(integration_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(integration_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(integration_test copy_test_data)

# Register integration tests with CTest
gtest_discover_tests(integration_test)

# Concurrency and thread safety test executable
add_executable(concurrency_test
    test/concurrency_test.cpp
)

target_link_libraries(concurrency_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(concurrency_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(concurrency_test copy_test_data)

# Register concurrency tests with CTest
gtest_discover_tests(concurrency_test)

# Comment line skipping test executable
add_executable(comment_line_test
    test/comment_line_test.cpp
)

target_link_libraries(comment_line_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(comment_line_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(comment_line_test copy_test_data)

# Register comment line tests with CTest
gtest_discover_tests(comment_line_test)

# Memory-mapped file utilities test executable
add_executable(mmap_util_test
    test/mmap_util_test.cpp
)

target_link_libraries(mmap_util_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(mmap_util_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(mmap_util_test copy_test_data)

# Register mmap utility tests with CTest
gtest_discover_tests(mmap_util_test)

# Index cache test executable
add_executable(index_cache_test
    test/index_cache_test.cpp
)

target_link_libraries(index_cache_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)

target_include_directories(index_cache_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_dependencies(index_cache_test copy_test_data)

# Register index cache tests with CTest
gtest_discover_tests(index_cache_test)

# Arrow output test (only if Arrow is enabled)
if(LIBVROOM_ENABLE_ARROW)
    add_executable(arrow_output_test test/arrow_output_test.cpp)
    target_link_libraries(arrow_output_test PRIVATE vroom GTest::gtest_main pthread)
    target_include_directories(arrow_output_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    add_dependencies(arrow_output_test copy_test_data)
    gtest_discover_tests(arrow_output_test)

    # Arrow file-based tests (loading real CSV files)
    add_executable(arrow_file_test test/arrow_file_test.cpp)
    target_link_libraries(arrow_file_test PRIVATE vroom GTest::gtest_main pthread)
    target_include_directories(arrow_file_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    add_dependencies(arrow_file_test copy_test_data)
    gtest_discover_tests(arrow_file_test)
endif()

endif() # BUILD_TESTING

# =============================================================================
# Benchmarks (optional, controlled by BUILD_BENCHMARKS)
# =============================================================================
if(BUILD_BENCHMARKS)
    # Benchmark executable - base sources
set(BENCHMARK_SOURCES
    benchmark/benchmark_main.cpp
    benchmark/basic_benchmarks.cpp
    benchmark/comparison_benchmarks.cpp
    benchmark/real_world_benchmarks.cpp
    benchmark/dimensions_benchmarks.cpp
    benchmark/performance_metrics.cpp
    benchmark/simd_benchmarks.cpp
    benchmark/energy_benchmarks.cpp
    benchmark/number_parsing_benchmarks.cpp
    benchmark/error_collection_benchmark.cpp
    benchmark/parser_overhead_benchmarks.cpp
)

# Add external parser benchmarks if any external parser is enabled
if(HAVE_ZSV OR HAVE_DUCKDB OR HAVE_ARROW)
    list(APPEND BENCHMARK_SOURCES benchmark/external_parser_benchmarks.cpp)
endif()

# Add Arrow convert benchmarks when Arrow is enabled
if(LIBVROOM_ENABLE_ARROW)
    list(APPEND BENCHMARK_SOURCES benchmark/arrow_convert_benchmarks.cpp)
endif()

add_executable(libvroom_benchmark ${BENCHMARK_SOURCES})

target_link_libraries(libvroom_benchmark PRIVATE
    vroom
    benchmark::benchmark
    benchmark::benchmark_main
    pthread
)

target_include_directories(libvroom_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Define benchmark mode to suppress debug output during benchmarking
target_compile_definitions(libvroom_benchmark PRIVATE LIBVROOM_BENCHMARK_MODE)

# Pass type detection enable flag to benchmarks
if(LIBVROOM_ENABLE_TYPE_DETECTION)
    target_compile_definitions(libvroom_benchmark PRIVATE LIBVROOM_ENABLE_TYPE_DETECTION)
endif()

# Conditionally link external CSV parsers
if(HAVE_ZSV)
    target_link_libraries(libvroom_benchmark PRIVATE zsv_lib)
    target_compile_definitions(libvroom_benchmark PRIVATE HAVE_ZSV)
endif()

if(HAVE_DUCKDB)
    target_link_libraries(libvroom_benchmark PRIVATE duckdb)
    target_compile_definitions(libvroom_benchmark PRIVATE HAVE_DUCKDB)
endif()

if(HAVE_ARROW)
    target_link_libraries(libvroom_benchmark PRIVATE Arrow::arrow_shared)
    target_compile_definitions(libvroom_benchmark PRIVATE HAVE_ARROW)
endif()

# Copy test data to build directory for benchmarks
add_custom_command(TARGET libvroom_benchmark POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/benchmark/data
)

# Quote mask micro-benchmark (scalar vs CLMul comparison)
# This is a standalone benchmark for measuring the performance improvement
# of the PCLMULQDQ/PMULL-based quote mask implementation vs the scalar loop.
# Not part of the main benchmark suite - run separately with ./quote_mask_benchmark
add_executable(quote_mask_benchmark benchmark/quote_mask_benchmark.cpp)
target_link_libraries(quote_mask_benchmark PRIVATE
    hwy
    benchmark::benchmark
)
target_include_directories(quote_mask_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

endif() # BUILD_BENCHMARKS

# =============================================================================
# Fuzz Testing Support
# =============================================================================
option(ENABLE_FUZZING "Enable fuzz testing targets (requires Clang with libFuzzer)" OFF)

if(ENABLE_FUZZING)
    message(STATUS "Fuzz testing enabled")

    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "Fuzz testing requires Clang compiler")
    endif()

    set(FUZZ_FLAGS -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer -g)
    set(FUZZ_LINK_FLAGS -fsanitize=fuzzer,address,undefined)

    add_executable(fuzz_csv_parser fuzz/fuzz_csv_parser.cpp)
    target_link_libraries(fuzz_csv_parser PRIVATE vroom)
    target_include_directories(fuzz_csv_parser PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_csv_parser PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_csv_parser PRIVATE ${FUZZ_LINK_FLAGS})

    add_executable(fuzz_dialect_detection fuzz/fuzz_dialect_detection.cpp)
    target_link_libraries(fuzz_dialect_detection PRIVATE vroom)
    target_include_directories(fuzz_dialect_detection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_dialect_detection PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_dialect_detection PRIVATE ${FUZZ_LINK_FLAGS})

    add_executable(fuzz_parse_auto fuzz/fuzz_parse_auto.cpp)
    target_link_libraries(fuzz_parse_auto PRIVATE vroom)
    target_include_directories(fuzz_parse_auto PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_parse_auto PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_parse_auto PRIVATE ${FUZZ_LINK_FLAGS})

    add_custom_command(TARGET fuzz_csv_parser POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/test/data ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus
        COMMENT "Setting up fuzz corpus from test data"
    )
endif()
