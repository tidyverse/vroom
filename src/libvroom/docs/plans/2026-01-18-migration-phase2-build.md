# Phase 2: Build System Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update CMakeLists.txt to build the new libvroom2 sources, remove old source files, and create the new API header.

**Context:** Phase 1 has copied all libvroom2 source files. Now we need to update the build system to compile them and remove the old files.

**Prerequisites:** Phase 1 (Core Parser Migration) must be complete.

**After completion:** Phases 3, 4, 5 can begin in parallel.

---

## Task 1: Update Main CMakeLists.txt

**Files:**
- Modify: `CMakeLists.txt`

**Step 1: Read current CMakeLists.txt to understand structure**

```bash
head -200 CMakeLists.txt
```

**Step 2: Update CMakeLists.txt**

Key changes needed:
1. Change `CMAKE_CXX_STANDARD` from 17 to 20
2. Replace VROOM_SOURCES with new file list
3. Add FetchContent for: fast_float, simdutf, BS::thread_pool
4. Add system dependencies: zstd, snappy (optional), lz4 (optional), zlib
5. Update include directories to add `include/vroom`
6. Remove Arrow-related optional dependencies

New source file list:
```cmake
set(VROOM_SOURCES
    src/reader/mmap_source.cpp
    src/reader/csv_reader.cpp
    src/parser/chunk_finder.cpp
    src/parser/simd_chunk_finder.cpp
    src/parser/line_parser.cpp
    src/parser/split_fields.cpp
    src/parser/quote_parity.cpp
    src/parser/simd_atoi.cpp
    src/schema/type_inference.cpp
    src/schema/type_parsers.cpp
    src/columns/column_builder.cpp
    src/encoding/plain.cpp
    src/encoding/rle.cpp
    src/encoding/delta_bitpacked.cpp
    src/encoding/delta_length.cpp
    src/encoding/hybrid_rle.cpp
    src/writer/thrift_compact.cpp
    src/writer/parquet_types.cpp
    src/writer/parquet_file.cpp
    src/writer/row_group.cpp
    src/writer/column_writer.cpp
    src/writer/page_writer.cpp
    src/writer/compression.cpp
    src/writer/statistics.cpp
    src/writer/dictionary.cpp
    src/simd/statistics_simd.cpp
)
```

New dependencies to add via FetchContent:
```cmake
# fast_float - SIMD float parsing
FetchContent_Declare(
    fast_float
    GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
    GIT_TAG v6.1.1
    GIT_SHALLOW TRUE
)

# simdutf - SIMD UTF-8 validation
FetchContent_Declare(
    simdutf
    GIT_REPOSITORY https://github.com/simdutf/simdutf.git
    GIT_TAG v5.5.0
    GIT_SHALLOW TRUE
)

# BS::thread_pool - Lightweight thread pool
FetchContent_Declare(
    thread_pool
    GIT_REPOSITORY https://github.com/bshoshany/thread-pool.git
    GIT_TAG v4.1.0
    GIT_SHALLOW TRUE
)
```

**Step 3: Test build**

```bash
rm -rf build && mkdir build && cd build
cmake .. -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF
make -j$(nproc) 2>&1 | head -100
```

**Step 4: Commit**

```bash
git add CMakeLists.txt
git commit -m "build: Update CMakeLists.txt for libvroom2 sources"
```

---

## Task 2: Remove Old Source Files

**Files:**
- Delete old src/*.cpp files
- Delete old include/*.h files

**Step 1: Remove old source files**

```bash
cd /home/jimhester/p/libvroom
rm -f src/two_pass.cpp src/value_extraction.cpp src/arrow_output.cpp
rm -f src/libvroom_c.cpp src/libvroom_types.cpp src/branchless_state_machine.cpp
rm -f src/streaming.cpp src/index_cache.cpp src/simd_dispatch.cpp
rm -f src/simd_number_parsing.cpp
```

**Step 2: Remove old header files**

```bash
rm -f include/two_pass.h include/value_extraction.h include/arrow_output.h
rm -f include/libvroom_c.h include/libvroom_types.h include/branchless_state_machine.h
rm -f include/streaming.h include/extraction_config.h include/index_cache.h
rm -f include/simd_number_parsing.h include/simd_highway.h include/simd_dispatch.h
rm -f include/type_detector.h include/debug_parser.h include/common_defs.h
```

**Step 3: Keep useful files**

Keep these if they're still referenced:
- include/dialect.h (dialect detection - may need updating)
- include/encoding.h (character encoding)
- include/error.h (error handling)
- include/io_util.h (I/O utilities)
- include/mmap_util.h (memory mapping)
- include/utf8.h (UTF-8 validation)
- include/mem_util.h (memory utilities)
- include/debug.h (debugging)

Check if any are still needed, otherwise remove them too.

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: Remove old libvroom source files"
```

---

## Task 3: Create New API Header

**Files:**
- Modify: `include/libvroom.h`

**Step 1: Replace libvroom.h with new version**

```cpp
/**
 * @file libvroom.h
 * @brief libvroom - High-performance CSV parser and Parquet writer
 * @version 2.0.0
 *
 * This is the main public header for the libvroom library.
 * Migrated from libvroom2 for improved performance.
 */

#ifndef LIBVROOM_H
#define LIBVROOM_H

#define LIBVROOM_VERSION_MAJOR 2
#define LIBVROOM_VERSION_MINOR 0
#define LIBVROOM_VERSION_PATCH 0
#define LIBVROOM_VERSION_STRING "2.0.0"

// Core headers
#include "vroom/vroom.h"
#include "vroom/types.h"
#include "vroom/options.h"

// Column builders
#include "vroom/arrow_column_builder.h"

// Parsing
#include "vroom/split_fields.h"
#include "vroom/quote_parity.h"

// Statistics and dictionary
#include "vroom/statistics.h"
#include "vroom/dictionary.h"

// Re-export vroom namespace as libvroom for compatibility
namespace libvroom = vroom;

#endif // LIBVROOM_H
```

**Step 2: Test that header compiles**

```bash
cd build && make -j$(nproc)
```

**Step 3: Commit**

```bash
git add include/libvroom.h
git commit -m "feat: Create new libvroom.h API header"
```

---

## Task 4: Fix Build Errors

**Note:** This task handles any compilation errors that arise from the migration.

**Step 1: Attempt full build**

```bash
cd /home/jimhester/p/libvroom/build
cmake .. -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF
make -j$(nproc) 2>&1
```

**Step 2: Fix any include path issues**

The libvroom2 code may have different include paths. Common fixes:
- Update `#include "vroom/..."` paths if needed
- Add missing include directories in CMakeLists.txt
- Fix namespace differences (vroom vs libvroom)

**Step 3: Fix any missing dependencies**

If compilation fails due to missing symbols, ensure:
- All FetchContent dependencies are properly linked
- Compression libraries are found and linked
- Thread library is linked

**Step 4: Verify clean build**

```bash
make clean && make -j$(nproc)
```

**Step 5: Commit fixes**

```bash
git add -A
git commit -m "fix: Resolve build errors after migration"
```

---

## Completion Checklist

- [ ] CMakeLists.txt updated with new sources and dependencies
- [ ] Old source files removed
- [ ] New libvroom.h API header created
- [ ] Project builds successfully
- [ ] All changes committed

**Next:** Phases 3 (Python Bindings), 4 (CLI), and 5 (Arrow IPC) can now run in parallel.
