# libvroom2 → libvroom Migration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace libvroom's CSV parser and add direct Parquet writing by migrating libvroom2's optimized code into the libvroom codebase.

**Architecture:** Keep libvroom's project structure (Python bindings, build system, tests), replace the core parsing engine with libvroom2's dual-state parser, add libvroom2's pipelined Parquet writer, and rewrite Python bindings for the new API.

**Tech Stack:** C++20, Google Highway SIMD, pybind11, scikit-build-core, zstd/snappy/lz4 compression

---

## Overview

### What We're Keeping from libvroom
- Repository and project structure
- Python packaging (pyproject.toml, scikit-build-core)
- pybind11 infrastructure
- Build system framework (CMakeLists.txt structure)
- Test infrastructure
- CLI skeleton

### What We're Replacing with libvroom2
- Core CSV parser (two_pass.h/cpp → chunk_finder, line_parser, split_fields, quote_parity)
- Column builders (ArrowConverter → ArrowColumnBuilder)
- Type inference
- Add: Direct Parquet writer (pipelined)
- Add: Parquet encodings (RLE, delta, hybrid)

### What We're Removing
- C API (libvroom_c.h/cpp) - not essential
- Arrow library dependency for Parquet - using direct writer instead
- Old value extraction code
- Streaming parser (can add back later if needed)

---

## Phase 1: Core Parser Migration

### Task 1: Set Up libvroom2 Code Directory

**Files:**
- Create: `src/parser/` directory
- Create: `src/writer/` directory
- Create: `src/encoding/` directory
- Create: `src/schema/` directory
- Create: `src/columns/` directory
- Create: `src/simd/` directory
- Create: `include/vroom/` directory

**Step 1: Create directory structure**

```bash
mkdir -p src/parser src/writer src/encoding src/schema src/columns src/simd
mkdir -p include/vroom
```

**Step 2: Verify directories created**

```bash
ls -la src/
ls -la include/
```

**Step 3: Commit**

```bash
git add src/ include/
git commit -m "chore: Create directory structure for libvroom2 migration"
```

---

### Task 2: Copy libvroom2 Headers

**Files:**
- Copy: `~/p/libvroom2/include/vroom/*.h` → `include/vroom/`

**Step 1: Copy all header files**

```bash
cp ~/p/libvroom2/include/vroom/*.h include/vroom/
```

**Step 2: Verify headers copied**

```bash
ls -la include/vroom/
```

Expected: types.h, options.h, vroom.h, arrow_column_builder.h, split_fields.h, quote_parity.h, statistics.h, dictionary.h, data_chunk.h, arrow_buffer.h, fast_arrow_context.h, fast_column_context.h, simd_atoi.h

**Step 3: Commit**

```bash
git add include/vroom/
git commit -m "feat: Add libvroom2 headers"
```

---

### Task 3: Copy libvroom2 Parser Sources

**Files:**
- Copy: `~/p/libvroom2/src/parser/*.cpp` → `src/parser/`
- Copy: `~/p/libvroom2/src/reader/*.cpp` → `src/reader/`

**Step 1: Copy parser files**

```bash
cp ~/p/libvroom2/src/parser/*.cpp src/parser/
mkdir -p src/reader
cp ~/p/libvroom2/src/reader/*.cpp src/reader/
```

**Step 2: Verify files copied**

```bash
ls -la src/parser/
ls -la src/reader/
```

Expected parser: chunk_finder.cpp, line_parser.cpp, split_fields.cpp, split_fields_iter.cpp, quote_parity.cpp, simd_chunk_finder.cpp, simd_atoi.cpp
Expected reader: csv_reader.cpp, mmap_source.cpp

**Step 3: Commit**

```bash
git add src/parser/ src/reader/
git commit -m "feat: Add libvroom2 parser sources"
```

---

### Task 4: Copy libvroom2 Column Builder Sources

**Files:**
- Copy: `~/p/libvroom2/src/columns/*.cpp` → `src/columns/`
- Copy: `~/p/libvroom2/src/schema/*.cpp` → `src/schema/`
- Copy: `~/p/libvroom2/src/simd/*.cpp` → `src/simd/`

**Step 1: Copy column and schema files**

```bash
cp ~/p/libvroom2/src/columns/*.cpp src/columns/
cp ~/p/libvroom2/src/schema/*.cpp src/schema/
cp ~/p/libvroom2/src/simd/*.cpp src/simd/
```

**Step 2: Verify files copied**

```bash
ls -la src/columns/
ls -la src/schema/
ls -la src/simd/
```

Expected columns: column_builder.cpp
Expected schema: type_inference.cpp, type_parsers.cpp
Expected simd: statistics_simd.cpp

**Step 3: Commit**

```bash
git add src/columns/ src/schema/ src/simd/
git commit -m "feat: Add libvroom2 column builder and schema sources"
```

---

### Task 5: Copy libvroom2 Parquet Writer Sources

**Files:**
- Copy: `~/p/libvroom2/src/writer/*.cpp` → `src/writer/`
- Copy: `~/p/libvroom2/src/encoding/*.cpp` → `src/encoding/`

**Step 1: Copy writer and encoding files**

```bash
cp ~/p/libvroom2/src/writer/*.cpp src/writer/
cp ~/p/libvroom2/src/encoding/*.cpp src/encoding/
```

**Step 2: Verify files copied**

```bash
ls -la src/writer/
ls -la src/encoding/
```

Expected writer: parquet_file.cpp, row_group.cpp, column_writer.cpp, page_writer.cpp, thrift_compact.cpp, parquet_types.cpp, compression.cpp, statistics.cpp, dictionary.cpp
Expected encoding: plain.cpp, rle.cpp, delta_bitpacked.cpp, delta_length.cpp, hybrid_rle.cpp

**Step 3: Commit**

```bash
git add src/writer/ src/encoding/
git commit -m "feat: Add libvroom2 Parquet writer and encoding sources"
```

---

## Phase 2: Build System Migration

### Task 6: Update CMakeLists.txt for New Sources

**Files:**
- Modify: `CMakeLists.txt`

**Step 1: Backup existing CMakeLists.txt**

```bash
cp CMakeLists.txt CMakeLists.txt.backup
```

**Step 2: Update source file list**

Replace the VROOM_SOURCES variable with libvroom2's source files. Keep the project structure, dependencies (Highway, compression libs), and build options. Update to C++20.

Key changes:
- Change `CMAKE_CXX_STANDARD` from 17 to 20
- Update source file list to new locations
- Add compression library dependencies (zstd, snappy, lz4, zlib)
- Add fast_float and simdutf dependencies
- Add BS::thread_pool dependency
- Remove Arrow library dependency
- Update include directories

**Step 3: Build and verify compilation**

```bash
mkdir -p build && cd build
cmake .. -DBUILD_TESTING=ON
make -j$(nproc)
```

**Step 4: Commit**

```bash
git add CMakeLists.txt
git commit -m "build: Update CMakeLists.txt for libvroom2 sources"
```

---

### Task 7: Remove Old Source Files

**Files:**
- Delete: `src/two_pass.cpp`
- Delete: `src/value_extraction.cpp`
- Delete: `src/arrow_output.cpp`
- Delete: `src/libvroom_c.cpp`
- Delete: `src/libvroom_types.cpp`
- Delete: `src/branchless_state_machine.cpp`
- Delete: `src/streaming.cpp`
- Delete: `include/two_pass.h`
- Delete: `include/value_extraction.h`
- Delete: `include/arrow_output.h`
- Delete: `include/libvroom_c.h`
- Delete: `include/libvroom_types.h`
- Delete: `include/branchless_state_machine.h`
- Delete: `include/streaming.h`
- Keep: `include/dialect.h`, `include/encoding.h`, `include/error.h`, `include/io_util.h`, `include/mmap_util.h`, `include/utf8.h` (if still useful)

**Step 1: Remove old source files**

```bash
rm -f src/two_pass.cpp src/value_extraction.cpp src/arrow_output.cpp
rm -f src/libvroom_c.cpp src/libvroom_types.cpp src/branchless_state_machine.cpp
rm -f src/streaming.cpp
```

**Step 2: Remove old header files**

```bash
rm -f include/two_pass.h include/value_extraction.h include/arrow_output.h
rm -f include/libvroom_c.h include/libvroom_types.h include/branchless_state_machine.h
rm -f include/streaming.h include/extraction_config.h include/index_cache.h
rm -f include/simd_number_parsing.h include/simd_highway.h include/simd_dispatch.h
```

**Step 3: Remove remaining old files**

```bash
rm -f src/index_cache.cpp src/simd_dispatch.cpp src/simd_number_parsing.cpp
```

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: Remove old libvroom source files"
```

---

### Task 8: Create Main API Header

**Files:**
- Create: `include/libvroom.h` (new version wrapping vroom/ headers)

**Step 1: Create new libvroom.h**

The new libvroom.h should:
- Include all vroom/ headers
- Re-export key classes in libvroom namespace
- Provide compatibility aliases if needed

```cpp
#ifndef LIBVROOM_H
#define LIBVROOM_H

#include "vroom/vroom.h"
#include "vroom/types.h"
#include "vroom/options.h"
#include "vroom/arrow_column_builder.h"

// Re-export vroom namespace as libvroom for backwards compatibility
namespace libvroom = vroom;

#endif // LIBVROOM_H
```

**Step 2: Build and verify**

```bash
cd build && make -j$(nproc)
```

**Step 3: Commit**

```bash
git add include/libvroom.h
git commit -m "feat: Create new libvroom.h API header"
```

---

## Phase 3: Python Bindings Migration

### Task 9: Update Python CMakeLists.txt

**Files:**
- Modify: `python/CMakeLists.txt`

**Step 1: Update source file list**

Update VROOM_SOURCES to use the new file locations. Update to C++20.

**Step 2: Add new dependencies**

Add FetchContent for fast_float, simdutf, BS::thread_pool. Add compression library linking.

**Step 3: Build Python module**

```bash
cd python
pip install -e . --no-build-isolation -v
```

**Step 4: Commit**

```bash
git add python/CMakeLists.txt
git commit -m "build: Update Python CMakeLists.txt for new sources"
```

---

### Task 10: Stub Out Python Bindings

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Create minimal working bindings**

Replace the existing bindings with a minimal version that:
- Exposes `read_csv(path) -> Table`
- Exposes `Table.__arrow_c_stream__()` for Arrow interop
- Exposes basic `Dialect` detection

The goal is to get something compiling first, then add features incrementally.

**Step 2: Build and test import**

```bash
cd python
pip install -e . --no-build-isolation
python -c "import vroom_csv; print(vroom_csv.__version__)"
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Stub out Python bindings for new API"
```

---

### Task 11: Implement read_csv() Python Binding

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Implement read_csv function**

```cpp
py::object read_csv(const std::string& path, /* options */) {
    vroom::CsvOptions csv_opts;
    vroom::CsvReader reader(path, csv_opts);
    auto columns = reader.read_all();
    // Convert to Arrow table via PyCapsule interface
    return create_arrow_table(columns);
}
```

**Step 2: Test with Python**

```python
import vroom_csv
table = vroom_csv.read_csv("test.csv")
print(table)
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Implement read_csv() Python binding"
```

---

### Task 12: Implement Arrow PyCapsule Export

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Implement __arrow_c_stream__ method**

The Table class needs to implement the Arrow PyCapsule protocol for zero-copy export to PyArrow, Polars, DuckDB.

Reference: Keep the existing ArrowSchema, ArrowArray, ArrowArrayStream structs and adapt them to work with libvroom2's ArrowColumnBuilder output.

**Step 2: Test Arrow interop**

```python
import vroom_csv
import pyarrow as pa

table = vroom_csv.read_csv("test.csv")
arrow_table = pa.table(table)
print(arrow_table.schema)
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Implement Arrow PyCapsule export"
```

---

### Task 13: Implement to_parquet() Python Binding

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Implement to_parquet function**

```cpp
void to_parquet(py::object table_or_path, const std::string& output_path, /* options */) {
    // If table_or_path is a string, read CSV first
    // Then write to Parquet using vroom::ParquetWriter
}
```

**Step 2: Test Parquet writing**

```python
import vroom_csv
vroom_csv.to_parquet("input.csv", "output.parquet")
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Implement to_parquet() Python binding"
```

---

### Task 14: Add Batched Reading Support

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Implement BatchedReader class**

```cpp
class BatchedReader {
    // Reads CSV in batches, yields Arrow RecordBatches
};
```

**Step 2: Test batched reading**

```python
import vroom_csv
for batch in vroom_csv.read_csv_batched("large.csv", batch_size=100000):
    print(f"Batch: {len(batch)} rows")
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Add batched CSV reading support"
```

---

## Phase 4: CLI Migration

### Task 15: Update CLI for CSV to Parquet

**Files:**
- Modify: `src/cli.cpp`

**Step 1: Simplify CLI to core conversion**

For now, implement just the CSV → Parquet conversion command. Other commands (head, tail, sample, stats) can be stubbed.

```cpp
// vroom convert input.csv output.parquet
// vroom convert input.csv  # outputs input.parquet
```

**Step 2: Build and test CLI**

```bash
cd build && make vroom
./vroom convert ../test/data/simple.csv output.parquet
```

**Step 3: Commit**

```bash
git add src/cli.cpp
git commit -m "feat: Update CLI for CSV to Parquet conversion"
```

---

### Task 16: Stub Remaining CLI Commands

**Files:**
- Modify: `src/cli.cpp`

**Step 1: Add stubs for other commands**

```cpp
// vroom head -n 10 file.csv   → "Not yet implemented"
// vroom tail -n 10 file.csv   → "Not yet implemented"
// vroom sample -n 100 file.csv → "Not yet implemented"
// vroom stats file.csv        → "Not yet implemented"
// vroom schema file.csv       → Works (uses type inference)
// vroom dialect file.csv      → Works (if dialect detection kept)
```

**Step 2: Commit**

```bash
git add src/cli.cpp
git commit -m "feat: Stub remaining CLI commands"
```

---

## Phase 5: Arrow IPC Output

### Task 17: Add Arrow IPC Writer

**Files:**
- Create: `src/writer/arrow_ipc.cpp`
- Create: `include/vroom/arrow_ipc.h`

**Step 1: Implement Arrow IPC format writer**

Arrow IPC is simpler than Parquet - it's basically the Arrow in-memory format serialized to disk. We can implement this using the same ArrowColumnBuilder output.

**Step 2: Add to_arrow_ipc() Python binding**

```python
vroom_csv.to_arrow_ipc("input.csv", "output.arrow")
```

**Step 3: Commit**

```bash
git add src/writer/arrow_ipc.cpp include/vroom/arrow_ipc.h python/src/bindings.cpp
git commit -m "feat: Add Arrow IPC file writer"
```

---

## Phase 6: Testing

### Task 18: Update C++ Tests

**Files:**
- Modify: `test/*.cpp`

**Step 1: Remove tests for removed functionality**

Delete or update tests that reference old API (two_pass, ArrowConverter, etc.)

**Step 2: Add tests for new API**

Add tests for:
- CsvReader basic parsing
- ArrowColumnBuilder
- ParquetWriter
- Type inference

**Step 3: Run tests**

```bash
cd build && ctest --output-on-failure
```

**Step 4: Commit**

```bash
git add test/
git commit -m "test: Update C++ tests for new API"
```

---

### Task 19: Update Python Tests

**Files:**
- Modify: `python/tests/*.py`

**Step 1: Update tests for new API**

Update test_basic.py, test_arrow.py, etc. to use the new API.

**Step 2: Run Python tests**

```bash
cd python && pytest tests/ -v
```

**Step 3: Commit**

```bash
git add python/tests/
git commit -m "test: Update Python tests for new API"
```

---

## Phase 7: Cleanup

### Task 20: Update Documentation

**Files:**
- Modify: `README.md`
- Modify: `python/README.md`

**Step 1: Update README with new API**

Document the new Python API and CLI usage.

**Step 2: Commit**

```bash
git add README.md python/README.md
git commit -m "docs: Update documentation for new API"
```

---

### Task 21: Final Cleanup

**Files:**
- Remove: `CMakeLists.txt.backup`
- Remove: Any remaining unused files

**Step 1: Clean up temporary files**

```bash
rm -f CMakeLists.txt.backup
```

**Step 2: Run full test suite**

```bash
cd build && ctest --output-on-failure
cd ../python && pytest tests/ -v
```

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: Final cleanup after migration"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-5 | Copy libvroom2 source files |
| 2 | 6-8 | Update build system |
| 3 | 9-14 | Python bindings |
| 4 | 15-16 | CLI |
| 5 | 17 | Arrow IPC output |
| 6 | 18-19 | Testing |
| 7 | 20-21 | Cleanup |

**Total: 21 tasks**

---

## Dependencies

From libvroom2 that need to be added to libvroom:
- fast_float (header-only)
- simdutf
- BS::thread_pool (header-only)
- zstd, snappy, lz4, zlib (compression)

Already in libvroom:
- Google Highway
- pybind11

Can be removed from libvroom:
- Apache Arrow (no longer needed for Parquet)
