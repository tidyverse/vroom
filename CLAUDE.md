# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

libvroom is a high-performance CSV parser library using portable SIMD instructions (via Google Highway), designed for future integration with R's [vroom](https://github.com/tidyverse/vroom) package. The parser uses a speculative multi-threaded two-pass algorithm based on research by Chang et al. (SIGMOD 2019) and SIMD techniques from Langdale & Lemire (simdjson).

## Naming and Authorship

This project is authored by Jim Hester, the original author of [vroom](https://github.com/tidyverse/vroom). The project was renamed from simdcsv to **libvroom** to:

1. Clearly indicate its relationship to vroom as the native SIMD parsing engine
2. Avoid confusion with abandoned simdjson-adjacent projects (e.g., geofflangdale/simdcsv)
3. Use conventional `lib*` naming for C/C++ libraries

## Breaking Changes Policy

libvroom is still experimental and pre-1.0. **Breaking changes are acceptable** when they improve performance, correctness, or API design. Strive to make the optimum performance choice even if it requires breaking changes. Don't add backwards-compatibility shims or deprecated code paths—just make the change cleanly.

## Performance improvements

When making potential performance improvments do not just use intuition and assume something will improve performance. Use prof to find hot spots in the current code, generate hypotheses on the potential issue and changes which could improve performance. Then test those hypothesis with benchmarks.

## Build Commands

```bash
# Configure and build (Release) - use -j for parallel compilation
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Minimal release build (library and CLI only, no tests/benchmarks)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF
cmake --build build -j$(nproc)

# Build shared library instead of static
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
cmake --build build -j$(nproc)

# Run all tests
cd build && ctest --output-on-failure -j$(nproc)

# Run specific test binary
./build/libvroom_test              # 42 well-formed CSV tests
./build/error_handling_test        # 37 error handling tests
./build/csv_parsing_test           # Integration tests

# Run benchmarks
./build/libvroom_benchmark

# Build with code coverage (gcov)
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
cmake --build build -j$(nproc)

# Build with LLVM source-based coverage (requires Clang, better for headers)
cmake -B build -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DENABLE_LLVM_COVERAGE=ON
cmake --build build -j$(nproc)
```

## Build Acceleration

**ccache** is automatically detected and used if installed. Install with:
```bash
# Ubuntu/Debian
sudo apt install ccache

# macOS
brew install ccache
```

ccache dramatically speeds up rebuilds by caching compilation results. View stats with `ccache -s`.

## Language Server

clangd is available for code intelligence (go-to-definition, find references, diagnostics). `CMAKE_EXPORT_COMPILE_COMMANDS` is enabled unconditionally in `CMakeLists.txt`, and a tracked symlink `compile_commands.json -> build/compile_commands.json` is checked into the repo. After any `cmake -B build` command, clangd will work automatically.

**Note**: clangd diagnostics (missing includes, unknown types) are **not real build errors** if the actual build succeeds. They indicate `build/compile_commands.json` hasn't been generated yet. Run any `cmake -B build` command to fix.

## Code Formatting

A `.clang-format` config is provided. Install the pre-commit hook to auto-format staged files:

```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

To manually format a file: `clang-format -i <file>`

## Git Workflow

- **No force pushing**: Avoid `git push --force` on branches
- **Update branches via merge**: When a branch needs updates from main, use `git merge main` instead of rebasing
- **Squash on final merge**: Use squash merge when merging PRs into main to keep history clean
- **Check for merge conflicts**: When opening a branch, check for merge conflicts with main. If CI status checks aren't appearing on a PR, merge conflicts are often the cause
- **Use auto-merge**: Enable auto-merge on PRs when CI is passing and there are no outstanding issues

```bash
# Check for conflicts before creating PR
git fetch origin main
git merge origin/main --no-commit --no-ff
# If no conflicts, abort the test merge
git merge --abort

# Check PR mergeability after creation
gh pr view <PR-NUMBER> --json mergeable,mergeStateStatus
```

## Key Files

| File | Purpose |
|------|---------|
| `include/libvroom.h` | **Main public API** - Parser class, unified interface |
| `include/libvroom_c.h` | C API wrapper for FFI bindings |
| `include/two_pass.h` | Core two-pass parsing algorithm with multi-threading |
| `include/dialect.h` | CSV dialect detection (delimiter, quoting, line endings) |
| `include/error.h` | Error codes, ErrorCollector, three error modes |
| `include/streaming.h` | Streaming parser for large files |
| `include/simd_highway.h` | Portable SIMD operations (Highway) |
| `src/cli.cpp` | CLI tool (`vroom count/head/select/pretty/info`) |

## Architecture

Two-pass speculative parsing algorithm (see `include/two_pass.h`):
1. **First pass**: Scan for line boundaries tracking quote parity to find safe split points
2. **Speculative chunking**: Divide file for parallel processing based on quote analysis
3. **Second pass**: SIMD field indexing (64 bytes/iteration) with state machine

SIMD via Google Highway 1.3.0: x86-64 (SSE4.2, AVX2), ARM (NEON), scalar fallback.

## API Surfaces

The library exposes functionality through multiple interfaces that must be kept in sync:

| API | Location | Description |
|-----|----------|-------------|
| C++ Core | `include/*.h` | Primary implementation, header-only where possible |
| C API | `include/libvroom_c.h`, `src/libvroom_c.cpp` | C wrapper for FFI compatibility |
| Python | `python/src/bindings.cpp` | pybind11 bindings, may have its own configuration |
| CLI | `src/cli.cpp` | Command-line interface |

**When adding features to the core library, check if these surfaces need updates:**
- New enum values → update all switch statements and type mappings
- New options/config → expose in C API and Python bindings
- New public methods → consider C API wrapper and Python exposure
- Behavioral changes → verify consistent behavior across all interfaces

## Documentation

| Topic | Location |
|-------|----------|
| Error handling (modes, types, recovery) | `docs/error_handling.md` |
| Index caching (CLI options, API, cache format) | `docs/caching.qmd` |
| Code coverage (tools, limitations, interpretation) | `docs/coverage.md` |
| Test data organization | `test/README.md` |
| CI workflows | `.github/workflows/README.md` |

## Dependencies (fetched via CMake FetchContent)

- Google Highway 1.3.0 - Portable SIMD
- Google Test 1.14.0 - Unit testing
- Google Benchmark 1.8.3 - Performance benchmarking

## Issue Labels

Use `gh issue create --label "label"` with the following labels:

| Label | Description |
|-------|-------------|
| `bug` | an unexpected problem or unintended behavior |
| `feature` | a feature request or enhancement |
| `documentation` | improvements or additions to documentation |
| `performance 🚀` | performance improvement |
| `testing 🧪` | test coverage or infrastructure |
| `cleanup 🧹` | code cleanup or refactoring |
| `api 🔌` | public API changes or additions |
| `c-api 🔧` | C API wrapper |
| `cli ⌨️` | vroom command line tool |
| `simd ⚡` | SIMD implementation or optimization |
| `arrow 🏹` | Apache Arrow integration |
| `R 🏴‍☠️` | R language bindings or integration |
| `python 🐍` | Python bindings or integration |
| `security 🔒` | security vulnerability or hardening |
| `critical ☠️` | must fix - security or correctness issue |
| `up next 📌` | next items to address from code review |
| `good first issue ❤️` | good issue for first-time contributors |
| `help wanted ❤️` | we'd love your help! |
| `duplicate` | this issue or pull request already exists |
| `wontfix ❌` | this will not be worked on |


## Related projects

These projects are checked out locally and can be useful references for features, ideas or optimization techniques.

### simdjson (`~/p/simdjson`)

High-performance SIMD JSON parser achieving gigabytes/second throughput. Uses a two-stage algorithm similar to libvroom: Stage 1 uses SIMD to find structural boundaries, Stage 2 handles variable-width content.

| Directory | Contents |
|-----------|----------|
| `src/generic/stage1/` | Core SIMD structural indexing - character scanning, UTF-8 validation |
| `src/generic/stage2/` | Tape builder, string/number parsing after boundaries identified |
| `include/simdjson/haswell/` | x86-64 AVX2 SIMD implementation (`simd.h`, `intrinsics.h`) |
| `include/simdjson/arm64/` | ARM NEON SIMD implementation |
| `src/internal/` | Lookup tables for character classification, number parsing |
| `doc/` | Algorithm documentation (`HACKING.md` explains two-stage approach) |

### DuckDB (`~/p/duckdb`)

High-performance analytical database with sophisticated CSV parsing, plus Arrow and Parquet integration. Useful reference for dialect detection, type inference, multi-threaded scanning, and columnar format conversion.

| Directory | Contents |
|-----------|----------|
| `src/execution/operator/csv_scanner/` | Complete CSV parsing engine |
| `src/execution/operator/csv_scanner/sniffer/` | Dialect detection, type detection, header detection |
| `src/execution/operator/csv_scanner/state_machine/` | State machine CSV parsing with caching |
| `src/common/vector_operations/` | Vectorized/SIMD-friendly bulk data operations |
| `src/function/table/` | CSV entry points (`read_csv.cpp`, `sniff_csv.cpp`) |
| `src/common/arrow/` | Arrow integration (`arrow_converter.cpp`, `arrow_appender.cpp`, type mapping) |
| `src/function/table/arrow/` | Arrow table functions, schema mapping between Arrow and DuckDB |
| `src/include/duckdb/common/arrow/appender/` | Type-specific Arrow appenders (scalar, list, struct, union) |
| `extension/parquet/` | Complete Parquet extension with reader/writer pipeline |
| `extension/parquet/reader/` | Column-specific Parquet readers (decimals, lists, structs, strings) |
| `extension/parquet/writer/` | Column-specific Parquet writers with encoding and compression |
| `extension/parquet/decoder/` | Low-level decoders (RLE, bit-packing, delta encoding, byte stream split) |

### Apache Arrow (`~/p/arrow`)

Universal columnar format and multi-language toolbox. Essential reference for outputting parsed CSV data to Arrow format.

| Directory | Contents |
|-----------|----------|
| `cpp/src/arrow/csv/` | CSV reader/writer with `parser.cc`, `chunker.cc`, `column_builder.cc` |
| `cpp/src/arrow/array/` | Columnar Array implementations (primitive, binary, nested types) |
| `cpp/src/arrow/ipc/` | Arrow IPC format, Feather writer (`writer.cc`, `feather.cc`) |
| `cpp/src/parquet/` | Parquet columnar format support |
| `cpp/src/arrow/io/` | I/O interfaces (InputStream, RandomAccessFile, buffering) |
| `format/` | FlatBuffers schemas (`.fbs`) defining Arrow's columnar format |

### vroom (`~/p/vroom`)

R package for fast delimited file reading using lazy Altrep columns. This is the package libvroom will integrate with as the native parsing engine.

| Directory | Contents |
|-----------|----------|
| `src/delimited_index.h` | Two-pass indexing with quote parity tracking (471 lines) |
| `src/delimited_index.cc` | Index implementation with multi-threaded chunking (432 lines) |
| `src/columns.h` | R Altrep integration for lazy column loading |
| `src/collectors.h` | Type collectors (integer, double, factor, datetime) |
| `src/DateTimeParser.h` | Locale-aware datetime parsing |
| `R/vroom.R` | Main R API - parameter handling and interface contract |
| `inst/bench/` | Benchmark datasets and performance testing |

### zsv (`~/p/zsv`)

"The world's fastest SIMD CSV parser" - a high-performance C library and extensible CLI. Uses portable SIMD vectors (SSE4.2, AVX2, AVX512) with minimal memory footprint (2.7x smaller than xsv, 52x smaller than DuckDB).

| Directory | Contents |
|-----------|----------|
| `src/zsv_internal.c` | Core state machine with conditional SIMD (16B/32B/64B vectors) |
| `src/vector_delim.c` | SIMD delimiter detection using `movemask` for bulk 32/64-byte scanning |
| `src/zsv_scan_delim.c` | Main scanning state machine with quote parity and field boundaries |
| `include/zsv/api.h` | Pull/push parser API (`zsv_new`, `zsv_parse_more`, `zsv_get_cell`) |
| `include/zsv/common.h` | Data structures (`zsv_cell`, `zsv_parser`, status codes) |
| `app/` | CLI commands: `select.c`, `2json.c`, `2db.c`, `compare.c`, `benchmark/` |
| `app/external/simdutf/` | SIMD UTF-8 validation library |

### data.table (`~/p/data.table`)

R package with the legendary `fread()` CSV parser. Uses memory-mapped I/O, multi-threaded OpenMP parsing, and sophisticated type detection with ordered type hierarchy and type bumping.

| Directory | Contents |
|-----------|----------|
| `src/fread.c` | Main CSV parser (3,002 lines) - type detection, parallel processing, quote handling |
| `src/fread.h` | Parser API with callback interface for type detection and buffer management |
| `src/freadLookups.h` | Lookup tables for fast type parsing and validation |
| `src/freadR.c` | R language bindings for fread |
| `src/fwrite.c` | High-performance CSV writer |
| `R/fread.R` | R interface with argument parsing and dialect detection |
| `inst/tests/` | Comprehensive test suite with real-world CSV edge cases |
| `vignettes/datatable-fread-and-fwrite.Rmd` | fread/fwrite usage documentation |
