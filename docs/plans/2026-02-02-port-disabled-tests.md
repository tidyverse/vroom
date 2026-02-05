# Port Disabled Test Suite to libvroom2 API

**Issue:** [#626](https://github.com/jimhester/libvroom/issues/626)
**Date:** 2026-02-02
**Status:** Triage & Plan

## Current State

| Metric | Count |
|--------|-------|
| Active test executables | 4 (`vroom_api_test`, `error_handling_test`, `dialect_detection_test`, `cli_test`) |
| Active tests passing (ctest) | 194 |
| Disabled test executables | 33 |
| Disabled test count (TEST macros) | 2,263 |

## Triage

Every disabled test file is categorized into one of three actions:

### Action: PORT — Test scenarios portable to new API

These test files exercise functionality that exists in libvroom2, but use old API
signatures (`Parser`, `TwoPass`, `allocate_padded_buffer`, `FileBuffer`, etc.) that
need rewriting to use `CsvReader`, `CsvOptions`, `AlignedBuffer`, etc.

| File | Tests | Old API Used | New API Target | Priority |
|------|-------|-------------|----------------|----------|
| `quote_mask_test.cpp` | 10 | `find_quote_mask()` from `simd_highway.h` | `find_quote_mask()` from `quote_parity.h` (same function, different header) | **P0 — SIMD correctness** |
| `csv_parsing_test.cpp` | 85 | `TwoPass`, `ParseIndex`, `load_file_to_ptr` | `CsvReader` + `read_all()` | **P0 — Core parsing** |
| `csv_parser_errors_test.cpp` | 31 | `Parser`, `ErrorCollector` | `CsvReader` with `error_mode` | **P0 — Error handling** |
| `csv_extended_test.cpp` | 62 | `TwoPass`, `ParseIndex` | `CsvReader` (real-world edge cases) | **P0 — Edge cases** |
| `simd_error_detection_test.cpp` | 36 | `Parser`, `ErrorCollector` | `CsvReader` with `error_mode` | **P0 — SIMD error boundaries** |
| `concurrency_test.cpp` | 25 | `allocate_padded_buffer`, `Parser` | `CsvReader` with varying `num_threads` | **P0 — Thread safety** |
| `integration_test.cpp` | 23 | `StreamReader`, `Parser`, `Dialect` | `CsvReader` (E2E pipeline) | **P1 — Integration** |
| `comment_line_test.cpp` | 51 | `Dialect`, `Parser` | `CsvReader` with `comment` char | **P1 — Comment support** |
| `row_filter_test.cpp` | 17 | `Parser`, `FileBuffer` | `CsvReader` (skip/nmax if supported) | **P1 — Row filtering** |
| `io_util_test.cpp` | 50 | `load_file_to_ptr`, `allocate_padded_buffer`, `mem_util.h` | `AlignedBuffer`, `load_file_to_ptr()` from new `io_util.h` | **P1 — I/O utilities** |
| `arrow_output_test.cpp` | 82 | Old Arrow output API | `ArrowColumnBuilder` + `ParquetWriter` | **P2 — Arrow output** |
| `arrow_file_test.cpp` | 27 | Old Arrow file API | `convert_csv_to_parquet()` | **P2 — Arrow files** |

**Subtotal: 499 tests** (12 files)

### Action: PORT SCENARIOS — Extract test data, delete old test code

These files test internal APIs that no longer exist, but contain valuable test
*scenarios* (CSV inputs + expected outcomes) that should be extracted into new
tests against the public API.

| File | Tests | What to Extract | Priority |
|------|-------|----------------|----------|
| `type_detection_test.cpp` | 195 | Type inference test cases → test `TypeInference` class | **P1** |
| `simd_number_parsing_test.cpp` | 194 | Number parsing edge cases → test via `CsvReader` type inference | **P2** |
| `two_pass_coverage_test.cpp` | 192 | Quote handling edge cases → test via `CsvReader` | **P2** |
| `branchless_test.cpp` | 54 | State machine scenarios → test via `CsvReader` | **P2** |
| `bounds_validation_test.cpp` | 38 | Buffer edge cases → test via `CsvReader` with crafted inputs | **P2** |
| `size_limits_test.cpp` | 36 | Oversized field/file scenarios → test via `CsvReader` | **P2** |
| `api_test.cpp` | 134 | Multi-threaded, type conversion, UTF-8 → already partially covered by `vroom_api_test`, port gaps | **P1** |
| `csv_parser_test.cpp` | 47 | Well-formed CSV test data files → test via `CsvReader` with `test/data/` files | **P1** |

**Subtotal: 890 tests** (8 files), but many scenarios overlap with PORT files above

### Action: DELETE — Features removed in libvroom2

These files test APIs that were deliberately removed in the libvroom2 migration.
Any needed functionality should be tracked as separate feature issues.

| File | Tests | Removed Feature | Follow-up Issue? |
|------|-------|----------------|-----------------|
| `streaming_test.cpp` | 109 | `StreamReader` / push-pull streaming API | Yes — streaming API is useful, track separately |
| `c_api_test.cpp` | 126 | `libvroom_c.h` / C FFI wrapper | Yes — C API needed for R/Python bindings |
| `column_major_test.cpp` | 21 | Column-major compaction | No — replaced by `ArrowColumnBuilder` |
| `value_extraction_test.cpp` | 142 | `ValueExtractor`, `extract_int()`, etc. | No — replaced by column builders |
| `column_config_test.cpp` | 31 | Per-column type hints config | Partial — `CsvOptions.columns` exists |
| `lazy_column_test.cpp` | 43 | `LazyColumn` deferred parsing | No — different architecture |
| `column_escape_test.cpp` | 18 | Escape info tracking per-column | No — handled internally |
| `mmap_util_test.cpp` | 19 | Old mmap utilities | No — replaced by `io_util.h` |
| `index_cache_test.cpp` | 99 | Index caching/persistence | Yes — useful for repeated reads |
| `buffer_lifetime_test.cpp` | 12 | Buffer sharing via `ParseIndex` | No — different architecture |
| `encoding_test.cpp` | 51 | Character encoding detection/transcoding | Yes — encoding support needed |
| `utf8_test.cpp` | 139 | UTF-8 validation, display width, grapheme clusters | Yes — UTF-8 utilities needed |
| `debug_test.cpp` | 64 | Debug tracing infrastructure | No — low priority |

**Subtotal: 874 tests** (13 files)

### Summary

| Action | Files | Tests | Notes |
|--------|-------|-------|-------|
| PORT (rewrite against new API) | 12 | 499 | Direct rewrite, keep test logic |
| PORT SCENARIOS (extract test data) | 8 | 890 | Extract inputs/expectations, new test code |
| DELETE (removed features) | 13 | 874 | Delete tests, create follow-up issues |
| **Total** | **33** | **2,263** | |

## Implementation Plan

Work is broken into 6 PRs, ordered by risk priority.

### PR 1: SIMD & Quote Mask Tests (P0)

**Goal:** Restore coverage for the most dangerous gap — SIMD correctness.

**Files to create/modify:**
- `test/quote_parity_test.cpp` — New file, port from `quote_mask_test.cpp`
- `test/simd_parsing_test.cpp` — New file, port SIMD boundary tests from
  `simd_error_detection_test.cpp` and `csv_parsing_test.cpp` (SIMD-specific subset)

**Tasks:**
1. Create `test/quote_parity_test.cpp`:
   - Port all 10 tests from `quote_mask_test.cpp`
   - Change `#include "simd_highway.h"` → `#include "quote_parity.h"`
   - Verify `find_quote_mask()` signature matches
   - Add tests for `prefix_xorsum_inclusive()` and `portable_prefix_xorsum_inclusive()`
   - Add tests for `scalar_find_quote_mask()` vs SIMD consistency
2. Create `test/simd_parsing_test.cpp`:
   - Port 36 tests from `simd_error_detection_test.cpp`
   - Rewrite `parseWithErrors()` helper to use `CsvReader` with `error_mode = PERMISSIVE`
   - Port SIMD boundary tests (63-byte, 64-byte, 65-byte, 128-byte) from `csv_parsing_test.cpp`
3. Wire into CMakeLists.txt
4. Verify with `ctest --output-on-failure`

**Est. new tests:** ~50

### PR 2: Core Parsing & Error Handling (P0)

**Goal:** Restore coverage for CSV parsing correctness and malformed input handling.

**Files to create/modify:**
- `test/csv_reader_test.cpp` — New file, port from `csv_parsing_test.cpp` + `csv_extended_test.cpp`
- `test/csv_errors_test.cpp` — New file, port from `csv_parser_errors_test.cpp`

**Tasks:**
1. Create `test/csv_reader_test.cpp`:
   - Port ~80 non-SIMD-specific tests from `csv_parsing_test.cpp`
   - Port ~60 tests from `csv_extended_test.cpp`
   - Use `TempCsvFile` helper pattern from `vroom_api_test.cpp`
   - Test via `CsvReader::open()` + `CsvReader::read_all()`
   - Verify row counts, column counts, schema detection
   - Cover: quoted fields, escaped quotes, newlines in quotes, various line endings,
     wide/narrow columns, empty fields, Unicode content, BOM handling
2. Create `test/csv_errors_test.cpp`:
   - Port 31 tests from `csv_parser_errors_test.cpp`
   - Use `CsvOptions::error_mode = PERMISSIVE` to collect errors
   - Verify `ErrorCode`, `ErrorSeverity`, error locations
   - Cover: unclosed quotes, inconsistent field counts, invalid quote escapes,
     null bytes, oversized fields
3. Wire into CMakeLists.txt

**Est. new tests:** ~170

### PR 3: Concurrency & Integration (P0/P1)

**Goal:** Restore multi-threading safety tests and end-to-end integration.

**Files to create/modify:**
- `test/concurrency_test.cpp` — Rewrite in-place
- `test/integration_test.cpp` — Rewrite in-place

**Tasks:**
1. Rewrite `test/concurrency_test.cpp`:
   - Port 25 tests, replace `allocate_padded_buffer`/`Parser` with `CsvReader`
   - Test with `num_threads` = 1, 2, 4, 8, and hardware_concurrency
   - Verify deterministic results regardless of thread count
   - Test multiple concurrent `CsvReader` instances
   - Stress test: many threads parsing same data simultaneously
2. Rewrite `test/integration_test.cpp`:
   - Port 23 tests, replace `StreamReader`/`Parser` with `CsvReader`
   - E2E: file load → parse → verify schema + row count + data content
   - Test with real test data files from `test/data/`
3. Wire into CMakeLists.txt

**Est. new tests:** ~48

### PR 4: Comment Lines, Row Filtering & I/O (P1)

**Goal:** Restore coverage for comment handling, row filtering, and I/O utilities.

**Files to create/modify:**
- `test/comment_line_test.cpp` — Rewrite in-place
- `test/row_filter_test.cpp` — Rewrite in-place
- `test/io_util_test.cpp` — Rewrite in-place

**Tasks:**
1. Rewrite `test/comment_line_test.cpp`:
   - Port 51 tests using `CsvOptions::comment`
   - Test various comment characters (`#`, `;`, `%`, `/`)
   - Test comments with different dialects
   - Test comment char inside quoted fields (should not be treated as comment)
2. Rewrite `test/row_filter_test.cpp`:
   - Port 17 tests — check if `CsvOptions` supports skip/nmax
   - If not supported yet, create issue and skip
3. Rewrite `test/io_util_test.cpp`:
   - Port 50 tests using `AlignedBuffer`, `load_file_to_ptr()`
   - Test alignment, padding, move semantics, empty files, large files
   - Test `read_stdin_to_ptr()` if testable
4. Wire into CMakeLists.txt

**Est. new tests:** ~100+

### PR 5: Type Inference & Arrow Output (P1/P2)

**Goal:** Restore type inference and Arrow/Parquet output coverage.

**Files to create/modify:**
- `test/type_inference_test.cpp` — New file, port scenarios from `type_detection_test.cpp`
- `test/arrow_conversion_test.cpp` — New file, port from `arrow_output_test.cpp` + `arrow_file_test.cpp`

**Tasks:**
1. Create `test/type_inference_test.cpp`:
   - Port key scenarios from 195 tests in `type_detection_test.cpp`
   - Test `TypeInference::infer_field()` for int/float/bool/date/string detection
   - Test `TypeInference::infer_from_sample()` for column-level inference
   - Test type promotion hierarchy (BOOL < INT32 < INT64 < FLOAT64 < STRING)
2. Create `test/arrow_conversion_test.cpp`:
   - Port from `arrow_output_test.cpp` (82 tests) and `arrow_file_test.cpp` (27 tests)
   - Test `convert_csv_to_parquet()` with various data types
   - Test compression options (ZSTD, GZIP, NONE)
   - Test with real-world test data files
3. Wire into CMakeLists.txt

**Est. new tests:** ~150+

### PR 6: Cleanup — Delete Old Tests & Create Follow-up Issues

**Goal:** Remove dead test code and track missing features.

**Tasks:**
1. Delete test files for removed features (13 files listed in DELETE section)
2. Remove commented-out `add_executable` lines from CMakeLists.txt (the `# add_executable(...)` lines only — do NOT remove active test definitions for `error_handling_test`, `dialect_detection_test`, or `cli_test`)
3. Create follow-up issues:
   - #633 — Add streaming parser API
   - #634 — Add C API wrapper (`libvroom_c.h`)
   - #635 — Add index caching for repeated reads
   - #636 — Add character encoding detection (UTF-16, Latin-1, UTF-8 utilities)
4. Update `test/README.md` to document new test organization

## Test Organization After Porting

```
test/
├── vroom_api_test.cpp          # Existing: basic CsvReader/Parquet happy path (20 tests)
├── error_handling_test.cpp     # Existing: ErrorCollector unit tests (37 tests)
├── dialect_detection_test.cpp  # Existing: dialect detection (varies)
├── cli_test.cpp                # Existing: CLI integration tests (varies)
├── quote_parity_test.cpp       # NEW: SIMD quote mask correctness
├── simd_parsing_test.cpp       # NEW: SIMD boundary edge cases
├── csv_reader_test.cpp         # NEW: core CSV parsing (from csv_parsing + csv_extended)
├── csv_errors_test.cpp         # NEW: malformed input handling
├── concurrency_test.cpp        # REWRITTEN: multi-threaded safety
├── integration_test.cpp        # REWRITTEN: E2E pipeline
├── comment_line_test.cpp       # REWRITTEN: comment line handling
├── row_filter_test.cpp         # REWRITTEN: skip/nmax filtering
├── io_util_test.cpp            # REWRITTEN: AlignedBuffer, file loading
├── type_inference_test.cpp     # NEW: type detection hierarchy
├── arrow_conversion_test.cpp   # NEW: CSV→Parquet conversion
└── data/                       # Test data files (unchanged)
```

## Risk Assessment

| Risk | Mitigation |
|------|-----------|
| SIMD correctness regression | PR 1 is first — most dangerous gap |
| Thread safety regression | PR 3 includes TSan-compatible tests |
| Scope creep | Each PR is independent and mergeable |
| Missing test data files | Reuse existing `test/data/` directory |
| API gaps discovered during porting | Create issues, don't block porting |
