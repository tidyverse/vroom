# Error Handling Integration Plan

## Overview

Port the error handling infrastructure from libvroom-v1 to the current v2 codebase, enabling rich error detection and reporting for malformed CSV files without performance impact.

## Goals

1. Restore error detection capabilities (quote errors, field count mismatches, encoding issues)
2. Zero performance cost when disabled (default mode)
3. Enable the existing 51 error_handling_test tests
4. Integrate with CLI and Python bindings

## Current State

**v2 error handling:**
```cpp
struct ConversionResult {
  std::string error;  // Single error message
  bool ok() const { return error.empty(); }
};
```

**Desired (from v1):**
```cpp
struct ParseError {
  ErrorCode code;        // UNCLOSED_QUOTE, INCONSISTENT_FIELD_COUNT, etc.
  ErrorSeverity severity; // WARNING, RECOVERABLE, FATAL
  size_t line, column, byte_offset;
  std::string message, context;
};

class ErrorCollector {
  ErrorMode mode_;  // FAIL_FAST, PERMISSIVE, BEST_EFFORT
  std::vector<ParseError> errors_;
};
```

## Error Codes

| Error Code | Severity | Detection Point |
|------------|----------|-----------------|
| `UNCLOSED_QUOTE` | FATAL | `LineParser` - quote not closed at line end |
| `INVALID_QUOTE_ESCAPE` | ERROR | `LineParser` - malformed `""` or `\"` |
| `QUOTE_IN_UNQUOTED_FIELD` | ERROR | `LineParser` - quote mid-field |
| `INCONSISTENT_FIELD_COUNT` | ERROR | `LineParser` - row has wrong column count |
| `EMPTY_HEADER` | ERROR | `CsvReader` - first row empty |
| `DUPLICATE_COLUMN_NAMES` | WARNING | `CsvReader` - header has duplicates |
| `MIXED_LINE_ENDINGS` | WARNING | `ChunkFinder` - CR/LF/CRLF mixed |
| `NULL_BYTE` | ERROR | `LineParser` - embedded null |
| `INVALID_UTF8` | ERROR | Optional validation pass |
| `IO_ERROR` | FATAL | `MmapSource` - file read failure |

## Error Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `DISABLED` | No error collection (default) | Maximum performance |
| `FAIL_FAST` | Stop on first error | Production validation |
| `PERMISSIVE` | Collect all, stop on FATAL | Data exploration |
| `BEST_EFFORT` | Ignore all, parse what's possible | Messy data import |

## API Design

**Updated CsvOptions:**
```cpp
struct CsvOptions {
  // ... existing fields ...

  // Error handling (new)
  ErrorMode error_mode = ErrorMode::DISABLED;
  size_t max_errors = 10000;
};
```

**Updated ConversionResult:**
```cpp
struct ConversionResult {
  std::string error;  // Keep for simple error case
  size_t rows = 0;
  size_t cols = 0;

  // Rich error info (new)
  std::vector<ParseError> parse_errors;  // Empty when DISABLED

  bool ok() const { return error.empty(); }
  bool has_warnings() const;
  bool has_fatal() const;
  std::string error_summary() const;  // "3 errors, 2 warnings"
};
```

## Implementation Steps

### Step 1: Port error infrastructure

**Files to create/copy:**
- `include/libvroom/error.h` - from v1, update namespace to `libvroom`
- `src/error.cpp` - from v1, update namespace

**Changes:**
- Add `DISABLED` to ErrorMode enum (new default for zero overhead)
- Update namespace from `libvroom` (v1) to `libvroom` (already correct)
- Add to CMakeLists.txt VROOM_SOURCES

### Step 2: Update options and result types

**Files to modify:**
- `include/libvroom/options.h` - add error_mode, max_errors to CsvOptions
- `include/libvroom/vroom.h` - add parse_errors to ConversionResult

**Add helper methods:**
- `ConversionResult::has_warnings()`
- `ConversionResult::has_fatal()`
- `ConversionResult::error_summary()`

### Step 3: Wire into LineParser

**Files to modify:**
- `include/libvroom/vroom.h` - LineParser class
- `src/parser/line_parser.cpp` - implementation

**Detection points to add:**
- Quote not closed at line end → UNCLOSED_QUOTE
- Invalid escape sequence → INVALID_QUOTE_ESCAPE
- Quote in unquoted field → QUOTE_IN_UNQUOTED_FIELD
- Wrong field count → INCONSISTENT_FIELD_COUNT
- Null byte in data → NULL_BYTE

**Performance strategy:**
```cpp
if (error_mode != ErrorMode::DISABLED) [[unlikely]] {
  if (quote_error_detected) {
    collector->add_error(...);
  }
}
```

### Step 4: Wire into CsvReader

**Files to modify:**
- `src/reader/csv_reader.cpp`

**Changes:**
- Create thread-local ErrorCollector for each worker thread
- Pass collector to LineParser
- Merge thread-local collectors after parallel processing
- Check for EMPTY_HEADER, DUPLICATE_COLUMN_NAMES
- Populate ConversionResult.parse_errors

### Step 5: Enable error_handling_test

**Files to modify:**
- `CMakeLists.txt` - uncomment error_handling_test
- `test/error_handling_test.cpp` - update any API differences

**Verification:**
- All 51 tests should pass
- Run with ASAN/UBSAN to check for memory issues

### Step 6: CLI integration

**Files to modify:**
- `src/cli.cpp`

**Changes:**
- Add `--strict` flag (sets error_mode = FAIL_FAST)
- Add `--max-errors <N>` option
- Display errors on conversion failure
- Exit code 1 on errors in strict mode

**Example usage:**
```bash
vroom convert data.csv -o data.parquet --strict
vroom convert data.csv -o data.parquet --max-errors 100
```

### Step 7: Python package integration

**Files to modify:**
- `python/src/bindings.cpp`
- `python/src/vroom_csv/__init__.py` (if exists)
- `python/tests/test_errors.py` (new)

**Changes:**
- Expose `ErrorMode` enum via pybind11
- Add `error_mode` and `max_errors` parameters to `read_csv()`
- Return errors as list of dicts with keys: code, severity, line, column, message, context
- Or create `ParseError` Python class

**Example Python usage:**
```python
import vroom_csv

# Default: no error collection
df = vroom_csv.read_csv("data.csv")

# Strict mode
df = vroom_csv.read_csv("data.csv", error_mode="fail_fast")

# Collect errors
result = vroom_csv.read_csv("data.csv", error_mode="permissive", return_errors=True)
for err in result.errors:
    print(f"Line {err.line}: {err.message}")
```

## Performance Considerations

1. **DISABLED mode is default** - no overhead for users who don't need errors
2. **Branch hints** - `[[unlikely]]` on error paths
3. **Thread-local collectors** - no lock contention during parallel parsing
4. **Error limit** - max_errors prevents memory exhaustion on severely malformed files
5. **Detection is "free"** - quote parity and field counting already happen during parsing

## Test Coverage

After implementation, the following tests should pass:
- `error_handling_test` - 51 tests for ErrorCollector, ParseError, modes
- `test/data/malformed/` - 16 malformed CSV test files

## Files Changed Summary

| File | Change Type |
|------|-------------|
| `include/libvroom/error.h` | New (port from v1) |
| `src/error.cpp` | New (port from v1) |
| `include/libvroom/options.h` | Modify |
| `include/libvroom/vroom.h` | Modify |
| `src/parser/line_parser.cpp` | Modify |
| `src/reader/csv_reader.cpp` | Modify |
| `src/cli.cpp` | Modify |
| `python/src/bindings.cpp` | Modify |
| `CMakeLists.txt` | Modify |
| `test/error_handling_test.cpp` | Enable + update |
