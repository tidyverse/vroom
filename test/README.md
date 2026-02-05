# libvroom Test Suite

This directory contains the test suite for libvroom, including test data files and Google Test-based test harness.

## Directory Structure

```
test/
├── csv_parser_test.cpp          # Well-formed CSV test harness
├── error_handling_test.cpp      # Error handling test harness
├── data/                         # Test CSV files
│   ├── basic/                    # Basic well-formed CSVs
│   │   ├── simple.csv            # 3x4 simple CSV
│   │   ├── simple_no_header.csv  # CSV without header row
│   │   ├── single_column.csv     # Single column CSV
│   │   ├── wide_columns.csv      # 20 columns
│   │   └── many_rows.csv         # 20+ rows
│   ├── quoted/                   # Quote handling tests
│   │   ├── quoted_fields.csv     # Standard quoted fields
│   │   ├── escaped_quotes.csv    # Escaped quotes ("") tests
│   │   ├── mixed_quoted.csv      # Mix of quoted/unquoted
│   │   ├── embedded_separators.csv # Commas inside quotes
│   │   └── newlines_in_quotes.csv  # Newlines in quoted fields
│   ├── separators/               # Different separator tests
│   │   ├── semicolon.csv         # Semicolon-separated
│   │   ├── tab.csv               # Tab-separated
│   │   └── pipe.csv              # Pipe-separated
│   ├── edge_cases/               # Edge case tests
│   │   ├── empty_fields.csv      # Empty field handling
│   │   ├── single_cell.csv       # Single cell CSV
│   │   ├── single_row_header_only.csv # Header only
│   │   ├── empty_file.csv        # Completely empty
│   │   └── whitespace_fields.csv # Whitespace handling
│   ├── line_endings/             # Line ending variations
│   │   ├── crlf.csv              # Windows (\\r\\n)
│   │   ├── lf.csv                # Unix (\\n)
│   │   ├── cr.csv                # Old Mac (\\r)
│   │   └── no_final_newline.csv  # Missing final newline
│   ├── real_world/               # Real-world data patterns
│   │   ├── financial.csv         # Stock market data
│   │   ├── contacts.csv          # Contact information
│   │   ├── unicode.csv           # International characters
│   │   └── product_catalog.csv   # E-commerce data
│   └── malformed/                # Malformed CSV error tests
│       ├── unclosed_quote.csv    # Quote not closed before newline
│       ├── unclosed_quote_eof.csv # Quote not closed at EOF
│       ├── quote_in_unquoted_field.csv # Invalid quote placement
│       ├── inconsistent_columns.csv    # Varying field counts
│       ├── invalid_quote_escape.csv    # Bad quote escaping
│       ├── duplicate_column_names.csv  # Duplicate headers
│       ├── mixed_line_endings.csv      # Inconsistent line endings
│       └── ... (16 total)        # See docs/error_handling.md
└── README.md                     # This file
```

## Test Data Categories

### 1. Basic Tests (`basic/`)
Well-formed CSV files covering fundamental parsing scenarios:
- Simple rectangular data
- Files with/without headers
- Single column/row scenarios
- Wide (many columns) and tall (many rows) files

### 2. Quoted Field Tests (`quoted/`)
Quote handling according to RFC 4180:
- Standard quoted fields with embedded separators
- Escaped quotes (doubled: `""`)
- Mixed quoted and unquoted fields
- Newlines within quoted fields (multiline values)

### 3. Separator Tests (`separators/`)
Different delimiter characters:
- Comma (`,`) - default
- Semicolon (`;`) - common in European locales
- Tab (`\t`) - TSV format
- Pipe (`|`) - common in Unix tools

### 4. Edge Cases (`edge_cases/`)
Unusual but valid CSV files:
- Empty fields (consecutive separators)
- Single cell files
- Header-only files (no data rows)
- Completely empty files
- Whitespace-heavy fields

### 5. Line Endings (`line_endings/`)
Different line termination styles:
- **CRLF** (`\r\n`) - Windows standard
- **LF** (`\n`) - Unix/Linux/macOS standard
- **CR** (`\r`) - Old Macintosh standard
- Files without final newline

### 6. Real-World Data (`real_world/`)
Representative real-world CSV patterns:
- **financial.csv**: Stock market OHLCV data
- **contacts.csv**: Contact information with quoted addresses
- **unicode.csv**: International characters (UTF-8)
- **product_catalog.csv**: E-commerce product data with complex fields

### 7. Malformed CSVs (`malformed/`)
Error detection and handling tests (16 files):
- **unclosed_quote.csv**: Quote not closed before newline
- **unclosed_quote_eof.csv**: Quote not closed at end of file
- **quote_in_unquoted_field.csv**: Quote in middle of unquoted field
- **inconsistent_columns.csv**: Rows with different field counts
- **invalid_quote_escape.csv**: Malformed quote escape sequences
- **empty_header.csv**: Missing or empty header row
- **duplicate_column_names.csv**: Duplicate column names
- **trailing_quote.csv**: Quote after unquoted field data
- **multiple_errors.csv**: Multiple error types in one file
- **mixed_line_endings.csv**: Inconsistent CRLF/LF/CR
- **null_byte.csv**: Contains null byte character
- And 5 more... (see `docs/error_handling.md` for complete list)

## Building and Running Tests

### Using CMake (Recommended)

```bash
# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build build

# Run all tests
cd build && ctest --output-on-failure

# Or run test executables directly
./build/libvroom_test          # Well-formed CSV tests (42 tests)
./build/error_handling_test   # Error handling tests (37 tests)
```

### Debug Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
./build/libvroom_test
```

## Test Organization

The test suite consists of two main test harnesses:

### 1. Well-Formed CSV Tests (`csv_parser_test.cpp` - 42 tests)
Validates correctly formatted CSV files:
- File existence and structure
- Line counts, field counts
- Content validation (quotes, separators, etc.)
- Special cases (empty files, UTF-8, line endings)

### 2. Error Handling Tests (`error_handling_test.cpp` - 37 tests)
Validates error detection and reporting:
- Error code and severity handling
- ParseError structure and formatting
- ErrorCollector functionality (strict/permissive/best-effort modes)
- ParseException throwing and catching
- Malformed CSV file detection (16 error scenarios)

**Note**: These are currently **infrastructure tests** that validate the error handling framework itself. Future work will integrate actual CSV parsing with error detection.

## Test Inspirations

This test suite was inspired by:

1. **Sep CSV Parser** (C#/.NET)
   - https://github.com/nietras/Sep/
   - World's fastest .NET CSV parser (21 GB/s)
   - Excellent fuzz testing and round-trip validation approach

2. **data.table** (R package)
   - https://github.com/Rdatatable/data.table/tree/master/inst/tests
   - Comprehensive real-world CSV test files
   - Strong focus on edge cases and regression tests

## Next Steps

1. **Parser integration**: Update tests to actually parse CSVs using libvroom
2. **Index validation**: Verify field positions are correctly identified
3. **Performance tests**: Add benchmarks for each test file
4. **Fuzz testing**: Implement randomized CSV generation (à la Sep)
5. **Malformed CSV tests**: Add separate suite for error handling
6. **Large file tests**: Add multi-MB test files for buffer management

## Test File Format Standards

All test files follow RFC 4180 CSV specification where applicable:
- Fields containing separators, newlines, or quotes must be quoted
- Quotes within quoted fields are escaped by doubling (`""`)
- Line endings can be CRLF, LF, or CR
- UTF-8 encoding for all files

## Contributing Test Files

When adding new test files:
1. Choose appropriate category subdirectory
2. Use descriptive filename (e.g., `complex_quotes.csv`)
3. Add corresponding test case in `csv_parser_test.cpp`
4. Document the specific edge case being tested
5. Keep files small (<10KB) unless testing large file handling
