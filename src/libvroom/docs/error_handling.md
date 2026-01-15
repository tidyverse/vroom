# Error Handling in libvroom

This document describes the error handling framework for libvroom, including error modes, error types, the ErrorCollector API, recovery behavior, and practical usage examples.

## Table of Contents

- [Overview](#overview)
- [Error Modes](#error-modes)
- [Error Types](#error-types)
- [Error Severity Levels](#error-severity-levels)
- [ErrorCollector API](#errorcollector-api)
- [ParseError Structure](#parseerror-structure)
- [Exception-Based Error Handling](#exception-based-error-handling)
- [Error Recovery Behavior](#error-recovery-behavior)
- [Code Examples](#code-examples)
- [Best Practices](#best-practices)
- [Test Files](#test-files)

## Overview

libvroom provides comprehensive error detection and reporting for malformed CSV files. The error handling system is designed to:

1. **Detect common CSV errors** - Quote escaping, field count mismatches, invalid characters
2. **Provide precise location information** - Line, column, and byte offset
3. **Support three error modes** - Strict, permissive, and best-effort parsing
4. **Never throw for parse errors** - All errors accessible via `result.errors()`
5. **Collect multiple errors** - See all problems in one pass (permissive mode)
6. **Support multi-threaded parsing** - Thread-local error collection with merging

### Key Design Principle

**`Parser::parse()` never throws exceptions for parse errors.** All parse errors are
returned via the `Result` object's internal `ErrorCollector`, accessible through
`result.errors()`, `result.has_errors()`, and `result.error_summary()`.

Exceptions are reserved exclusively for truly exceptional conditions such as:
- System-level memory allocation failures
- Internal programming errors (bugs in libvroom itself)

This design ensures predictable, non-exceptional control flow for all parsing
operations, making error handling simpler and more consistent.

## Error Modes

libvroom supports three error handling modes via the `ErrorMode` enum. Choose the appropriate mode based on your use case:

### STRICT Mode

```cpp
ErrorCollector errors(ErrorMode::FAIL_FAST);
```

- **Behavior**: Stop parsing on the first error encountered (any severity)
- **Best for**: Production data processing where any malformation is unacceptable
- **Use when**: CSV files must be perfectly formatted; you want fail-fast behavior
- **Recovery**: No recovery attempted; parsing stops immediately

### PERMISSIVE Mode

```cpp
ErrorCollector errors(ErrorMode::PERMISSIVE);
```

- **Behavior**: Collect all errors and continue parsing; stop only on FATAL errors
- **Best for**: Data validation, debugging malformed CSVs, data quality reporting
- **Use when**: You want to see all problems in a single pass
- **Recovery**: Attempts recovery for ERROR-level issues; problematic rows may be skipped

### BEST_EFFORT Mode

```cpp
ErrorCollector errors(ErrorMode::BEST_EFFORT);
```

- **Behavior**: Parse as much data as possible, ignoring errors entirely
- **Best for**: Importing messy real-world data where partial success is acceptable
- **Use when**: You need to extract whatever data is available from imperfect files
- **Recovery**: Maximum recovery; continues parsing through all errors

### Mode Comparison Table

| Aspect | STRICT | PERMISSIVE | BEST_EFFORT |
|--------|--------|------------|-------------|
| Stops on WARNING | Yes | No | No |
| Stops on ERROR | Yes | No | No |
| Stops on FATAL | Yes | Yes | No |
| Collects errors | Yes | Yes | Yes |
| Data integrity | Highest | Medium | Lowest |
| Error visibility | First only | All | All |

> **Note:** In STRICT mode, `should_stop()` returns true as soon as any error is recorded, regardless of severity. In BEST_EFFORT mode, the parser continues through all errors tracked by `ErrorCollector`.

## Error Types

libvroom defines 15 error types in `include/error.h` as the `ErrorCode` enum.

### Quote-Related Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `UNCLOSED_QUOTE` | FATAL | Implemented | Quoted field not closed before EOF |
| `INVALID_QUOTE_ESCAPE` | ERROR | Implemented | Invalid quote escape sequence (e.g., `"bad"escape"`) |
| `QUOTE_IN_UNQUOTED_FIELD` | ERROR | Implemented | Quote appears in middle of unquoted field |

**Examples:**

```csv
# UNCLOSED_QUOTE - quote never closed
"unclosed quote,field2

# INVALID_QUOTE_ESCAPE - should be "bad""escape"
"bad"escape"

# QUOTE_IN_UNQUOTED_FIELD - quote mid-field
field"with"quotes
```

### Field Structure Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `INCONSISTENT_FIELD_COUNT` | ERROR | Implemented | Row has different field count than header |
| `FIELD_TOO_LARGE` | ERROR | Implemented | Field exceeds maximum size limit |

**Example:**

```csv
# Header has 3 fields, row 2 has only 2
A,B,C
1,2,3
4,5
```

### Line Ending Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `MIXED_LINE_ENDINGS` | WARNING | Implemented | File uses inconsistent line endings |

### Character Encoding Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `INVALID_UTF8` | ERROR | Implemented | Invalid UTF-8 byte sequence |
| `NULL_BYTE` | ERROR | Implemented | Unexpected null byte in data |

### Structure Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `EMPTY_HEADER` | ERROR | Implemented | Header row is empty or missing |
| `DUPLICATE_COLUMN_NAMES` | WARNING | Implemented | Header contains duplicate column names |

**Examples:**

```csv
# EMPTY_HEADER - first line is empty

1,2,3

# DUPLICATE_COLUMN_NAMES - 'A' appears twice
A,B,A,C
1,2,3,4
```

### Separator Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `AMBIGUOUS_SEPARATOR` | ERROR | Implemented | Cannot reliably determine field separator |

### General Errors

| Error Code | Severity | Status | Description |
|------------|----------|--------|-------------|
| `FILE_TOO_LARGE` | FATAL | Implemented | File exceeds maximum size limit |
| `IO_ERROR` | FATAL | Implemented | File I/O error |
| `INTERNAL_ERROR` | FATAL | Implemented | Internal parser error (bug in libvroom) |

## Error Severity Levels

Errors are classified into three severity levels that determine parser behavior:

### WARNING

- **Effect on parsing**: Parser continues normally
- **Effect on `should_stop()`**: Does not trigger stop in any mode
- **Typical errors**: `MIXED_LINE_ENDINGS`, `DUPLICATE_COLUMN_NAMES`
- **Data impact**: Data is still usable; warning indicates potential quality issues

### ERROR

- **Effect on parsing**: Parser may skip affected row and continue (mode-dependent)
- **Effect on `should_stop()`**: Triggers stop in STRICT mode only
- **Typical errors**: `INCONSISTENT_FIELD_COUNT`, `QUOTE_IN_UNQUOTED_FIELD`, `EMPTY_HEADER`
- **Data impact**: Affected row may have incomplete or incorrect data

### FATAL

- **Effect on parsing**: Parser cannot continue reliably
- **Effect on `should_stop()`**: Triggers stop in STRICT and PERMISSIVE modes
- **Typical errors**: `UNCLOSED_QUOTE` at EOF, `IO_ERROR`, `INTERNAL_ERROR`
- **Data impact**: Parsing results may be incomplete or corrupted

### Severity Behavior Matrix

| Severity | STRICT stops? | PERMISSIVE stops? | BEST_EFFORT stops? |
|----------|---------------|-------------------|---------------------|
| WARNING  | Yes | No | No |
| ERROR    | Yes | No | No |
| FATAL    | Yes | Yes | No |

> **Implementation detail:** `should_stop()` in STRICT mode checks if any errors have been collected (`!errors_.empty()`), causing a stop regardless of severity. This means even WARNINGs will halt parsing in STRICT mode.

## ErrorCollector API

The `ErrorCollector` class is the primary interface for error handling in libvroom. It accumulates errors during parsing and provides methods to query and manage them.

### Construction

```cpp
// Default: STRICT mode, max 10000 errors
ErrorCollector errors;

// Specify mode
ErrorCollector errors(ErrorMode::PERMISSIVE);

// Specify mode and max errors limit
ErrorCollector errors(ErrorMode::PERMISSIVE, 5000);
```

The `max_errors` parameter prevents memory exhaustion when parsing malformed files that could generate excessive errors.

### Adding Errors

```cpp
// Add a ParseError object
ParseError err(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
               5, 10, 123, "Quote not closed", "\"unclosed");
errors.add_error(err);

// Convenience overload with individual parameters
errors.add_error(
    ErrorCode::INCONSISTENT_FIELD_COUNT,  // code
    ErrorSeverity::RECOVERABLE,                  // severity
    3,                                     // line (1-indexed)
    1,                                     // column (1-indexed)
    45,                                    // byte_offset
    "Expected 3 fields but found 2",       // message
    "1,2"                                  // context (optional)
);
```

### Querying State

```cpp
// Check if any errors were recorded
bool hasErrors = errors.has_errors();

// Check specifically for fatal errors
bool hasFatal = errors.has_fatal_errors();

// Get total error count
size_t count = errors.error_count();

// Check if error limit was reached
bool atLimit = errors.at_error_limit();

// Get current error mode
ErrorMode mode = errors.mode();

// Check if parsing should stop (depends on mode and error severity)
bool stop = errors.should_stop();
```

### Accessing Errors

```cpp
// Get read-only access to all errors
const std::vector<ParseError>& allErrors = errors.errors();

// Iterate through errors
for (const auto& err : errors.errors()) {
    std::cout << err.to_string() << std::endl;
}

// Get summary string
std::string summary = errors.summary();
// Output: "Total errors: 3 (Warnings: 1, Errors: 2)"
```

### Managing State

```cpp
// Clear all errors and reset fatal flag
errors.clear();

// Change error mode dynamically
errors.set_mode(ErrorMode::BEST_EFFORT);
```

### Multi-threaded Support

When using multi-threaded parsing, each thread should use its own `ErrorCollector`. After parsing completes, merge the results:

```cpp
// Each thread has its own collector
std::vector<ErrorCollector> thread_errors(num_threads);

// ... parallel parsing ...

// Merge all thread-local errors into a main collector
ErrorCollector main_errors(ErrorMode::PERMISSIVE);
main_errors.merge_sorted(thread_errors);
```

Individual merge operations:

```cpp
// Merge from another collector (respects max_errors limit)
main_errors.merge_from(other_collector);

// Sort errors by byte offset (for logical file order)
main_errors.sort_by_offset();
```

> **Thread Safety Note**: `ErrorCollector` is NOT thread-safe. Each thread must use its own collector instance during parsing.

## ParseError Structure

Each error is represented by a `ParseError` struct containing complete information about the error location and context:

```cpp
struct ParseError {
    ErrorCode code;           // Error type (from ErrorCode enum)
    ErrorSeverity severity;   // WARNING, ERROR, or FATAL

    // Location information (all 1-indexed for user display)
    size_t line;              // Line number where error occurred
    size_t column;            // Column number where error occurred
    size_t byte_offset;       // Byte offset from start of file

    // Context
    std::string message;      // Human-readable error description
    std::string context;      // Snippet of data around the error location
};
```

### Creating ParseErrors

```cpp
// Full constructor
ParseError error(
    ErrorCode::UNCLOSED_QUOTE,    // code
    ErrorSeverity::FATAL,          // severity
    5,                             // line
    10,                            // column
    123,                           // byte_offset
    "Quote not closed",            // message
    "\"unclosed"                   // context (optional)
);
```

### Converting to String

```cpp
ParseError error(...);
std::cout << error.to_string() << std::endl;

// Output:
// [FATAL] UNCLOSED_QUOTE at line 5, column 10 (byte 123): Quote not closed
//   Context: "unclosed
```

### Utility Functions

```cpp
// Convert error code to string name
const char* name = error_code_to_string(ErrorCode::UNCLOSED_QUOTE);
// Returns: "UNCLOSED_QUOTE"

// Convert severity to string
const char* sev = error_severity_to_string(ErrorSeverity::FATAL);
// Returns: "FATAL"
```

## Error Recovery Behavior

This section describes how libvroom attempts to recover from different error types in PERMISSIVE and BEST_EFFORT modes.

### Quote-Related Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| `UNCLOSED_QUOTE` | **No recovery possible.** The parser cannot determine where the field ends without a closing quote. In PERMISSIVE mode, this is FATAL and parsing stops. |
| `INVALID_QUOTE_ESCAPE` | Parser treats the problematic sequence as literal characters and continues to the next field or newline. The field value may be incorrect. |
| `QUOTE_IN_UNQUOTED_FIELD` | Parser continues reading the field as unquoted, treating quotes as literal characters. The field ends at the next delimiter or newline. |

### Field Structure Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| `INCONSISTENT_FIELD_COUNT` | Parser records the error and continues to the next row. The affected row is parsed with whatever fields are present. Missing fields appear as empty; extra fields may be ignored. |
| `FIELD_TOO_LARGE` | Parser records the error and skips the oversized field. Used in streaming API to prevent DoS attacks. |

### Line Ending Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| `MIXED_LINE_ENDINGS` | Parser normalizes all line endings during parsing. A warning is recorded but parsing continues normally. Data integrity is not affected. |

### Structure Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| `EMPTY_HEADER` | Parser records the error. Subsequent rows may be parsed without column name mapping. Field access by index still works. |
| `DUPLICATE_COLUMN_NAMES` | Parser records a warning but continues. All columns are accessible; accessing by duplicate name returns the first occurrence. |

### Character Encoding Error Recovery

| Error | Recovery Strategy |
|-------|-------------------|
| `NULL_BYTE` | Parser records the error at the null byte location. In recovery mode, the null byte is typically treated as end-of-field or skipped. |
| `INVALID_UTF8` | Parser records the error at the invalid byte sequence location. Validation continues to find additional errors. Enable via `ValidationLimits::validate_utf8`. |

### Recovery Example

```cpp
#include <libvroom.h>

using namespace libvroom;

// Parse a file with known issues
FileBuffer buf = load_file("messy_data.csv");
Parser parser;

// Parse - errors are automatically collected in result
auto result = parser.parse(buf.data(), buf.size());

// Parsing completed (unless FATAL error occurred)
if (result.successful) {
    std::cout << "Parsed " << result.total_indexes() << " fields" << std::endl;
}

// Check what issues were found using unified result.errors() API
if (result.has_errors()) {
    std::cout << "Encountered " << result.error_count() << " issues:" << std::endl;

    for (const auto& err : result.errors()) {
        if (err.severity == ErrorSeverity::WARNING) {
            // Warnings - data is usable
            std::cout << "[WARN] " << err.message << std::endl;
        } else if (err.severity == ErrorSeverity::RECOVERABLE) {
            // Errors - specific rows may be affected
            std::cout << "[ERR] Line " << err.line << ": " << err.message << std::endl;
        }
    }
}
```

## Exception-Based Error Handling

libvroom provides `ParseException` for traditional exception-based error handling workflows.

### ParseException Class

```cpp
class ParseException : public std::runtime_error {
public:
    // Construct from single error
    explicit ParseException(const ParseError& error);

    // Construct from multiple errors
    explicit ParseException(const std::vector<ParseError>& errors);

    // Get the first (primary) error
    const ParseError& error() const;

    // Get all errors
    const std::vector<ParseError>& errors() const;

    // Inherited: const char* what() const - returns formatted message
};
```

### Catching Parse Exceptions

```cpp
#include <libvroom.h>

using namespace libvroom;

try {
    // Some operations that may throw ParseException
    auto result = parse_csv(data);
} catch (const ParseException& e) {
    // Get the primary error message
    std::cerr << "Parse failed: " << e.what() << std::endl;

    // Access the first error for details
    const ParseError& primary = e.error();
    std::cerr << "Location: line " << primary.line
              << ", column " << primary.column << std::endl;

    // If multiple errors, iterate through all
    for (const auto& err : e.errors()) {
        std::cerr << "  " << err.to_string() << std::endl;
    }
}
```

### Throwing ParseException

When building custom parsing logic that integrates with libvroom's error handling:

```cpp
// Throw for a single error
ParseError error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
                 5, 10, 100, "Quote not closed");
throw ParseException(error);

// Throw for multiple collected errors
ErrorCollector collector(ErrorMode::PERMISSIVE);
// ... parsing that collects errors ...
if (collector.has_fatal_errors()) {
    throw ParseException(collector.errors());
}
```

## Code Examples

This section provides complete examples demonstrating common error handling patterns.

### Example 1: STRICT Mode - Production Parsing

Use STRICT mode when CSV files must be perfectly formatted:

```cpp
#include <libvroom.h>
#include <iostream>

using namespace libvroom;

bool parse_production_csv(const std::string& filepath) {
    // Load file
    FileBuffer buffer = load_file(filepath);
    Parser parser;

    // Parse and check for errors using unified result.errors() API
    auto result = parser.parse(buffer.data(), buffer.size());

    if (!result.successful || result.has_errors()) {
        // Log the error and reject the file
        std::cerr << "CSV validation failed: " << filepath << std::endl;
        for (const auto& err : result.errors()) {
            std::cerr << err.to_string() << std::endl;
        }
        return false;
    }

    std::cout << "Successfully parsed " << result.num_columns()
              << " columns" << std::endl;
    return true;
}
```

### Example 2: PERMISSIVE Mode - Data Validation Report

Use PERMISSIVE mode to generate a complete validation report:

```cpp
#include <libvroom.h>
#include <iostream>
#include <fstream>

using namespace libvroom;

void validate_csv_report(const std::string& filepath, const std::string& report_path) {
    FileBuffer buffer = load_file(filepath);
    Parser parser;

    // Parse - auto-detects dialect, errors collected in result
    auto result = parser.parse(buffer.data(), buffer.size());

    // Write validation report
    std::ofstream report(report_path);
    report << "CSV Validation Report\n";
    report << "=====================\n\n";
    report << "File: " << filepath << "\n";
    report << "Detected dialect: " << result.dialect.to_string() << "\n";
    report << "Parse completed: " << (result.successful ? "Yes" : "No") << "\n\n";

    if (result.has_errors()) {
        report << result.error_summary() << "\n\n";

        // Group errors by type
        report << "Errors by Line:\n";
        for (const auto& err : result.errors()) {
            report << "  Line " << err.line << ": "
                   << error_code_to_string(err.code) << " - "
                   << err.message << "\n";
        }
    } else {
        report << "No errors found. File is valid.\n";
    }
}
```

### Example 3: BEST_EFFORT Mode - Messy Data Import

Use BEST_EFFORT mode to extract whatever data is available:

```cpp
#include <libvroom.h>
#include <iostream>

using namespace libvroom;

void import_messy_data(const std::string& filepath) {
    FileBuffer buffer = load_file(filepath);
    Parser parser;

    // Parse - errors are automatically collected in result
    auto result = parser.parse(buffer.data(), buffer.size());

    std::cout << "Import Results:\n";
    std::cout << "  Fields found: " << result.total_indexes() << "\n";
    std::cout << "  Issues encountered: " << result.error_count() << "\n";

    if (result.has_errors()) {
        // Log warnings for monitoring
        size_t warnings = 0, recoverable = 0;
        for (const auto& err : result.errors()) {
            if (err.severity == ErrorSeverity::WARNING) warnings++;
            else recoverable++;
        }
        std::cout << "  Warnings: " << warnings << "\n";
        std::cout << "  Recovered errors: " << recoverable << "\n";
    }

    // Process the imported data (even if imperfect)
    // ...
}
```

### Example 4: Handling Specific Error Types

Filter and handle specific error types differently:

```cpp
#include <libvroom.h>
#include <iostream>

using namespace libvroom;

void process_with_error_handling(const std::string& filepath) {
    FileBuffer buffer = load_file(filepath);
    Parser parser;

    // Parse - errors are automatically collected in result
    auto result = parser.parse(buffer.data(), buffer.size());

    // Handle different error types using result.errors()
    for (const auto& err : result.errors()) {
        switch (err.code) {
            case ErrorCode::DUPLICATE_COLUMN_NAMES:
                // Just log and continue - minor issue
                std::cerr << "Note: Duplicate column '" << err.context
                          << "' at position " << err.column << std::endl;
                break;

            case ErrorCode::INCONSISTENT_FIELD_COUNT:
                // Track which rows have issues for later review
                std::cerr << "Row " << err.line << " has field count mismatch"
                          << std::endl;
                break;

            case ErrorCode::UNCLOSED_QUOTE:
                // Critical - data may be corrupted after this point
                std::cerr << "CRITICAL: Unclosed quote at line " << err.line
                          << " - subsequent data may be unreliable" << std::endl;
                break;

            case ErrorCode::MIXED_LINE_ENDINGS:
                // Cosmetic - ignore
                break;

            default:
                std::cerr << err.to_string() << std::endl;
        }
    }
}
```

### Example 5: Multi-threaded Parsing with Error Collection

The `Parser` class handles multi-threaded parsing with error collection automatically:

```cpp
#include <libvroom.h>
#include <thread>

using namespace libvroom;

void parallel_parse_with_errors(const uint8_t* data, size_t len) {
    const size_t num_threads = std::thread::hardware_concurrency();

    // Create parser with multiple threads
    Parser parser(num_threads);

    // Parse - errors are automatically collected in result
    auto result = parser.parse(data, len);

    // Report results
    if (result.successful) {
        std::cout << "Parsed with " << num_threads << " threads\n";
    }

    if (result.has_errors()) {
        std::cout << "Errors found:\n";
        std::cout << result.error_summary() << std::endl;
    }
}
```

## Best Practices

### For Production Use (STRICT Mode)

1. **Use STRICT mode** for production parsing where data integrity is critical
2. **Fail fast** - stop processing on first error to avoid propagating bad data
3. **Log errors** with full context for debugging
4. **Validate data sources** to catch errors early in the pipeline

```cpp
Parser parser;
auto result = parser.parse(data, len);

if (!result.successful || result.has_errors()) {
    // Log and reject
    logger.error("CSV parsing failed: " + result.error_summary());
    return Error::INVALID_CSV;
}

// Process validated data with confidence
process_data(result);
```

### For Data Validation (PERMISSIVE Mode)

1. **Use PERMISSIVE mode** to generate comprehensive validation reports
2. **Report all issues** to help users identify and fix data problems
3. **Provide line numbers and context** for easy correction
4. **Categorize errors** by severity for prioritized fixing

```cpp
Parser parser;
auto result = parser.parse(data, len);

if (result.has_errors()) {
    // Generate detailed report
    std::cout << "Validation found " << result.error_count() << " issues:\n\n";

    // Separate by severity
    for (const auto& err : result.errors()) {
        if (err.severity == ErrorSeverity::FATAL) {
            std::cout << "CRITICAL: ";
        }
        std::cout << "Line " << err.line << ": " << err.message << "\n";
    }
}
```

### For Data Import (BEST_EFFORT Mode)

1. **Use BEST_EFFORT mode** when you need to extract whatever data is available
2. **Log warnings** for data quality monitoring and auditing
3. **Validate extracted data** after parsing to catch logical inconsistencies
4. **Document data quality** in metadata for downstream consumers

```cpp
Parser parser;
auto result = parser.parse(messy_data, len);

std::cout << "Imported " << result.total_indexes() << " fields";
if (result.has_errors()) {
    std::cout << " with " << result.error_count() << " issues";
}
std::cout << std::endl;

// Track quality metrics
data_quality_log.record(filepath, result.total_indexes(), result.error_count());
```

### General Guidelines

1. **Choose the right mode for your use case** - don't use STRICT when PERMISSIVE would give better user feedback
2. **Always check for errors** even in BEST_EFFORT mode - errors provide valuable quality information
3. **Use `error_summary()` for logging** - it provides a concise overview without dumping every error
4. **Consider `max_errors` limit** - set appropriately for your data size to prevent memory issues
5. **Use result.errors()** - errors are automatically collected in the Result object, no external collector needed

## Test Files

The test suite includes 16 malformed CSV test files in `test/data/malformed/`:

| File | Error Type | Description |
|------|------------|-------------|
| `unclosed_quote.csv` | `UNCLOSED_QUOTE` | Quote not closed before newline |
| `unclosed_quote_eof.csv` | `UNCLOSED_QUOTE` | Quote not closed at end of file |
| `quote_in_unquoted_field.csv` | `QUOTE_IN_UNQUOTED_FIELD` | Quote in middle of unquoted field |
| `inconsistent_columns.csv` | `INCONSISTENT_FIELD_COUNT` | Some rows have different field counts |
| `inconsistent_columns_all_rows.csv` | `INCONSISTENT_FIELD_COUNT` | Every row has different count |
| `invalid_quote_escape.csv` | `INVALID_QUOTE_ESCAPE` | Malformed quote escape sequence |
| `empty_header.csv` | `EMPTY_HEADER` | Missing or empty header row |
| `duplicate_column_names.csv` | `DUPLICATE_COLUMN_NAMES` | Duplicate column names in header |
| `trailing_quote.csv` | `QUOTE_IN_UNQUOTED_FIELD` | Quote after unquoted field data |
| `quote_not_at_start.csv` | `QUOTE_IN_UNQUOTED_FIELD` | Quote appears mid-field |
| `multiple_errors.csv` | Multiple | Multiple error types in one file |
| `mixed_line_endings.csv` | `MIXED_LINE_ENDINGS` | Inconsistent line ending styles |
| `null_byte.csv` | `NULL_BYTE` | Contains null byte character |
| `triple_quote.csv` | `INVALID_QUOTE_ESCAPE` | Triple quote sequence (ambiguous) |
| `unescaped_quote_in_quoted.csv` | `INVALID_QUOTE_ESCAPE` | Unescaped quote inside quoted field |
| `quote_after_data.csv` | `QUOTE_IN_UNQUOTED_FIELD` | Quote appears after field data |

All test files are validated by `test/error_handling_test.cpp`.

## See Also

- `include/error.h` - Error handling API with detailed documentation
- `include/libvroom.h` - **Main public header** with unified `Parser` class
- `src/error.cpp` - Error handling implementation
- `test/error_handling_test.cpp` - Error handling unit tests (37 tests)
- `test/data/malformed/` - Malformed CSV test files for error detection
