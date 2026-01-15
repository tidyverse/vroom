# libvroom API Reference {#mainpage}

High-performance CSV parser using SIMD instructions.

## Quick Start

The simplest way to use libvroom is with the `Parser` class and `FileBuffer`:

```cpp
#include "libvroom.h"

// Load CSV file (RAII - automatic cleanup)
libvroom::FileBuffer buffer = libvroom::load_file("data.csv");

// Create parser with 4 threads
libvroom::Parser parser(4);

// Parse with auto-detection (default behavior)
auto result = parser.parse(buffer.data(), buffer.size());

if (result.success()) {
    std::cout << "Detected: " << result.dialect.to_string() << "\n";
    std::cout << "Columns: " << result.num_columns() << "\n";
}
// Memory automatically freed when buffer goes out of scope
```

### Common Patterns

```cpp
// 1. Auto-detect dialect, throw on errors (default - fastest)
auto result = parser.parse(buf, len);

// 2. Auto-detect dialect, collect errors
ErrorCollector errors(ErrorMode::PERMISSIVE);
auto result = parser.parse(buf, len, {.errors = &errors});

// 3. Explicit dialect, throw on errors
auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});

// 4. Explicit dialect with error collection
auto result = parser.parse(buf, len, {
    .dialect = Dialect::tsv(),
    .errors = &errors
});

// 5. Maximum performance with branchless algorithm
auto result = parser.parse(buf, len, ParseOptions::branchless());
```

---

## Core API

### Primary Classes

| Class | Description |
|-------|-------------|
| @ref libvroom::Parser | **Main parser class** - unified API for all parsing needs. |
| @ref libvroom::FileBuffer | RAII wrapper for file buffers with automatic cleanup. |
| @ref libvroom::ParseOptions | Configuration for parsing (dialect, errors, algorithm). |

### Supporting Classes

| Class | Description |
|-------|-------------|
| @ref libvroom::ParseIndex | Result structure containing parsed field positions. |
| @ref libvroom::ErrorCollector | Collects and manages parse errors. |

### Internal Classes (Deprecated for Direct Use)

| Class | Description |
|-------|-------------|
| @ref libvroom::TwoPass | Low-level parser implementation. Use `Parser` instead. |

### Dialect Detection

| Class/Function | Description |
|----------------|-------------|
| @ref libvroom::Dialect | CSV dialect configuration (delimiter, quote char, etc.). |
| @ref libvroom::DialectDetector | Automatic dialect detection engine. |
| @ref libvroom::detect_dialect() | Convenience function to detect dialect from buffer. |
| @ref libvroom::detect_dialect_file() | Convenience function to detect dialect from file. |

### Key Methods

| Method | Description |
|--------|-------------|
| @ref libvroom::Parser::parse "Parser::parse()" | **Unified parse method** - handles all use cases via ParseOptions. |

The `Parser::parse()` method replaces multiple legacy methods:
- Auto-detects dialect by default, or use explicit `{.dialect = ...}`
- Collects errors with `{.errors = &collector}`, or throws by default
- Choose algorithm with `{.algorithm = ParseAlgorithm::BRANCHLESS}` for optimization

### File I/O

| Function | Description |
|----------|-------------|
| @ref libvroom::load_file() | Load a file into a FileBuffer (recommended). |
| @ref libvroom::load_file_to_ptr() | Load a file into an AlignedBuffer with RAII. |
| @ref allocate_padded_buffer() | Allocate a padded buffer for SIMD operations. |

---

## Dialect Support

libvroom supports multiple CSV dialects beyond standard comma-separated:

```cpp
// Standard CSV (comma, double-quote)
auto result = parser.parse(data, len, {.dialect = libvroom::Dialect::csv()});

// Tab-separated values
auto result = parser.parse(data, len, {.dialect = libvroom::Dialect::tsv()});

// Semicolon-separated (European style)
auto result = parser.parse(data, len, {.dialect = libvroom::Dialect::semicolon()});

// Pipe-separated
auto result = parser.parse(data, len, {.dialect = libvroom::Dialect::pipe()});

// Custom dialect
libvroom::Dialect custom;
custom.delimiter = ':';
custom.quote_char = '\'';
auto result = parser.parse(data, len, custom);
```

### Automatic Detection

```cpp
// Detect dialect from file (standalone detection)
auto detected = libvroom::detect_dialect_file("mystery.csv");
if (detected.success()) {
    std::cout << "Delimiter: '" << detected.dialect.delimiter << "'\n";
    std::cout << "Confidence: " << detected.confidence << "\n";
}

// Parse with auto-detection (default behavior)
libvroom::Parser parser(4);
auto result = parser.parse(data, len);  // Auto-detects dialect
std::cout << "Detected: " << result.dialect.to_string() << "\n";
```

---

## Error Handling

libvroom supports three error handling modes:

| Mode | Behavior |
|------|----------|
| @ref libvroom::ErrorMode::FAIL_FAST "FAIL_FAST" | Stop on first error |
| @ref libvroom::ErrorMode::PERMISSIVE "PERMISSIVE" | Collect all errors, try to recover |
| @ref libvroom::ErrorMode::BEST_EFFORT "BEST_EFFORT" | Ignore errors, parse what's possible |

### Example with Error Handling

```cpp
#include "libvroom.h"

libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
libvroom::Parser parser(4);

// Use unified parse() with error collection via ParseOptions
auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});

if (errors.has_errors()) {
    std::cout << errors.summary() << std::endl;
    for (const auto& err : errors.errors()) {
        std::cerr << err.to_string() << std::endl;
    }
}
```

---

## Header Files

| Header | Purpose |
|--------|---------|
| @ref libvroom.h | **Main public header** - includes Parser, FileBuffer, and everything you need |
| @ref error.h | Error handling (ErrorCollector, ErrorCode, ParseError) |
| @ref dialect.h | Dialect configuration and detection |
| @ref io_util.h | File loading with SIMD alignment |
| @ref mem_util.h | Aligned memory allocation |
| @ref two_pass.h | Internal implementation (use libvroom.h instead) |

---

## Common Pitfalls

Watch out for these common mistakes when using libvroom:

### Memory Management

**Always use `aligned_free()` for `aligned_malloc()`**

Memory allocated with `aligned_malloc()` or `allocate_padded_buffer()` MUST be freed
with `aligned_free()`. Using standard `free()` or `delete` causes undefined behavior
on some platforms (especially Windows).

```cpp
// WRONG - undefined behavior on Windows
void* buf = aligned_malloc(64, 1024);
free(buf);  // DO NOT DO THIS

// CORRECT - use aligned_free()
void* buf = aligned_malloc(64, 1024);
aligned_free(buf);

// BETTER - use RAII wrappers that handle this automatically
AlignedPtr buf = make_aligned_ptr(1024, 64);
// Memory automatically freed when buf goes out of scope
```

**Check for allocation failures in production code**

`aligned_malloc()` and `allocate_padded_buffer()` return `nullptr` on failure.
Always check the return value before using the pointer.

```cpp
void* buffer = aligned_malloc(64, large_size);
if (buffer == nullptr) {
    // Handle allocation failure
    throw std::bad_alloc();
}
```

### SIMD Safety

**Ensure 32+ bytes padding for SIMD operations**

SIMD operations read data in fixed-size chunks (32-64 bytes). Without sufficient
padding at the end of your buffer, reads past the data boundary can cause crashes.

```cpp
// WRONG - no padding, may crash with SIMD
uint8_t* buf = new uint8_t[file_size];

// CORRECT - use padded allocation
uint8_t* buf = allocate_padded_buffer(file_size, 64);  // 64 bytes padding

// BETTER - use load_file() which handles padding automatically
auto buffer = libvroom::load_file("data.csv");  // Includes default 64-byte padding
```

### Move-Only Types

**`FileBuffer` and `AlignedBuffer` are move-only**

These RAII wrappers cannot be copied to prevent double-free bugs. Use move semantics
or references when passing them around.

```cpp
// WRONG - compilation error
libvroom::FileBuffer buf1 = libvroom::load_file("data.csv");
libvroom::FileBuffer buf2 = buf1;  // ERROR: copy constructor deleted

// CORRECT - use move semantics
libvroom::FileBuffer buf2 = std::move(buf1);  // OK, buf1 is now empty

// CORRECT - pass by reference
void process(const libvroom::FileBuffer& buf);
process(buf1);  // OK, no copy
```

---

## See Also

- [Error Handling Guide](error_handling.md) - Detailed error handling documentation
