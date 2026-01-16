# libvroom

<!-- badges: start -->
[![CI](https://github.com/jimhester/libvroom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jimhester/libvroom/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jimhester/libvroom/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/libvroom)
<!-- badges: end -->

High-performance CSV parser using SIMD instructions. Uses multi-threaded speculative parsing to process large files in parallel with throughput exceeding 4 GB/s on modern hardware.

## Installation

```bash
git clone https://github.com/jimhester/libvroom.git
cd libvroom
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

## Usage

### Command Line

The build produces a `vroom` command line tool:

```bash
vroom count data.csv              # Count rows
vroom head -n 20 data.csv         # Display first 20 rows
vroom tail -n 10 data.csv         # Display last 10 rows
vroom sample -n 100 data.csv      # Random sample of 100 rows
vroom select -c name,age data.csv # Select columns by name or index
vroom pretty data.csv             # Pretty-print with aligned columns
vroom info data.csv               # Get file info (rows, columns, dialect)
vroom dialect data.csv            # Detect and display CSV dialect
vroom schema data.csv             # Infer column types
vroom stats data.csv              # Column statistics (min, max, mean)
```

### C++ Library

```cpp
#include <libvroom.h>

// Load and parse a CSV file
libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
libvroom::Parser parser(4);  // Use 4 threads

auto result = parser.parse(buffer.data(), buffer.size());

if (result.success()) {
    std::cout << "Columns: " << result.num_columns() << "\n";
    std::cout << "Rows: " << result.num_rows() << "\n";

    // Iterate over rows
    for (auto row : result.rows()) {
        auto name = row.get<std::string>("name");
        auto age = row.get<int>("age");
        if (name.ok() && age.ok()) {
            std::cout << name.get() << ": " << age.get() << "\n";
        }
    }
}

// Check for errors
if (result.has_errors()) {
    std::cerr << result.error_summary() << "\n";
}
```

### Streaming Parser (Large Files)

```cpp
#include <streaming.h>

// Memory-efficient row-by-row processing
libvroom::StreamReader reader("large_file.csv");

for (const auto& row : reader) {
    std::cout << row[0].str() << "\n";
}
```

### C API (FFI Bindings)

```c
#include <libvroom_c.h>

libvroom_buffer_t* buffer = libvroom_buffer_load_file("data.csv");
libvroom_parser_t* parser = libvroom_parser_create();
libvroom_detection_result_t* detection = NULL;

libvroom_parse_auto(parser, buffer, index, errors, &detection);

printf("Columns: %zu\n", libvroom_detection_result_columns(detection));

// Cleanup
libvroom_detection_result_destroy(detection);
libvroom_parser_destroy(parser);
libvroom_buffer_destroy(buffer);
```

### CMake Integration

```cmake
include(FetchContent)
FetchContent_Declare(libvroom
  GIT_REPOSITORY https://github.com/jimhester/libvroom.git
  GIT_TAG main)
FetchContent_MakeAvailable(libvroom)

target_link_libraries(your_target PRIVATE libvroom_lib)
```

## Features

- **SIMD-accelerated parsing** via [Google Highway](https://github.com/google/highway) (x86-64 SSE4.2/AVX2/AVX-512, ARM NEON)
- **Multi-threaded** speculative chunking for parallel processing of large files
- **Streaming parser** for memory-efficient processing of files larger than RAM
- **Index caching** for instant re-reads of previously parsed files
- **Automatic dialect detection** (delimiter, quoting style, line endings, encoding)
- **Automatic encoding detection** with transcoding support (UTF-8, UTF-16, UTF-32, Latin-1)
- **Schema inference** with type detection (integer, float, boolean, string)
- **Three error modes**: `STRICT` (stop on first error), `PERMISSIVE` (collect all errors), `BEST_EFFORT` (ignore errors)
- **C API** for FFI integration with other languages
- **Cross-platform** support for Linux and macOS (x86-64 and ARM64)

## Performance

Single-threaded throughput on Apple Silicon (M3 Max):

| File Size | Throughput |
|-----------|------------|
| 10 MB | 3.1 GB/s |
| 100 MB | 4.4 GB/s |
| 200 MB | 4.7 GB/s |

Multi-threaded throughput reaches 6+ GB/s on large files. See [Benchmarks](https://jimhester.github.io/libvroom/benchmarks.html) for detailed comparisons.

## Documentation

- [Getting Started](https://jimhester.github.io/libvroom/getting-started.html) - Build instructions and basic usage
- [CLI Reference](https://jimhester.github.io/libvroom/cli.html) - Command line tool options
- [Streaming Parser](https://jimhester.github.io/libvroom/streaming.html) - Memory-efficient parsing for large files
- [Index Caching](https://jimhester.github.io/libvroom/caching.html) - Speed up repeated file reads
- [C API Reference](https://jimhester.github.io/libvroom/c-api.html) - C bindings for FFI
- [Architecture](https://jimhester.github.io/libvroom/architecture.html) - Two-pass algorithm details
- [Error Handling](https://jimhester.github.io/libvroom/error-handling.html) - Error modes and recovery
- [API Reference](https://jimhester.github.io/libvroom/api/) - Full API documentation

## How It Works

libvroom uses a two-pass algorithm based on [Chang et al. (SIGMOD 2019)](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf):

1. **First pass**: Scan for line boundaries while tracking quote parity to find safe split points
2. **Second pass**: SIMD-accelerated field indexing, processing 64 bytes at a time

This approach, combined with SIMD techniques from [Langdale & Lemire's simdjson](https://arxiv.org/abs/1902.08318), enables parallel parsing while correctly handling quoted fields that span chunk boundaries.
