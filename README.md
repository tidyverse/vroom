# libvroom

<!-- badges: start -->
[![CI](https://github.com/jimhester/libvroom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jimhester/libvroom/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jimhester/libvroom/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/libvroom)
<!-- badges: end -->

High-performance CSV to Parquet converter using SIMD instructions. Converts CSV files directly to Parquet format with automatic type inference, achieving throughput exceeding 4 GB/s on modern hardware.

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
# Convert CSV to Parquet (default command)
vroom input.csv -o output.parquet

# With compression (zstd, snappy, gzip, lz4, or none)
vroom input.csv -o output.parquet -c zstd

# Control row group size
vroom input.csv -o output.parquet -r 100000

# Custom delimiter and quote character
vroom input.csv -o output.parquet -d ';' -q "'"

# Verbose output with progress
vroom input.csv -o output.parquet -v

# Get help
vroom --help
```

### C++ Library

```cpp
#include <libvroom.h>

// Simple CSV to Parquet conversion
vroom::VroomOptions opts;
opts.input_path = "data.csv";
opts.output_path = "data.parquet";
opts.parquet.compression = vroom::Compression::ZSTD;

auto result = vroom::convert_csv_to_parquet(opts);

if (result.ok()) {
    std::cout << "Converted " << result.rows << " rows, "
              << result.cols << " columns\n";
} else {
    std::cerr << "Error: " << result.error << "\n";
}
```

### Using CsvReader directly

```cpp
#include <libvroom.h>

// Read CSV and access data programmatically
vroom::CsvOptions csv_opts;
csv_opts.separator = ',';
csv_opts.has_header = true;

vroom::CsvReader reader(csv_opts);
auto open_result = reader.open("data.csv");

if (open_result.ok) {
    // Access schema
    const auto& schema = reader.schema();
    for (const auto& col : schema) {
        std::cout << col.name << ": " << static_cast<int>(col.type) << "\n";
    }

    // Read all data
    auto read_result = reader.read_all();
    if (read_result.ok) {
        std::cout << "Read " << read_result.value.total_rows << " rows\n";
    }
}
```

### Python Bindings

```python
import vroom_csv

# Convert CSV to Parquet directly
vroom_csv.to_parquet("data.csv", "output.parquet", compression="zstd")

# Or read CSV for inspection
table = vroom_csv.read_csv("data.csv")
print(f"Columns: {table.column_names}")
print(f"Rows: {table.num_rows}")
```

### CMake Integration

```cmake
include(FetchContent)
FetchContent_Declare(libvroom
  GIT_REPOSITORY https://github.com/jimhester/libvroom.git
  GIT_TAG main)
FetchContent_MakeAvailable(libvroom)

target_link_libraries(your_target PRIVATE vroom)
```

## Features

- **SIMD-accelerated parsing** via [Google Highway](https://github.com/google/highway) (x86-64 SSE4.2/AVX2/AVX-512, ARM NEON)
- **Direct Parquet output** with no intermediate Arrow dependency
- **Multi-threaded** speculative chunking for parallel processing of large files
- **Automatic type inference** (integer, float, boolean, string) with SIMD-optimized parsers
- **Compression support**: ZSTD, Snappy, Gzip, LZ4, or uncompressed
- **Python bindings** with Arrow PyCapsule interface for zero-copy interop
- **UTF-8 validation** via [simdutf](https://github.com/simdutf/simdutf) for high-speed character validation
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
