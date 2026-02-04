# libvroom API Reference {#mainpage}

High-performance CSV to Parquet converter using SIMD instructions.

## Quick Start

The simplest way to use libvroom is with the `convert_csv_to_parquet()` function:

```cpp
#include "libvroom.h"

// Convert CSV to Parquet
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

---

## Core API

### Main Function

| Function | Description |
|----------|-------------|
| `vroom::convert_csv_to_parquet()` | Convert CSV file to Parquet format. |

### CSV Reading

| Class | Description |
|-------|-------------|
| `vroom::CsvReader` | CSV file reader with type inference. |
| `vroom::CsvOptions` | Configuration for CSV parsing (separator, quote, header). |

### Parquet Writing

| Class | Description |
|-------|-------------|
| `vroom::ParquetWriter` | Parquet file writer with compression support. |
| `vroom::ParquetOptions` | Configuration for Parquet output (compression, row group size). |

### Configuration

| Struct | Description |
|--------|-------------|
| `vroom::VroomOptions` | Combined options for CSV-to-Parquet conversion. |
| `vroom::ColumnSchema` | Column metadata (name, type, nullable). |

---

## CSV Options

```cpp
vroom::CsvOptions csv_opts;
csv_opts.separator = ',';       // Field delimiter
csv_opts.quote = '"';           // Quote character
csv_opts.has_header = true;     // First row is header
csv_opts.skip_rows = 0;         // Rows to skip before header
```

---

## Parquet Options

```cpp
vroom::ParquetOptions parquet_opts;
parquet_opts.compression = vroom::Compression::ZSTD;  // NONE, SNAPPY, ZSTD, LZ4
parquet_opts.row_group_size = 262144;                 // Rows per row group
```

### Compression Codecs

| Codec | Description |
|-------|-------------|
| `vroom::Compression::NONE` | No compression |
| `vroom::Compression::SNAPPY` | Fast compression |
| `vroom::Compression::ZSTD` | Best compression ratio |
| `vroom::Compression::LZ4` | Very fast compression |

---

## Data Types

libvroom automatically infers column types:

| Type | Description |
|------|-------------|
| `vroom::DataType::INT32` | 32-bit integers |
| `vroom::DataType::INT64` | 64-bit integers |
| `vroom::DataType::FLOAT64` | 64-bit floating point |
| `vroom::DataType::BOOL` | Boolean values |
| `vroom::DataType::STRING` | UTF-8 strings |

---

## Advanced Usage

### Reading CSV Data

```cpp
vroom::CsvOptions opts;
opts.separator = '\t';  // TSV file

vroom::CsvReader reader(opts);
auto open_result = reader.open("data.tsv");

if (open_result.ok) {
    // Get inferred schema
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

### Writing Parquet Files

```cpp
vroom::ParquetOptions opts;
opts.compression = vroom::Compression::ZSTD;

vroom::ParquetWriter writer(opts);
writer.open("output.parquet");

// Set schema
std::vector<vroom::ColumnSchema> schema = {
    {"name", vroom::DataType::STRING, true, 0},
    {"age", vroom::DataType::INT32, true, 1}
};
writer.set_schema(schema);

// Write data (using ArrowColumnBuilder)
// ... add data to columns ...

writer.write(columns);
writer.close();
```

---

## Command Line Tool

```bash
# Basic conversion
vroom input.csv output.parquet

# With compression
vroom input.csv output.parquet --compression zstd

# TSV file
vroom input.tsv output.parquet --separator '\t'

# Skip header row
vroom input.csv output.parquet --no-header
```
