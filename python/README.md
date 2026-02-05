# vroom-csv

[![PyPI version](https://badge.fury.io/py/vroom-csv.svg)](https://badge.fury.io/py/vroom-csv)
[![Python versions](https://img.shields.io/pypi/pyversions/vroom-csv.svg)](https://pypi.org/project/vroom-csv/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance CSV to Parquet converter with SIMD acceleration for Python.

## Features

- **SIMD-accelerated parsing** using Google Highway for portable vectorization
- **Direct Parquet output** without intermediate Arrow dependency
- **Automatic type inference** detects integers, floats, booleans, and strings
- **Compression support** for Parquet output (ZSTD, Snappy, Gzip, LZ4)

## Installation

```bash
pip install vroom-csv
```

For development:

```bash
pip install vroom-csv[dev]
```

## Quick Start

```python
import vroom_csv

# Convert CSV to Parquet directly
vroom_csv.to_parquet("data.csv", "output.parquet", compression="zstd")

# Or read CSV for inspection
table = vroom_csv.read_csv("data.csv")
print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")
print(f"Columns: {table.column_names}")
```

## API Reference

### `read_csv(path, separator=None, quote=None, has_header=True, num_threads=None)`

Read a CSV file and return a Table object.

```python
table = vroom_csv.read_csv("data.csv")
table = vroom_csv.read_csv("data.csv", separator=";", has_header=True)
```

**Parameters:**
- `path`: Path to the CSV file
- `separator`: Field separator (auto-detected if None, default: None)
- `quote`: Quote character (auto-detected if None, default: None)
- `has_header`: Whether the first row contains column headers (default: True)
- `num_threads`: Number of threads for parsing (auto if None, default: None)

### `to_parquet(input_path, output_path, compression="zstd")`

Convert a CSV file directly to Parquet format.

```python
vroom_csv.to_parquet("data.csv", "output.parquet")
vroom_csv.to_parquet("data.csv", "output.parquet", compression="snappy")
```

**Parameters:**
- `input_path`: Input CSV file path
- `output_path`: Output Parquet file path
- `compression`: Compression codec ("zstd", "snappy", "gzip", "lz4", "none")

### Table

The `Table` class represents parsed CSV data.

**Properties:**
- `num_rows`: Number of data rows (excluding header)
- `num_columns`: Number of columns
- `column_names`: List of column names

**Note:** Arrow PyCapsule interface for zero-copy interoperability is planned for a future release.

## License

MIT License - see LICENSE file in the repository root.
