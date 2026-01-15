# vroom-csv

[![PyPI version](https://badge.fury.io/py/vroom-csv.svg)](https://badge.fury.io/py/vroom-csv)
[![Python versions](https://img.shields.io/pypi/pyversions/vroom-csv.svg)](https://pypi.org/project/vroom-csv/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

High-performance CSV parser with SIMD acceleration for Python.

## Features

- **SIMD-accelerated parsing** using Google Highway for portable vectorization
- **Multi-threaded parsing** for large files
- **Automatic dialect detection** (delimiter, quoting, line endings)
- **Automatic type inference** detects integers, floats, booleans, and strings
- **Arrow PyCapsule interface** for zero-copy interoperability with PyArrow, Polars, DuckDB
- **Full type annotations** with IDE autocomplete support

## Installation

```bash
pip install vroom-csv
```

For Arrow interoperability:

```bash
pip install vroom-csv[arrow]   # PyArrow support
pip install vroom-csv[polars]  # Polars support
```

For development:

```bash
pip install vroom-csv[dev]
```

## Quick Start

```python
import vroom_csv

# Read a CSV file
table = vroom_csv.read_csv("data.csv")

print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")
print(f"Columns: {table.column_names}")

# Access data
names = table.column("name")
first_row = table.row(0)
```

## Arrow Interoperability

vroom-csv implements the Arrow PyCapsule interface for zero-copy data exchange:

### PyArrow

```python
import pyarrow as pa
import vroom_csv

table = vroom_csv.read_csv("data.csv")
arrow_table = pa.table(table)  # Zero-copy conversion

# Now use PyArrow's features
arrow_table.to_pandas()
```

### Polars

```python
import polars as pl
import vroom_csv

table = vroom_csv.read_csv("data.csv")
df = pl.from_arrow(table)  # Zero-copy conversion

# Now use Polars' features
df.filter(pl.col("age") > 30)
```

### DuckDB

```python
import duckdb
import vroom_csv

table = vroom_csv.read_csv("data.csv")
result = duckdb.query("SELECT * FROM table WHERE age > 30")
```

## API Reference

### `read_csv(path, ...)`

Read a CSV file and return a Table object.

```python
read_csv(
    path: str,
    delimiter: str | None = None,
    quote_char: str = '"',
    has_header: bool = True,
    skip_rows: int = 0,
    n_rows: int | None = None,
    usecols: list[str | int] | None = None,
    dtype: dict[str, str] | None = None,
    null_values: list[str] | None = None,
    empty_is_null: bool = True,
    encoding: str = "utf-8",
    num_threads: int = 1,
) -> Table
```

**Parameters:**
- `path`: Path to the CSV file
- `delimiter`: Field delimiter (auto-detected if None)
- `quote_char`: Quote character for quoted fields
- `has_header`: Whether the first row contains column headers
- `skip_rows`: Number of data rows to skip after the header
- `n_rows`: Maximum number of rows to read (None = all)
- `usecols`: List of column names or indices to read
- `dtype`: Dict mapping column names to types ("string", "int", "float", "bool")
- `null_values`: List of strings to treat as null values
- `empty_is_null`: Whether empty fields are treated as null
- `encoding`: File encoding (currently UTF-8 only)
- `num_threads`: Number of threads for parsing

### `detect_dialect(path)`

Detect CSV dialect (delimiter, quoting, line endings).

```python
dialect = vroom_csv.detect_dialect("data.csv")
print(dialect.delimiter)   # ','
print(dialect.quote_char)  # '"'
print(dialect.has_header)  # True
print(dialect.confidence)  # 0.95
```

### Table

The `Table` class represents parsed CSV data.

**Properties:**
- `num_rows`: Number of data rows (excluding header)
- `num_columns`: Number of columns
- `column_names`: List of column names

**Methods:**
- `column(index_or_name)`: Get column data as list
- `row(index)`: Get row data as list
- `has_errors()`: Check if any parse errors occurred
- `error_summary()`: Get a summary of parse errors
- `errors()`: Get list of parse error messages

**Arrow PyCapsule Protocol:**
- `__arrow_c_schema__()`: Export schema via Arrow C Data Interface
- `__arrow_c_stream__()`: Export data via Arrow C Stream Interface

## Examples

See the [examples directory](examples/) for Jupyter notebooks demonstrating:
- [Getting Started](examples/getting_started.ipynb) - Basic usage
- [Arrow Interoperability](examples/arrow_interop.ipynb) - PyArrow, Polars, DuckDB

## Benchmarks

Run performance benchmarks against pandas, Polars, PyArrow, and DuckDB:

```bash
cd benchmarks
pip install pandas polars pyarrow duckdb
python benchmark_csv.py --sizes 1,10,100
```

See [benchmarks/README.md](benchmarks/README.md) for details.

## License

MIT License - see LICENSE file in the repository root.
