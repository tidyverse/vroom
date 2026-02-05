"""
vroom-csv: High-performance CSV parser with SIMD acceleration.

This package provides Python bindings for the libvroom CSV parser, featuring:
- SIMD-accelerated parsing using Google Highway
- Multi-threaded parsing for large files
- Direct CSV to Parquet conversion

Basic Usage
-----------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")

CSV to Parquet Conversion
-------------------------
>>> vroom_csv.to_parquet("data.csv", "data.parquet", compression="zstd")

Note: This is a stub implementation after the libvroom2 migration.
Advanced features like BatchedReader, detect_dialect, and read_csv_rows
will be added in future releases.
"""

from vroom_csv._core import (
    Table,
    read_csv,
    to_parquet,
    to_arrow_ipc,
    VroomError,
    ParseError,
    IOError,
)

# Version from setuptools-scm, with fallback to _core for editable installs
try:
    from vroom_csv._version import version as __version__
except ImportError:
    from vroom_csv._core import __version__


__all__ = [
    "Table",
    "default_progress",
    "read_csv",
    "to_parquet",
    "to_arrow_ipc",
    "VroomError",
    "ParseError",
    "IOError",
    "__version__",
]
