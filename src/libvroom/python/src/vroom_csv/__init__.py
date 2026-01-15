"""
vroom-csv: High-performance CSV parser with SIMD acceleration.

This package provides Python bindings for the libvroom CSV parser, featuring:
- SIMD-accelerated parsing using Google Highway
- Multi-threaded parsing for large files
- Automatic dialect detection
- Arrow PyCapsule interface for zero-copy interoperability with PyArrow, Polars, DuckDB

Basic Usage
-----------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")

Dialect Detection
-----------------
>>> dialect = vroom_csv.detect_dialect("data.csv")
>>> print(f"Delimiter: {dialect.delimiter!r}, Has header: {dialect.has_header}")

Progress Reporting
------------------
>>> table = vroom_csv.read_csv("large.csv", progress=vroom_csv.default_progress)

Arrow Interoperability
----------------------
>>> import pyarrow as pa
>>> arrow_table = pa.table(vroom_csv.read_csv("data.csv"))

>>> import polars as pl
>>> df = pl.from_arrow(vroom_csv.read_csv("data.csv"))
"""

import sys

from vroom_csv._core import (
    BatchedReader,
    Dialect,
    RecordBatch,
    RowIterator,
    Table,
    detect_dialect,
    read_csv,
    read_csv_batched,
    read_csv_rows,
    VroomError,
    ParseError,
    IOError,
    LIBVROOM_VERSION,
)

# Version from setuptools-scm, with fallback to _core for editable installs
try:
    from vroom_csv._version import version as __version__
except ImportError:
    from vroom_csv._core import __version__


def _format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024  # type: ignore[assignment]
    return f"{num_bytes:.1f}PB"


def default_progress(bytes_read: int, total_bytes: int) -> None:
    """Default progress callback that displays a progress bar.

    Displays a progress bar to stderr showing percentage complete and
    bytes processed. Suitable for terminal output.

    Parameters
    ----------
    bytes_read : int
        Number of bytes processed so far.
    total_bytes : int
        Total number of bytes to process.

    Examples
    --------
    >>> import vroom_csv
    >>> table = vroom_csv.read_csv("large.csv", progress=vroom_csv.default_progress)
    [=========>          ] 50.0% (512.0MB / 1.0GB)
    """
    if total_bytes == 0:
        return

    pct = bytes_read / total_bytes
    bar_width = 30
    filled = int(bar_width * pct)
    bar = "=" * filled + (">" if filled < bar_width else "") + " " * (bar_width - filled - 1)

    bytes_str = _format_bytes(bytes_read)
    total_str = _format_bytes(total_bytes)

    sys.stderr.write(f"\r[{bar}] {pct * 100:5.1f}% ({bytes_str} / {total_str})")
    sys.stderr.flush()

    # Print newline when complete
    if bytes_read >= total_bytes:
        sys.stderr.write("\n")
        sys.stderr.flush()


__all__ = [
    "BatchedReader",
    "Dialect",
    "RecordBatch",
    "RowIterator",
    "Table",
    "default_progress",
    "detect_dialect",
    "read_csv",
    "read_csv_batched",
    "read_csv_rows",
    "VroomError",
    "ParseError",
    "IOError",
    "__version__",
    "LIBVROOM_VERSION",
]
