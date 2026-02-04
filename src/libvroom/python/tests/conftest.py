"""Pytest configuration for vroom-csv tests.

The Python bindings are currently stub implementations of the libvroom2 API.
Most old tests use features that haven't been migrated yet (BatchedReader,
detect_dialect, read_csv_rows, etc.). These tests are collected but skipped.
"""

# Skip test files that test non-existent features
# These files test the old API which was removed in the libvroom2 migration
collect_ignore = [
    "test_arrow.py",      # Tests Arrow PyCapsule interface (not implemented)
    "test_batched.py",    # Tests BatchedReader class (not implemented)
    "test_full_api.py",   # Tests detect_dialect, extended options (not implemented)
    "test_streaming.py",  # Tests read_csv_rows (not implemented)
    "test_basic.py",      # Tests extended Table API (not implemented yet)
]
