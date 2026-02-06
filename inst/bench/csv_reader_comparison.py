#!/usr/bin/env python3
"""
Cross-engine CSV reading benchmark (Python engines).

Benchmarks: libvroom (vroom_csv), polars, pyarrow, duckdb
Output: JSON to stdout for consumption by csv_reader_comparison.R

Usage:
  python3 csv_reader_comparison.py file1.csv file2.csv ...

Requires: pip install vroom-csv polars pyarrow duckdb
"""

import json
import os
import sys
import time


def median(xs):
    s = sorted(xs)
    return s[len(s) // 2]


def bench(fn, warmup=1, iterations=5):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        t = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - t
        times.append(elapsed)
    return median(times)


def main():
    files = sys.argv[1:]
    if not files:
        print("Usage: csv_reader_comparison.py file1.csv ...", file=sys.stderr)
        sys.exit(1)

    engines = {}

    # Try importing each engine
    try:
        import vroom_csv
        engines["libvroom_py"] = lambda path: vroom_csv.read_csv(path)
    except ImportError:
        pass

    try:
        import polars as pl
        engines["polars"] = lambda path: pl.read_csv(path)
    except ImportError:
        pass

    try:
        import pyarrow.csv as pa_csv
        engines["pyarrow"] = lambda path: pa_csv.read_csv(path)
    except ImportError:
        pass

    try:
        import duckdb
        engines["duckdb"] = lambda path: duckdb.sql(
            f"SELECT * FROM read_csv('{path}')"
        ).arrow()
    except ImportError:
        pass

    if not engines:
        print("No Python CSV engines found.", file=sys.stderr)
        sys.exit(1)

    results = []
    for path in files:
        if not os.path.exists(path):
            continue
        size_bytes = os.path.getsize(path)
        file_results = {"file": path, "size_bytes": size_bytes}

        for name, fn in engines.items():
            try:
                ms = bench(lambda: fn(path)) * 1000
                file_results[name] = ms
            except Exception as e:
                print(f"  {name} failed on {path}: {e}", file=sys.stderr)
                file_results[name] = None

        results.append(file_results)

    json.dump(results, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
