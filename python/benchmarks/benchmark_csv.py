#!/usr/bin/env python3
"""
Performance benchmarks comparing vroom-csv against other CSV parsers.

Compares parsing performance across:
- vroom-csv (this library)
- pandas
- Polars
- PyArrow
- DuckDB

Run with: python benchmark_csv.py [--sizes SIZES] [--output FILE]
"""

import argparse
import gc
import json
import os
import random
import statistics
import string
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Check for required packages
PACKAGES = {
    "vroom_csv": False,
    "pandas": False,
    "polars": False,
    "pyarrow": False,
    "duckdb": False,
}

try:
    import vroom_csv

    PACKAGES["vroom_csv"] = True
except ImportError:
    pass

try:
    import pandas as pd

    PACKAGES["pandas"] = True
except ImportError:
    pass

try:
    import polars as pl

    PACKAGES["polars"] = True
except ImportError:
    pass

try:
    import pyarrow.csv as pa_csv

    PACKAGES["pyarrow"] = True
except ImportError:
    pass

try:
    import duckdb

    PACKAGES["duckdb"] = True
except ImportError:
    pass


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    library: str
    file_size_mb: float
    num_rows: int
    num_cols: int
    mean_time_s: float
    std_time_s: float
    min_time_s: float
    max_time_s: float
    throughput_mb_s: float


def generate_csv_file(
    path: Path,
    num_rows: int,
    num_cols: int = 10,
    include_types: bool = True,
) -> int:
    """
    Generate a CSV file with mixed data types.

    Parameters
    ----------
    path : Path
        Output file path.
    num_rows : int
        Number of data rows to generate.
    num_cols : int
        Number of columns (default 10).
    include_types : bool
        If True, include int, float, and string columns. If False, all strings.

    Returns
    -------
    int
        File size in bytes.
    """
    random.seed(42)  # Reproducible data

    with open(path, "w") as f:
        # Header
        headers = [f"col_{i}" for i in range(num_cols)]
        f.write(",".join(headers) + "\n")

        # Data rows
        for _ in range(num_rows):
            row = []
            for col_idx in range(num_cols):
                if include_types:
                    col_type = col_idx % 4
                    if col_type == 0:
                        # Integer
                        row.append(str(random.randint(-1000000, 1000000)))
                    elif col_type == 1:
                        # Float
                        row.append(f"{random.uniform(-1000, 1000):.6f}")
                    elif col_type == 2:
                        # Boolean as string
                        row.append(random.choice(["true", "false"]))
                    else:
                        # String
                        length = random.randint(5, 20)
                        s = "".join(random.choices(string.ascii_letters, k=length))
                        row.append(s)
                else:
                    # All strings
                    length = random.randint(5, 20)
                    s = "".join(random.choices(string.ascii_letters, k=length))
                    row.append(s)
            f.write(",".join(row) + "\n")

    return path.stat().st_size


def benchmark_vroom_csv(path: str, num_runs: int = 5) -> list[float]:
    """Benchmark vroom-csv parsing."""
    if not PACKAGES["vroom_csv"]:
        return []

    times = []
    for _ in range(num_runs):
        gc.collect()
        start = time.perf_counter()
        table = vroom_csv.read_csv(path)
        # Access data to ensure parsing is complete
        _ = table.num_rows
        end = time.perf_counter()
        times.append(end - start)
        del table

    return times


def benchmark_pandas(path: str, num_runs: int = 5) -> list[float]:
    """Benchmark pandas CSV parsing."""
    if not PACKAGES["pandas"]:
        return []

    times = []
    for _ in range(num_runs):
        gc.collect()
        start = time.perf_counter()
        df = pd.read_csv(path)
        # Access data to ensure parsing is complete
        _ = len(df)
        end = time.perf_counter()
        times.append(end - start)
        del df

    return times


def benchmark_polars(path: str, num_runs: int = 5) -> list[float]:
    """Benchmark Polars CSV parsing."""
    if not PACKAGES["polars"]:
        return []

    times = []
    for _ in range(num_runs):
        gc.collect()
        start = time.perf_counter()
        df = pl.read_csv(path)
        # Access data to ensure parsing is complete
        _ = len(df)
        end = time.perf_counter()
        times.append(end - start)
        del df

    return times


def benchmark_pyarrow(path: str, num_runs: int = 5) -> list[float]:
    """Benchmark PyArrow CSV parsing."""
    if not PACKAGES["pyarrow"]:
        return []

    times = []
    for _ in range(num_runs):
        gc.collect()
        start = time.perf_counter()
        table = pa_csv.read_csv(path)
        # Access data to ensure parsing is complete
        _ = table.num_rows
        end = time.perf_counter()
        times.append(end - start)
        del table

    return times


def benchmark_duckdb(path: str, num_runs: int = 5) -> list[float]:
    """Benchmark DuckDB CSV parsing."""
    if not PACKAGES["duckdb"]:
        return []

    times = []
    for _ in range(num_runs):
        gc.collect()
        start = time.perf_counter()
        result = duckdb.read_csv(path)
        # Access data to ensure parsing is complete
        _ = result.fetchall()
        end = time.perf_counter()
        times.append(end - start)
        del result

    return times


def run_benchmark(
    file_size_mb: float,
    num_cols: int = 10,
    num_runs: int = 5,
) -> list[BenchmarkResult]:
    """
    Run benchmarks for a specific file size.

    Parameters
    ----------
    file_size_mb : float
        Target file size in megabytes.
    num_cols : int
        Number of columns.
    num_runs : int
        Number of benchmark runs per library.

    Returns
    -------
    list[BenchmarkResult]
        Results for each library.
    """
    # Estimate rows needed for target file size
    # Rough estimate: ~50 bytes per row with 10 columns
    bytes_per_row = 5 * num_cols  # ~5 bytes per field average
    target_bytes = int(file_size_mb * 1024 * 1024)
    num_rows = max(1000, target_bytes // bytes_per_row)

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "benchmark.csv"
        actual_size = generate_csv_file(csv_path, num_rows, num_cols)
        actual_size_mb = actual_size / (1024 * 1024)

        print(
            f"\nBenchmarking {actual_size_mb:.1f} MB file "
            f"({num_rows:,} rows x {num_cols} cols)"
        )
        print("-" * 60)

        benchmarks = [
            ("vroom-csv", benchmark_vroom_csv),
            ("pandas", benchmark_pandas),
            ("polars", benchmark_polars),
            ("pyarrow", benchmark_pyarrow),
            ("duckdb", benchmark_duckdb),
        ]

        for name, benchmark_func in benchmarks:
            times = benchmark_func(str(csv_path), num_runs)
            if times:
                mean_time = statistics.mean(times)
                std_time = statistics.stdev(times) if len(times) > 1 else 0
                min_time = min(times)
                max_time = max(times)
                throughput = actual_size_mb / mean_time

                result = BenchmarkResult(
                    library=name,
                    file_size_mb=actual_size_mb,
                    num_rows=num_rows,
                    num_cols=num_cols,
                    mean_time_s=mean_time,
                    std_time_s=std_time,
                    min_time_s=min_time,
                    max_time_s=max_time,
                    throughput_mb_s=throughput,
                )
                results.append(result)

                print(
                    f"{name:12} {mean_time:8.3f}s (+/- {std_time:.3f}s) "
                    f"| {throughput:8.1f} MB/s"
                )
            else:
                print(f"{name:12} (not installed)")

    return results


def print_summary(all_results: list[BenchmarkResult]) -> None:
    """Print a summary table of all results."""
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    # Group by file size
    sizes = sorted(set(r.file_size_mb for r in all_results))

    for size in sizes:
        size_results = [r for r in all_results if r.file_size_mb == size]
        size_results.sort(key=lambda r: r.mean_time_s)

        print(f"\n{size:.1f} MB ({size_results[0].num_rows:,} rows):")
        print(f"{'Library':12} {'Time (s)':>10} {'Throughput':>12} {'Relative':>10}")
        print("-" * 48)

        fastest = size_results[0].mean_time_s if size_results else 1
        for r in size_results:
            relative = r.mean_time_s / fastest
            print(
                f"{r.library:12} {r.mean_time_s:10.3f} "
                f"{r.throughput_mb_s:10.1f} MB/s {relative:9.2f}x"
            )


def save_results(results: list[BenchmarkResult], output_path: str) -> None:
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [
            {
                "library": r.library,
                "file_size_mb": r.file_size_mb,
                "num_rows": r.num_rows,
                "num_cols": r.num_cols,
                "mean_time_s": r.mean_time_s,
                "std_time_s": r.std_time_s,
                "min_time_s": r.min_time_s,
                "max_time_s": r.max_time_s,
                "throughput_mb_s": r.throughput_mb_s,
            }
            for r in results
        ],
    }
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark CSV parsers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="1,10,100",
        help="Comma-separated file sizes in MB (default: 1,10,100)",
    )
    parser.add_argument(
        "--cols",
        type=int,
        default=10,
        help="Number of columns (default: 10)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of runs per benchmark (default: 5)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file for results",
    )
    args = parser.parse_args()

    sizes = [float(s.strip()) for s in args.sizes.split(",")]

    print("CSV Parser Benchmark")
    print("=" * 60)
    print("\nInstalled libraries:")
    for lib, installed in PACKAGES.items():
        status = "OK" if installed else "not installed"
        print(f"  {lib}: {status}")

    if not PACKAGES["vroom_csv"]:
        print("\nERROR: vroom-csv must be installed to run benchmarks")
        sys.exit(1)

    all_results = []
    for size in sizes:
        results = run_benchmark(size, num_cols=args.cols, num_runs=args.runs)
        all_results.extend(results)

    print_summary(all_results)

    if args.output:
        save_results(all_results, args.output)


if __name__ == "__main__":
    main()
