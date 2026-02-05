#!/usr/bin/env python3
"""
Benchmarks specifically for Arrow export performance.

This benchmarks the code paths that were optimized in issue #529:
1. NullValueConfig.is_null_value() - null value checking
2. materialize_columns() - column materialization for Arrow export
3. Table.column() - individual column extraction

Run with: python benchmark_arrow_export.py
"""

import gc
import os
import random
import statistics
import string
import sys
import tempfile
import time
from pathlib import Path


def generate_csv_file(
    path: Path,
    num_rows: int,
    num_cols: int = 10,
    null_ratio: float = 0.1,
) -> int:
    """Generate a CSV file with mixed data types and some null values."""
    random.seed(42)
    null_values = ["", "NA", "N/A", "null", "NULL", "None", "NaN"]

    with open(path, "w") as f:
        headers = [f"col_{i}" for i in range(num_cols)]
        f.write(",".join(headers) + "\n")

        for _ in range(num_rows):
            row = []
            for col_idx in range(num_cols):
                if random.random() < null_ratio:
                    row.append(random.choice(null_values))
                else:
                    col_type = col_idx % 4
                    if col_type == 0:
                        row.append(str(random.randint(-1000000, 1000000)))
                    elif col_type == 1:
                        row.append(f"{random.uniform(-1000, 1000):.6f}")
                    elif col_type == 2:
                        row.append(random.choice(["true", "false"]))
                    else:
                        length = random.randint(5, 20)
                        s = "".join(random.choices(string.ascii_letters, k=length))
                        row.append(s)
            f.write(",".join(row) + "\n")

    return path.stat().st_size


def benchmark_arrow_export(csv_path: str, num_runs: int = 5) -> dict:
    """Benchmark Arrow export via PyArrow."""
    import pyarrow
    import vroom_csv

    times = []
    for _ in range(num_runs):
        gc.collect()
        # Parse CSV first (not timed - we want to isolate Arrow export)
        table = vroom_csv.read_csv(csv_path)

        start = time.perf_counter()
        # This triggers materialize_columns() and build_column_array()
        arrow_table = pyarrow.table(table)
        # Force materialization
        _ = arrow_table.num_rows
        end = time.perf_counter()

        times.append(end - start)
        del table, arrow_table

    return {
        "mean": statistics.mean(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
    }


def benchmark_column_access(csv_path: str, num_runs: int = 5) -> dict:
    """Benchmark individual column access."""
    import vroom_csv

    times = []
    for _ in range(num_runs):
        gc.collect()
        table = vroom_csv.read_csv(csv_path)

        start = time.perf_counter()
        # Access each column individually
        for i in range(table.num_columns):
            _ = table.column(i)
        end = time.perf_counter()

        times.append(end - start)
        del table

    return {
        "mean": statistics.mean(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
    }


def benchmark_batched_arrow_export(csv_path: str, batch_size: int = 10000, num_runs: int = 5) -> dict:
    """Benchmark batched reading with Arrow export."""
    import pyarrow
    import vroom_csv

    times = []
    for _ in range(num_runs):
        gc.collect()

        start = time.perf_counter()
        total_rows = 0
        for batch in vroom_csv.read_csv_batched(csv_path, batch_size=batch_size):
            # Convert each batch to Arrow
            arrow_batch = pyarrow.table(batch)
            total_rows += arrow_batch.num_rows
        end = time.perf_counter()

        times.append(end - start)

    return {
        "mean": statistics.mean(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
    }


def run_benchmarks(file_size_mb: float, num_cols: int = 20, null_ratio: float = 0.1, num_runs: int = 5):
    """Run all benchmarks for a given file size."""
    bytes_per_row = 8 * num_cols
    target_bytes = int(file_size_mb * 1024 * 1024)
    num_rows = max(1000, target_bytes // bytes_per_row)

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "benchmark.csv"
        actual_size = generate_csv_file(csv_path, num_rows, num_cols, null_ratio)
        actual_size_mb = actual_size / (1024 * 1024)

        print(f"\n{'='*70}")
        print(f"Benchmarking {actual_size_mb:.1f} MB file ({num_rows:,} rows x {num_cols} cols)")
        print(f"Null value ratio: {null_ratio*100:.0f}%")
        print(f"{'='*70}")

        results = {}

        # Benchmark 1: Full Arrow export
        print("\n1. Full Arrow Export (via pyarrow.table()):")
        r = benchmark_arrow_export(str(csv_path), num_runs)
        results["arrow_export"] = r
        print(f"   Mean: {r['mean']*1000:.2f} ms (+/- {r['std']*1000:.2f} ms)")
        print(f"   Throughput: {actual_size_mb / r['mean']:.1f} MB/s")

        # Benchmark 2: Individual column access
        print("\n2. Individual Column Access (table.column(i) for each column):")
        r = benchmark_column_access(str(csv_path), num_runs)
        results["column_access"] = r
        print(f"   Mean: {r['mean']*1000:.2f} ms (+/- {r['std']*1000:.2f} ms)")
        print(f"   Throughput: {actual_size_mb / r['mean']:.1f} MB/s")

        # Benchmark 3: Batched Arrow export
        print("\n3. Batched Arrow Export (10k rows per batch):")
        r = benchmark_batched_arrow_export(str(csv_path), batch_size=10000, num_runs=num_runs)
        results["batched_export"] = r
        print(f"   Mean: {r['mean']*1000:.2f} ms (+/- {r['std']*1000:.2f} ms)")
        print(f"   Throughput: {actual_size_mb / r['mean']:.1f} MB/s")

        return results


def main():
    print("Arrow Export Benchmark")
    print("=" * 70)
    print("\nThis benchmark measures the performance of:")
    print("  - Arrow table export (materialize_columns + build_column_array)")
    print("  - Individual column access (Table.column())")
    print("  - Batched Arrow export")
    print()

    try:
        import pyarrow
        import vroom_csv
        print(f"vroom-csv version: {vroom_csv.__version__}")
        print(f"PyArrow version: {pyarrow.__version__}")
    except ImportError as e:
        print(f"ERROR: Required package not found: {e}")
        sys.exit(1)

    # Run benchmarks with different sizes
    sizes = [1, 10, 50]
    all_results = {}

    for size in sizes:
        results = run_benchmarks(size, num_cols=20, null_ratio=0.1, num_runs=5)
        all_results[size] = results

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n{'Size (MB)':<12} {'Arrow Export':<20} {'Column Access':<20} {'Batched Export':<20}")
    print("-" * 72)
    for size in sizes:
        r = all_results[size]
        print(f"{size:<12} {r['arrow_export']['mean']*1000:>8.2f} ms       "
              f"{r['column_access']['mean']*1000:>8.2f} ms       "
              f"{r['batched_export']['mean']*1000:>8.2f} ms")


if __name__ == "__main__":
    main()
