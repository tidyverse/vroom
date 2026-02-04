#!/usr/bin/env python3
"""
Benchmark script focused on numeric parsing performance.
Generates data with mostly integer and float columns to isolate SIMD parser benefits.
"""

import os
import sys
import tempfile
import time
import random
import statistics


def generate_numeric_csv(num_rows: int, num_cols: int, int_ratio: float = 0.5) -> tuple[str, int]:
    """Generate a CSV file with only numeric columns (integers and floats)."""
    fd, path = tempfile.mkstemp(suffix='.csv')

    # Determine column types
    num_int_cols = int(num_cols * int_ratio)
    num_float_cols = num_cols - num_int_cols

    with os.fdopen(fd, 'w') as f:
        # Header
        headers = [f"int_{i}" for i in range(num_int_cols)] + [f"float_{i}" for i in range(num_float_cols)]
        f.write(','.join(headers) + '\n')

        # Data rows - no nulls, pure numeric parsing
        for _ in range(num_rows):
            row = []
            # Integer columns
            for _ in range(num_int_cols):
                row.append(str(random.randint(-999999999, 999999999)))
            # Float columns
            for _ in range(num_float_cols):
                row.append(f"{random.uniform(-99999.0, 99999.0):.6f}")
            f.write(','.join(row) + '\n')

    size = os.path.getsize(path)
    return path, size


def generate_integer_only_csv(num_rows: int, num_cols: int) -> tuple[str, int]:
    """Generate a CSV file with only integer columns."""
    fd, path = tempfile.mkstemp(suffix='.csv')

    with os.fdopen(fd, 'w') as f:
        headers = [f"col_{i}" for i in range(num_cols)]
        f.write(','.join(headers) + '\n')

        for _ in range(num_rows):
            row = [str(random.randint(-999999999, 999999999)) for _ in range(num_cols)]
            f.write(','.join(row) + '\n')

    size = os.path.getsize(path)
    return path, size


def generate_float_only_csv(num_rows: int, num_cols: int) -> tuple[str, int]:
    """Generate a CSV file with only float columns."""
    fd, path = tempfile.mkstemp(suffix='.csv')

    with os.fdopen(fd, 'w') as f:
        headers = [f"col_{i}" for i in range(num_cols)]
        f.write(','.join(headers) + '\n')

        for _ in range(num_rows):
            row = [f"{random.uniform(-99999.0, 99999.0):.6f}" for _ in range(num_cols)]
            f.write(','.join(row) + '\n')

    size = os.path.getsize(path)
    return path, size


def benchmark_arrow_export(csv_path: str, dtype: dict, iterations: int = 10) -> dict:
    """Benchmark Arrow export with explicit dtype to force numeric parsing."""
    import vroom_csv
    import pyarrow as pa

    # Warmup
    for _ in range(3):
        t = vroom_csv.read_csv(csv_path, dtype=dtype)
        _ = pa.table(t)

    # Benchmark
    times = []
    for _ in range(iterations):
        t = vroom_csv.read_csv(csv_path, dtype=dtype)

        start = time.perf_counter()
        arrow_table = pa.table(t)
        _ = arrow_table.num_rows
        end = time.perf_counter()

        times.append((end - start) * 1000)

    return {
        'mean': statistics.mean(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0,
        'min': min(times),
        'max': max(times),
    }


def benchmark_full_pipeline(csv_path: str, dtype: dict, iterations: int = 10) -> dict:
    """Benchmark full read + Arrow export pipeline."""
    import vroom_csv
    import pyarrow as pa

    # Warmup
    for _ in range(3):
        t = vroom_csv.read_csv(csv_path, dtype=dtype)
        _ = pa.table(t)

    # Benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        t = vroom_csv.read_csv(csv_path, dtype=dtype)
        arrow_table = pa.table(t)
        _ = arrow_table.num_rows
        end = time.perf_counter()

        times.append((end - start) * 1000)

    return {
        'mean': statistics.mean(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0,
        'min': min(times),
        'max': max(times),
    }


def run_benchmark_suite(label: str, csv_path: str, size_bytes: int, dtype: dict, iterations: int = 10):
    """Run full benchmark suite and return results."""
    size_mb = size_bytes / (1024 * 1024)

    print(f"  Benchmarking {label}...")
    arrow_stats = benchmark_arrow_export(csv_path, dtype, iterations)
    full_stats = benchmark_full_pipeline(csv_path, dtype, iterations)

    throughput_arrow = size_mb / (arrow_stats['mean'] / 1000)
    throughput_full = size_mb / (full_stats['mean'] / 1000)

    return {
        'label': label,
        'size_mb': size_mb,
        'arrow_mean': arrow_stats['mean'],
        'arrow_stdev': arrow_stats['stdev'],
        'full_mean': full_stats['mean'],
        'full_stdev': full_stats['stdev'],
        'throughput_arrow': throughput_arrow,
        'throughput_full': throughput_full,
    }


def main():
    import vroom_csv
    import pyarrow as pa

    print("=" * 80)
    print("Numeric Parsing Benchmark - SIMD Parser Performance")
    print("=" * 80)
    print(f"vroom-csv version: {vroom_csv.__version__}")
    print(f"PyArrow version: {pa.__version__}")
    print()

    num_cols = 20
    iterations = 5  # Fewer iterations for faster results

    # Test configurations - focus on larger sizes where parsing dominates
    configs = [
        (500_000, "500K rows"),
        (1_000_000, "1M rows"),
    ]

    all_results = []

    # =========================================================================
    # INTEGER-ONLY BENCHMARK
    # =========================================================================
    print("=" * 80)
    print("INTEGER-ONLY COLUMNS (all int64)")
    print("=" * 80)

    int_results = []
    for num_rows, label in configs:
        print(f"\nGenerating {label} integer file ({num_cols} columns)...")
        csv_path, size_bytes = generate_integer_only_csv(num_rows, num_cols)
        size_mb = size_bytes / (1024 * 1024)
        print(f"  File size: {size_mb:.1f} MB")

        # Force all columns to int64
        dtype = {f"col_{i}": "int64" for i in range(num_cols)}

        result = run_benchmark_suite(f"Int {label}", csv_path, size_bytes, dtype, iterations)
        int_results.append(result)

        print(f"  Arrow export: {result['arrow_mean']:.2f} ms (+/- {result['arrow_stdev']:.2f})")
        print(f"  Full pipeline: {result['full_mean']:.2f} ms (+/- {result['full_stdev']:.2f})")
        print(f"  Throughput: {result['throughput_full']:.1f} MB/s")

        os.unlink(csv_path)

    all_results.extend(int_results)

    # =========================================================================
    # FLOAT-ONLY BENCHMARK
    # =========================================================================
    print("\n" + "=" * 80)
    print("FLOAT-ONLY COLUMNS (all float64)")
    print("=" * 80)

    float_results = []
    for num_rows, label in configs:
        print(f"\nGenerating {label} float file ({num_cols} columns)...")
        csv_path, size_bytes = generate_float_only_csv(num_rows, num_cols)
        size_mb = size_bytes / (1024 * 1024)
        print(f"  File size: {size_mb:.1f} MB")

        # Force all columns to float64
        dtype = {f"col_{i}": "float64" for i in range(num_cols)}

        result = run_benchmark_suite(f"Float {label}", csv_path, size_bytes, dtype, iterations)
        float_results.append(result)

        print(f"  Arrow export: {result['arrow_mean']:.2f} ms (+/- {result['arrow_stdev']:.2f})")
        print(f"  Full pipeline: {result['full_mean']:.2f} ms (+/- {result['full_stdev']:.2f})")
        print(f"  Throughput: {result['throughput_full']:.1f} MB/s")

        os.unlink(csv_path)

    all_results.extend(float_results)

    # =========================================================================
    # MIXED NUMERIC BENCHMARK (50% int, 50% float)
    # =========================================================================
    print("\n" + "=" * 80)
    print("MIXED NUMERIC COLUMNS (50% int64, 50% float64)")
    print("=" * 80)

    mixed_results = []
    for num_rows, label in configs:
        print(f"\nGenerating {label} mixed numeric file ({num_cols} columns)...")
        csv_path, size_bytes = generate_numeric_csv(num_rows, num_cols, int_ratio=0.5)
        size_mb = size_bytes / (1024 * 1024)
        print(f"  File size: {size_mb:.1f} MB")

        # Set dtypes for mixed columns
        num_int_cols = num_cols // 2
        dtype = {}
        for i in range(num_int_cols):
            dtype[f"int_{i}"] = "int64"
        for i in range(num_cols - num_int_cols):
            dtype[f"float_{i}"] = "float64"

        result = run_benchmark_suite(f"Mixed {label}", csv_path, size_bytes, dtype, iterations)
        mixed_results.append(result)

        print(f"  Arrow export: {result['arrow_mean']:.2f} ms (+/- {result['arrow_stdev']:.2f})")
        print(f"  Full pipeline: {result['full_mean']:.2f} ms (+/- {result['full_stdev']:.2f})")
        print(f"  Throughput: {result['throughput_full']:.1f} MB/s")

        os.unlink(csv_path)

    all_results.extend(mixed_results)

    # =========================================================================
    # SUMMARY TABLE
    # =========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY - Arrow Export Times (lower is better)")
    print("=" * 80)
    print(f"{'Dataset':<25} {'Size':>10} {'Arrow Export':>15} {'Full Pipeline':>15} {'Throughput':>12}")
    print("-" * 80)

    for r in all_results:
        print(f"{r['label']:<25} {r['size_mb']:>9.1f}M {r['arrow_mean']:>12.2f} ms {r['full_mean']:>12.2f} ms {r['throughput_full']:>10.1f} MB/s")

    print("=" * 80)

    return all_results


if __name__ == "__main__":
    main()
