#!/usr/bin/env python3
"""Benchmark Polars CSV to Parquet conversion."""

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("Error: polars not installed. Install with: pip install polars")
    sys.exit(1)


def benchmark_file(csv_path: Path, iterations: int = 5, warmup: int = 1, compression: str = 'uncompressed') -> dict:
    """Benchmark Polars reading a CSV and writing to Parquet."""

    file_size = csv_path.stat().st_size

    # Warmup runs
    for _ in range(warmup):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
            df = pl.read_csv(csv_path)
            df.write_parquet(tmp.name)

    # Timed runs
    read_times = []
    write_times = []
    total_times = []

    for _ in range(iterations):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
            # Time CSV read
            start = time.perf_counter()
            df = pl.read_csv(csv_path)
            read_time = time.perf_counter() - start
            read_times.append(read_time)

            row_count = len(df)
            col_count = len(df.columns)

            # Time Parquet write
            start = time.perf_counter()
            df.write_parquet(tmp.name, compression=compression)
            write_time = time.perf_counter() - start
            write_times.append(write_time)

            total_times.append(read_time + write_time)

            # Get output size
            output_size = os.path.getsize(tmp.name)

    def stats(times):
        times = sorted(times)
        n = len(times)
        return {
            'min': times[0],
            'max': times[-1],
            'mean': sum(times) / n,
            'median': times[n // 2],
        }

    return {
        'file': csv_path.name,
        'file_size_bytes': file_size,
        'file_size_mb': file_size / (1024 * 1024),
        'rows': row_count,
        'columns': col_count,
        'output_size_bytes': output_size,
        'iterations': iterations,
        'read_time': stats(read_times),
        'write_time': stats(write_times),
        'total_time': stats(total_times),
        'throughput_mb_per_sec': file_size / (1024 * 1024) / stats(total_times)['median'],
        'throughput_rows_per_sec': row_count / stats(total_times)['median'],
    }


def main():
    parser = argparse.ArgumentParser(description='Benchmark Polars CSV to Parquet')
    parser.add_argument('input', type=Path, nargs='+',
                       help='CSV file(s) to benchmark')
    parser.add_argument('--iterations', '-n', type=int, default=5,
                       help='Number of iterations per file')
    parser.add_argument('--warmup', '-w', type=int, default=1,
                       help='Number of warmup iterations')
    parser.add_argument('--output', '-o', type=Path,
                       help='Output JSON file for results')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Only output JSON')
    parser.add_argument('--compression', '-c', type=str, default='uncompressed',
                       choices=['none', 'zstd', 'snappy', 'lz4', 'gzip', 'uncompressed'],
                       help='Compression codec (default: zstd)')
    args = parser.parse_args()

    # Normalize compression name for polars
    compression = args.compression
    if compression == 'none':
        compression = 'uncompressed'

    results = []

    for csv_path in args.input:
        if not csv_path.exists():
            print(f"Warning: {csv_path} not found, skipping", file=sys.stderr)
            continue

        if not args.quiet:
            print(f"Benchmarking: {csv_path.name}...", end=' ', flush=True)

        result = benchmark_file(csv_path, args.iterations, args.warmup, compression)
        results.append(result)

        if not args.quiet:
            print(f"done ({result['total_time']['median']*1000:.1f}ms, "
                  f"{result['throughput_mb_per_sec']:.1f} MB/s)")

    # Add metadata
    output = {
        'tool': 'polars',
        'version': pl.__version__,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'results': results,
    }

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        if not args.quiet:
            print(f"\nResults written to {args.output}")
    else:
        print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
