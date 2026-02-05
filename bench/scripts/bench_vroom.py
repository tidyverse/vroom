#!/usr/bin/env python3
"""Benchmark vroom CSV to Parquet conversion."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def find_vroom_binary() -> Path:
    """Find the vroom binary in common build locations."""
    # Check common build directories
    candidates = [
        Path('build/vroom'),
        Path('cmake-build-release/vroom'),
        Path('cmake-build-debug/vroom'),
        Path('../build/vroom'),
    ]

    # Also check if vroom is in PATH
    try:
        result = subprocess.run(['which', 'vroom'], capture_output=True, text=True)
        if result.returncode == 0:
            candidates.insert(0, Path(result.stdout.strip()))
    except Exception:
        pass

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()

    return None


def benchmark_file(vroom_path: Path, csv_path: Path, iterations: int = 5, warmup: int = 1, compression: str = 'none') -> dict:
    """Benchmark vroom reading a CSV and writing to Parquet."""

    file_size = csv_path.stat().st_size

    # Count rows (for comparison with polars)
    with open(csv_path, 'r') as f:
        row_count = sum(1 for _ in f) - 1  # Subtract header

    # Count columns from header
    with open(csv_path, 'r') as f:
        header = f.readline()
        col_count = len(header.split(','))

    # Warmup runs
    for _ in range(warmup):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
            # libvroom CLI: vroom input.csv -o output.parquet -c compression
            subprocess.run([str(vroom_path), str(csv_path), '-o', tmp.name, '-c', compression],
                         capture_output=True, check=True)

    # Timed runs
    total_times = []
    output_size = 0

    for _ in range(iterations):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
            start = time.perf_counter()
            # libvroom CLI: vroom input.csv -o output.parquet -c compression
            result = subprocess.run([str(vroom_path), str(csv_path), '-o', tmp.name, '-c', compression],
                                  capture_output=True)
            elapsed = time.perf_counter() - start

            if result.returncode != 0:
                print(f"Error running vroom: {result.stderr.decode()}", file=sys.stderr)
                return None

            total_times.append(elapsed)
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
        'total_time': stats(total_times),
        'throughput_mb_per_sec': file_size / (1024 * 1024) / stats(total_times)['median'],
        'throughput_rows_per_sec': row_count / stats(total_times)['median'],
    }


def get_vroom_version(vroom_path: Path) -> str:
    """Get vroom version string."""
    try:
        result = subprocess.run([str(vroom_path), '--version'],
                              capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description='Benchmark vroom CSV to Parquet')
    parser.add_argument('input', type=Path, nargs='+',
                       help='CSV file(s) to benchmark')
    parser.add_argument('--vroom', type=Path,
                       help='Path to vroom binary')
    parser.add_argument('--iterations', '-n', type=int, default=5,
                       help='Number of iterations per file')
    parser.add_argument('--warmup', '-w', type=int, default=1,
                       help='Number of warmup iterations')
    parser.add_argument('--output', '-o', type=Path,
                       help='Output JSON file for results')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Only output JSON')
    parser.add_argument('--compression', '-c', type=str, default='none',
                       choices=['none', 'zstd', 'snappy', 'lz4', 'gzip'],
                       help='Compression codec (default: zstd)')
    args = parser.parse_args()

    # Find vroom binary
    if args.vroom:
        vroom_path = args.vroom
    else:
        vroom_path = find_vroom_binary()

    if not vroom_path or not vroom_path.exists():
        print("Error: Could not find vroom binary. Use --vroom to specify path.",
              file=sys.stderr)
        sys.exit(1)

    if not args.quiet:
        print(f"Using vroom: {vroom_path}")

    results = []

    for csv_path in args.input:
        if not csv_path.exists():
            print(f"Warning: {csv_path} not found, skipping", file=sys.stderr)
            continue

        if not args.quiet:
            print(f"Benchmarking: {csv_path.name}...", end=' ', flush=True)

        result = benchmark_file(vroom_path, csv_path, args.iterations, args.warmup, args.compression)
        if result:
            results.append(result)

            if not args.quiet:
                print(f"done ({result['total_time']['median']*1000:.1f}ms, "
                      f"{result['throughput_mb_per_sec']:.1f} MB/s)")
        else:
            if not args.quiet:
                print("FAILED")

    # Add metadata
    output = {
        'tool': 'vroom',
        'version': get_vroom_version(vroom_path),
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
