#!/usr/bin/env python3
"""Compare benchmark results between vroom and polars."""

import argparse
import json
import sys
from pathlib import Path


def load_results(path: Path) -> dict:
    """Load benchmark results from JSON file."""
    with open(path, 'r') as f:
        return json.load(f)


def format_time(seconds: float) -> str:
    """Format time in human-readable units."""
    if seconds < 0.001:
        return f"{seconds * 1000000:.1f}us"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    else:
        return f"{seconds:.2f}s"


def format_size(bytes_: int) -> str:
    """Format size in human-readable units."""
    if bytes_ < 1024:
        return f"{bytes_}B"
    elif bytes_ < 1024 * 1024:
        return f"{bytes_ / 1024:.1f}KB"
    elif bytes_ < 1024 * 1024 * 1024:
        return f"{bytes_ / (1024 * 1024):.1f}MB"
    else:
        return f"{bytes_ / (1024 * 1024 * 1024):.2f}GB"


def compare_results(vroom_results: dict, polars_results: dict) -> list:
    """Compare results between vroom and polars."""
    comparisons = []

    # Build lookup by filename
    polars_by_file = {r['file']: r for r in polars_results['results']}
    vroom_by_file = {r['file']: r for r in vroom_results['results']}

    # Compare matching files
    all_files = set(polars_by_file.keys()) | set(vroom_by_file.keys())

    for filename in sorted(all_files):
        comparison = {'file': filename}

        polars_r = polars_by_file.get(filename)
        vroom_r = vroom_by_file.get(filename)

        if polars_r:
            comparison['polars'] = {
                'time_median': polars_r['total_time']['median'],
                'throughput_mb_s': polars_r['throughput_mb_per_sec'],
                'throughput_rows_s': polars_r['throughput_rows_per_sec'],
            }
            comparison['file_size_bytes'] = polars_r['file_size_bytes']
            comparison['rows'] = polars_r['rows']
            comparison['columns'] = polars_r['columns']

        if vroom_r:
            comparison['vroom'] = {
                'time_median': vroom_r['total_time']['median'],
                'throughput_mb_s': vroom_r['throughput_mb_per_sec'],
                'throughput_rows_s': vroom_r['throughput_rows_per_sec'],
            }
            if 'file_size_bytes' not in comparison:
                comparison['file_size_bytes'] = vroom_r['file_size_bytes']
                comparison['rows'] = vroom_r['rows']
                comparison['columns'] = vroom_r['columns']

        # Calculate speedup if both exist
        if polars_r and vroom_r:
            polars_time = polars_r['total_time']['median']
            vroom_time = vroom_r['total_time']['median']
            # Speedup > 1 means vroom is faster
            comparison['speedup'] = polars_time / vroom_time
            comparison['time_diff_pct'] = ((vroom_time - polars_time) / polars_time) * 100

        comparisons.append(comparison)

    return comparisons


def print_report(vroom_results: dict, polars_results: dict, comparisons: list):
    """Print a formatted comparison report."""
    print("=" * 80)
    print("BENCHMARK COMPARISON: vroom vs polars")
    print("=" * 80)
    print()
    print(f"vroom version:  {vroom_results.get('version', 'unknown')}")
    print(f"polars version: {polars_results.get('version', 'unknown')}")
    print()

    # Header
    print(f"{'File':<30} {'Size':>8} {'Rows':>10} {'Polars':>10} {'Vroom':>10} {'Speedup':>10}")
    print("-" * 80)

    total_polars_time = 0
    total_vroom_time = 0

    for c in comparisons:
        file_name = c['file'][:28] + '..' if len(c['file']) > 30 else c['file']
        size = format_size(c.get('file_size_bytes', 0))
        rows = f"{c.get('rows', 0):,}"

        polars_time = format_time(c['polars']['time_median']) if 'polars' in c else 'N/A'
        vroom_time = format_time(c['vroom']['time_median']) if 'vroom' in c else 'N/A'

        if 'speedup' in c:
            speedup = c['speedup']
            if speedup >= 1:
                speedup_str = f"{speedup:.2f}x"
            else:
                speedup_str = f"{1/speedup:.2f}x slower"
            total_polars_time += c['polars']['time_median']
            total_vroom_time += c['vroom']['time_median']
        else:
            speedup_str = 'N/A'

        print(f"{file_name:<30} {size:>8} {rows:>10} {polars_time:>10} {vroom_time:>10} {speedup_str:>10}")

    print("-" * 80)

    # Overall summary
    if total_polars_time > 0 and total_vroom_time > 0:
        overall_speedup = total_polars_time / total_vroom_time
        print()
        print(f"Total time (polars): {format_time(total_polars_time)}")
        print(f"Total time (vroom):  {format_time(total_vroom_time)}")
        print()
        if overall_speedup >= 1:
            print(f"Overall: vroom is {overall_speedup:.2f}x faster than polars")
        else:
            print(f"Overall: vroom is {1/overall_speedup:.2f}x slower than polars")

    print()

    # Throughput comparison
    print("THROUGHPUT COMPARISON (MB/s)")
    print("-" * 60)
    print(f"{'File':<30} {'Polars':>12} {'Vroom':>12}")
    print("-" * 60)

    for c in comparisons:
        file_name = c['file'][:28] + '..' if len(c['file']) > 30 else c['file']
        polars_tp = f"{c['polars']['throughput_mb_s']:.1f}" if 'polars' in c else 'N/A'
        vroom_tp = f"{c['vroom']['throughput_mb_s']:.1f}" if 'vroom' in c else 'N/A'
        print(f"{file_name:<30} {polars_tp:>12} {vroom_tp:>12}")

    print()


def main():
    parser = argparse.ArgumentParser(description='Compare vroom and polars benchmark results')
    parser.add_argument('--vroom', type=Path, required=True,
                       help='Path to vroom benchmark results JSON')
    parser.add_argument('--polars', type=Path, required=True,
                       help='Path to polars benchmark results JSON')
    parser.add_argument('--output', '-o', type=Path,
                       help='Output JSON file for comparison results')
    parser.add_argument('--json', action='store_true',
                       help='Output only JSON (no formatted report)')
    args = parser.parse_args()

    if not args.vroom.exists():
        print(f"Error: vroom results not found: {args.vroom}", file=sys.stderr)
        sys.exit(1)

    if not args.polars.exists():
        print(f"Error: polars results not found: {args.polars}", file=sys.stderr)
        sys.exit(1)

    vroom_results = load_results(args.vroom)
    polars_results = load_results(args.polars)

    comparisons = compare_results(vroom_results, polars_results)

    if args.json:
        output = {
            'vroom_version': vroom_results.get('version', 'unknown'),
            'polars_version': polars_results.get('version', 'unknown'),
            'comparisons': comparisons,
        }
        print(json.dumps(output, indent=2))
    else:
        print_report(vroom_results, polars_results, comparisons)

    if args.output:
        output = {
            'vroom_version': vroom_results.get('version', 'unknown'),
            'polars_version': polars_results.get('version', 'unknown'),
            'comparisons': comparisons,
        }
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Comparison results written to {args.output}")


if __name__ == '__main__':
    main()
