#!/usr/bin/env python3
"""Check for performance regressions in benchmark results.

This script compares current benchmark results against a baseline and
detects performance regressions that exceed a configurable threshold.

Usage:
    # Compare two local files (same-job relative benchmarking)
    python check_regression.py --head head.json --base base.json

    # Legacy mode: use hardcoded paths and cache-based baseline
    python check_regression.py
"""

import argparse
import json
import sys
import os
import shutil

REGRESSION_THRESHOLD = 0.15  # 15% regression threshold (increased for CI variability)
EXPECTED_BENCHMARK_COUNT = 7  # Number of benchmarks we expect to run


def load_benchmark(filepath, check_errors=False):
    """Load benchmark results from JSON file.

    Args:
        filepath: Path to benchmark JSON file
        check_errors: If True, fail on benchmark errors

    Returns:
        Tuple of (results dict, time_unit string) where results maps
        benchmark name -> average time, or (None, None) on error
    """
    if not os.path.exists(filepath):
        return None, None
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON from {filepath}: {e}")
        return None, None

    # Check for benchmark errors
    errors = []
    for bench in data.get('benchmarks', []):
        if bench.get('error_occurred') or bench.get('error_message'):
            error_msg = bench.get('error_message', 'Unknown error')
            errors.append(f"  {bench.get('name', 'unknown')}: {error_msg}")

    if errors and check_errors:
        print("ERROR: Benchmark execution errors detected:")
        for err in errors:
            print(err)
        return None, None

    # Extract benchmark results, keyed by name
    results = {}
    time_unit = 'ns'  # default
    for bench in data.get('benchmarks', []):
        name = bench.get('name', '')
        # Skip aggregate results (mean, median, stddev)
        if bench.get('aggregate_name'):
            continue
        # Skip benchmarks that had errors
        if bench.get('error_occurred') or bench.get('error_message'):
            continue
        # Use real_time as the primary metric
        if 'real_time' in bench:
            if name not in results:
                results[name] = []
            results[name].append(bench['real_time'])
            # Capture time unit from first valid benchmark
            if 'time_unit' in bench:
                time_unit = bench['time_unit']

    # Average multiple runs
    return {name: sum(times) / len(times) for name, times in results.items()}, time_unit


def compare_benchmarks(baseline, current, time_unit='ms', threshold=REGRESSION_THRESHOLD):
    """Compare current results against baseline and detect regressions."""
    regressions = []
    improvements = []
    unchanged = []
    skipped = []

    # Minimum time threshold to avoid division by zero and unreliable comparisons
    # for very fast benchmarks. We use 1ms as the threshold regardless of time_unit
    # since benchmarks faster than that are too noisy for reliable regression detection.
    MIN_TIME_THRESHOLD = {
        'ns': 1_000_000,  # 1ms in nanoseconds
        'us': 1_000,      # 1ms in microseconds
        'ms': 1,          # 1ms in milliseconds
        's': 0.001,       # 1ms in seconds
    }.get(time_unit, 1)

    for name, curr_time in current.items():
        if name not in baseline:
            unchanged.append((name, curr_time, None, None))
            continue

        base_time = baseline[name]

        # Skip comparison if baseline time is zero or below threshold
        if base_time <= 0 or base_time < MIN_TIME_THRESHOLD:
            skipped.append((name, base_time, curr_time))
            continue

        # Positive ratio means regression (slower), negative means improvement
        ratio = (curr_time - base_time) / base_time

        if ratio > threshold:
            regressions.append((name, base_time, curr_time, ratio))
        elif ratio < -threshold:
            improvements.append((name, base_time, curr_time, ratio))
        else:
            unchanged.append((name, base_time, curr_time, ratio))

    return regressions, improvements, unchanged, skipped


def main():
    parser = argparse.ArgumentParser(description='Check for performance regressions')
    parser.add_argument('--head', help='Path to HEAD benchmark results JSON')
    parser.add_argument('--base', help='Path to BASE benchmark results JSON')
    parser.add_argument('--threshold', type=float, default=REGRESSION_THRESHOLD,
                        help=f'Regression threshold (default: {REGRESSION_THRESHOLD})')
    args = parser.parse_args()

    # Check if this is a main branch push
    is_main_push = os.environ.get('GITHUB_EVENT_NAME') == 'push' and \
                   os.environ.get('GITHUB_REF') in ('refs/heads/main', 'refs/heads/master')

    # Determine file paths based on arguments or defaults
    if args.head and args.base:
        # Same-job relative benchmarking mode
        head_path = args.head
        base_path = args.base
        relative_mode = True
    else:
        # Legacy mode: use hardcoded paths
        head_path = 'build/benchmark_results.json'
        base_path = 'baseline_benchmark.json'
        relative_mode = False

    threshold = args.threshold

    # Load results (check for errors in current results)
    current, time_unit = load_benchmark(head_path, check_errors=True)
    baseline, _ = load_benchmark(base_path)

    if current is None:
        print("ERROR: Current benchmark results not found or invalid!")
        sys.exit(1)

    # Validate we have the expected number of benchmarks
    if len(current) < EXPECTED_BENCHMARK_COUNT:
        print(f"ERROR: Expected at least {EXPECTED_BENCHMARK_COUNT} benchmarks but got {len(current)}")
        print(f"Found benchmarks: {list(current.keys())}")
        sys.exit(1)

    print("=" * 70)
    print("PERFORMANCE REGRESSION CHECK")
    print("=" * 70)
    if relative_mode:
        print("Mode: Same-job relative benchmarking (HEAD vs BASE)")
    else:
        print("Mode: Cache-based baseline comparison")
    print(f"Regression threshold: {threshold * 100:.0f}%")
    print(f"Benchmarks in current results: {len(current)}")
    print(f"Current benchmarks: {sorted(current.keys())}")
    print()

    if baseline is None:
        if relative_mode:
            print("ERROR: BASE benchmark results not found!")
            sys.exit(1)
        print("No baseline found. This run will establish the baseline.")
        print()
        print("Current benchmark results:")
        for name, time in sorted(current.items()):
            print(f"  {name}: {time:.2f} {time_unit}")

        # Only save baseline on main branch pushes (not in relative mode)
        if is_main_push:
            shutil.copy('build/benchmark_results.json', 'baseline_benchmark.json')
            print()
            print("Baseline saved for future comparisons (main branch push).")
        else:
            print()
            print("Skipping baseline save (not a main branch push).")
    else:
        print(f"Benchmarks in baseline: {len(baseline)}")
        print(f"Baseline benchmarks: {sorted(baseline.keys())}")
        print()

        # Check for benchmark name mismatches
        current_names = set(current.keys())
        baseline_names = set(baseline.keys())
        only_in_current = current_names - baseline_names
        only_in_baseline = baseline_names - current_names
        if only_in_current or only_in_baseline:
            print("WARNING: Benchmark name mismatch detected!")
            if only_in_current:
                print(f"  Benchmarks only in current (new): {sorted(only_in_current)}")
            if only_in_baseline:
                print(f"  Benchmarks only in baseline (removed): {sorted(only_in_baseline)}")
            print()

        regressions, improvements, unchanged, skipped = compare_benchmarks(
            baseline, current, time_unit or 'ns', threshold)

        if skipped:
            print("SKIPPED (baseline time too small for reliable comparison):")
            for name, base, curr in skipped:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} {time_unit} (below threshold)")
                print(f"    Current:  {curr:.2f} {time_unit}")
            print()

        if regressions:
            print("REGRESSIONS DETECTED:")
            for name, base, curr, ratio in regressions:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} {time_unit}")
                print(f"    Current:  {curr:.2f} {time_unit}")
                print(f"    Change:   +{ratio * 100:.1f}% (REGRESSION)")
            print()

        if improvements:
            print("IMPROVEMENTS:")
            for name, base, curr, ratio in improvements:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} {time_unit}")
                print(f"    Current:  {curr:.2f} {time_unit}")
                print(f"    Change:   {ratio * 100:.1f}%")
            print()

        if unchanged:
            print("UNCHANGED (within threshold):")
            for name, base, curr, ratio in unchanged:
                if ratio is not None:
                    print(f"  {name}: {ratio * 100:+.1f}%")
                else:
                    print(f"  {name}: (new benchmark)")
            print()

        if not regressions:
            if is_main_push and not relative_mode:
                shutil.copy('build/benchmark_results.json', 'baseline_benchmark.json')
                print("Baseline updated with current results (main branch push).")
            print()
            print("SUCCESS: No performance regressions detected!")
        else:
            print()
            print(f"FAILURE: {len(regressions)} benchmark(s) regressed by more than {threshold * 100:.0f}%")
            # Set output for GitHub Actions
            with open(os.environ.get('GITHUB_OUTPUT', '/dev/null'), 'a') as f:
                f.write('regression=true\n')
            sys.exit(1)

    print("=" * 70)


if __name__ == '__main__':
    main()
