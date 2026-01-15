#!/usr/bin/env python3
"""Check for performance regressions in benchmark results.

This script compares current benchmark results against a baseline and
detects performance regressions that exceed a configurable threshold.
"""

import json
import sys
import os
import shutil

REGRESSION_THRESHOLD = 0.10  # 10% regression threshold
EXPECTED_BENCHMARK_COUNT = 4  # Number of benchmarks we expect to run


def load_benchmark(filepath, check_errors=False):
    """Load benchmark results from JSON file.

    Args:
        filepath: Path to benchmark JSON file
        check_errors: If True, fail on benchmark errors

    Returns:
        Dict of benchmark name -> average time in nanoseconds, or None on error
    """
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON from {filepath}: {e}")
        return None

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
        return None

    # Extract benchmark results, keyed by name
    results = {}
    for bench in data.get('benchmarks', []):
        name = bench.get('name', '')
        # Skip aggregate results (mean, median, stddev)
        if bench.get('aggregate_name'):
            continue
        # Skip benchmarks that had errors
        if bench.get('error_occurred') or bench.get('error_message'):
            continue
        # Use real_time as the primary metric (nanoseconds)
        if 'real_time' in bench:
            if name not in results:
                results[name] = []
            results[name].append(bench['real_time'])

    # Average multiple runs
    return {name: sum(times) / len(times) for name, times in results.items()}


def compare_benchmarks(baseline, current):
    """Compare current results against baseline and detect regressions."""
    regressions = []
    improvements = []
    unchanged = []
    skipped = []

    # Minimum time threshold (1ms in nanoseconds) to avoid division by zero
    # and unreliable comparisons for very fast benchmarks
    MIN_TIME_THRESHOLD_NS = 1_000_000

    for name, curr_time in current.items():
        if name not in baseline:
            unchanged.append((name, curr_time, None, None))
            continue

        base_time = baseline[name]

        # Skip comparison if baseline time is zero or below threshold
        if base_time <= 0 or base_time < MIN_TIME_THRESHOLD_NS:
            skipped.append((name, base_time, curr_time))
            continue

        # Positive ratio means regression (slower), negative means improvement
        ratio = (curr_time - base_time) / base_time

        if ratio > REGRESSION_THRESHOLD:
            regressions.append((name, base_time, curr_time, ratio))
        elif ratio < -REGRESSION_THRESHOLD:
            improvements.append((name, base_time, curr_time, ratio))
        else:
            unchanged.append((name, base_time, curr_time, ratio))

    return regressions, improvements, unchanged, skipped


def main():
    # Check if this is a main branch push
    is_main_push = os.environ.get('GITHUB_EVENT_NAME') == 'push' and \
                   os.environ.get('GITHUB_REF') in ('refs/heads/main', 'refs/heads/master')

    # Load results (check for errors in current results)
    current = load_benchmark('build/benchmark_results.json', check_errors=True)
    baseline = load_benchmark('baseline_benchmark.json')

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
    print(f"Regression threshold: {REGRESSION_THRESHOLD * 100:.0f}%")
    print(f"Benchmarks found: {len(current)}")
    print()

    if baseline is None:
        print("No baseline found. This run will establish the baseline.")
        print()
        print("Current benchmark results:")
        for name, time in sorted(current.items()):
            print(f"  {name}: {time:.2f} ns")

        # Only save baseline on main branch pushes
        if is_main_push:
            shutil.copy('build/benchmark_results.json', 'baseline_benchmark.json')
            print()
            print("Baseline saved for future comparisons (main branch push).")
        else:
            print()
            print("Skipping baseline save (not a main branch push).")
    else:
        regressions, improvements, unchanged, skipped = compare_benchmarks(baseline, current)

        if skipped:
            print("SKIPPED (baseline time too small for reliable comparison):")
            for name, base, curr in skipped:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} ns (below 1ms threshold)")
                print(f"    Current:  {curr:.2f} ns")
            print()

        if regressions:
            print("REGRESSIONS DETECTED:")
            for name, base, curr, ratio in regressions:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} ns")
                print(f"    Current:  {curr:.2f} ns")
                print(f"    Change:   +{ratio * 100:.1f}% (REGRESSION)")
            print()

        if improvements:
            print("IMPROVEMENTS:")
            for name, base, curr, ratio in improvements:
                print(f"  {name}")
                print(f"    Baseline: {base:.2f} ns")
                print(f"    Current:  {curr:.2f} ns")
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
            if is_main_push:
                shutil.copy('build/benchmark_results.json', 'baseline_benchmark.json')
                print("Baseline updated with current results (main branch push).")
            print()
            print("SUCCESS: No performance regressions detected!")
        else:
            print()
            print(f"FAILURE: {len(regressions)} benchmark(s) regressed by more than {REGRESSION_THRESHOLD * 100:.0f}%")
            # Set output for GitHub Actions
            with open(os.environ.get('GITHUB_OUTPUT', '/dev/null'), 'a') as f:
                f.write('regression=true\n')
            sys.exit(1)

    print("=" * 70)


if __name__ == '__main__':
    main()
