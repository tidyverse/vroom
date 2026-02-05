#!/usr/bin/env python3
"""
Analyze hypothesis benchmark results and generate a report.

This script processes Google Benchmark JSON output and evaluates each hypothesis
from Issue #611 based on the discriminatory tests.

Usage:
    ./build/libvroom_benchmark --benchmark_out=results.json --benchmark_out_format=json
    python scripts/analyze_hypotheses.py results.json > HYPOTHESIS_RESULTS.md

Or to see plain text output:
    python scripts/analyze_hypotheses.py results.json --format=text
"""

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    """Parsed benchmark result."""
    name: str
    real_time: float  # milliseconds
    cpu_time: float  # milliseconds
    iterations: int
    counters: dict

    @classmethod
    def from_json(cls, data: dict) -> 'BenchmarkResult':
        # Google Benchmark puts counters as top-level keys, not in a nested object
        # Extract known counter keys
        counter_keys = {'Rows', 'Cols', 'Threads', 'Accesses', 'AccessesPerSec',
                        'RowsPerSec', 'Fields', 'Size_MB', 'TypeChanges',
                        'SampleRows', 'EscapeCount', 'TotalFields',
                        'EscapeRatio_Input', 'ActualEscapeRatio', 'IsFlat',
                        'TotalCols', 'ColsIterated', 'FieldsAccessed', 'Stage'}
        counters = {k: v for k, v in data.items() if k in counter_keys}

        return cls(
            name=data.get('name', ''),
            real_time=data.get('real_time', 0),
            cpu_time=data.get('cpu_time', 0),
            iterations=data.get('iterations', 0),
            counters=counters,
        )


def load_results(filepath: str) -> list[BenchmarkResult]:
    """Load benchmark results from JSON file."""
    with open(filepath) as f:
        data = json.load(f)

    results = []
    for bench in data.get('benchmarks', []):
        # Skip aggregate entries (mean, median, stddev)
        if bench.get('aggregate_name'):
            continue
        results.append(BenchmarkResult.from_json(bench))

    return results


def group_by_prefix(results: list[BenchmarkResult], prefix: str) -> list[BenchmarkResult]:
    """Filter results by benchmark name prefix."""
    return [r for r in results if r.name.startswith(prefix)]


def analyze_h6(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H6: compact() is required for O(1) field access.

    Confirms H6 if:
    - Average sequential ratio > 1.15 (non-compact is >15% slower)
    - AND average random ratio > 1.5 (non-compact is >50% slower for random)
    - OR max sequential ratio > 1.4 (degradation at higher thread counts)
    """
    no_compact_seq = group_by_prefix(results, 'BM_H6_FieldAccess_NoCompact')
    with_compact_seq = group_by_prefix(results, 'BM_H6_FieldAccess_WithCompact')
    no_compact_rand = group_by_prefix(results, 'BM_H6_RandomAccess_NoCompact')
    with_compact_rand = group_by_prefix(results, 'BM_H6_RandomAccess_WithCompact')

    analysis = {
        'hypothesis': 'H6',
        'description': 'compact() is required for O(1) field access',
        'sequential_ratios': [],
        'random_ratios': [],
        'confirmed': None,
        'details': [],
    }

    # Match up corresponding tests by arguments
    for nc in no_compact_seq:
        # Find matching compact test
        for wc in with_compact_seq:
            nc_rows = nc.counters.get('Rows', 0)
            nc_threads = nc.counters.get('Threads', 0)
            if nc_rows == wc.counters.get('Rows') and nc_threads == wc.counters.get('Threads'):
                ratio = nc.real_time / wc.real_time if wc.real_time > 0 else float('inf')
                analysis['sequential_ratios'].append(ratio)
                analysis['details'].append(
                    f"Sequential {int(nc_rows):,} rows, "
                    f"{int(nc_threads)} threads: "
                    f"{nc.real_time:.2f}ms vs {wc.real_time:.2f}ms "
                    f"(ratio: {ratio:.2f}x)"
                )
                break

    for nc in no_compact_rand:
        for wc in with_compact_rand:
            nc_threads = nc.counters.get('Threads', 0)
            nc_accesses = nc.counters.get('Accesses', 0)
            wc_accesses = wc.counters.get('Accesses', 0)
            # Match by threads and accesses (random access benchmarks don't have Rows)
            if nc_threads == wc.counters.get('Threads') and nc_accesses == wc_accesses:
                ratio = nc.real_time / wc.real_time if wc.real_time > 0 else float('inf')
                analysis['random_ratios'].append(ratio)
                analysis['details'].append(
                    f"Random {int(nc_accesses):,} accesses, "
                    f"{int(nc_threads)} threads: "
                    f"{nc.real_time:.2f}ms vs {wc.real_time:.2f}ms "
                    f"(ratio: {ratio:.2f}x)"
                )
                break

    # Evaluate hypothesis
    if analysis['sequential_ratios'] and analysis['random_ratios']:
        avg_seq_ratio = sum(analysis['sequential_ratios']) / len(analysis['sequential_ratios'])
        avg_rand_ratio = sum(analysis['random_ratios']) / len(analysis['random_ratios'])
        max_seq_ratio = max(analysis['sequential_ratios'])

        analysis['avg_sequential_ratio'] = avg_seq_ratio
        analysis['avg_random_ratio'] = avg_rand_ratio
        analysis['max_sequential_ratio'] = max_seq_ratio

        # Criteria: seq > 1.15 AND random > 1.5, OR max_seq > 1.4 (thread scaling degradation)
        analysis['confirmed'] = (avg_seq_ratio > 1.15 and avg_rand_ratio > 1.5) or max_seq_ratio > 1.4

    return analysis


def analyze_h7(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H7: Row object creation is expensive for per-field access.

    Confirms H7 if: T_via_row / T_direct > 1.5 (Row object adds >50% overhead)
    """
    via_row = group_by_prefix(results, 'BM_H7_ViaRowObject')
    direct = group_by_prefix(results, 'BM_H7_DirectSpanAccess')
    via_extractor = group_by_prefix(results, 'BM_H7_ViaExtractor')

    analysis = {
        'hypothesis': 'H7',
        'description': 'Row object creation is expensive for per-field access',
        'row_vs_direct_ratios': [],
        'row_vs_extractor_ratios': [],
        'confirmed': None,
        'details': [],
    }

    for row_bench in via_row:
        rows = row_bench.counters.get('Rows', 0)
        for direct_bench in direct:
            if direct_bench.counters.get('Rows') == rows:
                ratio = row_bench.real_time / direct_bench.real_time if direct_bench.real_time > 0 else float('inf')
                analysis['row_vs_direct_ratios'].append(ratio)
                analysis['details'].append(
                    f"Row vs Direct ({int(rows):,} rows): "
                    f"{row_bench.real_time:.2f}ms vs {direct_bench.real_time:.2f}ms "
                    f"(ratio: {ratio:.2f}x)"
                )
                break

        for ext_bench in via_extractor:
            if ext_bench.counters.get('Rows') == rows:
                ratio = row_bench.real_time / ext_bench.real_time if ext_bench.real_time > 0 else float('inf')
                analysis['row_vs_extractor_ratios'].append(ratio)
                analysis['details'].append(
                    f"Row vs Extractor ({int(rows):,} rows): "
                    f"{row_bench.real_time:.2f}ms vs {ext_bench.real_time:.2f}ms "
                    f"(ratio: {ratio:.2f}x)"
                )
                break

    if analysis['row_vs_direct_ratios']:
        avg_ratio = sum(analysis['row_vs_direct_ratios']) / len(analysis['row_vs_direct_ratios'])
        analysis['avg_row_vs_direct_ratio'] = avg_ratio
        analysis['confirmed'] = avg_ratio > 1.5

    return analysis


def analyze_h1(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H1: Column-major index provides no net benefit.

    Confirms H1 if: T_B >= T_A for all test cases (column-major never wins)
    where T_A = row-major time, T_B = (transpose + column-major iteration)
    """
    row_major = group_by_prefix(results, 'BM_H1_RowMajor_ColumnIteration')
    col_major = group_by_prefix(results, 'BM_H1_ColMajor_ColumnIteration')
    transpose = group_by_prefix(results, 'BM_H1_TransposeOnly')
    full_row = group_by_prefix(results, 'BM_FullPipeline_RowMajor')
    full_col = group_by_prefix(results, 'BM_FullPipeline_ColMajor')

    analysis = {
        'hypothesis': 'H1',
        'description': 'Column-major index provides no net benefit over row-major',
        'comparisons': [],
        'transpose_times': [],
        'full_pipeline_comparisons': [],
        'confirmed': None,
        'details': [],
    }

    # Compare iteration times (after layout is established)
    for rm in row_major:
        rows = rm.counters.get('Rows', 0)
        cols = rm.counters.get('Cols', 0)
        for cm in col_major:
            if cm.counters.get('Rows') == rows and cm.counters.get('Cols') == cols:
                # Note: col_major time doesn't include transpose
                # For fair comparison, we need to look at full pipeline
                analysis['details'].append(
                    f"Iteration only ({int(rows):,}×{int(cols):,}): "
                    f"Row-major {rm.real_time:.2f}ms, Col-major {cm.real_time:.2f}ms"
                )
                break

    # Record transpose times
    for t in transpose:
        rows = t.counters.get('Rows', 0)
        cols = t.counters.get('Cols', 0)
        analysis['transpose_times'].append({
            'rows': rows, 'cols': cols, 'time_ms': t.real_time
        })
        analysis['details'].append(
            f"Transpose only ({int(rows):,}×{int(cols):,}): {t.real_time:.2f}ms"
        )

    # Full pipeline comparison
    col_major_wins = 0
    row_major_wins = 0
    for fr in full_row:
        rows = fr.counters.get('Rows', 0)
        cols = fr.counters.get('Cols', 0)
        for fc in full_col:
            if fc.counters.get('Rows') == rows and fc.counters.get('Cols') == cols:
                ratio = fc.real_time / fr.real_time if fr.real_time > 0 else 0
                analysis['full_pipeline_comparisons'].append({
                    'rows': rows, 'cols': cols,
                    'row_major_ms': fr.real_time,
                    'col_major_ms': fc.real_time,
                    'ratio': ratio
                })
                analysis['details'].append(
                    f"Full pipeline ({int(rows):,}×{int(cols):,}): "
                    f"Row-major {fr.real_time:.2f}ms, Col-major {fc.real_time:.2f}ms "
                    f"(col/row ratio: {ratio:.2f})"
                )
                if fc.real_time > fr.real_time:
                    row_major_wins += 1
                else:
                    col_major_wins += 1
                break

    # H1 confirmed if col-major never provides net benefit (always >= row-major)
    if analysis['full_pipeline_comparisons']:
        analysis['confirmed'] = col_major_wins == 0

    analysis['row_major_wins'] = row_major_wins
    analysis['col_major_wins'] = col_major_wins

    return analysis


def analyze_h3(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H3: Synchronization barriers dominate multi-threaded scaling.

    Examines scaling efficiency at different thread counts.
    """
    thread_scaling = group_by_prefix(results, 'BM_H3_ThreadScaling')

    analysis = {
        'hypothesis': 'H3',
        'description': 'Synchronization barriers dominate multi-threaded scaling',
        'scaling_data': [],
        'confirmed': None,
        'details': [],
    }

    # Group by file size
    by_size = defaultdict(list)
    for r in thread_scaling:
        size = r.counters.get('Size_MB', 0)
        by_size[size].append(r)

    for size, results_for_size in sorted(by_size.items()):
        results_for_size.sort(key=lambda x: x.counters.get('Threads', 0))

        base_time = None
        for r in results_for_size:
            threads = int(r.counters.get('Threads', 1))
            if threads == 1:
                base_time = r.real_time

            if base_time:
                speedup = base_time / r.real_time if r.real_time > 0 else 0
                efficiency = speedup / threads if threads > 0 else 0

                analysis['scaling_data'].append({
                    'size_mb': size,
                    'threads': threads,
                    'time_ms': r.real_time,
                    'speedup': speedup,
                    'efficiency': efficiency,
                })
                analysis['details'].append(
                    f"{size:.0f}MB, {threads} threads: "
                    f"{r.real_time:.2f}ms, speedup {speedup:.2f}x, "
                    f"efficiency {efficiency*100:.1f}%"
                )

    # H3 confirmed if scaling efficiency drops significantly at 8+ threads
    # (indicating barrier overhead)
    if analysis['scaling_data']:
        high_thread_efficiency = [
            d['efficiency'] for d in analysis['scaling_data']
            if d['threads'] >= 8
        ]
        low_thread_efficiency = [
            d['efficiency'] for d in analysis['scaling_data']
            if d['threads'] <= 4 and d['threads'] > 1
        ]

        if high_thread_efficiency and low_thread_efficiency:
            avg_high = sum(high_thread_efficiency) / len(high_thread_efficiency)
            avg_low = sum(low_thread_efficiency) / len(low_thread_efficiency)
            # Confirmed if high-thread efficiency is significantly worse
            analysis['confirmed'] = avg_high < 0.5 * avg_low
            analysis['avg_low_thread_efficiency'] = avg_low
            analysis['avg_high_thread_efficiency'] = avg_high

    return analysis


def analyze_h4(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H4: Zero-copy string extraction is viable for most CSV data.

    This is more of a data characterization than a performance comparison.
    """
    escape_analysis = group_by_prefix(results, 'BM_H4_EscapeAnalysis')

    analysis = {
        'hypothesis': 'H4',
        'description': 'Zero-copy string extraction is viable for most CSV data (>80% fields need no escape processing)',
        'escape_data': [],
        'confirmed': None,
        'details': [],
    }

    for r in escape_analysis:
        input_ratio = r.counters.get('EscapeRatio_Input', 0)
        actual_ratio = r.counters.get('ActualEscapeRatio', 0)
        escape_count = r.counters.get('EscapeCount', 0)
        total_fields = r.counters.get('TotalFields', 0)

        analysis['escape_data'].append({
            'input_ratio': input_ratio,
            'actual_ratio': actual_ratio,
            'escape_count': escape_count,
            'total_fields': total_fields,
        })
        analysis['details'].append(
            f"Input escape ratio {input_ratio*100:.0f}%: "
            f"found {int(escape_count):,}/{int(total_fields):,} fields with escapes "
            f"({actual_ratio*100:.1f}%)"
        )

    # H4 is about real-world data; this benchmark validates our detection works
    # The actual hypothesis validation requires running on real-world corpus
    analysis['confirmed'] = None  # Cannot confirm from synthetic data
    analysis['details'].append(
        "Note: H4 requires real-world CSV corpus analysis. "
        "Run escape detection on data.gov/Kaggle/UCI files to confirm."
    )

    return analysis


def analyze_h5(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H5: Parquet type widening is rare in real CSV data.
    """
    type_inference = group_by_prefix(results, 'BM_H5_TypeInference')

    analysis = {
        'hypothesis': 'H5',
        'description': 'Type widening is rare in real CSV data (<5% of files)',
        'type_data': [],
        'confirmed': None,
        'details': [],
    }

    for r in type_inference:
        rows = r.counters.get('Rows', 0)
        sample_rows = r.counters.get('SampleRows', 0)
        type_changes = r.counters.get('TypeChanges', 0)

        analysis['type_data'].append({
            'rows': rows,
            'sample_rows': sample_rows,
            'type_changes': type_changes,
        })
        analysis['details'].append(
            f"{int(rows):,} rows (sample {int(sample_rows):,}): "
            f"{int(type_changes)} type changes detected"
        )

    # H5 requires real-world corpus analysis
    analysis['confirmed'] = None
    analysis['details'].append(
        "Note: H5 requires real-world CSV corpus analysis. "
        "Type change detection shows the detection mechanism works."
    )

    return analysis


def analyze_h2(results: list[BenchmarkResult]) -> dict:
    """
    Analyze H2: Arrow Builder API is the primary bottleneck.

    Confirms H2 if: Arrow conversion time >> parse time (ratio > 2x)
    Refutes H2 if: Arrow conversion time is comparable to parse time

    Compares:
    - Parse-only baseline
    - Parse + field extraction
    - Parse + direct buffer simulation
    - Parse + Builder pattern simulation
    - (Optional) Full Arrow Builders conversion
    """
    parse_only = group_by_prefix(results, 'BM_H2_ParseOnly')
    parse_extract = group_by_prefix(results, 'BM_H2_ParseAndExtract')
    direct_buffer = group_by_prefix(results, 'BM_H2_DirectBufferSimulation')
    builder_pattern = group_by_prefix(results, 'BM_H2_BuilderPatternOverhead')
    arrow_full = group_by_prefix(results, 'BM_H2_ArrowBuilders_Full')
    arrow_no_inf = group_by_prefix(results, 'BM_H2_ArrowBuilders_NoInference')
    type_inf_only = group_by_prefix(results, 'BM_H2_TypeInferenceOnly')

    analysis = {
        'hypothesis': 'H2',
        'description': 'Arrow Builder API is the primary bottleneck (not index layout)',
        'comparisons': [],
        'confirmed': None,
        'details': [],
        'has_arrow_benchmarks': len(arrow_full) > 0,
    }

    # Group by (rows, cols) configuration
    configs = {}
    for r in parse_only:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key not in configs:
            configs[key] = {}
        configs[key]['parse_only'] = r.real_time

    for r in parse_extract:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['parse_extract'] = r.real_time

    for r in direct_buffer:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['direct_buffer'] = r.real_time

    for r in builder_pattern:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['builder_pattern'] = r.real_time

    for r in arrow_full:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['arrow_full'] = r.real_time

    for r in arrow_no_inf:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['arrow_no_inference'] = r.real_time

    for r in type_inf_only:
        rows = int(r.counters.get('Rows', 0))
        cols = int(r.counters.get('Cols', 0))
        key = (rows, cols)
        if key in configs:
            configs[key]['type_inference'] = r.real_time

    # Analyze each configuration
    arrow_overhead_ratios = []
    builder_overhead_ratios = []

    for (rows, cols), times in sorted(configs.items()):
        parse_time = times.get('parse_only', 0)
        if parse_time == 0:
            continue

        analysis['details'].append(f"\n### {rows:,} rows × {cols} columns:")

        # Parse only (baseline)
        analysis['details'].append(f"  Parse only: {parse_time:.2f}ms")

        # Parse + extraction overhead
        if 'parse_extract' in times:
            extract_time = times['parse_extract']
            extract_overhead = extract_time / parse_time if parse_time > 0 else 0
            analysis['details'].append(
                f"  Parse + extract: {extract_time:.2f}ms ({extract_overhead:.2f}x parse)"
            )

        # Direct buffer simulation
        if 'direct_buffer' in times:
            direct_time = times['direct_buffer']
            direct_ratio = direct_time / parse_time if parse_time > 0 else 0
            analysis['details'].append(
                f"  Direct buffer: {direct_time:.2f}ms ({direct_ratio:.2f}x parse)"
            )

        # Builder pattern simulation
        if 'builder_pattern' in times:
            builder_time = times['builder_pattern']
            builder_ratio = builder_time / parse_time if parse_time > 0 else 0
            builder_overhead_ratios.append(builder_ratio)
            # Compare to direct buffer
            if 'direct_buffer' in times:
                direct_time = times['direct_buffer']
                builder_vs_direct = builder_time / direct_time if direct_time > 0 else 0
                analysis['details'].append(
                    f"  Builder pattern: {builder_time:.2f}ms ({builder_ratio:.2f}x parse, "
                    f"{builder_vs_direct:.2f}x direct buffer)"
                )
            else:
                analysis['details'].append(
                    f"  Builder pattern: {builder_time:.2f}ms ({builder_ratio:.2f}x parse)"
                )

        # Full Arrow (if available)
        if 'arrow_full' in times:
            arrow_time = times['arrow_full']
            arrow_ratio = arrow_time / parse_time if parse_time > 0 else 0
            arrow_overhead_ratios.append(arrow_ratio)
            analysis['details'].append(
                f"  Arrow Builders (full): {arrow_time:.2f}ms ({arrow_ratio:.2f}x parse)"
            )

        # Arrow without inference
        if 'arrow_no_inference' in times:
            arrow_no_inf_time = times['arrow_no_inference']
            arrow_no_inf_ratio = arrow_no_inf_time / parse_time if parse_time > 0 else 0
            analysis['details'].append(
                f"  Arrow (no inference): {arrow_no_inf_time:.2f}ms ({arrow_no_inf_ratio:.2f}x parse)"
            )

        # Type inference only
        if 'type_inference' in times:
            type_inf_time = times['type_inference']
            type_inf_ratio = type_inf_time / parse_time if parse_time > 0 else 0
            analysis['details'].append(
                f"  Type inference only: {type_inf_time:.2f}ms ({type_inf_ratio:.2f}x parse)"
            )

        analysis['comparisons'].append({
            'rows': rows, 'cols': cols, **times
        })

    # Evaluate hypothesis
    # H2 confirmed if Arrow conversion is significantly slower than parsing
    # Using builder pattern as proxy if Arrow not available
    if arrow_overhead_ratios:
        avg_arrow_overhead = sum(arrow_overhead_ratios) / len(arrow_overhead_ratios)
        analysis['avg_arrow_overhead_ratio'] = avg_arrow_overhead
        # Confirmed if Arrow adds >2x overhead compared to parse-only
        analysis['confirmed'] = avg_arrow_overhead > 2.0
        analysis['details'].append(
            f"\nAverage Arrow/parse ratio: {avg_arrow_overhead:.2f}x"
        )
        if analysis['confirmed']:
            analysis['details'].append(
                "Conclusion: Arrow Builder API IS a significant bottleneck (>2x parse time)"
            )
        else:
            analysis['details'].append(
                "Conclusion: Arrow Builder API is NOT the primary bottleneck (<2x parse time)"
            )
    elif builder_overhead_ratios:
        avg_builder_overhead = sum(builder_overhead_ratios) / len(builder_overhead_ratios)
        analysis['avg_builder_overhead_ratio'] = avg_builder_overhead
        # Can only partially evaluate without real Arrow benchmarks
        analysis['details'].append(
            f"\nAverage Builder pattern/parse ratio: {avg_builder_overhead:.2f}x"
        )
        analysis['details'].append(
            "Note: Full Arrow benchmarks not available. Build with -DLIBVROOM_ENABLE_ARROW=ON "
            "and re-run to get complete H2 analysis."
        )
    else:
        analysis['details'].append(
            "Insufficient data to evaluate H2. Run H2 benchmarks first."
        )

    return analysis


def format_report_markdown(analyses: list[dict]) -> str:
    """Format analysis results as Markdown."""
    lines = ["# Hypothesis Benchmark Results\n"]

    for a in analyses:
        lines.append(f"## {a['hypothesis']}: {a['description']}\n")

        if a['confirmed'] is True:
            lines.append("**Status: CONFIRMED** ✓\n")
        elif a['confirmed'] is False:
            lines.append("**Status: REFUTED** ✗\n")
        else:
            lines.append("**Status: INCONCLUSIVE** (requires additional data)\n")

        lines.append("### Details\n")
        for detail in a.get('details', []):
            lines.append(f"- {detail}")

        lines.append("")

    return '\n'.join(lines)


def format_report_text(analyses: list[dict]) -> str:
    """Format analysis results as plain text."""
    lines = ["=" * 60, "HYPOTHESIS BENCHMARK RESULTS", "=" * 60, ""]

    for a in analyses:
        lines.append(f"{a['hypothesis']}: {a['description']}")
        lines.append("-" * 40)

        if a['confirmed'] is True:
            lines.append("Status: CONFIRMED")
        elif a['confirmed'] is False:
            lines.append("Status: REFUTED")
        else:
            lines.append("Status: INCONCLUSIVE (requires additional data)")

        lines.append("")
        for detail in a.get('details', []):
            lines.append(f"  {detail}")

        lines.append("")

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze hypothesis benchmark results"
    )
    parser.add_argument(
        "results_file",
        help="Path to benchmark results JSON file"
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "text"],
        default="markdown",
        help="Output format (default: markdown)"
    )

    args = parser.parse_args()

    try:
        results = load_results(args.results_file)
    except FileNotFoundError:
        print(f"Error: File not found: {args.results_file}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if not results:
        print("Error: No benchmark results found in file", file=sys.stderr)
        sys.exit(1)

    # Run analyses
    analyses = [
        analyze_h1(results),
        analyze_h2(results),
        analyze_h3(results),
        analyze_h4(results),
        analyze_h5(results),
        analyze_h6(results),
        analyze_h7(results),
    ]

    # Format and output
    if args.format == "markdown":
        print(format_report_markdown(analyses))
    else:
        print(format_report_text(analyses))


if __name__ == "__main__":
    main()
