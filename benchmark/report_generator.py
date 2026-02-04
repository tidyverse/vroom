#!/usr/bin/env python3
"""
Benchmark report generator and regression detection for libvroom.
Processes Google Benchmark JSON output and generates reports.
"""

import json
import sys
import argparse
import os
from datetime import datetime
import statistics
import subprocess
import matplotlib.pyplot as plt
import pandas as pd

class BenchmarkReporter:
    def __init__(self, json_file):
        """Initialize with benchmark JSON results."""
        with open(json_file, 'r') as f:
            self.data = json.load(f)
        self.benchmarks = self.data.get('benchmarks', [])
        
    def generate_markdown_report(self, output_file='benchmark_report.md'):
        """Generate a comprehensive markdown report."""
        with open(output_file, 'w') as f:
            f.write(f"# libvroom Benchmark Report\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")
            
            # System information
            f.write("## System Information\n\n")
            context = self.data.get('context', {})
            f.write(f"- **Date**: {context.get('date', 'N/A')}\n")
            f.write(f"- **Host**: {context.get('host_name', 'N/A')}\n")
            f.write(f"- **CPU**: {context.get('cpu_name', 'N/A')}\n")
            f.write(f"- **Cores**: {context.get('num_cpus', 'N/A')}\n")
            f.write(f"- **Library**: {context.get('library_build_type', 'N/A')}\n\n")
            
            # Performance summary
            self._write_performance_summary(f)
            
            # Detailed results by category
            self._write_detailed_results(f)
            
            # Scalability analysis
            self._write_scalability_analysis(f)
            
            # Performance trends
            self._write_performance_trends(f)
        
        print(f"Markdown report generated: {output_file}")
    
    def _write_performance_summary(self, f):
        """Write performance summary section."""
        f.write("## Performance Summary\n\n")
        
        # Calculate key metrics
        throughput_benchmarks = [b for b in self.benchmarks if 'GB/s' in b.get('real_time_other', {})]
        if throughput_benchmarks:
            throughputs = [b['real_time_other']['GB/s'] for b in throughput_benchmarks]
            f.write(f"- **Peak Throughput**: {max(throughputs):.2f} GB/s\n")
            f.write(f"- **Average Throughput**: {statistics.mean(throughputs):.2f} GB/s\n")
            f.write(f"- **Median Throughput**: {statistics.median(throughputs):.2f} GB/s\n")
        
        # Thread scaling
        thread_benchmarks = [b for b in self.benchmarks if 'ThreadScaling' in b['name']]
        if thread_benchmarks:
            max_efficiency = max([b['real_time_other'].get('ScalingEfficiency', 0) 
                                for b in thread_benchmarks if 'ScalingEfficiency' in b.get('real_time_other', {})])
            f.write(f"- **Best Thread Scaling Efficiency**: {max_efficiency:.1f}%\n")
        
        f.write("\n")
    
    def _write_detailed_results(self, f):
        """Write detailed benchmark results."""
        f.write("## Detailed Results\n\n")
        
        # Group benchmarks by category
        categories = {
            'File Size': [b for b in self.benchmarks if 'FileSizes' in b['name']],
            'Column Count': [b for b in self.benchmarks if 'ColumnCounts' in b['name']],
            'Data Types': [b for b in self.benchmarks if 'DataTypes' in b['name']],
            'Thread Scaling': [b for b in self.benchmarks if 'ThreadScaling' in b['name']],
            'Real World': [b for b in self.benchmarks if any(x in b['name'] for x in ['financial', 'genomics', 'taxi', 'log'])],
            'Cache Performance': [b for b in self.benchmarks if 'Cache' in b['name']],
            'Memory Bandwidth': [b for b in self.benchmarks if 'MemoryBandwidth' in b['name']]
        }
        
        for category, benchmarks in categories.items():
            if benchmarks:
                f.write(f"### {category}\n\n")
                f.write("| Benchmark | Time (ms) | Throughput (GB/s) | Other Metrics |\n")
                f.write("|-----------|-----------|-------------------|---------------|\n")
                
                for bench in benchmarks:
                    name = bench['name'].replace('BM_', '').replace(category.replace(' ', ''), '')
                    time_ms = bench['real_time'] / 1000000.0  # Convert ns to ms
                    throughput = bench.get('real_time_other', {}).get('GB/s', 'N/A')
                    
                    # Collect other metrics
                    other_metrics = []
                    real_time_other = bench.get('real_time_other', {})
                    for key, value in real_time_other.items():
                        if key not in ['GB/s', 'MB/s']:
                            if isinstance(value, float):
                                other_metrics.append(f"{key}: {value:.2f}")
                            else:
                                other_metrics.append(f"{key}: {value}")
                    
                    other_str = ", ".join(other_metrics) if other_metrics else "-"
                    f.write(f"| {name} | {time_ms:.2f} | {throughput} | {other_str} |\n")
                
                f.write("\n")
    
    def _write_scalability_analysis(self, f):
        """Write thread scalability analysis."""
        f.write("## Thread Scalability Analysis\n\n")
        
        scaling_benchmarks = [b for b in self.benchmarks if 'ThreadScaling' in b['name']]
        if not scaling_benchmarks:
            f.write("No thread scaling benchmarks found.\n\n")
            return
        
        # Extract thread count and throughput
        thread_data = []
        for bench in scaling_benchmarks:
            threads = bench.get('real_time_other', {}).get('Threads', 1)
            throughput = bench.get('real_time_other', {}).get('GB/s', 0)
            efficiency = bench.get('real_time_other', {}).get('ScalingEfficiency', 0)
            thread_data.append((threads, throughput, efficiency))
        
        thread_data.sort(key=lambda x: x[0])  # Sort by thread count
        
        f.write("| Threads | Throughput (GB/s) | Efficiency (%) | Speedup |\n")
        f.write("|---------|-------------------|----------------|----------|\n")
        
        baseline_throughput = None
        for threads, throughput, efficiency in thread_data:
            if threads == 1:
                baseline_throughput = throughput
                speedup = 1.0
            else:
                speedup = throughput / baseline_throughput if baseline_throughput else 0
            
            f.write(f"| {threads} | {throughput:.2f} | {efficiency:.1f} | {speedup:.2f}x |\n")
        
        f.write("\n")
    
    def _write_performance_trends(self, f):
        """Write performance trends analysis."""
        f.write("## Performance Trends\n\n")
        
        # File size vs throughput
        file_size_benchmarks = [b for b in self.benchmarks if 'FileSizes' in b['name']]
        if file_size_benchmarks:
            f.write("### Throughput vs File Size\n\n")
            f.write("| File Size | Throughput (GB/s) |\n")
            f.write("|-----------|-----------------|\n")
            
            for bench in file_size_benchmarks:
                size = bench.get('real_time_other', {}).get('ActualSize', 'Unknown')
                throughput = bench.get('real_time_other', {}).get('GB/s', 0)
                size_str = self._format_bytes(size) if isinstance(size, (int, float)) else size
                f.write(f"| {size_str} | {throughput:.2f} |\n")
            f.write("\n")
    
    def _format_bytes(self, size):
        """Format byte size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"

class RegressionDetector:
    def __init__(self, current_results_file, baseline_results_file=None):
        """Initialize regression detector."""
        self.current = BenchmarkReporter(current_results_file)
        self.baseline = BenchmarkReporter(baseline_results_file) if baseline_results_file else None
    
    def detect_regressions(self, threshold=10.0):
        """Detect performance regressions (threshold in %)."""
        if not self.baseline:
            print("No baseline provided, skipping regression detection.")
            return [], []

        regressions = []
        skipped = []

        # Minimum time threshold (1ms in nanoseconds) to avoid division by zero
        # and unreliable comparisons for very fast benchmarks
        MIN_TIME_THRESHOLD_NS = 1_000_000

        # Create lookup for baseline benchmarks
        baseline_lookup = {b['name']: b for b in self.baseline.benchmarks}

        for current_bench in self.current.benchmarks:
            name = current_bench['name']
            baseline_bench = baseline_lookup.get(name)

            if not baseline_bench:
                continue

            # Compare key metrics
            current_time = current_bench['real_time']
            baseline_time = baseline_bench['real_time']

            # Skip comparison if baseline time is zero or below threshold
            if baseline_time <= 0 or baseline_time < MIN_TIME_THRESHOLD_NS:
                skipped.append({
                    'name': name,
                    'current_time': current_time,
                    'baseline_time': baseline_time,
                    'reason': 'baseline time below 1ms threshold'
                })
                continue

            # Performance regression = current is slower than baseline
            time_change_percent = ((current_time - baseline_time) / baseline_time) * 100

            if time_change_percent > threshold:
                regressions.append({
                    'name': name,
                    'current_time': current_time,
                    'baseline_time': baseline_time,
                    'regression_percent': time_change_percent
                })

        return regressions, skipped
    
    def generate_regression_report(self, output_file='regression_report.md', threshold=10.0):
        """Generate regression detection report."""
        regressions, skipped = self.detect_regressions(threshold)

        with open(output_file, 'w') as f:
            f.write(f"# Regression Detection Report\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Threshold: {threshold}% performance degradation\n\n")

            if skipped:
                f.write(f"⏭️  **{len(skipped)} benchmark(s) skipped** (baseline time below 1ms threshold):\n\n")
                f.write("| Benchmark | Baseline Time (ns) | Current Time (ns) | Reason |\n")
                f.write("|-----------|-------------------|-------------------|--------|\n")
                for skip in skipped:
                    f.write(f"| {skip['name']} | {skip['baseline_time']:,.0f} | {skip['current_time']:,.0f} | {skip['reason']} |\n")
                f.write("\n")

            if not regressions:
                f.write("✅ **No performance regressions detected!**\n\n")
            else:
                f.write(f"⚠️  **{len(regressions)} performance regressions detected:**\n\n")
                f.write("| Benchmark | Current Time (ns) | Baseline Time (ns) | Regression (%) |\n")
                f.write("|-----------|-------------------|-------------------|----------------|\n")

                for reg in regressions:
                    f.write(f"| {reg['name']} | {reg['current_time']:,} | {reg['baseline_time']:,} | +{reg['regression_percent']:.1f}% |\n")
                f.write("\n")

        print(f"Regression report generated: {output_file}")
        return len(regressions)

def main():
    parser = argparse.ArgumentParser(description='Generate benchmark reports and detect regressions')
    parser.add_argument('results_file', help='JSON file with benchmark results')
    parser.add_argument('--baseline', help='Baseline JSON file for regression detection')
    parser.add_argument('--output', default='benchmark_report.md', help='Output markdown file')
    parser.add_argument('--regression-threshold', type=float, default=10.0, 
                       help='Regression threshold in percent (default: 10%%)')
    parser.add_argument('--fail-on-regression', action='store_true',
                       help='Exit with error code if regressions detected')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results_file):
        print(f"Error: Results file '{args.results_file}' not found")
        sys.exit(1)
    
    # Generate main report
    reporter = BenchmarkReporter(args.results_file)
    reporter.generate_markdown_report(args.output)
    
    # Regression detection
    if args.baseline:
        if not os.path.exists(args.baseline):
            print(f"Error: Baseline file '{args.baseline}' not found")
            sys.exit(1)
        
        detector = RegressionDetector(args.results_file, args.baseline)
        regression_count = detector.generate_regression_report(
            'regression_report.md', args.regression_threshold)
        
        if args.fail_on_regression and regression_count > 0:
            print(f"ERROR: {regression_count} performance regressions detected!")
            sys.exit(1)
    
    print("Benchmark reporting completed successfully.")

if __name__ == '__main__':
    main()