#!/bin/bash
#
# Automated benchmark runner with reporting and regression detection
#

set -e

# Configuration
BENCHMARK_EXECUTABLE="${1:-./libvroom_benchmark}"
OUTPUT_DIR="${2:-benchmark_results}"
BASELINE_FILE="${3:-}"
REGRESSION_THRESHOLD="${4:-10.0}"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Generate timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_FILE="$OUTPUT_DIR/benchmark_results_$TIMESTAMP.json"
REPORT_FILE="$OUTPUT_DIR/benchmark_report_$TIMESTAMP.md"

echo "=== libvroom Benchmark Suite ==="
echo "Timestamp: $TIMESTAMP"
echo "Results will be saved to: $RESULTS_FILE"
echo "Report will be saved to: $REPORT_FILE"
echo ""

# Check if benchmark executable exists
if [ ! -f "$BENCHMARK_EXECUTABLE" ]; then
    echo "Error: Benchmark executable '$BENCHMARK_EXECUTABLE' not found"
    echo "Please build the project first with: cmake --build . --target libvroom_benchmark"
    exit 1
fi

# System information
echo "=== System Information ==="
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "CPU: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d':' -f2 | xargs 2>/dev/null || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'Unknown')"
echo "Cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'Unknown')"
echo "Memory: $(free -h 2>/dev/null | grep '^Mem:' | awk '{print $2}' || echo 'Unknown')"
echo ""

# Run benchmarks with different configurations
echo "=== Running Benchmarks ==="

# Basic performance benchmarks
echo "Running basic performance benchmarks..."
$BENCHMARK_EXECUTABLE \
    --benchmark_filter="BM_(ParseFile|ParseSimple|ParseManyRows|ParseWideColumns)" \
    --benchmark_format=json \
    --benchmark_out="$RESULTS_FILE.basic" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

# Dimension benchmarks
echo "Running dimension benchmarks..."
$BENCHMARK_EXECUTABLE \
    --benchmark_filter="BM_(FileSizes|ColumnCounts|DataTypes|ThreadScaling|RowScaling)" \
    --benchmark_format=json \
    --benchmark_out="$RESULTS_FILE.dimensions" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

# Real-world benchmarks
echo "Running real-world data benchmarks..."
$BENCHMARK_EXECUTABLE \
    --benchmark_filter="BM_(financial|genomics|taxi|log|wide_table)" \
    --benchmark_format=json \
    --benchmark_out="$RESULTS_FILE.realworld" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

# Performance metrics benchmarks
echo "Running performance metrics benchmarks..."
$BENCHMARK_EXECUTABLE \
    --benchmark_filter="BM_(Cache|Instruction|MemoryBandwidth|BranchPrediction|SIMD)" \
    --benchmark_format=json \
    --benchmark_out="$RESULTS_FILE.metrics" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

# Comparison benchmarks
echo "Running comparison benchmarks..."
$BENCHMARK_EXECUTABLE \
    --benchmark_filter="BM_(libvroom_vs_naive|parsing_approaches|memory_bandwidth)" \
    --benchmark_format=json \
    --benchmark_out="$RESULTS_FILE.comparison" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

# Merge all JSON results
echo "Merging benchmark results..."
python3 -c "
import json
import glob
import sys

# Merge all JSON files
merged_results = {'benchmarks': [], 'context': {}}

# Read all partial result files
for file_pattern in ['$RESULTS_FILE.basic', '$RESULTS_FILE.dimensions', 
                     '$RESULTS_FILE.realworld', '$RESULTS_FILE.metrics', 
                     '$RESULTS_FILE.comparison']:
    try:
        with open(file_pattern, 'r') as f:
            data = json.load(f)
            merged_results['benchmarks'].extend(data.get('benchmarks', []))
            if not merged_results['context'] and 'context' in data:
                merged_results['context'] = data['context']
    except FileNotFoundError:
        print(f'Warning: {file_pattern} not found', file=sys.stderr)

# Write merged results
with open('$RESULTS_FILE', 'w') as f:
    json.dump(merged_results, f, indent=2)

print(f'Merged {len(merged_results[\"benchmarks\"])} benchmarks into $RESULTS_FILE')
"

# Clean up partial files
rm -f "$RESULTS_FILE".{basic,dimensions,realworld,metrics,comparison}

# Generate reports
echo ""
echo "=== Generating Reports ==="

# Check if Python dependencies are available
if ! python3 -c "import json, pandas, matplotlib" 2>/dev/null; then
    echo "Warning: Python dependencies (pandas, matplotlib) not found"
    echo "Install with: pip3 install pandas matplotlib"
    echo "Generating basic report only..."
    
    # Basic report without Python dependencies
    echo "# libvroom Benchmark Results" > "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    echo "Generated: $(date)" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    echo "## Raw Results" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    echo '```json' >> "$REPORT_FILE"
    cat "$RESULTS_FILE" >> "$REPORT_FILE"
    echo '```' >> "$REPORT_FILE"
else
    # Full report with Python script
    if [ -f "benchmark/report_generator.py" ]; then
        python3 benchmark/report_generator.py \
            "$RESULTS_FILE" \
            --output "$REPORT_FILE" \
            $([ -n "$BASELINE_FILE" ] && echo "--baseline $BASELINE_FILE") \
            --regression-threshold "$REGRESSION_THRESHOLD"
    else
        echo "Warning: benchmark/report_generator.py not found, generating basic report"
    fi
fi

# Summary
echo ""
echo "=== Benchmark Summary ==="
total_benchmarks=$(jq '.benchmarks | length' "$RESULTS_FILE" 2>/dev/null || echo "Unknown")
echo "Total benchmarks run: $total_benchmarks"

# Calculate peak throughput
peak_throughput=$(jq -r '.benchmarks[].real_time_other | select(.["GB/s"] != null) | .["GB/s"] | select(. != null)' "$RESULTS_FILE" 2>/dev/null | sort -nr | head -1 || echo "N/A")
echo "Peak throughput: ${peak_throughput} GB/s"

# Check for regressions
if [ -n "$BASELINE_FILE" ] && [ -f "$BASELINE_FILE" ]; then
    echo ""
    echo "=== Regression Detection ==="
    if [ -f "benchmark/report_generator.py" ]; then
        if python3 benchmark/report_generator.py \
            "$RESULTS_FILE" \
            --baseline "$BASELINE_FILE" \
            --regression-threshold "$REGRESSION_THRESHOLD" \
            --fail-on-regression; then
            echo "✅ No performance regressions detected"
        else
            echo "❌ Performance regressions detected! Check regression_report.md"
            exit 1
        fi
    fi
fi

echo ""
echo "=== Results ==="
echo "📊 Full results: $RESULTS_FILE"
echo "📋 Report: $REPORT_FILE"
echo "✅ Benchmark suite completed successfully"