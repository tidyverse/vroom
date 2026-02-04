#!/bin/bash
#
# Run full benchmark suite comparing vroom vs polars
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$PROJECT_ROOT/bench/data"
RESULTS_DIR="$PROJECT_ROOT/bench/results"

# Activate venv if it exists
if [[ -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# Default settings
ITERATIONS=5
WARMUP=1
SIZES="1000,10000,100000"
TYPES="narrow_numeric,narrow_mixed,string_heavy"
BUILD_TYPE="Release"

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run benchmark suite comparing vroom vs polars.

Options:
    -n, --iterations N    Number of benchmark iterations (default: $ITERATIONS)
    -w, --warmup N        Number of warmup iterations (default: $WARMUP)
    -s, --sizes SIZES     Comma-separated row counts (default: $SIZES)
    -t, --types TYPES     Comma-separated CSV types (default: $TYPES)
    -b, --build TYPE      CMake build type (default: $BUILD_TYPE)
    --skip-build          Skip building vroom
    --skip-generate       Skip generating test data
    --skip-polars         Skip polars benchmarks
    --skip-vroom          Skip vroom benchmarks
    -h, --help            Show this help message

CSV Types:
    narrow_numeric  - 10 columns, numeric data
    narrow_mixed    - 10 columns, mixed types
    wide_numeric    - 100 columns, numeric data
    string_heavy    - 10 columns, mostly strings
    quoted          - Fields with embedded commas/newlines
    nulls           - 10% null values

Example:
    $0 --sizes 10000,100000,1000000 --types narrow_numeric,string_heavy
EOF
}

# Parse arguments
SKIP_BUILD=false
SKIP_GENERATE=false
SKIP_POLARS=false
SKIP_VROOM=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -w|--warmup)
            WARMUP="$2"
            shift 2
            ;;
        -s|--sizes)
            SIZES="$2"
            shift 2
            ;;
        -t|--types)
            TYPES="$2"
            shift 2
            ;;
        -b|--build)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-generate)
            SKIP_GENERATE=true
            shift
            ;;
        --skip-polars)
            SKIP_POLARS=true
            shift
            ;;
        --skip-vroom)
            SKIP_VROOM=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

echo "========================================"
echo "Vroom vs Polars Benchmark Suite"
echo "========================================"
echo "Project root: $PROJECT_ROOT"
echo "Iterations: $ITERATIONS"
echo "Warmup: $WARMUP"
echo "Sizes: $SIZES"
echo "Types: $TYPES"
echo ""

# Step 1: Build vroom
if [[ "$SKIP_BUILD" != "true" ]]; then
    echo "[1/5] Building vroom ($BUILD_TYPE)..."
    BUILD_DIR="$PROJECT_ROOT/build"
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    cmake -DCMAKE_BUILD_TYPE="$BUILD_TYPE" ..
    cmake --build . --target vroom -j$(nproc)

    VROOM_PATH="$BUILD_DIR/vroom"
    echo "Built: $VROOM_PATH"
else
    echo "[1/5] Skipping build..."
    VROOM_PATH="$PROJECT_ROOT/build/vroom"
fi

if [[ ! -x "$VROOM_PATH" ]]; then
    echo "Error: vroom binary not found at $VROOM_PATH"
    exit 1
fi

cd "$PROJECT_ROOT"

# Step 2: Generate test data
if [[ "$SKIP_GENERATE" != "true" ]]; then
    echo ""
    echo "[2/5] Generating test data..."
    mkdir -p "$DATA_DIR"
    python3 "$SCRIPT_DIR/generate_test_data.py" \
        --output-dir "$DATA_DIR" \
        --sizes "$SIZES" \
        --types "$TYPES"
else
    echo ""
    echo "[2/5] Skipping test data generation..."
fi

# Collect CSV files to benchmark
CSV_FILES=$(find "$DATA_DIR" -name "*.csv" -type f | sort)
if [[ -z "$CSV_FILES" ]]; then
    echo "Error: No CSV files found in $DATA_DIR"
    exit 1
fi

echo ""
echo "CSV files to benchmark:"
for f in $CSV_FILES; do
    echo "  - $(basename $f)"
done

# Step 3: Run polars benchmarks
mkdir -p "$RESULTS_DIR"
POLARS_RESULTS="$RESULTS_DIR/polars_results.json"

if [[ "$SKIP_POLARS" != "true" ]]; then
    echo ""
    echo "[3/5] Running polars benchmarks..."

    # Check if polars is installed
    if ! python3 -c "import polars" 2>/dev/null; then
        echo "Warning: polars not installed. Install with: pip install polars"
        echo "Skipping polars benchmarks."
        SKIP_POLARS=true
    else
        python3 "$SCRIPT_DIR/bench_polars.py" \
            --iterations "$ITERATIONS" \
            --warmup "$WARMUP" \
            --output "$POLARS_RESULTS" \
            $CSV_FILES
    fi
else
    echo ""
    echo "[3/5] Skipping polars benchmarks..."
fi

# Step 4: Run vroom benchmarks
VROOM_RESULTS="$RESULTS_DIR/vroom_results.json"

if [[ "$SKIP_VROOM" != "true" ]]; then
    echo ""
    echo "[4/5] Running vroom benchmarks..."
    python3 "$SCRIPT_DIR/bench_vroom.py" \
        --vroom "$VROOM_PATH" \
        --iterations "$ITERATIONS" \
        --warmup "$WARMUP" \
        --output "$VROOM_RESULTS" \
        $CSV_FILES
else
    echo ""
    echo "[4/5] Skipping vroom benchmarks..."
fi

# Step 5: Generate comparison report
echo ""
echo "[5/5] Generating comparison report..."

if [[ -f "$POLARS_RESULTS" && -f "$VROOM_RESULTS" ]]; then
    COMPARISON_RESULTS="$RESULTS_DIR/comparison.json"
    python3 "$SCRIPT_DIR/compare.py" \
        --vroom "$VROOM_RESULTS" \
        --polars "$POLARS_RESULTS" \
        --output "$COMPARISON_RESULTS"
elif [[ -f "$VROOM_RESULTS" ]]; then
    echo ""
    echo "Vroom-only results:"
    cat "$VROOM_RESULTS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f\"vroom version: {data.get('version', 'unknown')}\")
print()
for r in data['results']:
    print(f\"{r['file']}: {r['total_time']['median']*1000:.1f}ms ({r['throughput_mb_per_sec']:.1f} MB/s)\")
"
elif [[ -f "$POLARS_RESULTS" ]]; then
    echo ""
    echo "Polars-only results:"
    cat "$POLARS_RESULTS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f\"polars version: {data.get('version', 'unknown')}\")
print()
for r in data['results']:
    print(f\"{r['file']}: {r['total_time']['median']*1000:.1f}ms ({r['throughput_mb_per_sec']:.1f} MB/s)\")
"
else
    echo "No benchmark results found."
fi

echo ""
echo "========================================"
echo "Benchmark complete!"
echo "Results saved to: $RESULTS_DIR"
echo "========================================"
