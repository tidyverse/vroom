#!/bin/bash
#
# CLI Benchmark: Compare libvroom (vroom) vs zsv with parallel options
#
# This script tests real-world CLI performance with file I/O,
# comparing both single-threaded and multi-threaded modes.
#

set -e

# Configuration
SCSV="./build/vroom"
ZSV="${ZSV:-$HOME/p/zsv/local/bin/zsv}"
TEMP_DIR="/tmp/libvroom_benchmark"
NUM_ITERATIONS=${NUM_ITERATIONS:-5}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check for dependencies
if [ ! -f "$SCSV" ]; then
    echo -e "${RED}Error: vroom not found at $SCSV${NC}"
    echo "Build with: cmake -B build && cmake --build build --target vroom-cli"
    exit 1
fi

if [ ! -f "$ZSV" ]; then
    echo -e "${RED}Error: zsv not found at $ZSV${NC}"
    echo "Set ZSV environment variable or build zsv"
    exit 1
fi

# Create temp directory
mkdir -p "$TEMP_DIR"

echo -e "${BLUE}=== CLI Benchmark: vroom vs zsv ===${NC}"
echo "vroom: $SCSV"
echo "zsv: $ZSV"
echo "Iterations: $NUM_ITERATIONS"
echo ""

# Get CPU info
CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo "unknown")
echo "CPU cores: $CPU_CORES"
echo ""

# Generate test data of various sizes
generate_csv() {
    local size_mb=$1
    local output_file="$TEMP_DIR/test_${size_mb}mb.csv"

    if [ -f "$output_file" ]; then
        echo "Using existing test file: $output_file"
        return
    fi

    echo -e "${YELLOW}Generating ${size_mb}MB test file...${NC}"

    # Calculate approximate number of rows needed
    # Each row is approximately 100 bytes
    local target_bytes=$((size_mb * 1024 * 1024))
    local rows=$((target_bytes / 100))

    # Header
    echo "id,name,email,age,city,country,salary,department,hire_date,active" > "$output_file"

    # Generate data
    local i=0
    while [ $(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null) -lt $target_bytes ]; do
        for j in {1..1000}; do
            echo "$i,User$i,user$i@example.com,$((20 + i % 50)),City$((i % 100)),Country$((i % 50)),$((30000 + i % 100000)),Dept$((i % 10)),2020-01-$((1 + i % 28)),$([ $((i % 2)) -eq 0 ] && echo true || echo false)"
            ((i++))
        done
    done >> "$output_file"

    local actual_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null)
    echo "Generated: $output_file ($(numfmt --to=iec $actual_size 2>/dev/null || echo "${actual_size} bytes"))"
}

# Run a single benchmark and return average time in milliseconds
run_benchmark() {
    local cmd="$1"
    local iterations=$NUM_ITERATIONS
    local total_time=0

    for i in $(seq 1 $iterations); do
        # Use /usr/bin/time for accurate timing (macOS)
        local start=$(python3 -c "import time; print(int(time.time() * 1000))")
        eval "$cmd" > /dev/null 2>&1
        local end=$(python3 -c "import time; print(int(time.time() * 1000))")
        local elapsed=$((end - start))
        total_time=$((total_time + elapsed))
    done

    echo $((total_time / iterations))
}

# Run benchmark suite for a given file size
benchmark_file() {
    local size_mb=$1
    local test_file="$TEMP_DIR/test_${size_mb}mb.csv"
    local file_size=$(stat -f%z "$test_file" 2>/dev/null || stat -c%s "$test_file" 2>/dev/null)

    echo ""
    echo -e "${GREEN}=== Benchmarking ${size_mb}MB file (${file_size} bytes) ===${NC}"
    echo ""

    echo "| Tool | Threads | Time (ms) | Throughput (MB/s) |"
    echo "|------|---------|-----------|-------------------|"

    # zsv single-threaded
    local zsv_1t=$(run_benchmark "$ZSV count $test_file")
    local zsv_1t_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($zsv_1t / 1000)" | bc)
    echo "| zsv | 1 | $zsv_1t | $zsv_1t_throughput |"

    # zsv with --parallel (auto-detect cores)
    local zsv_parallel=$(run_benchmark "$ZSV count --parallel $test_file")
    local zsv_parallel_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($zsv_parallel / 1000)" | bc)
    echo "| zsv | --parallel | $zsv_parallel | $zsv_parallel_throughput |"

    # zsv with specific thread counts
    for threads in 2 4 8; do
        if [ "$threads" -le "$CPU_CORES" ]; then
            local zsv_nt=$(run_benchmark "$ZSV count -j $threads $test_file")
            local zsv_nt_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($zsv_nt / 1000)" | bc)
            echo "| zsv | $threads | $zsv_nt | $zsv_nt_throughput |"
        fi
    done

    # vroom single-threaded
    local vroom_1t=$(run_benchmark "$SCSV count -t 1 $test_file")
    local vroom_1t_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($vroom_1t / 1000)" | bc)
    echo "| vroom | 1 | $vroom_1t | $vroom_1t_throughput |"

    # vroom with auto-detect (default mode, no -t flag)
    local vroom_auto=$(run_benchmark "$SCSV count $test_file")
    local vroom_auto_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($vroom_auto / 1000)" | bc)
    echo "| vroom | auto | $vroom_auto | $vroom_auto_throughput |"

    # vroom with specific thread counts
    for threads in 2 4 8; do
        if [ "$threads" -le "$CPU_CORES" ]; then
            local vroom_nt=$(run_benchmark "$SCSV count -t $threads $test_file")
            local vroom_nt_throughput=$(echo "scale=2; $file_size / 1024 / 1024 / ($vroom_nt / 1000)" | bc)
            echo "| vroom | $threads | $vroom_nt | $vroom_nt_throughput |"
        fi
    done

    echo ""
    echo "Analysis:"
    echo "  zsv single -> parallel speedup: $(echo "scale=2; $zsv_1t / $zsv_parallel" | bc)x"
    echo "  vroom single -> auto speedup: $(echo "scale=2; $vroom_1t / $vroom_auto" | bc)x"
    echo "  vroom (1t) vs zsv (1t): $(echo "scale=2; $zsv_1t / $vroom_1t" | bc)x (>1 means vroom faster)"
    echo "  vroom (auto) vs zsv (parallel): $(echo "scale=2; $zsv_parallel / $vroom_auto" | bc)x (>1 means vroom faster)"

    # Store results for final summary
    echo "$size_mb,$zsv_1t,$zsv_parallel,$vroom_1t,$vroom_auto" >> "$TEMP_DIR/results.csv"
}

# Main benchmark
echo -e "${YELLOW}Generating test files...${NC}"
echo ""

# Generate test files of different sizes
for size in 10 50 100; do
    generate_csv $size
done

echo ""
echo -e "${BLUE}Running benchmarks...${NC}"

# Initialize results file
echo "size_mb,zsv_1t,zsv_parallel,vroom_1t,vroom_auto" > "$TEMP_DIR/results.csv"

# Run benchmarks for each size
for size in 10 50 100; do
    benchmark_file $size
done

echo ""
echo -e "${BLUE}=== Summary ===${NC}"
echo ""
cat "$TEMP_DIR/results.csv" | column -t -s,

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
echo "Test files stored in: $TEMP_DIR"
echo ""
echo "To clean up: rm -rf $TEMP_DIR"
