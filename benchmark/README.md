# libvroom Enhanced Benchmark Suite

This directory contains a comprehensive benchmark suite for libvroom, implementing the enhanced benchmarking framework outlined in the Production Readiness Plan (Section 6).

## Overview

The benchmark suite provides:
- **Comprehensive dimensions testing** (file sizes, column counts, data types, thread counts)
- **Real-world dataset benchmarks** (financial, genomics, NYC taxi, log data)
- **Performance metrics** (throughput, efficiency, scalability, cache performance)
- **SIMD effectiveness analysis** (vector width, quote detection, memory access patterns)
- **Energy efficiency measurements** (Linux RAPL when available)
- **Automated reporting and regression detection**

## Benchmark Categories

### 1. Basic Benchmarks (`basic_benchmarks.cpp`)
- File parsing with different thread counts
- Quoted field handling
- Different separator types
- Memory allocation and index creation performance

### 2. Dimension Benchmarks (`dimensions_benchmarks.cpp`)
- **File Sizes**: 1KB to 100MB testing memory hierarchy effects
- **Column Counts**: 2 to 500 columns testing field parsing scalability
- **Data Types**: integers, floats, strings, mixed data testing type-specific performance
- **Thread Scaling**: 1-16 threads with efficiency metrics
- **Row Scaling**: 100 to 1M rows testing record processing
- **Matrix Testing**: Comprehensive rows × columns combinations

### 3. Real-World Benchmarks (`real_world_benchmarks.cpp`)
- **NYC Taxi Data**: Realistic 19-column dataset (1K-1M rows)
- **Financial Data**: Time-series financial data (timestamps, OHLCV)
- **Genomics Data**: Biological sequence data with quality scores
- **Log Data**: Server log files with structured fields
- **Wide Tables**: Tables with 100+ columns

### 4. Performance Metrics (`performance_metrics.cpp`)
- **Cache Performance**: L1/L2/L3/memory testing
- **Instruction Efficiency**: Instructions per byte analysis
- **Thread Scaling Efficiency**: Speedup and efficiency calculations
- **Memory Bandwidth**: Bandwidth utilization measurement
- **Branch Prediction**: Impact of predictable vs random patterns
- **SIMD Utilization**: Memory alignment effects on vectorization

### 5. SIMD Benchmarks (`simd_benchmarks.cpp`)
- **SIMD vs Scalar**: Performance comparison for different CSV patterns
- **Vector Width Effectiveness**: 128-bit (SSE/NEON) vs 256-bit (AVX2) vs 512-bit (AVX-512)
- **Quote Detection**: SIMD effectiveness with varying quote density
- **Separator Detection**: Performance with different separators (comma, tab, etc.)
- **Memory Access Patterns**: Sequential vs strided vs random access

### 6. Energy Benchmarks (`energy_benchmarks.cpp`) *(Optional)*
- **Energy per Byte**: Energy consumption scaling with file size
- **Thread Energy Efficiency**: Energy consumption vs thread count
- **Power Estimation**: CPU power consumption estimates
- **Thermal Impact**: Sustained workload performance
- **Linux RAPL Integration**: Hardware energy counters when available

### 7. Comparison Benchmarks (`comparison_benchmarks.cpp`)
- **libvroom vs Naive Parser**: Performance comparison
- **Multiple Parsing Approaches**: Different algorithm comparisons
- **Memory Bandwidth**: Raw memory throughput baseline

## Usage

### Building
```bash
mkdir build && cd build
cmake ..
make libvroom_benchmark
```

### Running All Benchmarks
```bash
./benchmark/run_benchmarks.sh ./build/libvroom_benchmark
```

### Running Specific Categories
```bash
# Basic benchmarks only
./libvroom_benchmark --benchmark_filter="BM_Parse.*"

# Dimension benchmarks only
./libvroom_benchmark --benchmark_filter="BM_(FileSizes|ColumnCounts|DataTypes)"

# Real-world benchmarks only
./libvroom_benchmark --benchmark_filter="BM_(financial|genomics|taxi|log)"

# SIMD benchmarks only
./libvroom_benchmark --benchmark_filter="BM_.*SIMD.*"
```

### Generating Reports
```bash
# Run with JSON output
./libvroom_benchmark --benchmark_format=json --benchmark_out=results.json

# Generate HTML report
python3 benchmark/report_generator.py results.json --output=report.md
```

### Regression Detection
```bash
# Compare against baseline
python3 benchmark/report_generator.py current.json \
  --baseline baseline.json \
  --regression-threshold 10.0 \
  --fail-on-regression
```

## Automated Benchmark Script

The `run_benchmarks.sh` script provides a complete automated benchmarking solution:

```bash
# Basic usage
./benchmark/run_benchmarks.sh [executable] [output_dir] [baseline_file] [threshold]

# Examples
./benchmark/run_benchmarks.sh ./build/libvroom_benchmark
./benchmark/run_benchmarks.sh ./build/libvroom_benchmark results/ baseline.json 5.0
```

Features:
- System information collection
- Structured benchmark execution
- JSON result aggregation
- Automated report generation
- Regression detection with CI integration
- Error handling and cleanup

## Performance Metrics

### Throughput Metrics
- **GB/s**: Primary throughput metric (Gigabytes per second)
- **MB/s**: Megabytes per second for smaller datasets
- **Records/s**: Records processed per second
- **Fields/s**: Individual fields processed per second

### Efficiency Metrics
- **Scaling Efficiency**: Thread scaling efficiency percentage
- **Speedup**: Parallel speedup ratio
- **Cache Efficiency**: L1/L2/L3 cache performance indicators
- **Bandwidth Utilization**: Memory bandwidth utilization percentage

### Energy Metrics *(Linux only)*
- **Energy per GB**: Joules consumed per GB processed
- **GB per Watt**: Performance per watt (efficiency)
- **Package/Core/DRAM Energy**: Component-specific energy consumption

## CI Integration

For continuous integration, use:

```bash
# In CI pipeline
./benchmark/run_benchmarks.sh ./build/libvroom_benchmark ci_results/ baseline.json 10.0
if [ $? -ne 0 ]; then
  echo "Performance regression detected!"
  exit 1
fi
```

## Expected Performance Targets

Based on the Production Readiness Plan:

### Target Performance (Phase 2-4)
- **Peak Throughput**: >5 GB/s on modern x86-64 (AVX2)
- **AVX-512**: >8 GB/s on AVX-512 capable systems
- **Thread Scaling**: >80% efficiency up to 16 threads
- **Memory Overhead**: <10% over file size

### Quality Metrics
- **Regression Threshold**: <10% performance degradation
- **Stability**: >99% success rate across all benchmarks
- **Platform Coverage**: x86-64 (AVX2, AVX-512), ARM64 (NEON)

## Platform-Specific Notes

### Linux
- RAPL energy measurements available on Intel CPUs
- Full performance counter access
- Recommended for comprehensive benchmarking

### macOS
- Energy measurements via power estimates
- Apple Silicon ARM64 testing supported
- Instruments integration for detailed profiling

### Windows
- Performance counter integration planned
- MSVC compatibility for benchmark suite

## Troubleshooting

### Common Issues

1. **Missing Dependencies**:
   ```bash
   pip3 install pandas matplotlib  # For report generation
   ```

2. **Permission Issues** (Linux RAPL):
   ```bash
   sudo chmod +r /sys/class/powercap/intel-rapl/*/energy_uj
   ```

3. **Memory Issues**:
   - Large benchmarks may require >8GB RAM
   - Reduce test sizes if needed

4. **Build Issues**:
   - Ensure Google Benchmark is properly fetched
   - Check C++17 compiler support

### Performance Analysis

For detailed performance analysis:

1. **Profiling**: Use `perf` (Linux) or Instruments (macOS)
2. **Memory**: Check with AddressSanitizer or Valgrind
3. **SIMD**: Verify with `perf stat -M simd` or similar tools

## Contributing

When adding new benchmarks:

1. Follow existing naming convention (`BM_CategoryName_SpecificTest`)
2. Add appropriate counters for analysis
3. Include error handling for robustness
4. Update this documentation
5. Test on multiple platforms if possible

## References

- [Google Benchmark User Guide](https://github.com/google/benchmark/blob/main/docs/user_guide.md)
- [libvroom Production Readiness Plan](../PRODUCTION_READINESS_PLAN.md#6-enhanced-benchmark-suite)
- [Intel RAPL Documentation](https://intel.com/content/www/us/en/developer/articles/technical/software-security-guidance/advisory-guidance/running-average-power-limit.html)