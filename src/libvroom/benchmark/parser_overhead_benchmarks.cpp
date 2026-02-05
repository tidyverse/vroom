/**
 * @file parser_overhead_benchmarks.cpp
 * @brief Benchmarks for CSV parsing performance and regression detection.
 *
 * These benchmarks are used by the CI performance regression workflow
 * (.github/workflows/benchmark.yml) to detect regressions between commits.
 *
 * The workflow runs a subset of these benchmarks with strict thresholds:
 * - BM_CountRows: SIMD row counting throughput
 * - BM_SplitFields: SIMD field splitting throughput
 * - BM_CsvReaderExplicit: Full CsvReader pipeline with explicit dialect
 * - BM_CsvReaderMultiThread/N: Multi-threaded CsvReader scaling
 */

#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>

namespace {

// Default dataset size for benchmarks
// These parameters are chosen to give stable benchmark times (>10ms per iteration)
// for reliable regression detection in CI. The generated data is ~32MB.
// See GitHub issue #508 for context on why smaller sizes give unstable results.
constexpr size_t kDefaultRows = 500000; // 500K rows
constexpr size_t kDefaultCols = 10;

// Generate a large CSV file for benchmarking
std::string generate_large_csv(size_t rows, size_t cols) {
  std::ostringstream ss;
  std::mt19937 gen(42); // Fixed seed for reproducibility

  // Header
  for (size_t col = 0; col < cols; ++col) {
    if (col > 0)
      ss << ",";
    ss << "col_" << col;
  }
  ss << "\n";

  // Data rows
  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      if (col > 0)
        ss << ",";
      // Mix of integers, floats, and short strings
      switch ((row + col) % 4) {
      case 0:
        ss << gen() % 10000;
        break;
      case 1:
        ss << (gen() % 10000) / 100.0;
        break;
      case 2:
        ss << "value" << (gen() % 1000);
        break;
      case 3:
        ss << gen() % 1000000;
        break;
      }
    }
    ss << "\n";
  }

  return ss.str();
}

// Helper class to manage test data for these benchmarks
class BenchmarkData {
public:
  static BenchmarkData& instance() {
    static BenchmarkData data;
    return data;
  }

  struct DataSet {
    libvroom::AlignedBuffer buffer;
    std::string temp_path;
  };

  const DataSet& get(const std::string& name, size_t rows, size_t cols) {
    auto key = name + "_" + std::to_string(rows) + "x" + std::to_string(cols);
    auto it = datasets_.find(key);
    if (it != datasets_.end()) {
      return it->second;
    }

    // Generate CSV data
    std::string csv_data = generate_large_csv(rows, cols);

    // Create aligned buffer
    auto buffer = libvroom::AlignedBuffer::allocate(csv_data.size());
    std::memcpy(buffer.data(), csv_data.data(), csv_data.size());

    // Write to temp file for CsvReader benchmarks
    std::string temp_path = "/tmp/libvroom_bench_" + key + ".csv";
    std::ofstream out(temp_path);
    out << csv_data;
    out.close();

    auto [inserted_it, success] =
        datasets_.emplace(key, DataSet{std::move(buffer), std::move(temp_path)});
    return inserted_it->second;
  }

  ~BenchmarkData() {
    for (auto& [key, ds] : datasets_) {
      if (!ds.temp_path.empty()) {
        std::remove(ds.temp_path.c_str());
      }
    }
  }

private:
  BenchmarkData() = default;
  std::map<std::string, DataSet> datasets_;
};

} // namespace

// ============================================================================
// LOW-LEVEL SIMD BENCHMARKS - Measure public SIMD operations
// ============================================================================

/**
 * @brief Benchmark: SIMD row counting
 *
 * Measures the throughput of count_rows_simd(), which scans the buffer
 * for newlines while tracking quote state. This is the core first-pass
 * operation for determining file structure.
 */
static void BM_CountRows(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);

  for (auto _ : state) {
    auto [row_count, last_row_end] = libvroom::count_rows_simd(
        reinterpret_cast<const char*>(ds.buffer.data()), ds.buffer.size());
    benchmark::DoNotOptimize(row_count);
    benchmark::DoNotOptimize(last_row_end);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CountRows)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark: SIMD field splitting
 *
 * Measures the throughput of split_fields_simd() on each row.
 * This is the core second-pass operation that identifies field boundaries.
 */
static void BM_SplitFields(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);
  const char* data = reinterpret_cast<const char*>(ds.buffer.data());
  size_t size = ds.buffer.size();

  // Find the first data line (skip header)
  size_t start = 0;
  while (start < size && data[start] != '\n')
    ++start;
  ++start; // skip newline

  // Find a representative line
  size_t line_end = start;
  while (line_end < size && data[line_end] != '\n')
    ++line_end;
  size_t line_len = line_end - start;

  for (auto _ : state) {
    auto fields = libvroom::split_fields_simd(data + start, line_len);
    benchmark::DoNotOptimize(fields);
  }

  state.SetBytesProcessed(static_cast<int64_t>(line_len * state.iterations()));
  state.counters["LineLength"] = static_cast<double>(line_len);
}
BENCHMARK(BM_SplitFields)->Unit(benchmark::kMicrosecond);

/**
 * @brief Benchmark: Dual-state chunk analysis
 *
 * Measures analyze_chunk_dual_state_simd() which computes row stats
 * for both starting-inside and starting-outside quote states in one pass.
 */
static void BM_DualStateAnalysis(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);

  for (auto _ : state) {
    auto stats = libvroom::analyze_chunk_dual_state_simd(
        reinterpret_cast<const char*>(ds.buffer.data()), ds.buffer.size());
    benchmark::DoNotOptimize(stats);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_DualStateAnalysis)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark: Dialect detection only
 *
 * Measures just the dialect detection step, which samples the data
 * to determine delimiter, quote char, and line endings.
 */
static void BM_DialectDetection(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);

  libvroom::DialectDetector detector;

  for (auto _ : state) {
    auto result = detector.detect(ds.buffer.data(), ds.buffer.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_DialectDetection)->Unit(benchmark::kMillisecond);

// ============================================================================
// CSVREADER BENCHMARKS - Full parsing pipeline
// ============================================================================

/**
 * @brief Benchmark: CsvReader with explicit dialect (no detection)
 *
 * Measures full CsvReader pipeline with a known dialect, skipping detection.
 */
static void BM_CsvReaderExplicit(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);

  libvroom::CsvOptions opts;
  opts.separator = ',';
  opts.quote = '"';
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(ds.temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CsvReaderExplicit)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark: CsvReader with auto-detection (default)
 *
 * Full CsvReader pipeline with dialect auto-detection enabled.
 */
static void BM_CsvReaderAutoDetect(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);

  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(ds.temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CsvReaderAutoDetect)->Unit(benchmark::kMillisecond);

// ============================================================================
// MULTI-THREADED COMPARISONS
// ============================================================================

/**
 * @brief Benchmark: CsvReader with multiple threads
 *
 * Critical for detecting issue #591-type regressions where multi-threaded
 * parsing gets slower with more threads.
 */
static void BM_CsvReaderMultiThread(benchmark::State& state) {
  auto& ds = BenchmarkData::instance().get("test", kDefaultRows, kDefaultCols);
  int n_threads = static_cast<int>(state.range(0));

  libvroom::CsvOptions opts;
  opts.separator = ',';
  opts.quote = '"';
  opts.num_threads = static_cast<size_t>(n_threads);

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(ds.temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CsvReaderMultiThread)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// ============================================================================
// FILE SIZE SCALING
// ============================================================================

/**
 * @brief Benchmark: Row counting scaling with file size
 */
static void BM_CountRowsScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  auto& ds = BenchmarkData::instance().get("scaling", rows, 10);

  for (auto _ : state) {
    auto [row_count, last_row_end] = libvroom::count_rows_simd(
        reinterpret_cast<const char*>(ds.buffer.data()), ds.buffer.size());
    benchmark::DoNotOptimize(row_count);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CountRowsScaling)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000)
    ->Arg(500000)
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark: CsvReader scaling with file size
 */
static void BM_CsvReaderScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  auto& ds = BenchmarkData::instance().get("scaling", rows, 10);

  libvroom::CsvOptions opts;
  opts.separator = ',';
  opts.quote = '"';
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(ds.temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(ds.buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["FileSize_MB"] = static_cast<double>(ds.buffer.size()) / (1024.0 * 1024.0);
}
BENCHMARK(BM_CsvReaderScaling)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000)
    ->Arg(500000)
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// OVERHEAD BREAKDOWN - Detailed analysis of each overhead component
// ============================================================================

/**
 * @brief Benchmark: Measure overhead of CsvOptions creation
 */
static void BM_CsvOptionsCreation(benchmark::State& state) {
  for (auto _ : state) {
    libvroom::CsvOptions opts;
    benchmark::DoNotOptimize(opts);
  }
}
BENCHMARK(BM_CsvOptionsCreation)->Unit(benchmark::kNanosecond);

/**
 * @brief Benchmark: Measure overhead of ErrorCollector
 */
static void BM_ErrorCollectorCreation(benchmark::State& state) {
  for (auto _ : state) {
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    benchmark::DoNotOptimize(errors);
  }
}
BENCHMARK(BM_ErrorCollectorCreation)->Unit(benchmark::kNanosecond);
