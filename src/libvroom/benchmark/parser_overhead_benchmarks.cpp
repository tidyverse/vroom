/**
 * @file parser_overhead_benchmarks.cpp
 * @brief Benchmarks to investigate Parser::parse() overhead vs raw TwoPass operations.
 *
 * This benchmark file was created to investigate GitHub issue #443:
 * "Parser::parse() throughput overhead vs raw TwoPass"
 *
 * The issue identified that Parser::parse() achieves ~170 MB/s while raw
 * TwoPass index building achieves 1.7-4.7 GB/s - a 10-25x difference.
 *
 * These benchmarks decompose Parser::parse() into its constituent operations
 * to identify which steps contribute most to the overhead.
 */

#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <random>
#include <sstream>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

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

  const libvroom::AlignedBuffer& get_buffer(const std::string& name, size_t rows, size_t cols) {
    auto key = name + "_" + std::to_string(rows) + "x" + std::to_string(cols);
    auto it = buffers_.find(key);
    if (it != buffers_.end()) {
      return it->second;
    }

    // Generate and store the buffer
    std::string csv_data = generate_large_csv(rows, cols);
    auto data = static_cast<uint8_t*>(aligned_malloc(64, csv_data.size() + LIBVROOM_PADDING));
    std::memcpy(data, csv_data.data(), csv_data.size());
    // Zero out padding
    std::memset(data + csv_data.size(), 0, LIBVROOM_PADDING);

    libvroom::AlignedBuffer buffer(AlignedPtr(data), csv_data.size());
    auto [inserted_it, success] = buffers_.emplace(key, std::move(buffer));
    return inserted_it->second;
  }

private:
  std::map<std::string, libvroom::AlignedBuffer> buffers_;
};

} // namespace

// ============================================================================
// DECOMPOSED BENCHMARKS - Measure each step of Parser::parse() separately
// ============================================================================

/**
 * @brief Benchmark 1: Raw first_pass_simd only
 *
 * This measures just the initial SIMD scan that counts separators and finds
 * safe split points. This is the pure index-building throughput.
 */
static void BM_RawFirstPass(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);

  for (auto _ : state) {
    auto stats = libvroom::TwoPass::first_pass_simd(buffer.data(), 0, buffer.size, '"', ',');
    benchmark::DoNotOptimize(stats);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_RawFirstPass)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 2: First pass + Index allocation
 *
 * Measures first pass plus the memory allocation for the index structure.
 */
static void BM_FirstPassPlusAllocation(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::TwoPass parser;

  for (auto _ : state) {
    auto stats = libvroom::TwoPass::first_pass_simd(buffer.data(), 0, buffer.size, '"', ',');
    auto idx = parser.init_counted(stats.n_separators, 1);
    benchmark::DoNotOptimize(idx);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_FirstPassPlusAllocation)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 3: First pass + Allocation + Second pass SIMD
 *
 * Measures the complete raw index building pipeline without any higher-level
 * Parser operations. This represents the theoretical maximum throughput.
 */
static void BM_RawTwoPassComplete(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::TwoPass parser;

  for (auto _ : state) {
    // First pass: count separators
    auto stats = libvroom::TwoPass::first_pass_simd(buffer.data(), 0, buffer.size, '"', ',');

    // Allocate index
    auto idx = parser.init_counted(stats.n_separators, 1);

    // Second pass: build index
    auto n_indexes =
        libvroom::TwoPass::second_pass_simd(buffer.data(), 0, buffer.size, &idx, 0, ',', '"');
    idx.n_indexes[0] = n_indexes;

    benchmark::DoNotOptimize(idx);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_RawTwoPassComplete)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 4: Dialect detection only
 *
 * Measures just the dialect detection step, which samples the data
 * to determine delimiter, quote char, and line endings.
 */
static void BM_DialectDetectionOnly(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);

  for (auto _ : state) {
    auto result = libvroom::detect_dialect(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_DialectDetectionOnly)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 5: Parser::parse() with explicit dialect (no detection)
 *
 * Parser::parse() with an explicit dialect should skip detection overhead.
 */
static void BM_ParserWithExplicitDialect(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size, {.dialect = libvroom::Dialect::csv()});
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserWithExplicitDialect)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 6: Parser::parse() with auto-detection (default)
 *
 * Full Parser::parse() with dialect auto-detection enabled.
 */
static void BM_ParserWithAutoDetect(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserWithAutoDetect)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 7: Parser::parse() with explicit dialect + BRANCHLESS algorithm
 *
 * Using the branchless algorithm which should be faster for some patterns.
 */
static void BM_ParserBranchless(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size,
                               libvroom::ParseOptions::branchless(libvroom::Dialect::csv()));
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserBranchless)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 8: Parser::parse() with TWO_PASS algorithm explicitly
 */
static void BM_ParserTwoPassAlgo(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::Parser parser(1);

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::csv();
  opts.algorithm = libvroom::ParseAlgorithm::TWO_PASS;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserTwoPassAlgo)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 9: Parser::parse() with SPECULATIVE algorithm
 */
static void BM_ParserSpeculative(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  libvroom::Parser parser(1);

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::csv();
  opts.algorithm = libvroom::ParseAlgorithm::SPECULATIVE;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserSpeculative)->Unit(benchmark::kMillisecond);

// ============================================================================
// MULTI-THREADED COMPARISONS
// ============================================================================

/**
 * @brief Benchmark 10: Raw TwoPass with multiple threads
 */
static void BM_RawTwoPassMultiThread(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  int n_threads = static_cast<int>(state.range(0));
  libvroom::TwoPass parser;

  for (auto _ : state) {
    auto stats = libvroom::TwoPass::first_pass_simd(buffer.data(), 0, buffer.size, '"', ',');
    auto idx = parser.init_counted(stats.n_separators, n_threads);
    // For multi-threaded, we just call parse_two_pass which handles threading
    parser.parse_two_pass(buffer.data(), idx, buffer.size, libvroom::Dialect::csv());
    benchmark::DoNotOptimize(idx);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_RawTwoPassMultiThread)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 11: Parser::parse() with multiple threads
 */
static void BM_ParserMultiThread(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  int n_threads = static_cast<int>(state.range(0));
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size, {.dialect = libvroom::Dialect::csv()});
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserMultiThread)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 11b: Raw TwoPass with optimized per-thread allocation (issue #591)
 *
 * This benchmark compares the original worst-case allocation (BM_RawTwoPassMultiThread)
 * with the new optimized per-thread allocation. For N separators and T threads:
 * - Original: allocates T * N (each thread gets space for all separators)
 * - Optimized: allocates ~N (each thread gets space for its ~N/T separators)
 */
static void BM_RawTwoPassOptimized(benchmark::State& state) {
  auto& buffer = BenchmarkData::instance().get_buffer("test", kDefaultRows, kDefaultCols);
  int n_threads = static_cast<int>(state.range(0));
  libvroom::TwoPass parser;

  for (auto _ : state) {
    // Use the optimized method that does per-thread allocation
    auto idx =
        parser.parse_optimized(buffer.data(), buffer.size, n_threads, libvroom::Dialect::csv());
    benchmark::DoNotOptimize(idx);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_RawTwoPassOptimized)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// ============================================================================
// FILE SIZE SCALING
// ============================================================================

/**
 * @brief Benchmark 12: Raw TwoPass scaling with file size
 */
static void BM_RawTwoPassScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  auto& buffer = BenchmarkData::instance().get_buffer("scaling", rows, 10);
  libvroom::TwoPass parser;

  for (auto _ : state) {
    auto stats = libvroom::TwoPass::first_pass_simd(buffer.data(), 0, buffer.size, '"', ',');
    auto idx = parser.init_counted(stats.n_separators, 1);
    auto n_indexes =
        libvroom::TwoPass::second_pass_simd(buffer.data(), 0, buffer.size, &idx, 0, ',', '"');
    idx.n_indexes[0] = n_indexes;
    benchmark::DoNotOptimize(idx);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_RawTwoPassScaling)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000)
    ->Arg(500000)
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Benchmark 13: Parser::parse() scaling with file size
 */
static void BM_ParserScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  auto& buffer = BenchmarkData::instance().get_buffer("scaling", rows, 10);
  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size, {.dialect = libvroom::Dialect::csv()});
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["FileSize_MB"] = static_cast<double>(buffer.size) / (1024.0 * 1024.0);
}
BENCHMARK(BM_ParserScaling)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000)
    ->Arg(500000)
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// OVERHEAD BREAKDOWN - Detailed analysis of each overhead component
// ============================================================================

/**
 * @brief Benchmark 14: Measure overhead of Result object creation
 */
static void BM_ResultObjectCreation(benchmark::State& state) {
  for (auto _ : state) {
    libvroom::Parser::Result result;
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK(BM_ResultObjectCreation)->Unit(benchmark::kNanosecond);

/**
 * @brief Benchmark 15: Measure overhead of ParseOptions
 */
static void BM_ParseOptionsCreation(benchmark::State& state) {
  for (auto _ : state) {
    libvroom::ParseOptions opts = libvroom::ParseOptions::defaults();
    benchmark::DoNotOptimize(opts);
  }
}
BENCHMARK(BM_ParseOptionsCreation)->Unit(benchmark::kNanosecond);

/**
 * @brief Benchmark 16: Measure overhead of ErrorCollector
 */
static void BM_ErrorCollectorCreation(benchmark::State& state) {
  for (auto _ : state) {
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    benchmark::DoNotOptimize(errors);
  }
}
BENCHMARK(BM_ErrorCollectorCreation)->Unit(benchmark::kNanosecond);
