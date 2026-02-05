/**
 * @file hypothesis_benchmarks.cpp
 * @brief Discriminatory benchmarks for hypothesis-driven optimization decisions.
 *
 * This file implements benchmarks to test the key hypotheses from Issue #611:
 *
 * H3: Synchronization barriers dominate multi-threaded scaling
 *
 * Benchmarks that required internal Parser/TwoPass/ParseIndex/ValueExtractor
 * APIs (H1, H2 field-access, H4, H5, H6, H7) have been removed since the
 * v2 CsvReader API does not expose those internals.
 *
 * Retained benchmarks use the CsvReader public API to measure thread scaling
 * and end-to-end parse throughput.
 */

#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

// ============================================================================
// CSV Generation Utilities
// ============================================================================

/**
 * @brief Generate CSV data with specified dimensions.
 *
 * @param target_rows Target number of rows
 * @param cols Number of columns
 * @param type_pattern Pattern for column types: 'i'=int, 'd'=double, 's'=string
 * @return Generated CSV content as a string
 */
std::string generate_csv(size_t target_rows, size_t cols, const std::string& type_pattern = "") {
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> int_dist(0, 99999);
  std::uniform_real_distribution<double> dbl_dist(-1000.0, 1000.0);

  // Generate type pattern if not provided
  std::string types = type_pattern;
  if (types.empty()) {
    // Default: alternating int, double, string
    for (size_t i = 0; i < cols; ++i) {
      switch (i % 3) {
      case 0:
        types += 'i';
        break;
      case 1:
        types += 'd';
        break;
      case 2:
        types += 's';
        break;
      }
    }
  }
  // Extend or truncate to match cols
  while (types.size() < cols)
    types += types;
  types.resize(cols);

  std::ostringstream oss;

  // Write header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Random strings pool
  std::vector<std::string> str_pool;
  for (int i = 0; i < 100; ++i) {
    str_pool.push_back("str" + std::to_string(i) + "_value");
  }

  // Generate rows
  for (size_t r = 0; r < target_rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';
      switch (types[c]) {
      case 'i':
        oss << int_dist(rng);
        break;
      case 'd':
        oss << std::fixed << std::setprecision(4) << dbl_dist(rng);
        break;
      case 's':
        oss << str_pool[rng() % str_pool.size()];
        break;
      default:
        oss << int_dist(rng);
        break;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// Helper for temporary files
class TempCSVFile {
private:
  std::string filename_;

public:
  explicit TempCSVFile(const std::string& content)
      : filename_("/tmp/libvroom_hyp_" + std::to_string(std::random_device{}()) + ".csv") {
    std::ofstream file(filename_);
    file << content;
    file.close();
  }

  ~TempCSVFile() { std::remove(filename_.c_str()); }

  const std::string& path() const { return filename_; }
};

// CSV cache for repeated benchmarks
struct CachedCSV {
  std::shared_ptr<TempCSVFile> temp_file;
  size_t actual_size;
  size_t rows;
  size_t cols;
};

std::map<std::tuple<size_t, size_t, std::string>, CachedCSV> csv_cache;

CachedCSV& get_or_create_csv(size_t rows, size_t cols, const std::string& type_pattern = "") {
  auto key = std::make_tuple(rows, cols, type_pattern);
  auto it = csv_cache.find(key);
  if (it != csv_cache.end()) {
    return it->second;
  }

  // Generate CSV
  std::string csv = generate_csv(rows, cols, type_pattern);

  CachedCSV cached;
  cached.temp_file = std::make_shared<TempCSVFile>(csv);
  cached.actual_size = csv.size();
  cached.rows = rows;
  cached.cols = cols;

  auto result = csv_cache.emplace(key, std::move(cached));
  return result.first->second;
}

} // anonymous namespace

// ============================================================================
// H3: Synchronization Barrier Overhead
// ============================================================================

/**
 * @brief Benchmark parse time at varying thread counts.
 *
 * Measures scaling efficiency to identify barrier overhead.
 */
static void BM_H3_ThreadScaling(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));
  size_t target_size = static_cast<size_t>(state.range(1));

  // Generate a file of approximately target_size
  size_t cols = 20;
  size_t approx_rows = target_size / (cols * 10); // ~10 bytes per field
  auto& cached = get_or_create_csv(approx_rows, cols);

  libvroom::CsvOptions opts;
  opts.num_threads = static_cast<size_t>(n_threads);

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(cached.temp_file->path());
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
}

// H3 registration - thread counts: 1, 2, 4, 8, 16
static void H3_Arguments(benchmark::internal::Benchmark* b) {
  // Thread counts x target file sizes
  std::vector<int64_t> threads = {1, 2, 4, 8, 16};
  std::vector<int64_t> sizes = {10 * 1024 * 1024, 100 * 1024 * 1024}; // 10MB, 100MB

  for (auto size : sizes) {
    for (auto thread : threads) {
      b->Args({thread, size});
    }
  }
}

BENCHMARK(BM_H3_ThreadScaling)->Apply(H3_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// H2: Parse Throughput at Different Sizes (simplified)
// ============================================================================

/**
 * @brief Baseline: Parse only via CsvReader, no internal access.
 *
 * Measures the raw end-to-end parsing throughput at various data sizes.
 */
static void BM_H2_ParseOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(cached.temp_file->path());
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

// H2 registration
static void H2_Arguments(benchmark::internal::Benchmark* b) {
  // {rows, cols}
  b->Args({10000, 10});
  b->Args({100000, 10});
  b->Args({1000000, 10});
  b->Args({100000, 100});
}

BENCHMARK(BM_H2_ParseOnly)->Apply(H2_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();
