/**
 * @file end_to_end_benchmarks.cpp
 * @brief Benchmarks for end-to-end parse time.
 *
 * This file benchmarks the complete parsing pipeline to measure:
 * 1. BM_ParseOnly - Parse CSV via CsvReader (baseline)
 * 2. BM_CsvToArrow - CSV parse + Table construction (CSV→Arrow)
 * 3. BM_CsvToParquet - CSV to Parquet conversion (CSV→Parquet)
 */

#include "libvroom.h"
#include "libvroom/convert.h"
#include "libvroom/table.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

/**
 * @brief Generate CSV data with specified dimensions.
 *
 * Generates CSV content with random numeric data. Each cell contains a
 * number between 0-9999, which gives realistic field widths (1-4 digits).
 *
 * @param target_size Target size in bytes (approximate)
 * @param cols Number of columns
 * @return Generated CSV content as a string
 */
std::string generate_csv(size_t target_size, size_t cols) {
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> dist(0, 9999);

  std::ostringstream oss;

  // Write header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Estimate bytes per row: ~5 chars per field (avg 2.5 digits + comma/newline)
  size_t header_size = oss.str().size();
  size_t bytes_per_row = cols * 5;
  size_t target_rows =
      (target_size > header_size) ? (target_size - header_size) / bytes_per_row : 1;

  // Generate rows
  for (size_t r = 0; r < target_rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';
      oss << dist(rng);
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
      : filename_("/tmp/libvroom_e2e_" + std::to_string(std::random_device{}()) + ".csv") {
    std::ofstream file(filename_);
    file << content;
    file.close();
  }

  ~TempCSVFile() { std::remove(filename_.c_str()); }

  const std::string& path() const { return filename_; }
};

// Cache generated CSV temp files to avoid regeneration between iterations
struct CachedCSV {
  std::shared_ptr<TempCSVFile> temp_file;
  size_t actual_size;
  size_t cols;
  size_t rows; // Approximate rows
};

std::map<std::pair<size_t, size_t>, CachedCSV> csv_cache;

CachedCSV& get_or_create_csv(size_t target_size, size_t cols) {
  auto key = std::make_pair(target_size, cols);
  auto it = csv_cache.find(key);
  if (it != csv_cache.end()) {
    return it->second;
  }

  // Generate CSV
  std::string csv = generate_csv(target_size, cols);

  // Estimate rows
  size_t approx_rows = csv.size() / (cols * 5);

  CachedCSV cached;
  cached.temp_file = std::make_shared<TempCSVFile>(csv);
  cached.actual_size = csv.size();
  cached.cols = cols;
  cached.rows = approx_rows;

  auto result = csv_cache.emplace(key, std::move(cached));
  return result.first->second;
}

} // anonymous namespace

// ============================================================================
// BM_ParseOnly - Parse CSV via CsvReader (baseline)
// ============================================================================

static void BM_ParseOnly(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::CsvOptions opts;
  opts.num_threads = static_cast<size_t>(n_threads);

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(cached.temp_file->path());
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// ============================================================================
// Benchmark Registration
// ============================================================================

// Test matrix:
// - File sizes: 1MB, 10MB, 100MB
// - Columns: 10, 100
// - Threads: 1, 4

static void CustomArguments(benchmark::internal::Benchmark* b) {
  // {target_size, cols, threads}
  std::vector<int64_t> sizes = {1 * 1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024};
  std::vector<int64_t> cols = {10, 100};
  std::vector<int64_t> threads = {1, 4};

  for (auto size : sizes) {
    for (auto col : cols) {
      for (auto thread : threads) {
        b->Args({size, col, thread});
      }
    }
  }
}

BENCHMARK(BM_ParseOnly)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// BM_CsvToArrow - End-to-end CSV to Arrow Table conversion
// ============================================================================

static void BM_CsvToArrow(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);

  for (auto _ : state) {
    libvroom::CsvOptions opts;
    opts.num_threads = static_cast<size_t>(n_threads);

    libvroom::CsvReader reader(opts);
    reader.open(cached.temp_file->path());
    auto result = reader.read_all();
    if (!result.ok) {
      state.SkipWithError(result.error.c_str());
      return;
    }
    auto table = libvroom::Table::from_parsed_chunks(reader.schema(), std::move(result.value));
    benchmark::DoNotOptimize(table);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

BENCHMARK(BM_CsvToArrow)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// BM_CsvToParquet - End-to-end CSV to Parquet conversion
// ============================================================================

static void BM_CsvToParquet(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  std::string parquet_path = cached.temp_file->path() + ".parquet";

  for (auto _ : state) {
    libvroom::VroomOptions opts;
    opts.input_path = cached.temp_file->path();
    opts.output_path = parquet_path;
    opts.csv.num_threads = static_cast<size_t>(n_threads);

    auto result = libvroom::convert_csv_to_parquet(opts);
    if (!result.ok()) {
      state.SkipWithError(result.error.c_str());
      return;
    }
    benchmark::DoNotOptimize(result);
  }

  std::remove(parquet_path.c_str());

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

BENCHMARK(BM_CsvToParquet)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();
