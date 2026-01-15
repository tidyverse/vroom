/**
 * @file arrow_convert_benchmarks.cpp
 * @brief Benchmarks for Arrow conversion and columnar export functionality.
 *
 * These benchmarks measure performance of:
 * - CSV to Arrow table conversion
 * - Arrow table to Feather/Parquet export
 * - End-to-end CSV to columnar format conversion
 *
 * Only compiled when LIBVROOM_ENABLE_ARROW is defined.
 */

#ifdef LIBVROOM_ENABLE_ARROW

#include "libvroom.h"

#include "arrow_output.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <string>

namespace {

// Generate synthetic CSV data for benchmarking
std::string generate_csv_data(size_t num_rows, size_t num_cols) {
  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < num_cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows with mixed types
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> int_dist(0, 1000000);
  std::uniform_real_distribution<double> dbl_dist(0.0, 1000.0);

  for (size_t r = 0; r < num_rows; ++r) {
    for (size_t c = 0; c < num_cols; ++c) {
      if (c > 0)
        oss << ',';
      switch (c % 4) {
      case 0: // Integer column
        oss << int_dist(rng);
        break;
      case 1: // Double column
        oss << std::fixed << std::setprecision(2) << dbl_dist(rng);
        break;
      case 2: // String column
        oss << "value_" << r << "_" << c;
        break;
      case 3: // Boolean column
        oss << (rng() % 2 == 0 ? "true" : "false");
        break;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// Helper to create a buffer from string data
struct BenchmarkBuffer {
  uint8_t* data;
  size_t len;

  explicit BenchmarkBuffer(const std::string& content) {
    len = content.size();
    data = libvroom::allocate_padded_buffer(len, 64);
    std::memcpy(data, content.data(), len);
  }

  ~BenchmarkBuffer() {
    if (data)
      libvroom::aligned_free(data);
  }

  BenchmarkBuffer(const BenchmarkBuffer&) = delete;
  BenchmarkBuffer& operator=(const BenchmarkBuffer&) = delete;
};

} // namespace

// Benchmark CSV to Arrow table conversion
static void BM_CSVToArrowTable(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, num_cols);
  BenchmarkBuffer buffer(csv_data);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;

  for (auto _ : state) {
    libvroom::two_pass parser;
    libvroom::index idx = parser.init(buffer.len, 1);
    parser.parse(buffer.data, idx, buffer.len);

    libvroom::ArrowConverter converter(opts);
    auto result = converter.convert(buffer.data, buffer.len, idx);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
  state.counters["DataSize"] = static_cast<double>(buffer.len);
}

BENCHMARK(BM_CSVToArrowTable)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({10000, 50})
    ->Args({10000, 100})
    ->Unit(benchmark::kMillisecond);

// Benchmark Arrow table to Feather export
static void BM_ArrowToFeather(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, num_cols);
  BenchmarkBuffer buffer(csv_data);

  // Parse and convert once outside the benchmark loop
  libvroom::two_pass parser;
  libvroom::index idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;
  libvroom::ArrowConverter converter(opts);
  auto convert_result = converter.convert(buffer.data, buffer.len, idx);

  if (!convert_result.ok()) {
    state.SkipWithError(convert_result.error_message.c_str());
    return;
  }

  std::string tmp_path = "/tmp/benchmark_output.feather";

  for (auto _ : state) {
    auto result = libvroom::write_feather(convert_result.table, tmp_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(tmp_path);

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
}

BENCHMARK(BM_ArrowToFeather)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({10000, 50})
    ->Unit(benchmark::kMillisecond);

#ifdef LIBVROOM_ENABLE_PARQUET
// Benchmark Arrow table to Parquet export with different compression
static void BM_ArrowToParquet(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto compression = static_cast<libvroom::ParquetWriteOptions::Compression>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, 10);
  BenchmarkBuffer buffer(csv_data);

  // Parse and convert once outside the benchmark loop
  libvroom::two_pass parser;
  libvroom::index idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;
  libvroom::ArrowConverter converter(opts);
  auto convert_result = converter.convert(buffer.data, buffer.len, idx);

  if (!convert_result.ok()) {
    state.SkipWithError(convert_result.error_message.c_str());
    return;
  }

  std::string tmp_path = "/tmp/benchmark_output.parquet";
  libvroom::ParquetWriteOptions parquet_opts;
  parquet_opts.compression = compression;

  for (auto _ : state) {
    auto result = libvroom::write_parquet(convert_result.table, tmp_path, parquet_opts);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(tmp_path);

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);

  // Label compression type
  const char* compression_names[] = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4"};
  state.SetLabel(compression_names[static_cast<int>(compression)]);
}

BENCHMARK(BM_ArrowToParquet)
    ->Args({10000, 0})  // UNCOMPRESSED
    ->Args({10000, 1})  // SNAPPY
    ->Args({10000, 2})  // GZIP
    ->Args({10000, 3})  // ZSTD
    ->Args({10000, 4})  // LZ4
    ->Args({100000, 1}) // SNAPPY with more rows
    ->Unit(benchmark::kMillisecond);
#endif // LIBVROOM_ENABLE_PARQUET

// End-to-end benchmark: CSV file to Feather file
static void BM_CSVToFeatherEndToEnd(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));

  std::string csv_data = generate_csv_data(num_rows, 10);

  // Write CSV to temp file
  std::string csv_path = "/tmp/benchmark_input.csv";
  std::string feather_path = "/tmp/benchmark_output.feather";
  {
    std::ofstream out(csv_path);
    out << csv_data;
  }

  for (auto _ : state) {
    auto result = libvroom::csv_to_feather(csv_path, feather_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(csv_path);
  std::filesystem::remove(feather_path);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
}

BENCHMARK(BM_CSVToFeatherEndToEnd)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);

#ifdef LIBVROOM_ENABLE_PARQUET
// End-to-end benchmark: CSV file to Parquet file
static void BM_CSVToParquetEndToEnd(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));

  std::string csv_data = generate_csv_data(num_rows, 10);

  // Write CSV to temp file
  std::string csv_path = "/tmp/benchmark_input.csv";
  std::string parquet_path = "/tmp/benchmark_output.parquet";
  {
    std::ofstream out(csv_path);
    out << csv_data;
  }

  for (auto _ : state) {
    auto result = libvroom::csv_to_parquet(csv_path, parquet_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(csv_path);
  std::filesystem::remove(parquet_path);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
}

BENCHMARK(BM_CSVToParquetEndToEnd)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);
#endif // LIBVROOM_ENABLE_PARQUET

#endif // LIBVROOM_ENABLE_ARROW
