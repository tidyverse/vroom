/**
 * @file error_collection_benchmark.cpp
 * @brief Benchmark comparing CsvReader parsing with and without error collection.
 *
 * This benchmark measures the performance gap between:
 * - CsvReader with error_mode=DISABLED (no error collection, maximum performance)
 * - CsvReader with error_mode=PERMISSIVE (error collection path)
 *
 * The goal is to optimize the error collection path to be as close to the
 * fast path as possible.
 */

#include "libvroom.h"

#include "benchmark/benchmark.h"

#include <cstdio>
#include <fstream>
#include <random>
#include <sstream>
#include <string>

// Generate test CSV data
static std::string generate_csv_data(size_t rows, size_t cols) {
  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows with some quoted fields
  std::mt19937 gen(42);
  std::uniform_int_distribution<> dist(0, 99);

  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';

      // 10% chance of quoted field
      if (dist(gen) < 10) {
        oss << "\"value_" << r << "_" << c << "\"";
      } else {
        oss << "value_" << r << "_" << c;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// CsvReader parsing without error collection (fast path)
static void BM_CsvReader_NoErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  std::string csv_data = generate_csv_data(rows, 10);

  // Write to temp file
  std::string temp_path = "/tmp/libvroom_error_bench.csv";
  {
    std::ofstream out(temp_path);
    out << csv_data;
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1;
  opts.error_mode = libvroom::ErrorMode::DISABLED;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);

  std::remove(temp_path.c_str());
}

// CsvReader parsing with error collection (permissive mode)
static void BM_CsvReader_WithErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  std::string csv_data = generate_csv_data(rows, 10);

  // Write to temp file
  std::string temp_path = "/tmp/libvroom_error_bench_errors.csv";
  {
    std::ofstream out(temp_path);
    out << csv_data;
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);

  std::remove(temp_path.c_str());
}

// Register benchmarks with varying sizes
BENCHMARK(BM_CsvReader_NoErrors)
    ->Arg(10000)  // ~0.3 MB
    ->Arg(50000)  // ~1.5 MB
    ->Arg(100000) // ~3 MB
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_CsvReader_WithErrors)
    ->Arg(10000)  // ~0.3 MB
    ->Arg(50000)  // ~1.5 MB
    ->Arg(100000) // ~3 MB
    ->Unit(benchmark::kMillisecond);
