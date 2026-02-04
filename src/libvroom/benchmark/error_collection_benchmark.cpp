/**
 * @file error_collection_benchmark.cpp
 * @brief Benchmark comparing branchless parsing with and without error collection.
 *
 * This benchmark measures the performance gap between:
 * - parse_branchless() - SIMD-optimized fast path (no error collection)
 * - parse_branchless_with_errors() - error collection path
 *
 * The goal is to optimize the error collection path to be as close to the
 * fast path as possible.
 */

#include "libvroom.h"

#include "benchmark/benchmark.h"

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

// Branchless parsing without error collection (fast path)
static void BM_Branchless_NoErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  std::string csv_data = generate_csv_data(rows, 10);

  // Add padding for SIMD
  std::vector<uint8_t> buffer(csv_data.size() + 64, 0);
  std::memcpy(buffer.data(), csv_data.data(), csv_data.size());

  libvroom::Parser parser(1);
  libvroom::ParseOptions options;
  options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), csv_data.size(), options);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
}

// Branchless parsing with error collection (current implementation)
static void BM_Branchless_WithErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  std::string csv_data = generate_csv_data(rows, 10);

  // Add padding for SIMD
  std::vector<uint8_t> buffer(csv_data.size() + 64, 0);
  std::memcpy(buffer.data(), csv_data.data(), csv_data.size());

  libvroom::Parser parser(1);
  libvroom::ParseOptions options;
  options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  options.errors = &errors;

  for (auto _ : state) {
    errors.clear();
    auto result = parser.parse(buffer.data(), csv_data.size(), options);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(errors);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
}

// Register benchmarks with varying sizes
BENCHMARK(BM_Branchless_NoErrors)
    ->Arg(10000)  // ~0.3 MB
    ->Arg(50000)  // ~1.5 MB
    ->Arg(100000) // ~3 MB
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Branchless_WithErrors)
    ->Arg(10000)  // ~0.3 MB
    ->Arg(50000)  // ~1.5 MB
    ->Arg(100000) // ~3 MB
    ->Unit(benchmark::kMillisecond);
