#include "libvroom.h"

#include <benchmark/benchmark.h>

// Global variables for shared test data
std::map<std::string, libvroom::AlignedBuffer> test_data;

// Initialize test data
static void InitializeBenchmarkData() {
  // Load common test files if they exist
  std::vector<std::string> test_files = {
      "benchmark/data/basic/simple.csv",       "benchmark/data/basic/many_rows.csv",
      "benchmark/data/basic/wide_columns.csv", "test/data/basic/simple.csv",
      "test/data/basic/many_rows.csv",         "test/data/basic/wide_columns.csv"};

  for (const auto& file : test_files) {
    try {
      auto buffer = libvroom::load_file_to_ptr(file, LIBVROOM_PADDING);
      if (buffer) {
        test_data.emplace(file, std::move(buffer));
      }
    } catch (...) {
      // File doesn't exist, skip it
    }
  }
}

// Cleanup function
static void CleanupBenchmarkData() {
  // RAII handles memory cleanup automatically
  test_data.clear();
}

BENCHMARK_MAIN();