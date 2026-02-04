#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <chrono>
#include <fstream>
#include <random>
#include <thread>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Performance metrics and efficiency benchmarks

// Cache performance benchmark
static void BM_CachePerformance(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));

  // Create data that fits in different cache levels
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Fill with realistic CSV-like data pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      data[i] = '\n'; // Newlines
    else if (i % 10 == 0)
      data[i] = ','; // Commas
    else if (i % 50 == 0)
      data[i] = '"'; // Quotes
    else
      data[i] = 'a' + (i % 26); // Letters
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Cache level indicators
  if (data_size <= 32 * 1024) {
    state.counters["CacheLevel"] = 1.0; // L1
  } else if (data_size <= 256 * 1024) {
    state.counters["CacheLevel"] = 2.0; // L2
  } else if (data_size <= 8 * 1024 * 1024) {
    state.counters["CacheLevel"] = 3.0; // L3
  } else {
    state.counters["CacheLevel"] = 4.0; // Main memory
  }

  aligned_free(data);
}

BENCHMARK(BM_CachePerformance)
    ->Arg(16 * 1024)        // L1 cache size
    ->Arg(128 * 1024)       // L2 cache size
    ->Arg(4 * 1024 * 1024)  // L3 cache size
    ->Arg(32 * 1024 * 1024) // Main memory
    ->Unit(benchmark::kMicrosecond);

// Instructions per byte benchmark
static void BM_InstructionEfficiency(benchmark::State& state) {
  // Load test file
  std::string filename = "test/data/basic/many_rows.csv";

  if (test_data.find(filename) == test_data.end()) {
    try {
      auto buffer = libvroom::load_file_to_ptr(filename, LIBVROOM_PADDING);
      if (!buffer) {
        state.SkipWithError(("Failed to load " + filename).c_str());
        return;
      }
      test_data.emplace(filename, std::move(buffer));
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  }

  const auto& buffer = test_data.at(filename);
  libvroom::Parser parser(1);

  // Measure cycles before and after
  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["NsPerByte"] = static_cast<double>(duration) / (state.iterations() * buffer.size);
  state.counters["FileSize"] = static_cast<double>(buffer.size);
}

BENCHMARK(BM_InstructionEfficiency)->Unit(benchmark::kMillisecond);

// Thread scaling efficiency benchmark
static void BM_ThreadScalingEfficiency(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));

  // Use a reasonably large file for thread scaling
  std::string filename = "test/data/basic/many_rows.csv";

  if (test_data.find(filename) == test_data.end()) {
    try {
      auto buffer = libvroom::load_file_to_ptr(filename, LIBVROOM_PADDING);
      if (!buffer) {
        state.SkipWithError(("Failed to load " + filename).c_str());
        return;
      }
      test_data.emplace(filename, std::move(buffer));
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  }

  const auto& buffer = test_data.at(filename);
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize"] = static_cast<double>(buffer.size);

  // Calculate efficiency metrics
  static double single_thread_throughput = 0.0;
  if (n_threads == 1) {
  } else if (single_thread_throughput > 0.0) {
    double ideal_throughput = single_thread_throughput * n_threads;
    (void)ideal_throughput;
  }
}

BENCHMARK(BM_ThreadScalingEfficiency)
    ->DenseRange(1, 16, 1) // 1 to 16 threads
    ->Unit(benchmark::kMillisecond);

// Memory bandwidth utilization benchmark
static void BM_MemoryBandwidth(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));

  // Create synthetic data
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Pattern that exercises memory bandwidth
  for (size_t i = 0; i < data_size; ++i) {
    data[i] = static_cast<uint8_t>(i % 256);
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Estimate memory bandwidth utilization
  // Typical modern systems have ~25-50 GB/s memory bandwidth
  double estimated_peak_bandwidth = 30.0; // GB/s (conservative estimate)
  (void)estimated_peak_bandwidth;

  aligned_free(data);
}

BENCHMARK(BM_MemoryBandwidth)
    ->RangeMultiplier(4)
    ->Range(1024 * 1024, 256 * 1024 * 1024) // 1MB to 256MB
    ->Unit(benchmark::kMillisecond);

// Branch prediction benchmark
static void BM_BranchPrediction(benchmark::State& state) {
  size_t pattern_type = static_cast<size_t>(state.range(0));
  size_t data_size = 1024 * 1024; // 1MB

  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Different branch prediction patterns
  switch (pattern_type) {
  case 0: // Predictable pattern
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 100 == 0)
        data[i] = '\n';
      else if (i % 10 == 0)
        data[i] = ',';
      else
        data[i] = 'a';
    }
    break;
  case 1: // Random pattern (bad for branch prediction)
  {
    std::mt19937 gen(12345); // Fixed seed for reproducibility
    std::uniform_int_distribution<> dist(0, 255);
    for (size_t i = 0; i < data_size; ++i) {
      data[i] = static_cast<uint8_t>(dist(gen));
    }
  } break;
  case 2: // Quotes heavy (lots of quote state changes)
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 5 == 0)
        data[i] = '"';
      else if (i % 10 == 0)
        data[i] = ',';
      else if (i % 50 == 0)
        data[i] = '\n';
      else
        data[i] = 'a';
    }
    break;
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1); // Single thread to isolate branches

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["PatternType"] = static_cast<double>(pattern_type);

  aligned_free(data);
}

BENCHMARK(BM_BranchPrediction)
    ->DenseRange(0, 2, 1) // 0=predictable, 1=random, 2=quote-heavy
    ->Unit(benchmark::kMillisecond);

// SIMD utilization benchmark (measures effectiveness of vectorization)
static void BM_SIMDUtilization(benchmark::State& state) {
  size_t alignment = static_cast<size_t>(state.range(0));
  size_t data_size = 1024 * 1024; // 1MB

  // Test different alignments to see SIMD effectiveness
  auto base_data =
      static_cast<uint8_t*>(aligned_malloc(64, data_size + alignment + LIBVROOM_PADDING));
  auto data = base_data + alignment; // Offset by alignment

  // Fill with CSV-like pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      data[i] = '\n';
    else if (i % 10 == 0)
      data[i] = ',';
    else
      data[i] = 'a' + (i % 26);
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["Alignment"] = static_cast<double>(alignment);
  state.counters["DataSize"] = static_cast<double>(data_size);

  aligned_free(base_data);
}

BENCHMARK(BM_SIMDUtilization)
    ->Arg(0)  // 64-byte aligned (optimal for AVX-512)
    ->Arg(1)  // 1-byte offset (misaligned)
    ->Arg(8)  // 8-byte offset
    ->Arg(16) // 16-byte offset
    ->Arg(32) // 32-byte offset
    ->Unit(benchmark::kMicrosecond);
