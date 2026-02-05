#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <random>
#include <thread>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Performance metrics and efficiency benchmarks

// Cache performance benchmark
static void BM_CachePerformance(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));

  // Create data that fits in different cache levels as a CSV temp file
  std::string csv_data;
  csv_data.reserve(data_size);

  // Fill with realistic CSV-like data pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      csv_data += '\n'; // Newlines
    else if (i % 10 == 0)
      csv_data += ','; // Commas
    else if (i % 50 == 0)
      csv_data += '"'; // Quotes
    else
      csv_data += static_cast<char>('a' + (i % 26)); // Letters
  }

  std::string temp_path = "/tmp/libvroom_cache_" + std::to_string(data_size) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
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

  std::remove(temp_path.c_str());
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
  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  // Measure cycles before and after
  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(filename);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["NsPerByte"] =
      static_cast<double>(duration) / (state.iterations() * buffer.size());
  state.counters["FileSize"] = static_cast<double>(buffer.size());
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
  libvroom::CsvOptions opts;
  opts.num_threads = static_cast<size_t>(n_threads);

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(filename);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize"] = static_cast<double>(buffer.size());

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

  // Create synthetic data as a CSV temp file
  std::string csv_data;
  csv_data.reserve(data_size);

  // Pattern that exercises memory bandwidth
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      csv_data += '\n';
    else if (i % 10 == 0)
      csv_data += ',';
    else
      csv_data += static_cast<char>('a' + (i % 26));
  }

  std::string temp_path = "/tmp/libvroom_membw_" + std::to_string(data_size) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Estimate memory bandwidth utilization
  // Typical modern systems have ~25-50 GB/s memory bandwidth
  double estimated_peak_bandwidth = 30.0; // GB/s (conservative estimate)
  (void)estimated_peak_bandwidth;

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_MemoryBandwidth)
    ->RangeMultiplier(4)
    ->Range(1024 * 1024, 256 * 1024 * 1024) // 1MB to 256MB
    ->Unit(benchmark::kMillisecond);

// Branch prediction benchmark
static void BM_BranchPrediction(benchmark::State& state) {
  size_t pattern_type = static_cast<size_t>(state.range(0));
  size_t data_size = 1024 * 1024; // 1MB

  std::string csv_data;
  csv_data.reserve(data_size);

  // Different branch prediction patterns
  switch (pattern_type) {
  case 0: // Predictable pattern
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 100 == 0)
        csv_data += '\n';
      else if (i % 10 == 0)
        csv_data += ',';
      else
        csv_data += 'a';
    }
    break;
  case 1: // Random pattern (bad for branch prediction)
  {
    std::mt19937 gen(12345); // Fixed seed for reproducibility
    std::uniform_int_distribution<> dist(0, 255);
    for (size_t i = 0; i < data_size; ++i) {
      csv_data += static_cast<char>(dist(gen));
    }
  } break;
  case 2: // Quotes heavy (lots of quote state changes)
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 5 == 0)
        csv_data += '"';
      else if (i % 10 == 0)
        csv_data += ',';
      else if (i % 50 == 0)
        csv_data += '\n';
      else
        csv_data += 'a';
    }
    break;
  }

  std::string temp_path = "/tmp/libvroom_branch_" + std::to_string(pattern_type) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1; // Single thread to isolate branches

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["PatternType"] = static_cast<double>(pattern_type);

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_BranchPrediction)
    ->DenseRange(0, 2, 1) // 0=predictable, 1=random, 2=quote-heavy
    ->Unit(benchmark::kMillisecond);

// SIMD utilization benchmark (measures effectiveness of vectorization)
static void BM_SIMDUtilization(benchmark::State& state) {
  size_t alignment = static_cast<size_t>(state.range(0));
  size_t data_size = 1024 * 1024; // 1MB

  // Create CSV data and write to temp file (alignment is handled internally by CsvReader)
  std::string csv_data;
  csv_data.reserve(data_size);

  // Fill with CSV-like pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      csv_data += '\n';
    else if (i % 10 == 0)
      csv_data += ',';
    else
      csv_data += static_cast<char>('a' + (i % 26));
  }

  std::string temp_path = "/tmp/libvroom_simd_" + std::to_string(alignment) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["Alignment"] = static_cast<double>(alignment);
  state.counters["DataSize"] = static_cast<double>(data_size);

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_SIMDUtilization)
    ->Arg(0)  // 64-byte aligned (optimal for AVX-512)
    ->Arg(1)  // 1-byte offset (misaligned)
    ->Arg(8)  // 8-byte offset
    ->Arg(16) // 16-byte offset
    ->Arg(32) // 32-byte offset
    ->Unit(benchmark::kMicrosecond);
