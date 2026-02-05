#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Simple CSV parser for comparison (naive implementation)
class NaiveCSVParser {
public:
  static std::vector<std::vector<std::string>> parse(const std::string& data) {
    std::vector<std::vector<std::string>> result;
    std::istringstream stream(data);
    std::string line;

    while (std::getline(stream, line)) {
      std::vector<std::string> row;
      std::istringstream line_stream(line);
      std::string field;

      while (std::getline(line_stream, field, ',')) {
        row.push_back(field);
      }
      result.push_back(row);
    }

    return result;
  }
};

// Stream-based CSV parser for comparison
class StreamCSVParser {
public:
  static size_t count_records(const std::string& data) {
    size_t count = 0;
    for (char c : data) {
      if (c == '\n')
        count++;
    }
    return count;
  }

  static size_t count_fields(const std::string& data) {
    size_t count = 0;
    for (char c : data) {
      if (c == ',' || c == '\n')
        count++;
    }
    return count;
  }
};

// Benchmark libvroom vs naive parser
static void BM_libvroom_vs_naive(benchmark::State& state, const std::string& filename) {
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
  bool use_libvroom = state.range(0) == 1;

  if (use_libvroom) {
    libvroom::CsvOptions opts;
    opts.num_threads = 1;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      auto open_res = reader.open(filename);
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }
  } else {
    // Naive parser
    std::string str_data(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    for (auto _ : state) {
      auto result = NaiveCSVParser::parse(str_data);
      benchmark::DoNotOptimize(result);
    }
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Parser"] = use_libvroom ? 1.0 : 0.0; // 1 = libvroom, 0 = naive
}

static void BM_libvroom_vs_naive_simple(benchmark::State& state) {
  BM_libvroom_vs_naive(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_libvroom_vs_naive_simple)
    ->Arg(0) // Naive parser
    ->Arg(1) // libvroom
    ->Unit(benchmark::kMillisecond);

static void BM_libvroom_vs_naive_many_rows(benchmark::State& state) {
  BM_libvroom_vs_naive(state, "test/data/basic/many_rows.csv");
}
BENCHMARK(BM_libvroom_vs_naive_many_rows)
    ->Arg(0) // Naive parser
    ->Arg(1) // libvroom
    ->Unit(benchmark::kMillisecond);

// Benchmark different parsing approaches
static void BM_parsing_approaches(benchmark::State& state, const std::string& filename) {
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
  int approach = static_cast<int>(state.range(0));
  std::string str_data(reinterpret_cast<const char*>(buffer.data()), buffer.size());

  switch (approach) {
  case 0: { // Character-by-character counting
    for (auto _ : state) {
      size_t count = StreamCSVParser::count_records(str_data);
      benchmark::DoNotOptimize(count);
    }
    break;
  }
  case 1: { // Field counting
    for (auto _ : state) {
      size_t count = StreamCSVParser::count_fields(str_data);
      benchmark::DoNotOptimize(count);
    }
    break;
  }
  case 2: { // Full naive parsing
    for (auto _ : state) {
      auto result = NaiveCSVParser::parse(str_data);
      benchmark::DoNotOptimize(result);
    }
    break;
  }
  case 3: { // libvroom indexing
    libvroom::CsvOptions opts;
    opts.num_threads = 1;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(filename);
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }
    break;
  }
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Approach"] = static_cast<double>(approach);
}

static void BM_parsing_approaches_simple(benchmark::State& state) {
  BM_parsing_approaches(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_parsing_approaches_simple)
    ->DenseRange(0, 3, 1) // Test all 4 approaches
    ->Unit(benchmark::kMillisecond);

static void BM_parsing_approaches_quoted(benchmark::State& state) {
  BM_parsing_approaches(state, "test/data/quoted/quoted_fields.csv");
}
BENCHMARK(BM_parsing_approaches_quoted)
    ->DenseRange(0, 3, 1) // Test all 4 approaches
    ->Unit(benchmark::kMillisecond);

// Memory bandwidth benchmark
static void BM_memory_bandwidth(benchmark::State& state) {
  size_t size = static_cast<size_t>(state.range(0));
  auto* data = static_cast<char*>(libvroom::aligned_alloc_portable(size));
  if (!data) {
    state.SkipWithError("Failed to allocate aligned memory");
    return;
  }

  // Initialize data
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(i % 256);
  }

  for (auto _ : state) {
    volatile char sum = 0;
    for (size_t i = 0; i < size; ++i) {
      sum += data[i];
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(size * state.iterations()));

  // Google Benchmark calculates throughput automatically

  libvroom::aligned_free_portable(data);
}
BENCHMARK(BM_memory_bandwidth)
    ->Range(1024, 1024 * 1024 * 100) // 1KB to 100MB
    ->Unit(benchmark::kMillisecond);
