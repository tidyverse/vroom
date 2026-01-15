#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <memory>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Basic parsing benchmark for different file sizes
static void BM_ParseFile(benchmark::State& state, const std::string& filename) {
  // Try to load the file if not already loaded
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
  int n_threads = static_cast<int>(state.range(0));
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  // Performance metrics are calculated automatically by Google Benchmark
  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["FileSize"] = static_cast<double>(buffer.size);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// Benchmark different thread counts
static void BM_ParseSimple_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_ParseSimple_Threads)->RangeMultiplier(2)->Range(1, 16)->Unit(benchmark::kMillisecond);

static void BM_ParseManyRows_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/many_rows.csv");
}
BENCHMARK(BM_ParseManyRows_Threads)
    ->RangeMultiplier(2)
    ->Range(1, 16)
    ->Unit(benchmark::kMillisecond);

static void BM_ParseWideColumns_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/wide_columns.csv");
}
BENCHMARK(BM_ParseWideColumns_Threads)
    ->RangeMultiplier(2)
    ->Range(1, 16)
    ->Unit(benchmark::kMillisecond);

// Benchmark different file types
static void BM_ParseQuoted(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/quoted_fields.csv");
}
BENCHMARK(BM_ParseQuoted)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseWithEmbeddedSeparators(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/embedded_separators.csv");
}
BENCHMARK(BM_ParseWithEmbeddedSeparators)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseWithNewlines(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/newlines_in_quotes.csv");
}
BENCHMARK(BM_ParseWithNewlines)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// Benchmark different separators
static void BM_ParseTabSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/tab.csv");
}
BENCHMARK(BM_ParseTabSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseSemicolonSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/semicolon.csv");
}
BENCHMARK(BM_ParseSemicolonSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParsePipeSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/pipe.csv");
}
BENCHMARK(BM_ParsePipeSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// Memory allocation benchmark
static void BM_MemoryAllocation(benchmark::State& state) {
  size_t file_size = static_cast<size_t>(state.range(0));

  for (auto _ : state) {
    auto data = aligned_malloc(64, file_size + LIBVROOM_PADDING);
    benchmark::DoNotOptimize(data);
    aligned_free(data);
  }

  state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
}
BENCHMARK(BM_MemoryAllocation)
    ->Range(1024, 1024 * 1024 * 100) // 1KB to 100MB
    ->Unit(benchmark::kMicrosecond);

// Index creation benchmark
static void BM_IndexCreation(benchmark::State& state) {
  size_t file_size = static_cast<size_t>(state.range(0));
  int n_threads = static_cast<int>(state.range(1));

  libvroom::TwoPass tp;

  for (auto _ : state) {
    auto result = tp.init(file_size, n_threads);
    benchmark::DoNotOptimize(result);
  }

  state.counters["FileSize"] = static_cast<double>(file_size);
  state.counters["Threads"] = static_cast<double>(n_threads);
}
BENCHMARK(BM_IndexCreation)
    ->Ranges({{1024, 1024 * 1024 * 100}, {1, 16}}) // File sizes 1KB-100MB, threads 1-16
    ->Unit(benchmark::kMicrosecond);

// Index creation with counted allocation benchmark
// Shows memory savings from the optimized allocation strategy
static void BM_IndexCreationCounted(benchmark::State& state) {
  size_t file_size = static_cast<size_t>(state.range(0));
  int n_threads = static_cast<int>(state.range(1));
  // Simulate different separator densities (1%, 5%, 10% of file size)
  double separator_ratio = 0.05; // 5% separator density is typical for CSV
  uint64_t separator_count = static_cast<uint64_t>(file_size * separator_ratio);

  libvroom::TwoPass tp;

  for (auto _ : state) {
    auto result = tp.init_counted(separator_count, n_threads);
    benchmark::DoNotOptimize(result);
  }

  // Calculate memory savings
  // Old allocation: (file_size + 8) * n_threads * sizeof(uint64_t)
  // New allocation: (separator_count + 8) * n_threads * sizeof(uint64_t)
  size_t old_alloc = (file_size + 8) * n_threads * sizeof(uint64_t);
  size_t new_alloc = (separator_count + 8) * n_threads * sizeof(uint64_t);
  double savings_ratio = static_cast<double>(old_alloc) / static_cast<double>(new_alloc);

  state.counters["FileSize"] = static_cast<double>(file_size);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Separators"] = static_cast<double>(separator_count);
  state.counters["MemorySavingsRatio"] = savings_ratio;
  state.counters["OldAllocMB"] = static_cast<double>(old_alloc) / (1024.0 * 1024.0);
  state.counters["NewAllocMB"] = static_cast<double>(new_alloc) / (1024.0 * 1024.0);
}
BENCHMARK(BM_IndexCreationCounted)
    ->Ranges({{1024, 1024 * 1024 * 100}, {1, 16}}) // File sizes 1KB-100MB, threads 1-16
    ->Unit(benchmark::kMicrosecond);