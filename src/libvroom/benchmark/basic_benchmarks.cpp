#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <cstdlib>
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

  libvroom::CsvOptions opts;
  opts.num_threads = static_cast<size_t>(n_threads);

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(filename);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  // Performance metrics are calculated automatically by Google Benchmark
  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["FileSize"] = static_cast<double>(buffer.size());
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
    auto* data =
        static_cast<uint8_t*>(libvroom::aligned_alloc_portable(file_size + LIBVROOM_PADDING));
    if (!data) {
      state.SkipWithError("Failed to allocate memory");
      return;
    }
    benchmark::DoNotOptimize(data);
    libvroom::aligned_free_portable(data);
  }

  state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
}
BENCHMARK(BM_MemoryAllocation)
    ->Range(1024, 1024 * 1024 * 100) // 1KB to 100MB
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Write Pattern Benchmarks: Sequential vs Strided
// ============================================================================
// These benchmarks measure the cache penalty of different memory access patterns
// when writing index data during CSV parsing.
//
// Context: When parsing CSV, we need to store field offsets. Two layouts:
// - Row-major (current): fields stored sequentially per row [r0c0, r0c1, r0c2, r1c0, ...]
// - Column-major (ALTREP): fields stored by column [r0c0, r1c0, r2c0, ..., r0c1, r1c1, ...]
//
// Row-major writes sequentially during parsing (cache-friendly).
// Column-major requires strided writes during parsing (potentially cache-hostile).

// Write to contiguous memory (simulates row-major index construction)
// This is the current approach: fields are written sequentially as rows are parsed
static void BM_WriteSequential(benchmark::State& state) {
  const size_t rows = static_cast<size_t>(state.range(0));
  const size_t cols = static_cast<size_t>(state.range(1));
  const size_t total_elements = rows * cols;
  const size_t total_bytes = total_elements * sizeof(uint64_t);

  // Allocate aligned memory
  auto* data = static_cast<uint64_t*>(libvroom::aligned_alloc_portable(total_bytes));
  if (!data) {
    state.SkipWithError("Failed to allocate memory");
    return;
  }

  for (auto _ : state) {
    // Sequential write: iterate through memory linearly
    // Simulates writing field offsets as we parse row-by-row
    for (size_t i = 0; i < total_elements; ++i) {
      data[i] = i; // Simple value to avoid optimizer removing the write
    }
    benchmark::ClobberMemory();
  }

  libvroom::aligned_free_portable(data);

  state.SetBytesProcessed(static_cast<int64_t>(total_bytes * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["TotalMB"] = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
  state.counters["GB/s"] = benchmark::Counter(static_cast<double>(total_bytes),
                                              benchmark::Counter::kIsIterationInvariantRate,
                                              benchmark::Counter::kIs1024);
}

// Write with stride (simulates column-major index construction during parsing)
// When parsing row-by-row but storing column-major, each field write jumps by stride
// Stride = rows * sizeof(uint64_t) bytes between consecutive fields in a row
static void BM_WriteStrided(benchmark::State& state) {
  const size_t rows = static_cast<size_t>(state.range(0));
  const size_t cols = static_cast<size_t>(state.range(1));
  const size_t total_elements = rows * cols;
  const size_t total_bytes = total_elements * sizeof(uint64_t);
  const size_t stride = rows; // In elements (stride in bytes = rows * 8)

  // Allocate aligned memory
  auto* data = static_cast<uint64_t*>(libvroom::aligned_alloc_portable(total_bytes));
  if (!data) {
    state.SkipWithError("Failed to allocate memory");
    return;
  }

  for (auto _ : state) {
    // Strided write: for each row, write fields with stride between columns
    // Memory layout is column-major: [col0: r0,r1,r2,...][col1: r0,r1,r2,...]
    // But we parse row-by-row, so row 0 writes to positions 0, stride, 2*stride, ...
    for (size_t row = 0; row < rows; ++row) {
      for (size_t col = 0; col < cols; ++col) {
        // Column-major index: col * rows + row
        data[col * stride + row] = row * cols + col;
      }
    }
    benchmark::ClobberMemory();
  }

  libvroom::aligned_free_portable(data);

  state.SetBytesProcessed(static_cast<int64_t>(total_bytes * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["TotalMB"] = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
  state.counters["StrideBytes"] = static_cast<double>(stride * sizeof(uint64_t));
  state.counters["GB/s"] = benchmark::Counter(static_cast<double>(total_bytes),
                                              benchmark::Counter::kIsIterationInvariantRate,
                                              benchmark::Counter::kIs1024);
}

// Test matrix: Rows x Cols
// Rows: 10K, 100K, 1M (to see how working set size affects cache behavior)
// Cols: 10, 100, 500 (typical CSV column counts)
//
// Working set sizes:
// - 10K rows x 10 cols = 800KB (fits in L3)
// - 10K rows x 100 cols = 8MB (borderline L3)
// - 10K rows x 500 cols = 40MB (exceeds L3)
// - 100K rows x 10 cols = 8MB (borderline L3)
// - 100K rows x 100 cols = 80MB (exceeds L3)
// - 100K rows x 500 cols = 400MB (way exceeds L3)
// - 1M rows x 10 cols = 80MB (exceeds L3)
// - 1M rows x 100 cols = 800MB (way exceeds L3)
// - 1M rows x 500 cols = 4GB (very large)

static void WriteSequentialArgs(benchmark::internal::Benchmark* b) {
  // Rows: 10K, 100K, 1M; Cols: 10, 100, 500
  for (int64_t rows : {10000, 100000, 1000000}) {
    for (int64_t cols : {10, 100, 500}) {
      b->Args({rows, cols});
    }
  }
}

static void WriteStridedArgs(benchmark::internal::Benchmark* b) {
  // Same matrix as sequential for direct comparison
  for (int64_t rows : {10000, 100000, 1000000}) {
    for (int64_t cols : {10, 100, 500}) {
      b->Args({rows, cols});
    }
  }
}

BENCHMARK(BM_WriteSequential)->Apply(WriteSequentialArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_WriteStrided)->Apply(WriteStridedArgs)->Unit(benchmark::kMillisecond);
