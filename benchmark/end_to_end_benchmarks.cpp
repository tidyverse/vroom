/**
 * @file end_to_end_benchmarks.cpp
 * @brief Benchmarks for end-to-end parse time with and without transpose.
 *
 * This file benchmarks the complete parsing pipeline to measure:
 * 1. BM_ParseOnly - Parse CSV to per-thread index (baseline)
 * 2. BM_ParseAndCompact - Parse CSV + compact to flat row-major index
 * 3. BM_ParseAndTranspose - Parse CSV + compact + transpose to column-major
 *
 * These benchmarks validate the hypothesis that transpose is <5% of total
 * parse time for typical workloads (Issue #599/#602).
 */

#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
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

/**
 * @brief Transpose row-major flat index to column-major.
 *
 * Converts from flat_indexes[row * ncols + col] format
 * to col_indexes[col * nrows + row] format.
 *
 * @param flat_indexes Source flat index array (row-major)
 * @param nrows Number of rows
 * @param ncols Number of columns
 * @return Column-major index array
 */
std::unique_ptr<uint64_t[]> transpose_to_column_major(const uint64_t* flat_indexes, size_t nrows,
                                                      size_t ncols) {
  size_t total = nrows * ncols;
  auto col_indexes = std::make_unique<uint64_t[]>(total);

  for (size_t row = 0; row < nrows; ++row) {
    for (size_t col = 0; col < ncols; ++col) {
      col_indexes[col * nrows + row] = flat_indexes[row * ncols + col];
    }
  }

  return col_indexes;
}

// Cache generated CSV data to avoid regeneration between iterations
struct CachedCSV {
  libvroom::AlignedBuffer buffer;
  size_t actual_size;
  size_t cols;
  size_t rows; // Approximate rows (for transpose sizing)
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

  // Allocate aligned buffer with SIMD padding
  AlignedPtr ptr = make_aligned_ptr(csv.size(), LIBVROOM_PADDING);
  std::memcpy(ptr.get(), csv.data(), csv.size());

  // Estimate rows (for transpose sizing)
  size_t approx_rows = csv.size() / (cols * 5);

  CachedCSV cached;
  cached.buffer = libvroom::AlignedBuffer(std::move(ptr), csv.size());
  cached.actual_size = csv.size();
  cached.cols = cols;
  cached.rows = approx_rows;

  auto result = csv_cache.emplace(key, std::move(cached));
  return result.first->second;
}

} // anonymous namespace

// ============================================================================
// BM_ParseOnly - Parse CSV to per-thread index (baseline)
// ============================================================================

static void BM_ParseOnly(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// ============================================================================
// BM_ParseAndCompact - Parse CSV + compact to flat row-major index
// ============================================================================

static void BM_ParseAndCompact(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// ============================================================================
// BM_ParseAndTranspose - Parse CSV + compact + transpose to column-major
// ============================================================================

static void BM_ParseAndTranspose(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();

    // Transpose to column-major
    // flat_indexes contains row * cols + col positions
    // We need to know nrows: total_indexes / cols
    size_t total = result.idx.flat_indexes_count;
    size_t ncols = result.num_columns();
    size_t nrows = (ncols > 0) ? total / ncols : 0;

    if (nrows > 0 && ncols > 0) {
      auto col_major = transpose_to_column_major(result.idx.flat_indexes, nrows, ncols);
      benchmark::DoNotOptimize(col_major);
    }

    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// ============================================================================
// BM_TransposeOnly - Measure transpose overhead in isolation
// ============================================================================

static void BM_TransposeOnly(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::Parser parser(n_threads);

  // Parse and compact once outside the benchmark loop
  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  size_t total = result.idx.flat_indexes_count;
  size_t ncols = result.num_columns();
  size_t nrows = (ncols > 0) ? total / ncols : 0;

  for (auto _ : state) {
    if (nrows > 0 && ncols > 0) {
      auto col_major = transpose_to_column_major(result.idx.flat_indexes, nrows, ncols);
      benchmark::DoNotOptimize(col_major);
    }
  }

  // Report bytes transposed (8 bytes per uint64_t)
  size_t bytes_transposed = total * sizeof(uint64_t);
  state.SetBytesProcessed(static_cast<int64_t>(bytes_transposed * state.iterations()));
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Index_MB"] = static_cast<double>(bytes_transposed) / (1024.0 * 1024.0);
}

// ============================================================================
// BM_CompactOnly - Measure compact overhead in isolation
// ============================================================================

static void BM_CompactOnly(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(target_size, cols);
  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    // Parse fresh each time (since compact is in-place and idempotent)
    // Pause timing during parse to measure only compact overhead
    state.PauseTiming();
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    state.ResumeTiming();

    result.compact();

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

BENCHMARK(BM_ParseAndCompact)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK(BM_ParseAndTranspose)
    ->Apply(CustomArguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_TransposeOnly)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK(BM_CompactOnly)->Apply(CustomArguments)->Unit(benchmark::kMillisecond)->UseRealTime();
