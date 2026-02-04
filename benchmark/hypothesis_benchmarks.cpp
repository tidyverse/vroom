/**
 * @file hypothesis_benchmarks.cpp
 * @brief Discriminatory benchmarks for hypothesis-driven optimization decisions.
 *
 * This file implements benchmarks to test the key hypotheses from Issue #611:
 *
 * H1: Column-major index provides no net benefit over row-major after transpose
 * H2: Arrow Builder API is the primary bottleneck (not index layout)
 * H3: Synchronization barriers dominate multi-threaded scaling
 * H4: Zero-copy string extraction is viable for most CSV data
 * H5: Parquet type widening is rare in real CSV data
 * H6: compact() is required for O(1) field access
 * H7: Row object creation is expensive for per-field access
 *
 * Each benchmark is designed to discriminate between hypotheses and guide
 * implementation decisions.
 *
 * IMPORTANT: H6 benchmarks use TwoPass API directly to avoid Parser's
 * auto-compaction behavior.
 */

#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"
#include "two_pass.h"

#include <benchmark/benchmark.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

// ============================================================================
// CSV Generation Utilities
// ============================================================================

/**
 * @brief Generate CSV data with specified dimensions.
 *
 * @param target_rows Target number of rows
 * @param cols Number of columns
 * @param type_pattern Pattern for column types: 'i'=int, 'd'=double, 's'=string
 * @return Generated CSV content as a string
 */
std::string generate_csv(size_t target_rows, size_t cols, const std::string& type_pattern = "") {
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> int_dist(0, 99999);
  std::uniform_real_distribution<double> dbl_dist(-1000.0, 1000.0);

  // Generate type pattern if not provided
  std::string types = type_pattern;
  if (types.empty()) {
    // Default: alternating int, double, string
    for (size_t i = 0; i < cols; ++i) {
      switch (i % 3) {
      case 0:
        types += 'i';
        break;
      case 1:
        types += 'd';
        break;
      case 2:
        types += 's';
        break;
      }
    }
  }
  // Extend or truncate to match cols
  while (types.size() < cols)
    types += types;
  types.resize(cols);

  std::ostringstream oss;

  // Write header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Random strings pool
  std::vector<std::string> str_pool;
  for (int i = 0; i < 100; ++i) {
    str_pool.push_back("str" + std::to_string(i) + "_value");
  }

  // Generate rows
  for (size_t r = 0; r < target_rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';
      switch (types[c]) {
      case 'i':
        oss << int_dist(rng);
        break;
      case 'd':
        oss << std::fixed << std::setprecision(4) << dbl_dist(rng);
        break;
      case 's':
        oss << str_pool[rng() % str_pool.size()];
        break;
      default:
        oss << int_dist(rng);
        break;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

/**
 * @brief Generate CSV with escape sequences for H4 testing.
 *
 * @param target_rows Number of rows
 * @param cols Number of columns
 * @param escape_ratio Ratio of fields with escape sequences (0.0-1.0)
 */
std::string generate_csv_with_escapes(size_t target_rows, size_t cols, double escape_ratio) {
  std::mt19937 rng(42);
  std::uniform_real_distribution<double> prob_dist(0.0, 1.0);

  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows
  for (size_t r = 0; r < target_rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';

      if (prob_dist(rng) < escape_ratio) {
        // Field with escape sequence
        oss << "\"value" << r << "\"\"inside\"\"field\"";
      } else {
        // Normal field
        oss << "value" << r << "_" << c;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// CSV cache for repeated benchmarks
struct CachedCSV {
  libvroom::AlignedBuffer buffer;
  size_t actual_size;
  size_t rows;
  size_t cols;
};

std::map<std::tuple<size_t, size_t, std::string>, CachedCSV> csv_cache;

CachedCSV& get_or_create_csv(size_t rows, size_t cols, const std::string& type_pattern = "") {
  auto key = std::make_tuple(rows, cols, type_pattern);
  auto it = csv_cache.find(key);
  if (it != csv_cache.end()) {
    return it->second;
  }

  // Generate CSV
  std::string csv = generate_csv(rows, cols, type_pattern);

  // Allocate aligned buffer
  AlignedPtr ptr = make_aligned_ptr(csv.size(), LIBVROOM_PADDING);
  std::memcpy(ptr.get(), csv.data(), csv.size());

  CachedCSV cached;
  cached.buffer = libvroom::AlignedBuffer(std::move(ptr), csv.size());
  cached.actual_size = csv.size();
  cached.rows = rows;
  cached.cols = cols;

  auto result = csv_cache.emplace(key, std::move(cached));
  return result.first->second;
}

} // anonymous namespace

// ============================================================================
// H6: compact() is Required for O(1) Field Access
// ============================================================================
//
// IMPORTANT: These benchmarks use TwoPass API directly instead of Parser
// because Parser::parse() auto-compacts the index. TwoPass::parse() does NOT
// auto-compact, allowing us to measure the true difference.

/**
 * @brief Benchmark field access WITHOUT compact() using TwoPass API.
 *
 * Without compact(), field access is O(n_threads) as it must search
 * through per-thread regions.
 */
static void BM_H6_FieldAccess_NoCompact(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(rows, cols);

  // Use TwoPass directly to avoid Parser's auto-compaction
  libvroom::TwoPass tp;
  libvroom::ParseIndex idx = tp.init(cached.actual_size, static_cast<size_t>(n_threads));
  tp.parse(cached.buffer.data(), idx, cached.actual_size);
  // Index is NOT compacted - flat_indexes should be empty
  // Set columns to enable num_rows() calculation
  idx.columns = cols;

  size_t actual_rows = idx.num_rows();

  // Access pattern: iterate all fields in column 0
  for (auto _ : state) {
    for (size_t row = 0; row < actual_rows; ++row) {
      auto span = idx.get_field_span(row, 0);
      benchmark::DoNotOptimize(span);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(actual_rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["IsFlat"] = static_cast<double>(idx.is_flat() ? 1 : 0);
  state.counters["RowsPerSec"] = benchmark::Counter(static_cast<double>(actual_rows),
                                                    benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark field access WITH compact().
 *
 * After compact(), field access is O(1) via flat index.
 */
static void BM_H6_FieldAccess_WithCompact(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));

  auto& cached = get_or_create_csv(rows, cols);

  // Use TwoPass directly and explicitly compact
  libvroom::TwoPass tp;
  libvroom::ParseIndex idx = tp.init(cached.actual_size, static_cast<size_t>(n_threads));
  tp.parse(cached.buffer.data(), idx, cached.actual_size);
  // Set columns to enable num_rows() calculation
  idx.columns = cols;
  idx.compact(); // Explicitly compact to flat index

  size_t actual_rows = idx.num_rows();

  // Access pattern: iterate all fields in column 0
  for (auto _ : state) {
    for (size_t row = 0; row < actual_rows; ++row) {
      auto span = idx.get_field_span(row, 0);
      benchmark::DoNotOptimize(span);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(actual_rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["IsFlat"] = static_cast<double>(idx.is_flat() ? 1 : 0);
  state.counters["RowsPerSec"] = benchmark::Counter(static_cast<double>(actual_rows),
                                                    benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark random field access without compact.
 */
static void BM_H6_RandomAccess_NoCompact(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));
  size_t num_accesses = 10000;

  auto& cached = get_or_create_csv(rows, cols);

  // Use TwoPass directly to avoid Parser's auto-compaction
  libvroom::TwoPass tp;
  libvroom::ParseIndex idx = tp.init(cached.actual_size, static_cast<size_t>(n_threads));
  tp.parse(cached.buffer.data(), idx, cached.actual_size);
  // Index is NOT compacted
  // Set columns to enable num_rows() calculation
  idx.columns = cols;

  size_t actual_rows = idx.num_rows();

  // Pre-generate random access pattern
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> row_dist(0, actual_rows - 1);
  std::uniform_int_distribution<size_t> col_dist(0, cols - 1);

  std::vector<std::pair<size_t, size_t>> access_pattern(num_accesses);
  for (size_t i = 0; i < num_accesses; ++i) {
    access_pattern[i] = {row_dist(rng), col_dist(rng)};
  }

  for (auto _ : state) {
    for (const auto& [row, col] : access_pattern) {
      auto span = idx.get_field_span(row, col);
      benchmark::DoNotOptimize(span);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Accesses"] = static_cast<double>(num_accesses);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["IsFlat"] = static_cast<double>(idx.is_flat() ? 1 : 0);
  state.counters["AccessesPerSec"] = benchmark::Counter(
      static_cast<double>(num_accesses), benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark random field access with compact.
 */
static void BM_H6_RandomAccess_WithCompact(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int n_threads = static_cast<int>(state.range(2));
  size_t num_accesses = 10000;

  auto& cached = get_or_create_csv(rows, cols);

  // Use TwoPass directly and explicitly compact
  libvroom::TwoPass tp;
  libvroom::ParseIndex idx = tp.init(cached.actual_size, static_cast<size_t>(n_threads));
  tp.parse(cached.buffer.data(), idx, cached.actual_size);
  // Set columns to enable num_rows() calculation
  idx.columns = cols;
  idx.compact(); // Explicitly compact

  size_t actual_rows = idx.num_rows();

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> row_dist(0, actual_rows - 1);
  std::uniform_int_distribution<size_t> col_dist(0, cols - 1);

  std::vector<std::pair<size_t, size_t>> access_pattern(num_accesses);
  for (size_t i = 0; i < num_accesses; ++i) {
    access_pattern[i] = {row_dist(rng), col_dist(rng)};
  }

  for (auto _ : state) {
    for (const auto& [row, col] : access_pattern) {
      auto span = idx.get_field_span(row, col);
      benchmark::DoNotOptimize(span);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Accesses"] = static_cast<double>(num_accesses);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["IsFlat"] = static_cast<double>(idx.is_flat() ? 1 : 0);
  state.counters["AccessesPerSec"] = benchmark::Counter(
      static_cast<double>(num_accesses), benchmark::Counter::kIsIterationInvariantRate);
}

// H6 registration
static void H6_Arguments(benchmark::internal::Benchmark* b) {
  // {rows, cols, threads}
  std::vector<std::tuple<int64_t, int64_t, int64_t>> configs = {
      {100000, 10, 1}, {100000, 10, 4}, {1000000, 10, 1}, {1000000, 10, 4}, {1000000, 10, 8},
  };
  for (const auto& [rows, cols, threads] : configs) {
    b->Args({rows, cols, threads});
  }
}

BENCHMARK(BM_H6_FieldAccess_NoCompact)
    ->Apply(H6_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H6_FieldAccess_WithCompact)
    ->Apply(H6_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H6_RandomAccess_NoCompact)
    ->Apply(H6_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H6_RandomAccess_WithCompact)
    ->Apply(H6_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// ============================================================================
// H7: Row Object Creation Cost
// ============================================================================

/**
 * @brief Benchmark access via Row object pattern.
 *
 * This is the pattern: result_.row(row).get_string_view(col)
 * which creates a temporary Row object for each access.
 */
static void BM_H7_ViaRowObject(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  // Access all fields in column 0 using Row objects (via iterator)
  for (auto _ : state) {
    size_t row_idx = 0;
    for (auto row : result.rows()) {
      auto sv = row.get_string_view(0);
      benchmark::DoNotOptimize(sv);
      ++row_idx;
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["RowsPerSec"] =
      benchmark::Counter(static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark direct field span access (bypassing Row object).
 *
 * Uses result.idx.get_field_span() directly, avoiding Row object creation.
 */
static void BM_H7_DirectSpanAccess(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  const uint8_t* buf = cached.buffer.data();

  // Access all fields in column 0 directly via span
  for (auto _ : state) {
    for (size_t row = 0; row < rows; ++row) {
      auto span = result.idx.get_field_span(row, 0);
      if (span.is_valid()) {
        std::string_view sv(reinterpret_cast<const char*>(buf + span.start), span.length());
        benchmark::DoNotOptimize(sv);
      }
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["RowsPerSec"] =
      benchmark::Counter(static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark ValueExtractor get_string_view (middle ground).
 */
static void BM_H7_ViaExtractor(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  // Create extractor directly
  libvroom::ValueExtractor extractor(cached.buffer.data(), cached.actual_size, result.idx);

  for (auto _ : state) {
    for (size_t row = 0; row < rows; ++row) {
      auto sv = extractor.get_string_view(row, 0);
      benchmark::DoNotOptimize(sv);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["RowsPerSec"] =
      benchmark::Counter(static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

// H7 registration
static void H7_Arguments(benchmark::internal::Benchmark* b) {
  // {rows, cols}
  b->Args({100000, 10});
  b->Args({1000000, 10});
}

BENCHMARK(BM_H7_ViaRowObject)->Apply(H7_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK(BM_H7_DirectSpanAccess)
    ->Apply(H7_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H7_ViaExtractor)->Apply(H7_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// H1: Column-Major Index Overhead
// ============================================================================

/**
 * @brief Benchmark row-major column iteration (no transpose).
 */
static void BM_H1_RowMajor_ColumnIteration(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact(); // Row-major flat index

  const uint8_t* buf = cached.buffer.data();

  // Iterate column 0 with row-major index
  for (auto _ : state) {
    uint64_t sum = 0;
    for (size_t row = 0; row < rows; ++row) {
      auto span = result.idx.get_field_span(row, 0);
      sum += span.start;
    }
    benchmark::DoNotOptimize(sum);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["RowsPerSec"] =
      benchmark::Counter(static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Benchmark column-major iteration (with transpose overhead).
 */
static void BM_H1_ColMajor_ColumnIteration(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.idx.compact_column_major(4); // Column-major transpose

  // Iterate column 0 with column-major index (contiguous access)
  for (auto _ : state) {
    uint64_t sum = 0;
    if (result.idx.col_indexes) {
      const uint64_t* col_data = result.idx.col_indexes;
      for (size_t row = 0; row < rows; ++row) {
        sum += col_data[row]; // col 0 is at indices 0..rows-1
      }
    }
    benchmark::DoNotOptimize(sum);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["RowsPerSec"] =
      benchmark::Counter(static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

/**
 * @brief Measure transpose time in isolation.
 */
static void BM_H1_TransposeOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    state.PauseTiming();
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact(); // First compact to row-major
    state.ResumeTiming();

    // Now time the transpose
    result.idx.compact_column_major(4);
    benchmark::DoNotOptimize(result.idx.col_indexes);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Fields"] = static_cast<double>(rows * cols);
}

// H1 registration - test matrix from issue
static void H1_Arguments(benchmark::internal::Benchmark* b) {
  // (rows, cols) from issue: [(10K,10), (100K,10), (1M,10), (100K,100),
  // (100K,1000)]
  b->Args({10000, 10});
  b->Args({100000, 10});
  b->Args({1000000, 10});
  b->Args({100000, 100});
  // b->Args({100000, 1000});  // Too large for in-memory benchmark
}

BENCHMARK(BM_H1_RowMajor_ColumnIteration)
    ->Apply(H1_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H1_ColMajor_ColumnIteration)
    ->Apply(H1_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H1_TransposeOnly)->Apply(H1_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

/**
 * @brief Full pipeline: parse + compact + iterate N columns (row-major).
 *
 * This benchmark measures the total cost of iterating multiple columns
 * with row-major layout to establish break-even point with column-major.
 */
static void BM_H1_FullPipeline_RowMajor_MultiCol(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t total_cols = static_cast<size_t>(state.range(1));
  size_t cols_to_iterate = static_cast<size_t>(state.range(2));

  auto& cached = get_or_create_csv(rows, total_cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact(); // Row-major

    // Iterate cols_to_iterate columns
    uint64_t sum = 0;
    for (size_t col = 0; col < cols_to_iterate; ++col) {
      for (size_t row = 0; row < rows; ++row) {
        auto span = result.idx.get_field_span(row, col);
        sum += span.start;
      }
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["TotalCols"] = static_cast<double>(total_cols);
  state.counters["ColsIterated"] = static_cast<double>(cols_to_iterate);
  state.counters["FieldsAccessed"] = static_cast<double>(rows * cols_to_iterate);
}

/**
 * @brief Full pipeline: parse + transpose + iterate N columns (column-major).
 *
 * This benchmark measures the total cost including transpose overhead
 * to find when column-major layout pays off.
 */
static void BM_H1_FullPipeline_ColMajor_MultiCol(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t total_cols = static_cast<size_t>(state.range(1));
  size_t cols_to_iterate = static_cast<size_t>(state.range(2));

  auto& cached = get_or_create_csv(rows, total_cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.idx.compact_column_major(4); // Column-major transpose

    // Iterate cols_to_iterate columns (contiguous access per column)
    uint64_t sum = 0;
    if (result.idx.col_indexes) {
      for (size_t col = 0; col < cols_to_iterate; ++col) {
        const uint64_t* col_data = result.idx.column(col);
        if (col_data) {
          for (size_t row = 0; row < rows; ++row) {
            sum += col_data[row];
          }
        }
      }
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["TotalCols"] = static_cast<double>(total_cols);
  state.counters["ColsIterated"] = static_cast<double>(cols_to_iterate);
  state.counters["FieldsAccessed"] = static_cast<double>(rows * cols_to_iterate);
}

// H1 Break-even registration - vary columns iterated to find break-even
static void H1_BreakEven_Arguments(benchmark::internal::Benchmark* b) {
  // (rows, total_cols, cols_to_iterate)
  // Test 1M rows x 10 cols, iterating 1, 2, 5, 10 columns
  b->Args({1000000, 10, 1});
  b->Args({1000000, 10, 2});
  b->Args({1000000, 10, 5});
  b->Args({1000000, 10, 10});
  // Test 100K rows x 100 cols, iterating various amounts
  b->Args({100000, 100, 1});
  b->Args({100000, 100, 5});
  b->Args({100000, 100, 10});
  b->Args({100000, 100, 50});
  b->Args({100000, 100, 100});
}

BENCHMARK(BM_H1_FullPipeline_RowMajor_MultiCol)
    ->Apply(H1_BreakEven_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H1_FullPipeline_ColMajor_MultiCol)
    ->Apply(H1_BreakEven_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// ============================================================================
// H3: Synchronization Barrier Overhead
// ============================================================================

/**
 * @brief Benchmark parse time at varying thread counts.
 *
 * Measures scaling efficiency to identify barrier overhead.
 */
static void BM_H3_ThreadScaling(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));
  size_t target_size = static_cast<size_t>(state.range(1));

  // Generate a file of approximately target_size
  size_t cols = 20;
  size_t approx_rows = target_size / (cols * 10); // ~10 bytes per field
  auto& cached = get_or_create_csv(approx_rows, cols);

  libvroom::Parser parser(n_threads);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Size_MB"] = static_cast<double>(cached.actual_size) / (1024.0 * 1024.0);
}

// H3 registration - thread counts: 1, 2, 4, 8, 16
static void H3_Arguments(benchmark::internal::Benchmark* b) {
  // Thread counts × target file sizes
  std::vector<int64_t> threads = {1, 2, 4, 8, 16};
  std::vector<int64_t> sizes = {10 * 1024 * 1024, 100 * 1024 * 1024}; // 10MB, 100MB

  for (auto size : sizes) {
    for (auto thread : threads) {
      b->Args({thread, size});
    }
  }
}

BENCHMARK(BM_H3_ThreadScaling)->Apply(H3_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// H4: Escape Sequence Frequency Analysis
// ============================================================================

/**
 * @brief Count escape sequences in CSV data.
 *
 * This benchmark scans CSV data to count fields with escape sequences,
 * validating the hypothesis that most CSV data doesn't need escape processing.
 */
static void BM_H4_EscapeAnalysis(benchmark::State& state) {
  double escape_ratio = state.range(0) / 100.0; // 0-100% as integer
  size_t rows = 100000;
  size_t cols = 10;

  // Generate CSV with specified escape ratio
  std::string csv = generate_csv_with_escapes(rows, cols, escape_ratio);
  AlignedPtr ptr = make_aligned_ptr(csv.size(), LIBVROOM_PADDING);
  std::memcpy(ptr.get(), csv.data(), csv.size());

  libvroom::Parser parser(4);
  auto result = parser.parse(ptr.get(), csv.size(), {.dialect = libvroom::Dialect::csv()});
  result.compact();

  const uint8_t* buf = ptr.get();
  size_t escape_count = 0;

  for (auto _ : state) {
    escape_count = 0;
    // Count fields containing "" escape sequence
    for (size_t row = 0; row < rows; ++row) {
      for (size_t col = 0; col < cols; ++col) {
        auto span = result.idx.get_field_span(row, col);
        if (span.is_valid()) {
          std::string_view field(reinterpret_cast<const char*>(buf + span.start), span.length());
          // Check for doubled quote (escape sequence)
          if (field.find("\"\"") != std::string_view::npos) {
            ++escape_count;
          }
        }
      }
    }
    benchmark::DoNotOptimize(escape_count);
  }

  state.counters["EscapeRatio_Input"] = escape_ratio;
  state.counters["EscapeCount"] = static_cast<double>(escape_count);
  state.counters["TotalFields"] = static_cast<double>(rows * cols);
  state.counters["ActualEscapeRatio"] =
      static_cast<double>(escape_count) / static_cast<double>(rows * cols);
}

// H4 registration - escape ratios: 0%, 5%, 20%, 50%, 80%
BENCHMARK(BM_H4_EscapeAnalysis)
    ->Arg(0)  // 0% escapes
    ->Arg(5)  // 5% escapes
    ->Arg(20) // 20% escapes
    ->Arg(50) // 50% escapes
    ->Arg(80) // 80% escapes
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// ============================================================================
// H5: Type Widening Detection
// ============================================================================

/**
 * @brief Simulate type inference on CSV data.
 *
 * This benchmark simulates streaming type inference to detect how often
 * type widening would be needed (e.g., int64 -> double).
 */
static void BM_H5_TypeInference(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t sample_rows = static_cast<size_t>(state.range(1));

  // Generate CSV with mixed types that might need widening
  // Pattern: some columns start as int but have doubles later
  auto& cached = get_or_create_csv(rows, 10, "iiiiddddsss");

  libvroom::Parser parser(4);
  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  const uint8_t* buf = cached.buffer.data();

  // Simulate type inference: scan first sample_rows to infer types
  // Then scan remaining rows to detect type changes
  size_t type_changes = 0;

  for (auto _ : state) {
    type_changes = 0;
    size_t cols = result.num_columns();

    // This is a simplified simulation - in practice we'd track inferred types
    // For benchmark purposes, we just measure the cost of scanning for type
    // changes
    for (size_t col = 0; col < cols; ++col) {
      bool saw_decimal = false;

      for (size_t row = 0; row < rows; ++row) {
        auto span = result.idx.get_field_span(row, col);
        if (span.is_valid()) {
          std::string_view field(reinterpret_cast<const char*>(buf + span.start), span.length());

          // Check if field contains decimal point (would force int -> double)
          bool has_decimal = field.find('.') != std::string_view::npos;

          if (has_decimal && !saw_decimal && row >= sample_rows) {
            // Type change detected after sample window
            ++type_changes;
          }
          saw_decimal = saw_decimal || has_decimal;
        }
      }
    }
    benchmark::DoNotOptimize(type_changes);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["SampleRows"] = static_cast<double>(sample_rows);
  state.counters["TypeChanges"] = static_cast<double>(type_changes);
}

// H5 registration
static void H5_Arguments(benchmark::internal::Benchmark* b) {
  // {total_rows, sample_rows}
  b->Args({100000, 1000});
  b->Args({1000000, 1000});
}

BENCHMARK(BM_H5_TypeInference)->Apply(H5_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

// ============================================================================
// Full Pipeline Benchmarks (for comparison)
// ============================================================================

/**
 * @brief Full pipeline: parse + compact + column iteration.
 */
static void BM_FullPipeline_RowMajor(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();

    // Iterate all values in column 0
    uint64_t sum = 0;
    for (size_t row = 0; row < rows; ++row) {
      auto span = result.idx.get_field_span(row, 0);
      sum += span.start;
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

/**
 * @brief Full pipeline: parse + transpose + column iteration.
 */
static void BM_FullPipeline_ColMajor(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.idx.compact_column_major(4);

    // Iterate all values in column 0 (contiguous)
    uint64_t sum = 0;
    if (result.idx.col_indexes) {
      const uint64_t* col_data = result.idx.col_indexes;
      for (size_t row = 0; row < rows; ++row) {
        sum += col_data[row];
      }
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

BENCHMARK(BM_FullPipeline_RowMajor)
    ->Apply(H1_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_FullPipeline_ColMajor)
    ->Apply(H1_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

// ============================================================================
// H2: Arrow Builder API Bottleneck Analysis
// ============================================================================
//
// This hypothesis tests whether Arrow Builder API is the primary bottleneck
// compared to index layout. We compare:
// 1. Parse-only time (baseline)
// 2. Parse + field extraction (without Arrow)
// 3. Parse + Arrow conversion (full Builders path)
// 4. Direct buffer writes (simulating zero-copy approach)
//
// If H2 is true: Arrow conversion time >> parse time
// If H2 is false: Arrow conversion time is comparable to parse time

/**
 * @brief Baseline: Parse only, no conversion.
 *
 * Measures the raw parsing throughput without any value extraction.
 */
static void BM_H2_ParseOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 0; // Parse only
}

/**
 * @brief Parse + field extraction without conversion.
 *
 * Measures time to extract all field values as string_views.
 * This is the minimum work needed to access all data.
 */
static void BM_H2_ParseAndExtract(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();

    // Extract all fields as string_views (simulating minimum extraction cost)
    const uint8_t* buf = cached.buffer.data();
    uint64_t sum = 0;
    for (size_t row = 0; row < rows; ++row) {
      for (size_t col = 0; col < cols; ++col) {
        auto span = result.idx.get_field_span(row, col);
        if (span.is_valid()) {
          // Access the field data (forces memory access)
          sum += buf[span.start];
        }
      }
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 1; // Parse + extract
}

/**
 * @brief Simulate direct buffer construction (zero-copy ideal).
 *
 * This benchmark simulates what a direct buffer approach would cost:
 * pre-allocated arrays with direct writes instead of Builder appends.
 * Uses pre-allocated std::vector instead of Arrow Builders.
 */
static void BM_H2_DirectBufferSimulation(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  // Pre-allocate output buffers (simulating direct Arrow buffer construction)
  std::vector<std::vector<int64_t>> int_columns(cols);
  for (auto& col : int_columns) {
    col.resize(rows);
  }

  for (auto _ : state) {
    auto result = parser.parse(cached.buffer.data(), cached.actual_size);
    result.compact();

    const uint8_t* buf = cached.buffer.data();

    // Direct buffer writes (no Builder overhead)
    for (size_t row = 0; row < rows; ++row) {
      for (size_t col = 0; col < cols; ++col) {
        auto span = result.idx.get_field_span(row, col);
        if (span.is_valid()) {
          // Simulate type conversion and direct write
          // This is the minimum cost for populating typed arrays
          int_columns[col][row] = static_cast<int64_t>(span.start);
        }
      }
    }
    benchmark::DoNotOptimize(int_columns);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 2; // Direct buffer
}

/**
 * @brief Simulate per-element Builder overhead.
 *
 * This benchmark isolates the cost of the Builder append pattern:
 * calling a function per element vs direct array assignment.
 */
static void BM_H2_BuilderPatternOverhead(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);
  libvroom::Parser parser(4);

  // Simulate Builder with a vector that grows (like Arrow Builders)
  auto result = parser.parse(cached.buffer.data(), cached.actual_size);
  result.compact();

  for (auto _ : state) {
    // Simulate Builder pattern: clear and re-append
    std::vector<std::vector<int64_t>> builder_columns(cols);
    for (auto& col : builder_columns) {
      col.reserve(rows); // Reserve like Arrow Builders do
    }

    const uint8_t* buf = cached.buffer.data();

    // Builder-style appends (function call per element)
    for (size_t row = 0; row < rows; ++row) {
      for (size_t col = 0; col < cols; ++col) {
        auto span = result.idx.get_field_span(row, col);
        if (span.is_valid()) {
          // Append pattern (like Builder.Append())
          builder_columns[col].push_back(static_cast<int64_t>(span.start));
        }
      }
    }
    benchmark::DoNotOptimize(builder_columns);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 3; // Builder pattern
}

// H2 registration
static void H2_Arguments(benchmark::internal::Benchmark* b) {
  // {rows, cols}
  b->Args({10000, 10});
  b->Args({100000, 10});
  b->Args({1000000, 10});
  b->Args({100000, 100});
}

BENCHMARK(BM_H2_ParseOnly)->Apply(H2_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK(BM_H2_ParseAndExtract)->Apply(H2_Arguments)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK(BM_H2_DirectBufferSimulation)
    ->Apply(H2_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H2_BuilderPatternOverhead)
    ->Apply(H2_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

#ifdef LIBVROOM_ENABLE_ARROW
#include "arrow_output.h"

/**
 * @brief Full Arrow conversion via ArrowConverter (Builders path).
 *
 * This measures the actual Arrow conversion including type inference
 * and Builder-based column construction.
 */
static void BM_H2_ArrowBuilders_Full(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;

  for (auto _ : state) {
    libvroom::TwoPass tp;
    libvroom::ParseIndex idx = tp.init(cached.actual_size, 4);
    tp.parse(cached.buffer.data(), idx, cached.actual_size);

    libvroom::ArrowConverter converter(opts);
    auto result = converter.convert(cached.buffer.data(), cached.actual_size, idx);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 4; // Full Arrow
}

/**
 * @brief Arrow conversion without type inference.
 *
 * Measures Arrow Builders cost when types are pre-specified (no inference).
 */
static void BM_H2_ArrowBuilders_NoInference(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = false; // Skip type inference, treat all as strings

  for (auto _ : state) {
    libvroom::TwoPass tp;
    libvroom::ParseIndex idx = tp.init(cached.actual_size, 4);
    tp.parse(cached.buffer.data(), idx, cached.actual_size);

    libvroom::ArrowConverter converter(opts);
    auto result = converter.convert(cached.buffer.data(), cached.actual_size, idx);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(cached.actual_size * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 5; // Arrow no inference
}

/**
 * @brief Type inference only (no column building).
 *
 * Isolates the cost of type inference from the Builder cost.
 */
static void BM_H2_TypeInferenceOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  auto& cached = get_or_create_csv(rows, cols);

  // Parse once
  libvroom::TwoPass tp;
  libvroom::ParseIndex idx = tp.init(cached.actual_size, 4);
  tp.parse(cached.buffer.data(), idx, cached.actual_size);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;
  libvroom::ArrowConverter converter(opts);

  for (auto _ : state) {
    auto types = converter.infer_types(cached.buffer.data(), cached.actual_size, idx);
    benchmark::DoNotOptimize(types);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Stage"] = 6; // Type inference only
}

BENCHMARK(BM_H2_ArrowBuilders_Full)
    ->Apply(H2_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H2_ArrowBuilders_NoInference)
    ->Apply(H2_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_H2_TypeInferenceOnly)
    ->Apply(H2_Arguments)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

#endif // LIBVROOM_ENABLE_ARROW
