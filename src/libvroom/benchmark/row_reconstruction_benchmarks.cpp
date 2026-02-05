/**
 * @file row_reconstruction_benchmarks.cpp
 * @brief Benchmarks for row reconstruction from column-major index.
 *
 * This file benchmarks the cost of reconstructing rows from a column-major
 * index layout. This validates whether O(cols) row access is acceptable for
 * CLI operations (head/tail) and type detection.
 *
 * Related: Issue #599 (index layout evaluation), Issue #603 (this benchmark)
 *
 * Hypothesis:
 * - Single row reconstruction: < 1 μs
 * - head/tail 10 rows: < 10 μs
 * - Type detection 1000 rows: < 1 ms
 *
 * These times should be negligible compared to I/O and display overhead.
 */

#include <benchmark/benchmark.h>
#include <cstdint>
#include <random>
#include <vector>

/**
 * @brief Simulated column-major index for benchmarking.
 *
 * In a column-major layout, all values for column 0 are stored contiguously,
 * followed by all values for column 1, etc.
 *
 * Layout: col_indexes[col * nrows + row] = field_offset
 *
 * Column access is O(1) with sequential memory.
 * Row access requires O(cols) lookups with strided memory access.
 */
class ColumnMajorIndex {
public:
  size_t nrows;
  size_t ncols;
  std::vector<uint64_t> col_indexes;

  ColumnMajorIndex(size_t rows, size_t cols) : nrows(rows), ncols(cols), col_indexes(rows * cols) {
    // Fill with realistic field offsets (sequential within each column)
    // In a real CSV, field offsets would increase as we move through the file
    uint64_t offset = 0;
    for (size_t row = 0; row < nrows; ++row) {
      for (size_t col = 0; col < ncols; ++col) {
        col_indexes[col * nrows + row] = offset;
        offset += 10; // Average field width ~10 bytes
      }
    }
  }

  // O(1) column access - returns pointer to contiguous column data
  const uint64_t* column(size_t col) const { return &col_indexes[col * nrows]; }

  // O(cols) row access - reconstructs row by gathering from each column
  void get_row(size_t row, std::vector<uint64_t>& out) const {
    for (size_t col = 0; col < ncols; ++col) {
      out[col] = col_indexes[col * nrows + row];
    }
  }

  // O(cols) single field access by (row, col) - common for type detection
  uint64_t get_field(size_t row, size_t col) const { return col_indexes[col * nrows + row]; }
};

/**
 * @brief Simulated row-major index for comparison.
 *
 * In a row-major layout, all fields for row 0 are stored contiguously,
 * followed by all fields for row 1, etc.
 *
 * Layout: row_indexes[row * ncols + col] = field_offset
 *
 * Row access is O(1) with sequential memory.
 * Column access requires O(rows) lookups with strided memory access.
 */
class RowMajorIndex {
public:
  size_t nrows;
  size_t ncols;
  std::vector<uint64_t> row_indexes;

  RowMajorIndex(size_t rows, size_t cols) : nrows(rows), ncols(cols), row_indexes(rows * cols) {
    // Fill with realistic field offsets (sequential in file order)
    uint64_t offset = 0;
    for (size_t row = 0; row < nrows; ++row) {
      for (size_t col = 0; col < ncols; ++col) {
        row_indexes[row * ncols + col] = offset;
        offset += 10;
      }
    }
  }

  // O(1) row access - returns pointer to contiguous row data
  const uint64_t* row(size_t row_idx) const { return &row_indexes[row_idx * ncols]; }

  // O(rows) column access - would need to gather from each row
  void get_column(size_t col, std::vector<uint64_t>& out) const {
    for (size_t row = 0; row < nrows; ++row) {
      out[row] = row_indexes[row * ncols + col];
    }
  }

  // O(1) single field access by (row, col)
  uint64_t get_field(size_t row, size_t col) const { return row_indexes[row * ncols + col]; }
};

// ============================================================================
// Column-Major Row Reconstruction Benchmarks
// ============================================================================

/**
 * @brief Fetch a single row from column-major index.
 *
 * Parameters:
 * - range(0): total rows (100K or 1M)
 * - range(1): columns (10, 100, 500)
 */
static void BM_RowReconstruction_Single_ColMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));

  ColumnMajorIndex index(nrows, ncols);
  std::vector<uint64_t> row_buffer(ncols);

  // Access middle row to avoid edge effects
  size_t target_row = nrows / 2;

  for (auto _ : state) {
    index.get_row(target_row, row_buffer);
    benchmark::DoNotOptimize(row_buffer.data());
    benchmark::ClobberMemory();
  }

  // Report metrics
  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["Lookups"] = static_cast<double>(ncols);
  state.counters["TimePerLookup_ns"] =
      benchmark::Counter(static_cast<double>(ncols), benchmark::Counter::kIsIterationInvariantRate |
                                                         benchmark::Counter::kInvert);
}

// Single row reconstruction with various dimensions
BENCHMARK(BM_RowReconstruction_Single_ColMajor)
    ->Args({100000, 10})   // 100K rows, 10 cols
    ->Args({100000, 100})  // 100K rows, 100 cols
    ->Args({100000, 500})  // 100K rows, 500 cols
    ->Args({1000000, 10})  // 1M rows, 10 cols
    ->Args({1000000, 100}) // 1M rows, 100 cols
    ->Args({1000000, 500}) // 1M rows, 500 cols
    ->Unit(benchmark::kNanosecond);

/**
 * @brief Fetch N rows from column-major index (simulating head/tail).
 *
 * Parameters:
 * - range(0): total rows
 * - range(1): columns
 * - range(2): rows to fetch
 */
static void BM_RowReconstruction_Batch_ColMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t rows_to_fetch = static_cast<size_t>(state.range(2));

  ColumnMajorIndex index(nrows, ncols);
  std::vector<uint64_t> row_buffer(ncols);

  for (auto _ : state) {
    // Fetch first N rows (head operation)
    for (size_t i = 0; i < rows_to_fetch; ++i) {
      index.get_row(i, row_buffer);
      benchmark::DoNotOptimize(row_buffer.data());
    }
    benchmark::ClobberMemory();
  }

  // Report metrics
  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["RowsFetched"] = static_cast<double>(rows_to_fetch);
  state.counters["TotalLookups"] = static_cast<double>(rows_to_fetch * ncols);
  state.counters["TimePerRow_ns"] = benchmark::Counter(
      static_cast<double>(rows_to_fetch),
      benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

// Batch row reconstruction (head/tail simulation - 10 rows)
BENCHMARK(BM_RowReconstruction_Batch_ColMajor)
    ->Args({100000, 10, 10})   // 100K rows, 10 cols, fetch 10
    ->Args({100000, 100, 10})  // 100K rows, 100 cols, fetch 10
    ->Args({100000, 500, 10})  // 100K rows, 500 cols, fetch 10
    ->Args({1000000, 10, 10})  // 1M rows, 10 cols, fetch 10
    ->Args({1000000, 100, 10}) // 1M rows, 100 cols, fetch 10
    ->Args({1000000, 500, 10}) // 1M rows, 500 cols, fetch 10
    ->Unit(benchmark::kNanosecond);

/**
 * @brief Fetch sampled rows from column-major index (simulating type detection).
 *
 * Type detection typically samples ~1000 rows distributed throughout the file.
 *
 * Parameters:
 * - range(0): total rows
 * - range(1): columns
 * - range(2): rows to sample
 */
static void BM_RowReconstruction_Sampled_ColMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t rows_to_sample = static_cast<size_t>(state.range(2));

  ColumnMajorIndex index(nrows, ncols);
  std::vector<uint64_t> row_buffer(ncols);

  // Pre-compute evenly distributed sample row indices
  std::vector<size_t> sample_rows(rows_to_sample);
  size_t stride = nrows / rows_to_sample;
  for (size_t i = 0; i < rows_to_sample; ++i) {
    sample_rows[i] = i * stride;
  }

  for (auto _ : state) {
    for (size_t row_idx : sample_rows) {
      index.get_row(row_idx, row_buffer);
      benchmark::DoNotOptimize(row_buffer.data());
    }
    benchmark::ClobberMemory();
  }

  // Report metrics
  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["RowsSampled"] = static_cast<double>(rows_to_sample);
  state.counters["TotalLookups"] = static_cast<double>(rows_to_sample * ncols);
  state.counters["TimePerRow_ns"] = benchmark::Counter(
      static_cast<double>(rows_to_sample),
      benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

// Sampled row reconstruction (type detection simulation - 1000 rows)
BENCHMARK(BM_RowReconstruction_Sampled_ColMajor)
    ->Args({100000, 10, 1000})   // 100K rows, 10 cols, sample 1000
    ->Args({100000, 100, 1000})  // 100K rows, 100 cols, sample 1000
    ->Args({100000, 500, 1000})  // 100K rows, 500 cols, sample 1000
    ->Args({1000000, 10, 1000})  // 1M rows, 10 cols, sample 1000
    ->Args({1000000, 100, 1000}) // 1M rows, 100 cols, sample 1000
    ->Args({1000000, 500, 1000}) // 1M rows, 500 cols, sample 1000
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Row-Major Comparison Benchmarks (baseline)
// ============================================================================

/**
 * @brief Fetch a single row from row-major index (baseline comparison).
 */
static void BM_RowReconstruction_Single_RowMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));

  RowMajorIndex index(nrows, ncols);
  const uint64_t* row_ptr = nullptr;

  size_t target_row = nrows / 2;

  for (auto _ : state) {
    // Row-major: O(1) access to contiguous row data
    row_ptr = index.row(target_row);
    benchmark::DoNotOptimize(row_ptr);
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
}

BENCHMARK(BM_RowReconstruction_Single_RowMajor)
    ->Args({100000, 10})
    ->Args({100000, 100})
    ->Args({100000, 500})
    ->Args({1000000, 10})
    ->Args({1000000, 100})
    ->Args({1000000, 500})
    ->Unit(benchmark::kNanosecond);

/**
 * @brief Fetch N rows from row-major index (baseline comparison).
 */
static void BM_RowReconstruction_Batch_RowMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t rows_to_fetch = static_cast<size_t>(state.range(2));

  RowMajorIndex index(nrows, ncols);
  const uint64_t* row_ptr = nullptr;

  for (auto _ : state) {
    for (size_t i = 0; i < rows_to_fetch; ++i) {
      row_ptr = index.row(i);
      benchmark::DoNotOptimize(row_ptr);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["RowsFetched"] = static_cast<double>(rows_to_fetch);
}

BENCHMARK(BM_RowReconstruction_Batch_RowMajor)
    ->Args({100000, 10, 10})
    ->Args({100000, 100, 10})
    ->Args({100000, 500, 10})
    ->Args({1000000, 10, 10})
    ->Args({1000000, 100, 10})
    ->Args({1000000, 500, 10})
    ->Unit(benchmark::kNanosecond);

/**
 * @brief Fetch sampled rows from row-major index (baseline comparison).
 */
static void BM_RowReconstruction_Sampled_RowMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t rows_to_sample = static_cast<size_t>(state.range(2));

  RowMajorIndex index(nrows, ncols);
  const uint64_t* row_ptr = nullptr;

  std::vector<size_t> sample_rows(rows_to_sample);
  size_t stride = nrows / rows_to_sample;
  for (size_t i = 0; i < rows_to_sample; ++i) {
    sample_rows[i] = i * stride;
  }

  for (auto _ : state) {
    for (size_t row_idx : sample_rows) {
      row_ptr = index.row(row_idx);
      benchmark::DoNotOptimize(row_ptr);
    }
    benchmark::ClobberMemory();
  }

  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["RowsSampled"] = static_cast<double>(rows_to_sample);
}

BENCHMARK(BM_RowReconstruction_Sampled_RowMajor)
    ->Args({100000, 10, 1000})
    ->Args({100000, 100, 1000})
    ->Args({100000, 500, 1000})
    ->Args({1000000, 10, 1000})
    ->Args({1000000, 100, 1000})
    ->Args({1000000, 500, 1000})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Column Iteration Benchmarks (ALTREP use case)
// ============================================================================

/**
 * @brief Iterate through entire column from column-major index (ALTREP hot path).
 *
 * This is the primary use case for column-major storage - sequential iteration
 * through all values of a single column.
 */
static void BM_ColumnIteration_ColMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));

  ColumnMajorIndex index(nrows, ncols);
  uint64_t sum = 0;

  // Target first column
  size_t target_col = 0;

  for (auto _ : state) {
    const uint64_t* col_data = index.column(target_col);
    sum = 0;
    for (size_t i = 0; i < nrows; ++i) {
      sum += col_data[i];
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(nrows * sizeof(uint64_t) * state.iterations()));
  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
}

BENCHMARK(BM_ColumnIteration_ColMajor)
    ->Args({100000, 10})
    ->Args({100000, 100})
    ->Args({1000000, 10})
    ->Args({1000000, 100})
    ->Unit(benchmark::kMicrosecond);

/**
 * @brief Iterate through entire column from row-major index (ALTREP baseline).
 *
 * This shows the cost of column access with row-major storage - strided
 * memory access pattern.
 */
static void BM_ColumnIteration_RowMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));

  RowMajorIndex index(nrows, ncols);
  std::vector<uint64_t> col_buffer(nrows);
  uint64_t sum = 0;

  size_t target_col = 0;

  for (auto _ : state) {
    index.get_column(target_col, col_buffer);
    sum = 0;
    for (size_t i = 0; i < nrows; ++i) {
      sum += col_buffer[i];
    }
    benchmark::DoNotOptimize(sum);
  }

  state.SetBytesProcessed(static_cast<int64_t>(nrows * sizeof(uint64_t) * state.iterations()));
  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
}

BENCHMARK(BM_ColumnIteration_RowMajor)
    ->Args({100000, 10})
    ->Args({100000, 100})
    ->Args({1000000, 10})
    ->Args({1000000, 100})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Per-Field Access Benchmarks (random access pattern)
// ============================================================================

/**
 * @brief Random field access from column-major index.
 *
 * Simulates random access patterns during value extraction.
 */
static void BM_RandomFieldAccess_ColMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t num_accesses = static_cast<size_t>(state.range(2));

  ColumnMajorIndex index(nrows, ncols);

  // Pre-generate random access pattern
  std::mt19937_64 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<size_t> row_dist(0, nrows - 1);
  std::uniform_int_distribution<size_t> col_dist(0, ncols - 1);

  std::vector<std::pair<size_t, size_t>> access_pattern(num_accesses);
  for (size_t i = 0; i < num_accesses; ++i) {
    access_pattern[i] = {row_dist(rng), col_dist(rng)};
  }

  uint64_t sum = 0;

  for (auto _ : state) {
    sum = 0;
    for (const auto& [row, col] : access_pattern) {
      sum += index.get_field(row, col);
    }
    benchmark::DoNotOptimize(sum);
  }

  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["Accesses"] = static_cast<double>(num_accesses);
  state.counters["TimePerAccess_ns"] = benchmark::Counter(
      static_cast<double>(num_accesses),
      benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_RandomFieldAccess_ColMajor)
    ->Args({100000, 100, 10000})
    ->Args({1000000, 100, 10000})
    ->Unit(benchmark::kMicrosecond);

/**
 * @brief Random field access from row-major index (baseline).
 */
static void BM_RandomFieldAccess_RowMajor(benchmark::State& state) {
  size_t nrows = static_cast<size_t>(state.range(0));
  size_t ncols = static_cast<size_t>(state.range(1));
  size_t num_accesses = static_cast<size_t>(state.range(2));

  RowMajorIndex index(nrows, ncols);

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> row_dist(0, nrows - 1);
  std::uniform_int_distribution<size_t> col_dist(0, ncols - 1);

  std::vector<std::pair<size_t, size_t>> access_pattern(num_accesses);
  for (size_t i = 0; i < num_accesses; ++i) {
    access_pattern[i] = {row_dist(rng), col_dist(rng)};
  }

  uint64_t sum = 0;

  for (auto _ : state) {
    sum = 0;
    for (const auto& [row, col] : access_pattern) {
      sum += index.get_field(row, col);
    }
    benchmark::DoNotOptimize(sum);
  }

  state.counters["Rows"] = static_cast<double>(nrows);
  state.counters["Cols"] = static_cast<double>(ncols);
  state.counters["Accesses"] = static_cast<double>(num_accesses);
  state.counters["TimePerAccess_ns"] = benchmark::Counter(
      static_cast<double>(num_accesses),
      benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_RandomFieldAccess_RowMajor)
    ->Args({100000, 100, 10000})
    ->Args({1000000, 100, 10000})
    ->Unit(benchmark::kMicrosecond);
