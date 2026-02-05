/**
 * @file transpose_benchmarks.cpp
 * @brief Benchmarks for transposing row-major indices to column-major.
 *
 * Part of #599 - evaluating index layout strategies.
 * Measures the cost of transposing flat_indexes[row * cols + col]
 * to col_indexes[col * rows + row].
 */

#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

// SIMD headers
#ifdef __x86_64__
#include <immintrin.h>
#endif

#ifdef __aarch64__
#include <arm_neon.h>
#endif

/**
 * @brief Single-threaded transpose from row-major to column-major.
 *
 * Input:  row_major[row * cols + col] = value for (row, col)
 * Output: col_major[col * rows + row] = value for (row, col)
 */
static void transpose_single_threaded(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                      size_t cols) {
  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

/**
 * @brief Multi-threaded transpose from row-major to column-major.
 *
 * Parallelizes by columns - each thread handles a contiguous range of columns.
 * This provides good cache locality for the output (each thread writes to
 * contiguous memory in its column range).
 */
static void transpose_multi_threaded(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                     size_t cols, size_t n_threads) {
  if (n_threads <= 1) {
    transpose_single_threaded(row_major, col_major, rows, cols);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  // Divide columns among threads
  size_t cols_per_thread = (cols + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t col_start = t * cols_per_thread;
    size_t col_end = std::min(col_start + cols_per_thread, cols);

    if (col_start >= cols)
      break;

    threads.emplace_back([=]() {
      for (size_t col = col_start; col < col_end; ++col) {
        for (size_t row = 0; row < rows; ++row) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

/**
 * @brief Blocked transpose for better cache utilization.
 *
 * Processes the matrix in blocks that fit in L1/L2 cache.
 * Block size of 64 is chosen to fit within typical 32KB L1 cache:
 * 64 * 64 * 8 bytes = 32KB
 */
static void transpose_blocked(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                              size_t cols, size_t block_size = 64) {
  for (size_t row_block = 0; row_block < rows; row_block += block_size) {
    for (size_t col_block = 0; col_block < cols; col_block += block_size) {
      // Process one block
      size_t row_end = std::min(row_block + block_size, rows);
      size_t col_end = std::min(col_block + block_size, cols);

      for (size_t row = row_block; row < row_end; ++row) {
        for (size_t col = col_block; col < col_end; ++col) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    }
  }
}

/**
 * @brief Multi-threaded blocked transpose.
 */
static void transpose_blocked_multi_threaded(const uint64_t* row_major, uint64_t* col_major,
                                             size_t rows, size_t cols, size_t n_threads,
                                             size_t block_size = 64) {
  if (n_threads <= 1) {
    transpose_blocked(row_major, col_major, rows, cols, block_size);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  // Divide column blocks among threads
  size_t n_col_blocks = (cols + block_size - 1) / block_size;
  size_t blocks_per_thread = (n_col_blocks + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t block_start = t * blocks_per_thread;
    size_t block_end = std::min(block_start + blocks_per_thread, n_col_blocks);

    if (block_start >= n_col_blocks)
      break;

    threads.emplace_back([=]() {
      for (size_t cb = block_start; cb < block_end; ++cb) {
        size_t col_block = cb * block_size;
        size_t col_end = std::min(col_block + block_size, cols);

        for (size_t row_block = 0; row_block < rows; row_block += block_size) {
          size_t row_end = std::min(row_block + block_size, rows);

          for (size_t row = row_block; row < row_end; ++row) {
            for (size_t col = col_block; col < col_end; ++col) {
              col_major[col * rows + row] = row_major[row * cols + col];
            }
          }
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

// =============================================================================
// SIMD Transpose Implementations
// =============================================================================

/**
 * @brief Column-first scalar transpose (sequential writes, strided reads).
 *
 * This is the opposite loop order from transpose_single_threaded().
 * Hypothesis: Sequential writes are more important than sequential reads
 * because write buffers can coalesce and memory write bandwidth is the bottleneck.
 */
static void transpose_column_first_scalar(const uint64_t* row_major, uint64_t* col_major,
                                          size_t rows, size_t cols) {
  for (size_t col = 0; col < cols; ++col) {
    for (size_t row = 0; row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

#ifdef __x86_64__
/**
 * @brief SIMD 4x4 block transpose using AVX2.
 *
 * Processes 4 rows x 4 columns at a time using AVX2 intrinsics.
 * Uses unpack and permute instructions to transpose in registers.
 */
static void transpose_simd_4x4_block(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                     size_t cols) {
  // Process 4x4 blocks
  size_t row_blocks = rows / 4;
  size_t col_blocks = cols / 4;

  for (size_t rb = 0; rb < row_blocks; ++rb) {
    for (size_t cb = 0; cb < col_blocks; ++cb) {
      size_t row_base = rb * 4;
      size_t col_base = cb * 4;

      // Load 4 rows (each row has 4 contiguous elements for these columns)
      __m256i r0 = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&row_major[(row_base + 0) * cols + col_base]));
      __m256i r1 = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&row_major[(row_base + 1) * cols + col_base]));
      __m256i r2 = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&row_major[(row_base + 2) * cols + col_base]));
      __m256i r3 = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&row_major[(row_base + 3) * cols + col_base]));

      // Step 1: Interleave within 128-bit lanes
      __m256i t0 = _mm256_unpacklo_epi64(r0, r1); // [a00, a10, a02, a12]
      __m256i t1 = _mm256_unpackhi_epi64(r0, r1); // [a01, a11, a03, a13]
      __m256i t2 = _mm256_unpacklo_epi64(r2, r3); // [a20, a30, a22, a32]
      __m256i t3 = _mm256_unpackhi_epi64(r2, r3); // [a21, a31, a23, a33]

      // Step 2: Permute across 128-bit lanes
      __m256i o0 = _mm256_permute2x128_si256(t0, t2, 0x20); // [a00, a10, a20, a30]
      __m256i o1 = _mm256_permute2x128_si256(t1, t3, 0x20); // [a01, a11, a21, a31]
      __m256i o2 = _mm256_permute2x128_si256(t0, t2, 0x31); // [a02, a12, a22, a32]
      __m256i o3 = _mm256_permute2x128_si256(t1, t3, 0x31); // [a03, a13, a23, a33]

      // Store 4 columns (each column has 4 contiguous elements for these rows)
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[(col_base + 0) * rows + row_base]),
                          o0);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[(col_base + 1) * rows + row_base]),
                          o1);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[(col_base + 2) * rows + row_base]),
                          o2);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[(col_base + 3) * rows + row_base]),
                          o3);
    }
  }

  // Handle remaining columns (not divisible by 4)
  for (size_t col = col_blocks * 4; col < cols; ++col) {
    for (size_t row = 0; row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }

  // Handle remaining rows (not divisible by 4)
  for (size_t row = row_blocks * 4; row < rows; ++row) {
    for (size_t col = 0; col < col_blocks * 4; ++col) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

/**
 * @brief Scalar gather + SIMD store.
 *
 * Load 4 scalars from strided positions, pack into SIMD register, store sequentially.
 * Hypothesis: Sequential stores are the bottleneck, so optimize stores even if
 * loads are scalar.
 */
static void transpose_scalar_gather_simd_store(const uint64_t* row_major, uint64_t* col_major,
                                               size_t rows, size_t cols) {
  for (size_t col = 0; col < cols; ++col) {
    size_t row = 0;

    // Process 4 rows at a time with SIMD stores
    for (; row + 4 <= rows; row += 4) {
      // Scalar gather from strided positions
      uint64_t v0 = row_major[(row + 0) * cols + col];
      uint64_t v1 = row_major[(row + 1) * cols + col];
      uint64_t v2 = row_major[(row + 2) * cols + col];
      uint64_t v3 = row_major[(row + 3) * cols + col];

      // Pack into SIMD register and store sequentially
      __m256i v = _mm256_set_epi64x(static_cast<long long>(v3), static_cast<long long>(v2),
                                    static_cast<long long>(v1), static_cast<long long>(v0));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[col * rows + row]), v);
    }

    // Handle remaining rows
    for (; row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

/**
 * @brief Scalar gather + SIMD store + software prefetch.
 *
 * Same as above but with software prefetching for the strided reads.
 */
static void transpose_scalar_gather_simd_store_prefetch(const uint64_t* row_major,
                                                        uint64_t* col_major, size_t rows,
                                                        size_t cols) {
  constexpr size_t PREFETCH_DISTANCE = 16; // Prefetch 16 rows ahead

  for (size_t col = 0; col < cols; ++col) {
    size_t row = 0;

    // Process 4 rows at a time with SIMD stores
    for (; row + 4 <= rows; row += 4) {
      // Prefetch future rows for this column
      if (row + PREFETCH_DISTANCE < rows) {
        _mm_prefetch(
            reinterpret_cast<const char*>(&row_major[(row + PREFETCH_DISTANCE) * cols + col]),
            _MM_HINT_T0);
      }

      // Scalar gather from strided positions
      uint64_t v0 = row_major[(row + 0) * cols + col];
      uint64_t v1 = row_major[(row + 1) * cols + col];
      uint64_t v2 = row_major[(row + 2) * cols + col];
      uint64_t v3 = row_major[(row + 3) * cols + col];

      // Pack into SIMD register and store sequentially
      __m256i v = _mm256_set_epi64x(static_cast<long long>(v3), static_cast<long long>(v2),
                                    static_cast<long long>(v1), static_cast<long long>(v0));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(&col_major[col * rows + row]), v);
    }

    // Handle remaining rows
    for (; row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

/**
 * @brief Non-temporal (streaming) stores.
 *
 * Uses _mm256_stream_si256 to bypass cache on writes.
 * Good for large arrays that won't be read again soon.
 * Requires 32-byte aligned destination.
 */
static void transpose_nontemporal_store(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                        size_t cols) {
  for (size_t col = 0; col < cols; ++col) {
    size_t row = 0;

    // Find aligned start position for this column
    uint64_t* col_ptr = &col_major[col * rows];
    size_t aligned_start = (reinterpret_cast<uintptr_t>(col_ptr) % 32 == 0)
                               ? 0
                               : (32 - (reinterpret_cast<uintptr_t>(col_ptr) % 32)) / 8;

    // Handle unaligned prefix with scalar stores
    for (; row < aligned_start && row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }

    // Process aligned portion with non-temporal stores
    for (; row + 4 <= rows; row += 4) {
      uint64_t v0 = row_major[(row + 0) * cols + col];
      uint64_t v1 = row_major[(row + 1) * cols + col];
      uint64_t v2 = row_major[(row + 2) * cols + col];
      uint64_t v3 = row_major[(row + 3) * cols + col];

      __m256i v = _mm256_set_epi64x(static_cast<long long>(v3), static_cast<long long>(v2),
                                    static_cast<long long>(v1), static_cast<long long>(v0));
      _mm256_stream_si256(reinterpret_cast<__m256i*>(&col_major[col * rows + row]), v);
    }

    // Handle remaining rows
    for (; row < rows; ++row) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }

  // Memory fence to ensure all streaming stores are visible
  _mm_sfence();
}
#endif // __x86_64__

// =============================================================================
// Helper: portable aligned allocation for uint64_t arrays
// =============================================================================

static uint64_t* alloc_aligned_u64(size_t count) {
  return static_cast<uint64_t*>(libvroom::aligned_alloc_portable(count * sizeof(uint64_t)));
}

// =============================================================================
// Benchmarks
// =============================================================================

/**
 * @brief BM_TransposeSingleThreaded - Measure single-threaded transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols
 */
static void BM_TransposeSingleThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t total_elements = rows * cols;

  // Allocate row-major and column-major arrays
  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  // Initialize row-major with sequential values (simulating byte positions)
  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10; // Simulate byte positions
  }

  for (auto _ : state) {
    transpose_single_threaded(row_major, col_major, rows, cols);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  // Report metrics
  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2; // read + write
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);
  state.counters["MemoryMB"] =
      static_cast<double>(total_elements * sizeof(uint64_t) * 2) / (1024.0 * 1024.0);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

/**
 * @brief BM_TransposeMultiThreaded - Measure multi-threaded transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = threads
 */
static void BM_TransposeMultiThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t n_threads = static_cast<size_t>(state.range(2));
  size_t total_elements = rows * cols;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_multi_threaded(row_major, col_major, rows, cols, n_threads);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

/**
 * @brief BM_TransposeBlocked - Measure blocked transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols
 */
static void BM_TransposeBlocked(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t total_elements = rows * cols;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_blocked(row_major, col_major, rows, cols);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

/**
 * @brief BM_TransposeBlockedMultiThreaded - Measure blocked multi-threaded transpose.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = threads
 */
static void BM_TransposeBlockedMultiThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t n_threads = static_cast<size_t>(state.range(2));
  size_t total_elements = rows * cols;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_blocked_multi_threaded(row_major, col_major, rows, cols, n_threads);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

/**
 * @brief BM_TransposeScaling - Compare transpose methods at different scales.
 *
 * Measures single-threaded, multi-threaded (4 threads), and blocked transpose.
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = method
 * Method: 0 = single-threaded, 1 = multi-threaded (4), 2 = blocked, 3 = blocked multi-threaded (4)
 */
static void BM_TransposeScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int method = static_cast<int>(state.range(2));
  size_t total_elements = rows * cols;
  size_t n_threads = 4;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  const char* method_name = "unknown";
  for (auto _ : state) {
    switch (method) {
    case 0:
      transpose_single_threaded(row_major, col_major, rows, cols);
      method_name = "single";
      break;
    case 1:
      transpose_multi_threaded(row_major, col_major, rows, cols, n_threads);
      method_name = "multi4";
      break;
    case 2:
      transpose_blocked(row_major, col_major, rows, cols);
      method_name = "blocked";
      break;
    case 3:
      transpose_blocked_multi_threaded(row_major, col_major, rows, cols, n_threads);
      method_name = "blocked_multi4";
      break;
    }
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  // Suppress unused variable warning
  (void)method_name;

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Method"] = static_cast<double>(method);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

// =============================================================================
// Benchmark Registration
// =============================================================================

// Test matrix from issue #600:
// Rows: 10K, 100K, 1M, 10M
// Cols: 10, 100, 500

// Single-threaded transpose - full test matrix
BENCHMARK(BM_TransposeSingleThreaded)
    ->Args({10000, 10})     // 10K rows, 10 cols
    ->Args({10000, 100})    // 10K rows, 100 cols
    ->Args({10000, 500})    // 10K rows, 500 cols
    ->Args({100000, 10})    // 100K rows, 10 cols
    ->Args({100000, 100})   // 100K rows, 100 cols
    ->Args({100000, 500})   // 100K rows, 500 cols
    ->Args({1000000, 10})   // 1M rows, 10 cols
    ->Args({1000000, 100})  // 1M rows, 100 cols
    ->Args({1000000, 500})  // 1M rows, 500 cols
    ->Args({10000000, 10})  // 10M rows, 10 cols
    ->Args({10000000, 100}) // 10M rows, 100 cols
    ->Unit(benchmark::kMillisecond);

// Multi-threaded transpose - compare thread counts
// Args: rows, cols, threads
BENCHMARK(BM_TransposeMultiThreaded)
    // 100K rows, 100 cols - varying threads
    ->Args({100000, 100, 1})
    ->Args({100000, 100, 2})
    ->Args({100000, 100, 4})
    ->Args({100000, 100, 8})
    // 1M rows, 100 cols - varying threads
    ->Args({1000000, 100, 1})
    ->Args({1000000, 100, 2})
    ->Args({1000000, 100, 4})
    ->Args({1000000, 100, 8})
    // 1M rows, 500 cols - varying threads
    ->Args({1000000, 500, 1})
    ->Args({1000000, 500, 2})
    ->Args({1000000, 500, 4})
    ->Args({1000000, 500, 8})
    ->Unit(benchmark::kMillisecond);

// Blocked transpose - full test matrix
BENCHMARK(BM_TransposeBlocked)
    ->Args({10000, 10})
    ->Args({10000, 100})
    ->Args({10000, 500})
    ->Args({100000, 10})
    ->Args({100000, 100})
    ->Args({100000, 500})
    ->Args({1000000, 10})
    ->Args({1000000, 100})
    ->Args({1000000, 500})
    ->Args({10000000, 10})
    ->Args({10000000, 100})
    ->Unit(benchmark::kMillisecond);

// Blocked multi-threaded transpose
BENCHMARK(BM_TransposeBlockedMultiThreaded)
    ->Args({100000, 100, 4})
    ->Args({1000000, 100, 4})
    ->Args({1000000, 500, 4})
    ->Args({10000000, 10, 4})
    ->Args({10000000, 100, 4})
    ->Unit(benchmark::kMillisecond);

// Scaling comparison - all methods at key sizes
// Args: rows, cols, method (0=single, 1=multi4, 2=blocked, 3=blocked_multi4)
BENCHMARK(BM_TransposeScaling)
    // 100K x 100 - all methods
    ->Args({100000, 100, 0})
    ->Args({100000, 100, 1})
    ->Args({100000, 100, 2})
    ->Args({100000, 100, 3})
    // 1M x 100 - all methods
    ->Args({1000000, 100, 0})
    ->Args({1000000, 100, 1})
    ->Args({1000000, 100, 2})
    ->Args({1000000, 100, 3})
    // 1M x 500 - all methods
    ->Args({1000000, 500, 0})
    ->Args({1000000, 500, 1})
    ->Args({1000000, 500, 2})
    ->Args({1000000, 500, 3})
    ->Unit(benchmark::kMillisecond);

// =============================================================================
// Multi-threaded SIMD Implementations
// =============================================================================

/**
 * @brief Multi-threaded row-first scalar transpose.
 * Each thread handles a range of rows.
 */
static void transpose_row_first_mt(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                   size_t cols, size_t n_threads) {
  if (n_threads <= 1) {
    transpose_single_threaded(row_major, col_major, rows, cols);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  size_t rows_per_thread = (rows + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t row_start = t * rows_per_thread;
    size_t row_end = std::min(row_start + rows_per_thread, rows);

    if (row_start >= rows)
      break;

    threads.emplace_back([=]() {
      for (size_t row = row_start; row < row_end; ++row) {
        for (size_t col = 0; col < cols; ++col) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

/**
 * @brief Multi-threaded column-first scalar transpose.
 * Each thread handles a range of columns (better write locality per thread).
 */
static void transpose_col_first_mt(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                   size_t cols, size_t n_threads) {
  if (n_threads <= 1) {
    transpose_column_first_scalar(row_major, col_major, rows, cols);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  size_t cols_per_thread = (cols + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t col_start = t * cols_per_thread;
    size_t col_end = std::min(col_start + cols_per_thread, cols);

    if (col_start >= cols)
      break;

    threads.emplace_back([=]() {
      for (size_t col = col_start; col < col_end; ++col) {
        for (size_t row = 0; row < rows; ++row) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

#ifdef __x86_64__
/**
 * @brief Multi-threaded SIMD 4x4 block transpose.
 * Each thread handles a range of row blocks.
 */
static void transpose_simd_4x4_mt(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                  size_t cols, size_t n_threads) {
  if (n_threads <= 1) {
    transpose_simd_4x4_block(row_major, col_major, rows, cols);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  size_t row_blocks = rows / 4;
  size_t col_blocks = cols / 4;
  size_t blocks_per_thread = (row_blocks + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t rb_start = t * blocks_per_thread;
    size_t rb_end = std::min(rb_start + blocks_per_thread, row_blocks);

    if (rb_start >= row_blocks)
      break;

    threads.emplace_back([=]() {
      for (size_t rb = rb_start; rb < rb_end; ++rb) {
        for (size_t cb = 0; cb < col_blocks; ++cb) {
          size_t row_base = rb * 4;
          size_t col_base = cb * 4;

          __m256i r0 = _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(&row_major[(row_base + 0) * cols + col_base]));
          __m256i r1 = _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(&row_major[(row_base + 1) * cols + col_base]));
          __m256i r2 = _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(&row_major[(row_base + 2) * cols + col_base]));
          __m256i r3 = _mm256_loadu_si256(
              reinterpret_cast<const __m256i*>(&row_major[(row_base + 3) * cols + col_base]));

          __m256i t0 = _mm256_unpacklo_epi64(r0, r1);
          __m256i t1 = _mm256_unpackhi_epi64(r0, r1);
          __m256i t2 = _mm256_unpacklo_epi64(r2, r3);
          __m256i t3 = _mm256_unpackhi_epi64(r2, r3);

          __m256i o0 = _mm256_permute2x128_si256(t0, t2, 0x20);
          __m256i o1 = _mm256_permute2x128_si256(t1, t3, 0x20);
          __m256i o2 = _mm256_permute2x128_si256(t0, t2, 0x31);
          __m256i o3 = _mm256_permute2x128_si256(t1, t3, 0x31);

          _mm256_storeu_si256(
              reinterpret_cast<__m256i*>(&col_major[(col_base + 0) * rows + row_base]), o0);
          _mm256_storeu_si256(
              reinterpret_cast<__m256i*>(&col_major[(col_base + 1) * rows + row_base]), o1);
          _mm256_storeu_si256(
              reinterpret_cast<__m256i*>(&col_major[(col_base + 2) * rows + row_base]), o2);
          _mm256_storeu_si256(
              reinterpret_cast<__m256i*>(&col_major[(col_base + 3) * rows + row_base]), o3);
        }
      }

      // Handle remaining columns for this thread's rows
      size_t row_start = rb_start * 4;
      size_t row_end = std::min(rb_end * 4, rows);
      for (size_t col = col_blocks * 4; col < cols; ++col) {
        for (size_t row = row_start; row < row_end; ++row) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Handle remaining rows (not divisible by 4) - single threaded for simplicity
  for (size_t row = row_blocks * 4; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}
#endif // __x86_64__

// =============================================================================
// SIMD Benchmarks
// =============================================================================

/**
 * @brief BM_TransposeSIMD - Compare SIMD transpose methods.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = method
 * Methods:
 *   0 = Row-first scalar (baseline, sequential reads, strided writes)
 *   1 = Column-first scalar (strided reads, sequential writes)
 *   2 = SIMD 4x4 block transpose (AVX2)
 *   3 = Scalar gather + SIMD store
 *   4 = Scalar gather + SIMD store + prefetch
 *   5 = Non-temporal stores
 */
static void BM_TransposeSIMD(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int method = static_cast<int>(state.range(2));
  size_t total_elements = rows * cols;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  const char* method_name = "unknown";
  for (auto _ : state) {
    switch (method) {
    case 0:
      transpose_single_threaded(row_major, col_major, rows, cols);
      method_name = "row_first_scalar";
      break;
    case 1:
      transpose_column_first_scalar(row_major, col_major, rows, cols);
      method_name = "col_first_scalar";
      break;
#ifdef __x86_64__
    case 2:
      transpose_simd_4x4_block(row_major, col_major, rows, cols);
      method_name = "simd_4x4_block";
      break;
    case 3:
      transpose_scalar_gather_simd_store(row_major, col_major, rows, cols);
      method_name = "scalar_gather_simd_store";
      break;
    case 4:
      transpose_scalar_gather_simd_store_prefetch(row_major, col_major, rows, cols);
      method_name = "scalar_gather_simd_store_prefetch";
      break;
    case 5:
      transpose_nontemporal_store(row_major, col_major, rows, cols);
      method_name = "nontemporal_store";
      break;
#endif
    default:
      // Fallback for non-x86 or unknown method
      transpose_single_threaded(row_major, col_major, rows, cols);
      method_name = "fallback";
      break;
    }
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  // Suppress unused variable warning
  (void)method_name;

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Method"] = static_cast<double>(method);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

// SIMD method comparison at key sizes
// Methods: 0=row_first, 1=col_first, 2=simd_4x4, 3=gather_store, 4=gather_prefetch, 5=nontemporal
BENCHMARK(BM_TransposeSIMD)
    // 100K x 100 - all methods
    ->Args({100000, 100, 0}) // row-first scalar
    ->Args({100000, 100, 1}) // col-first scalar
    ->Args({100000, 100, 2}) // simd 4x4 block
    ->Args({100000, 100, 3}) // scalar gather + simd store
    ->Args({100000, 100, 4}) // + prefetch
    ->Args({100000, 100, 5}) // non-temporal
    // 1M x 100 - all methods
    ->Args({1000000, 100, 0})
    ->Args({1000000, 100, 1})
    ->Args({1000000, 100, 2})
    ->Args({1000000, 100, 3})
    ->Args({1000000, 100, 4})
    ->Args({1000000, 100, 5})
    // 1M x 500 - all methods (wide matrix)
    ->Args({1000000, 500, 0})
    ->Args({1000000, 500, 1})
    ->Args({1000000, 500, 2})
    ->Args({1000000, 500, 3})
    ->Args({1000000, 500, 4})
    ->Args({1000000, 500, 5})
    // 10M x 10 - all methods (tall narrow matrix)
    ->Args({10000000, 10, 0})
    ->Args({10000000, 10, 1})
    ->Args({10000000, 10, 2})
    ->Args({10000000, 10, 3})
    ->Args({10000000, 10, 4})
    ->Args({10000000, 10, 5})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief BM_TransposeSIMD_MT - Compare multi-threaded transpose methods.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols,
 *             state.range(2) = method, state.range(3) = threads
 * Methods:
 *   0 = Row-first scalar MT
 *   1 = Column-first scalar MT
 *   2 = SIMD 4x4 block MT
 */
static void BM_TransposeSIMD_MT(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int method = static_cast<int>(state.range(2));
  size_t n_threads = static_cast<size_t>(state.range(3));
  size_t total_elements = rows * cols;

  auto row_major = alloc_aligned_u64(total_elements);
  auto col_major = alloc_aligned_u64(total_elements);

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    libvroom::aligned_free_portable(row_major);
    libvroom::aligned_free_portable(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  const char* method_name = "unknown";
  for (auto _ : state) {
    switch (method) {
    case 0:
      transpose_row_first_mt(row_major, col_major, rows, cols, n_threads);
      method_name = "row_first_mt";
      break;
    case 1:
      transpose_col_first_mt(row_major, col_major, rows, cols, n_threads);
      method_name = "col_first_mt";
      break;
#ifdef __x86_64__
    case 2:
      transpose_simd_4x4_mt(row_major, col_major, rows, cols, n_threads);
      method_name = "simd_4x4_mt";
      break;
#endif
    default:
      transpose_row_first_mt(row_major, col_major, rows, cols, n_threads);
      method_name = "fallback";
      break;
    }
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  (void)method_name;

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Method"] = static_cast<double>(method);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  libvroom::aligned_free_portable(row_major);
  libvroom::aligned_free_portable(col_major);
}

// Multi-threaded comparison
// Methods: 0=row_first_mt, 1=col_first_mt, 2=simd_4x4_mt
// Args: rows, cols, method, threads
BENCHMARK(BM_TransposeSIMD_MT)
    // 1M x 100 - compare methods at different thread counts
    ->Args({1000000, 100, 0, 1}) // row-first, 1 thread (baseline)
    ->Args({1000000, 100, 0, 2})
    ->Args({1000000, 100, 0, 4})
    ->Args({1000000, 100, 0, 8})
    ->Args({1000000, 100, 1, 1}) // col-first, 1 thread
    ->Args({1000000, 100, 1, 2})
    ->Args({1000000, 100, 1, 4})
    ->Args({1000000, 100, 1, 8})
    ->Args({1000000, 100, 2, 1}) // simd 4x4, 1 thread
    ->Args({1000000, 100, 2, 2})
    ->Args({1000000, 100, 2, 4})
    ->Args({1000000, 100, 2, 8})
    // 1M x 500 - wide matrix
    ->Args({1000000, 500, 0, 1})
    ->Args({1000000, 500, 0, 4})
    ->Args({1000000, 500, 0, 8})
    ->Args({1000000, 500, 1, 1})
    ->Args({1000000, 500, 1, 4})
    ->Args({1000000, 500, 1, 8})
    ->Args({1000000, 500, 2, 1})
    ->Args({1000000, 500, 2, 4})
    ->Args({1000000, 500, 2, 8})
    // 10M x 10 - narrow matrix
    ->Args({10000000, 10, 0, 1})
    ->Args({10000000, 10, 0, 4})
    ->Args({10000000, 10, 0, 8})
    ->Args({10000000, 10, 1, 1})
    ->Args({10000000, 10, 1, 4})
    ->Args({10000000, 10, 1, 8})
    ->Args({10000000, 10, 2, 1})
    ->Args({10000000, 10, 2, 4})
    ->Args({10000000, 10, 2, 8})
    ->Unit(benchmark::kMillisecond);
