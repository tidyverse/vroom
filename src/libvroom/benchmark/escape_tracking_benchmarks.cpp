/**
 * @file escape_tracking_benchmarks.cpp
 * @brief Benchmarks for per-column escape tracking and fast-path extraction.
 *
 * This file benchmarks the performance impact of computing per-column escape
 * info and using it for fast-path string extraction.
 *
 * Related: Issue #616 (per-column escape tracking)
 *
 * Hypothesis:
 * - Computing escape info: < 10% of parse time (amortized over extractions)
 * - Fast-path extraction: 2-5x faster for escape-free columns
 * - Overall speedup: significant for string-heavy workloads
 */

#include "libvroom.h"

#include "value_extraction.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

// Generate CSV data with controlled escape characteristics
std::string generate_csv(size_t rows, size_t cols, double quote_ratio, double escape_ratio) {
  std::mt19937 gen(42); // Fixed seed for reproducibility
  std::uniform_real_distribution<> dist(0.0, 1.0);

  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows
  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';

      bool quoted = dist(gen) < quote_ratio;
      bool escaped = quoted && dist(gen) < escape_ratio;

      if (escaped) {
        // Field with escaped quotes: "value ""with"" quotes"
        oss << "\"value" << r << "_" << c << " \"\"escaped\"\" data\"";
      } else if (quoted) {
        // Simple quoted field: "value"
        oss << "\"value" << r << "_" << c << "\"";
      } else {
        // Unquoted field
        oss << "value" << r << "_" << c;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// Aligned buffer for SIMD operations
class AlignedTestBuffer {
public:
  explicit AlignedTestBuffer(const std::string& content) {
    size_ = content.size();
    // Allocate with padding for SIMD
    buffer_ = static_cast<uint8_t*>(aligned_alloc(64, size_ + 64));
    std::memcpy(buffer_, content.data(), size_);
    std::memset(buffer_ + size_, 0, 64);
  }

  ~AlignedTestBuffer() { free(buffer_); }

  const uint8_t* data() const { return buffer_; }
  size_t size() const { return size_; }

private:
  uint8_t* buffer_;
  size_t size_;
};

} // namespace

// ============================================================================
// Escape Info Computation Overhead
// ============================================================================

/**
 * @brief Measure the overhead of computing escape info.
 *
 * Compares parse time with and without escape info computation.
 */
static void BM_EscapeInfoComputation(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  double quote_ratio = 0.3; // 30% quoted fields (typical)

  std::string csv = generate_csv(rows, cols, quote_ratio, 0.0);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size());
    result.idx.compute_column_escape_info(buffer.data(), buffer.size(), '"');
    benchmark::DoNotOptimize(result.idx.col_escape_info);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

BENCHMARK(BM_EscapeInfoComputation)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({1000, 100})
    ->Args({10000, 100})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Measure just the escape info scan (without parsing).
 *
 * Note: We measure by calling compute_column_escape_info() which is idempotent.
 * The first call does the work, subsequent calls return early. We parse fresh
 * for each iteration to measure the actual scan cost.
 */
static void BM_EscapeInfoScanOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  double quote_ratio = 0.3;

  std::string csv = generate_csv(rows, cols, quote_ratio, 0.0);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;

  for (auto _ : state) {
    // Parse fresh each time to get a clean ParseIndex without escape info
    auto result = parser.parse(buffer.data(), buffer.size());
    result.idx.compute_column_escape_info(buffer.data(), buffer.size(), '"');
    benchmark::DoNotOptimize(result.idx.col_escape_info);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

BENCHMARK(BM_EscapeInfoScanOnly)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({1000, 100})
    ->Args({10000, 100})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// String Extraction Performance
// ============================================================================

/**
 * @brief Extract all strings from a column WITHOUT escape info (baseline).
 */
static void BM_ExtractColumn_NoEscapeInfo(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  double quote_ratio = state.range(1) / 100.0;

  std::string csv = generate_csv(rows, 10, quote_ratio, 0.0);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  // Do NOT compute escape info - this is the baseline

  for (auto _ : state) {
    auto strings = extractor.extract_column_string(0);
    benchmark::DoNotOptimize(strings.data());
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["QuoteRatio"] = quote_ratio;
}

BENCHMARK(BM_ExtractColumn_NoEscapeInfo)
    ->Args({10000, 0})   // 0% quoted
    ->Args({10000, 30})  // 30% quoted
    ->Args({10000, 100}) // 100% quoted
    ->Args({100000, 0})
    ->Args({100000, 30})
    ->Args({100000, 100})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Extract all strings from a column WITH escape info (optimized).
 */
static void BM_ExtractColumn_WithEscapeInfo(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  double quote_ratio = state.range(1) / 100.0;

  std::string csv = generate_csv(rows, 10, quote_ratio, 0.0);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  // Compute escape info for fast-path
  extractor.compute_column_escape_info();

  for (auto _ : state) {
    auto strings = extractor.extract_column_string(0);
    benchmark::DoNotOptimize(strings.data());
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["ZeroCopy"] = extractor.column_allows_zero_copy(0) ? 1.0 : 0.0;
}

BENCHMARK(BM_ExtractColumn_WithEscapeInfo)
    ->Args({10000, 0})   // 0% quoted
    ->Args({10000, 30})  // 30% quoted
    ->Args({10000, 100}) // 100% quoted
    ->Args({100000, 0})
    ->Args({100000, 30})
    ->Args({100000, 100})
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// Optimistic Per-Field Approach (Alternative Implementation)
// ============================================================================

/**
 * @brief Optimistic unescape: try fast path, detect and fallback if needed.
 *
 * This avoids pre-computation by scanning during extraction.
 * Algorithm:
 * 1. If field starts with quote, scan for "" pattern
 * 2. If no "" found, just strip outer quotes (fast)
 * 3. If "" found, do full unescape (slow)
 */
std::string get_string_optimistic(std::string_view field, char quote_char) {
  if (field.empty()) {
    return std::string();
  }

  // Not quoted - return as-is
  if (field.front() != quote_char) {
    return std::string(field);
  }

  // Quoted field - check for doubled quotes
  if (field.size() < 2 || field.back() != quote_char) {
    return std::string(field); // Malformed, return as-is
  }

  // Scan interior for doubled quotes
  const char* p = field.data() + 1;
  const char* end = field.data() + field.size() - 1;
  while (p < end) {
    if (*p == quote_char) {
      if (p + 1 < end && *(p + 1) == quote_char) {
        // Found doubled quote - need full unescape
        // Fall back to slow path
        std::string result;
        result.reserve(field.size() - 2);
        for (const char* q = field.data() + 1; q < end;) {
          if (*q == quote_char && q + 1 < end && *(q + 1) == quote_char) {
            result += quote_char;
            q += 2;
          } else {
            result += *q++;
          }
        }
        return result;
      }
      // Single quote at end is the closing quote
      break;
    }
    ++p;
  }

  // No doubled quotes - just strip outer quotes
  return std::string(field.substr(1, field.size() - 2));
}

/**
 * @brief Column extraction using optimistic per-field approach.
 */
static void BM_ExtractColumn_Optimistic(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  double quote_ratio = state.range(1) / 100.0;
  double escape_ratio = state.range(2) / 100.0;

  std::string csv = generate_csv(rows, 10, quote_ratio, escape_ratio);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    std::vector<std::string> strings;
    strings.reserve(extractor.num_rows());
    for (size_t row = 0; row < extractor.num_rows(); ++row) {
      auto sv = extractor.get_string_view(row, 0);
      strings.push_back(get_string_optimistic(sv, '"'));
    }
    benchmark::DoNotOptimize(strings.data());
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_ExtractColumn_Optimistic)
    ->Args({10000, 30, 0})  // 30% quoted, 0% escaped (typical)
    ->Args({10000, 30, 5})  // 30% quoted, 5% escaped
    ->Args({10000, 100, 0}) // 100% quoted, 0% escaped
    ->Args({10000, 100, 5}) // 100% quoted, 5% escaped
    ->Args({100000, 30, 0})
    ->Args({100000, 30, 5})
    ->Args({100000, 100, 0})
    ->Args({100000, 100, 5})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Single field optimistic extraction (microbenchmark).
 */
static void BM_GetString_Optimistic(benchmark::State& state) {
  bool quoted = state.range(0) != 0;
  bool escaped = state.range(1) != 0;

  std::string csv;
  if (escaped) {
    csv = "a\n\"val\"\"ue\"\n"; // Has doubled quote
  } else if (quoted) {
    csv = "a\n\"value\"\n";
  } else {
    csv = "a\nvalue\n";
  }
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    auto sv = extractor.get_string_view(0, 0);
    auto str = get_string_optimistic(sv, '"');
    benchmark::DoNotOptimize(str);
  }

  state.counters["Quoted"] = quoted ? 1.0 : 0.0;
  state.counters["Escaped"] = escaped ? 1.0 : 0.0;
}

BENCHMARK(BM_GetString_Optimistic)
    ->Args({0, 0}) // Unquoted
    ->Args({1, 0}) // Quoted, no escape
    ->Args({1, 1}) // Quoted with escape
    ->Unit(benchmark::kNanosecond);

// ============================================================================
// Single String Extraction (microbenchmark)
// ============================================================================

/**
 * @brief Single get_string() call without escape info.
 */
static void BM_GetString_NoEscapeInfo(benchmark::State& state) {
  bool quoted = state.range(0) != 0;

  std::string csv = quoted ? "a\n\"value\"\n" : "a\nvalue\n";
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    auto str = extractor.get_string(0, 0);
    benchmark::DoNotOptimize(str);
  }

  state.counters["Quoted"] = quoted ? 1.0 : 0.0;
}

BENCHMARK(BM_GetString_NoEscapeInfo)
    ->Arg(0) // Unquoted
    ->Arg(1) // Quoted
    ->Unit(benchmark::kNanosecond);

/**
 * @brief Single get_string() call with escape info.
 */
static void BM_GetString_WithEscapeInfo(benchmark::State& state) {
  bool quoted = state.range(0) != 0;

  std::string csv = quoted ? "a\n\"value\"\n" : "a\nvalue\n";
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);
  extractor.compute_column_escape_info();

  for (auto _ : state) {
    auto str = extractor.get_string(0, 0);
    benchmark::DoNotOptimize(str);
  }

  state.counters["Quoted"] = quoted ? 1.0 : 0.0;
  state.counters["ZeroCopy"] = extractor.column_allows_zero_copy(0) ? 1.0 : 0.0;
}

BENCHMARK(BM_GetString_WithEscapeInfo)
    ->Arg(0) // Unquoted
    ->Arg(1) // Quoted
    ->Unit(benchmark::kNanosecond);

// ============================================================================
// Realistic Workload: Mixed Extraction
// ============================================================================

/**
 * @brief Simulate realistic workload: parse + compute escape info + extract multiple columns.
 */
static void BM_RealisticWorkload(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  bool use_escape_info = state.range(1) != 0;

  // Generate realistic CSV: 30% quoted, 5% with escapes
  std::string csv = generate_csv(rows, 10, 0.3, 0.05);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size());
    libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

    if (use_escape_info) {
      extractor.compute_column_escape_info();
    }

    // Extract 5 columns (typical workload)
    for (size_t col = 0; col < 5; ++col) {
      auto strings = extractor.extract_column_string(col);
      benchmark::DoNotOptimize(strings.data());
    }
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["UseEscapeInfo"] = use_escape_info ? 1.0 : 0.0;
}

BENCHMARK(BM_RealisticWorkload)
    ->Args({10000, 0})  // Without escape info
    ->Args({10000, 1})  // With escape info
    ->Args({100000, 0}) // Without escape info
    ->Args({100000, 1}) // With escape info
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Realistic workload using optimistic per-field approach.
 */
static void BM_RealisticWorkload_Optimistic(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));

  // Generate realistic CSV: 30% quoted, 5% with escapes
  std::string csv = generate_csv(rows, 10, 0.3, 0.05);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size());
    libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

    // Extract 5 columns using optimistic approach
    for (size_t col = 0; col < 5; ++col) {
      std::vector<std::string> strings;
      strings.reserve(extractor.num_rows());
      for (size_t row = 0; row < extractor.num_rows(); ++row) {
        auto sv = extractor.get_string_view(row, col);
        strings.push_back(get_string_optimistic(sv, '"'));
      }
      benchmark::DoNotOptimize(strings.data());
    }
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
}

BENCHMARK(BM_RealisticWorkload_Optimistic)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);

// ============================================================================
// Comparison: Pre-computed vs Optimistic for varying escape ratios
// ============================================================================

/**
 * @brief Compare approaches across different escape ratios.
 *
 * This helps understand when each approach is best.
 */
static void BM_Comparison_VaryingEscapeRatio(benchmark::State& state) {
  size_t rows = 100000;
  int approach = static_cast<int>(state.range(0)); // 0=baseline, 1=precomputed, 2=optimistic
  double escape_ratio = state.range(1) / 100.0;

  // 30% quoted fields with varying escape ratio
  std::string csv = generate_csv(rows, 10, 0.3, escape_ratio);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  if (approach == 1) {
    // Pre-compute escape info
    extractor.compute_column_escape_info();
  }

  for (auto _ : state) {
    if (approach == 2) {
      // Optimistic approach
      std::vector<std::string> strings;
      strings.reserve(extractor.num_rows());
      for (size_t row = 0; row < extractor.num_rows(); ++row) {
        auto sv = extractor.get_string_view(row, 0);
        strings.push_back(get_string_optimistic(sv, '"'));
      }
      benchmark::DoNotOptimize(strings.data());
    } else {
      // Baseline (0) or precomputed (1) - both use extract_column_string
      auto strings = extractor.extract_column_string(0);
      benchmark::DoNotOptimize(strings.data());
    }
  }

  state.counters["Approach"] = static_cast<double>(approach);
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_Comparison_VaryingEscapeRatio)
    // Baseline (no escape info)
    ->Args({0, 0})  // 0% escapes
    ->Args({0, 5})  // 5% escapes
    ->Args({0, 20}) // 20% escapes
    // Precomputed escape info
    ->Args({1, 0})
    ->Args({1, 5})
    ->Args({1, 20})
    // Optimistic per-field
    ->Args({2, 0})
    ->Args({2, 5})
    ->Args({2, 20})
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// SIMD Escape Detection Overhead Measurement
// ============================================================================

/**
 * @brief Simulate the overhead of adding escape detection to SIMD pass.
 *
 * Measures just the additional operation: quotes & (quotes >> 1)
 * This tells us if adding this to the hot path would be negligible.
 */
static void BM_SIMDEscapeDetectionOverhead(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));

  std::string csv = generate_csv(rows, 10, 0.3, 0.05);
  AlignedTestBuffer buffer(csv);

  // Pre-generate some quote masks (simulating what SIMD would produce)
  std::vector<uint64_t> quote_masks;
  quote_masks.reserve(buffer.size() / 64 + 1);
  for (size_t i = 0; i < buffer.size(); i += 64) {
    // Simulate a quote mask with ~10% quote positions
    uint64_t mask = 0;
    for (int j = 0; j < 64 && i + j < buffer.size(); ++j) {
      if (buffer.data()[i + j] == '"') {
        mask |= (1ULL << j);
      }
    }
    quote_masks.push_back(mask);
  }

  for (auto _ : state) {
    bool has_doubled = false;
    for (uint64_t quotes : quote_masks) {
      // This is the additional operation we'd add to the SIMD pass
      uint64_t doubled = quotes & (quotes >> 1);
      has_doubled |= (doubled != 0);
    }
    benchmark::DoNotOptimize(has_doubled);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Blocks"] = static_cast<double>(quote_masks.size());
}

BENCHMARK(BM_SIMDEscapeDetectionOverhead)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Unit(benchmark::kMicrosecond);

/**
 * @brief Compare full parse time with vs without escape tracking overhead.
 */
static void BM_ParseWithEscapeTracking(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  bool track_escapes = state.range(1) != 0;

  std::string csv = generate_csv(rows, 10, 0.3, 0.05);
  AlignedTestBuffer buffer(csv);

  libvroom::Parser parser;

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size());

    if (track_escapes) {
      // Simulate what would happen if we tracked escapes in SIMD pass
      // Walk through buffer and detect doubled quotes
      bool has_escapes = false;
      const uint8_t* data = buffer.data();
      size_t len = buffer.size();

      for (size_t i = 0; i + 1 < len && !has_escapes; ++i) {
        if (data[i] == '"' && data[i + 1] == '"') {
          has_escapes = true;
        }
      }
      benchmark::DoNotOptimize(has_escapes);
    }

    benchmark::DoNotOptimize(result.idx.indexes);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["TrackEscapes"] = track_escapes ? 1.0 : 0.0;
}

BENCHMARK(BM_ParseWithEscapeTracking)
    ->Args({10000, 0}) // Without tracking
    ->Args({10000, 1}) // With tracking (simulated)
    ->Args({100000, 0})
    ->Args({100000, 1})
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// Per-Block Bitmap Approach
// ============================================================================

/**
 * @brief Simulate per-block escape bitmap: build bitmap during "parsing".
 *
 * This measures the overhead of tracking escaped blocks during parsing.
 * Each bit represents whether a 64-byte block contains doubled quotes.
 */
static void BM_PerBlockBitmap_Build(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));

  std::string csv = generate_csv(rows, 10, 0.3, 0.05);
  AlignedTestBuffer buffer(csv);

  for (auto _ : state) {
    // Build the escape bitmap
    size_t num_blocks = (buffer.size() + 63) / 64;
    std::vector<uint8_t> escape_bitmap((num_blocks + 7) / 8, 0);

    const uint8_t* data = buffer.data();
    for (size_t block = 0; block < num_blocks; ++block) {
      size_t start = block * 64;
      size_t end = std::min(start + 64, buffer.size());

      bool block_has_escape = false;
      for (size_t i = start; i + 1 < end; ++i) {
        if (data[i] == '"' && data[i + 1] == '"') {
          block_has_escape = true;
          break;
        }
      }

      if (block_has_escape) {
        escape_bitmap[block / 8] |= (1 << (block % 8));
      }
    }
    benchmark::DoNotOptimize(escape_bitmap.data());
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Blocks"] = static_cast<double>((buffer.size() + 63) / 64);
}

BENCHMARK(BM_PerBlockBitmap_Build)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);

/**
 * @brief Per-block bitmap extraction: check bitmap before unescaping.
 *
 * For a field, check if it spans any blocks with escapes.
 * If no overlapping blocks have escapes, skip unescape entirely.
 */
std::string get_string_per_block(std::string_view field, char quote_char,
                                 const std::vector<uint8_t>& escape_bitmap,
                                 size_t field_start_offset) {
  if (field.empty()) {
    return std::string();
  }

  // Not quoted - return as-is
  if (field.front() != quote_char) {
    return std::string(field);
  }

  // Check for closing quote
  if (field.size() < 2 || field.back() != quote_char) {
    return std::string(field); // Malformed
  }

  // Check if field spans any blocks with escapes
  size_t start_block = field_start_offset / 64;
  size_t end_block = (field_start_offset + field.size() - 1) / 64;

  bool might_have_escapes = false;
  for (size_t block = start_block; block <= end_block && !might_have_escapes; ++block) {
    if (escape_bitmap[block / 8] & (1 << (block % 8))) {
      might_have_escapes = true;
    }
  }

  if (!might_have_escapes) {
    // Fast path: no escapes in this region, just strip quotes
    return std::string(field.substr(1, field.size() - 2));
  }

  // Slow path: might have escapes, do full unescape
  std::string result;
  result.reserve(field.size() - 2);
  const char* p = field.data() + 1;
  const char* end = field.data() + field.size() - 1;
  while (p < end) {
    if (*p == quote_char && p + 1 < end && *(p + 1) == quote_char) {
      result += quote_char;
      p += 2;
    } else {
      result += *p++;
    }
  }
  return result;
}

static void BM_PerBlockBitmap_Extract(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  double escape_ratio = state.range(1) / 100.0;

  std::string csv = generate_csv(rows, 10, 0.3, escape_ratio);
  AlignedTestBuffer buffer(csv);

  // Pre-build the escape bitmap
  size_t num_blocks = (buffer.size() + 63) / 64;
  std::vector<uint8_t> escape_bitmap((num_blocks + 7) / 8, 0);

  const uint8_t* data = buffer.data();
  for (size_t block = 0; block < num_blocks; ++block) {
    size_t start = block * 64;
    size_t end = std::min(start + 64, buffer.size());

    bool block_has_escape = false;
    for (size_t i = start; i + 1 < end; ++i) {
      if (data[i] == '"' && data[i + 1] == '"') {
        block_has_escape = true;
        break;
      }
    }

    if (block_has_escape) {
      escape_bitmap[block / 8] |= (1 << (block % 8));
    }
  }

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    std::vector<std::string> strings;
    strings.reserve(extractor.num_rows());
    for (size_t row = 0; row < extractor.num_rows(); ++row) {
      auto sv = extractor.get_string_view(row, 0);
      // Calculate offset from buffer start
      size_t offset = sv.data() - reinterpret_cast<const char*>(buffer.data());
      strings.push_back(get_string_per_block(sv, '"', escape_bitmap, offset));
    }
    benchmark::DoNotOptimize(strings.data());
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_PerBlockBitmap_Extract)
    ->Args({10000, 0})  // 0% escapes
    ->Args({10000, 5})  // 5% escapes
    ->Args({10000, 20}) // 20% escapes
    ->Args({100000, 0})
    ->Args({100000, 5})
    ->Args({100000, 20})
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// Head-to-Head Comparison: Optimistic vs Per-Block vs Global Flag
// ============================================================================

/**
 * @brief Global file-level escape flag approach.
 *
 * Track a single bit: does the file have ANY doubled quotes?
 * If not, skip all escape processing. If yes, use full unescape.
 */
static void BM_GlobalFlag_Extract(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  double escape_ratio = state.range(1) / 100.0;

  std::string csv = generate_csv(rows, 10, 0.3, escape_ratio);
  AlignedTestBuffer buffer(csv);

  // Detect if file has any escapes
  bool file_has_escapes = false;
  const uint8_t* data = buffer.data();
  for (size_t i = 0; i + 1 < buffer.size() && !file_has_escapes; ++i) {
    if (data[i] == '"' && data[i + 1] == '"') {
      file_has_escapes = true;
    }
  }

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    std::vector<std::string> strings;
    strings.reserve(extractor.num_rows());
    for (size_t row = 0; row < extractor.num_rows(); ++row) {
      auto sv = extractor.get_string_view(row, 0);

      if (sv.empty()) {
        strings.emplace_back();
        continue;
      }

      if (!file_has_escapes) {
        // Fast path: no escapes anywhere, just handle quotes
        if (sv.front() == '"' && sv.size() >= 2 && sv.back() == '"') {
          strings.push_back(std::string(sv.substr(1, sv.size() - 2)));
        } else {
          strings.push_back(std::string(sv));
        }
      } else {
        // Slow path: file has escapes, do full unescape
        strings.push_back(get_string_optimistic(sv, '"'));
      }
    }
    benchmark::DoNotOptimize(strings.data());
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["EscapeRatio"] = escape_ratio;
  state.counters["FileHasEscapes"] = file_has_escapes ? 1.0 : 0.0;
}

BENCHMARK(BM_GlobalFlag_Extract)
    ->Args({10000, 0})
    ->Args({10000, 5})
    ->Args({10000, 20})
    ->Args({100000, 0})
    ->Args({100000, 5})
    ->Args({100000, 20})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Full head-to-head: all approaches at varying escape ratios.
 *
 * Compares:
 * - 0: Baseline (always unescape)
 * - 1: Optimistic per-field
 * - 2: Per-block bitmap
 * - 3: Global file flag
 */
static void BM_HeadToHead_AllApproaches(benchmark::State& state) {
  size_t rows = 100000;
  int approach = static_cast<int>(state.range(0));
  double escape_ratio = state.range(1) / 100.0;

  std::string csv = generate_csv(rows, 10, 0.3, escape_ratio);
  AlignedTestBuffer buffer(csv);

  // Pre-build escape bitmap for approach 2
  size_t num_blocks = (buffer.size() + 63) / 64;
  std::vector<uint8_t> escape_bitmap((num_blocks + 7) / 8, 0);

  // Detect escapes for approaches 2 and 3
  bool file_has_escapes = false;
  const uint8_t* data = buffer.data();
  for (size_t block = 0; block < num_blocks; ++block) {
    size_t start = block * 64;
    size_t end = std::min(start + 64, buffer.size());
    for (size_t i = start; i + 1 < end; ++i) {
      if (data[i] == '"' && data[i + 1] == '"') {
        escape_bitmap[block / 8] |= (1 << (block % 8));
        file_has_escapes = true;
        break;
      }
    }
  }

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  libvroom::ValueExtractor extractor(buffer.data(), buffer.size(), result.idx);

  for (auto _ : state) {
    std::vector<std::string> strings;
    strings.reserve(extractor.num_rows());

    for (size_t row = 0; row < extractor.num_rows(); ++row) {
      auto sv = extractor.get_string_view(row, 0);

      if (approach == 0) {
        // Baseline: always use full unescape (via get_string_optimistic which
        // does unescape when needed)
        if (sv.empty()) {
          strings.emplace_back();
        } else if (sv.front() != '"') {
          strings.push_back(std::string(sv));
        } else if (sv.size() < 2 || sv.back() != '"') {
          strings.push_back(std::string(sv));
        } else {
          // Full unescape path
          std::string result;
          result.reserve(sv.size() - 2);
          const char* p = sv.data() + 1;
          const char* end = sv.data() + sv.size() - 1;
          while (p < end) {
            if (*p == '"' && p + 1 < end && *(p + 1) == '"') {
              result += '"';
              p += 2;
            } else {
              result += *p++;
            }
          }
          strings.push_back(std::move(result));
        }
      } else if (approach == 1) {
        // Optimistic per-field
        strings.push_back(get_string_optimistic(sv, '"'));
      } else if (approach == 2) {
        // Per-block bitmap
        size_t offset = sv.data() - reinterpret_cast<const char*>(buffer.data());
        strings.push_back(get_string_per_block(sv, '"', escape_bitmap, offset));
      } else {
        // Global flag (approach == 3)
        if (sv.empty()) {
          strings.emplace_back();
        } else if (!file_has_escapes) {
          if (sv.front() == '"' && sv.size() >= 2 && sv.back() == '"') {
            strings.push_back(std::string(sv.substr(1, sv.size() - 2)));
          } else {
            strings.push_back(std::string(sv));
          }
        } else {
          strings.push_back(get_string_optimistic(sv, '"'));
        }
      }
    }
    benchmark::DoNotOptimize(strings.data());
  }

  const char* approach_names[] = {"Baseline", "Optimistic", "PerBlock", "GlobalFlag"};
  state.SetLabel(approach_names[approach]);
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_HeadToHead_AllApproaches)
    // 0% escapes - clean file
    ->Args({0, 0}) // Baseline
    ->Args({1, 0}) // Optimistic
    ->Args({2, 0}) // Per-block
    ->Args({3, 0}) // Global flag
    // 5% escapes - typical
    ->Args({0, 5})
    ->Args({1, 5})
    ->Args({2, 5})
    ->Args({3, 5})
    // 20% escapes - high
    ->Args({0, 20})
    ->Args({1, 20})
    ->Args({2, 20})
    ->Args({3, 20})
    ->Unit(benchmark::kMillisecond);
