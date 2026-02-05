/**
 * @file arrow_convert_benchmarks.cpp
 * @brief Benchmarks for Arrow conversion and columnar export functionality.
 *
 * These benchmarks measure performance of:
 * - CSV to Arrow table conversion
 * - Arrow table to Feather/Parquet export
 * - End-to-end CSV to columnar format conversion
 *
 * Only compiled when LIBVROOM_ENABLE_ARROW is defined.
 */

#ifdef LIBVROOM_ENABLE_ARROW

#include "libvroom.h"

#include "arrow_output.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <string>

namespace {

// Generate synthetic CSV data for benchmarking
std::string generate_csv_data(size_t num_rows, size_t num_cols) {
  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < num_cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows with mixed types
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> int_dist(0, 1000000);
  std::uniform_real_distribution<double> dbl_dist(0.0, 1000.0);

  for (size_t r = 0; r < num_rows; ++r) {
    for (size_t c = 0; c < num_cols; ++c) {
      if (c > 0)
        oss << ',';
      switch (c % 4) {
      case 0: // Integer column
        oss << int_dist(rng);
        break;
      case 1: // Double column
        oss << std::fixed << std::setprecision(2) << dbl_dist(rng);
        break;
      case 2: // String column
        oss << "value_" << r << "_" << c;
        break;
      case 3: // Boolean column
        oss << (rng() % 2 == 0 ? "true" : "false");
        break;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// Helper to create a buffer from string data
struct BenchmarkBuffer {
  uint8_t* data;
  size_t len;

  explicit BenchmarkBuffer(const std::string& content) {
    len = content.size();
    data = allocate_padded_buffer(len, 64);
    std::memcpy(data, content.data(), len);
  }

  ~BenchmarkBuffer() {
    if (data)
      aligned_free(data);
  }

  BenchmarkBuffer(const BenchmarkBuffer&) = delete;
  BenchmarkBuffer& operator=(const BenchmarkBuffer&) = delete;
};

} // namespace

// Benchmark CSV to Arrow table conversion
static void BM_CSVToArrowTable(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, num_cols);
  BenchmarkBuffer buffer(csv_data);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;

  for (auto _ : state) {
    libvroom::TwoPass parser;
    libvroom::ParseIndex idx = parser.init(buffer.len, 1);
    parser.parse(buffer.data, idx, buffer.len);

    libvroom::ArrowConverter converter(opts);
    auto result = converter.convert(buffer.data, buffer.len, idx);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
  state.counters["DataSize"] = static_cast<double>(buffer.len);
}

BENCHMARK(BM_CSVToArrowTable)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({10000, 50})
    ->Args({10000, 100})
    ->Unit(benchmark::kMillisecond);

// Benchmark Arrow table to Feather export
static void BM_ArrowToFeather(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, num_cols);
  BenchmarkBuffer buffer(csv_data);

  // Parse and convert once outside the benchmark loop
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;
  libvroom::ArrowConverter converter(opts);
  auto convert_result = converter.convert(buffer.data, buffer.len, idx);

  if (!convert_result.ok()) {
    state.SkipWithError(convert_result.error_message.c_str());
    return;
  }

  std::string tmp_path = "/tmp/benchmark_output.feather";

  for (auto _ : state) {
    auto result = libvroom::write_feather(convert_result.table, tmp_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(tmp_path);

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
}

BENCHMARK(BM_ArrowToFeather)
    ->Args({1000, 10})
    ->Args({10000, 10})
    ->Args({100000, 10})
    ->Args({10000, 50})
    ->Unit(benchmark::kMillisecond);

#ifdef LIBVROOM_ENABLE_PARQUET
// Benchmark Arrow table to Parquet export with different compression
static void BM_ArrowToParquet(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto compression = static_cast<libvroom::ParquetWriteOptions::Compression>(state.range(1));

  std::string csv_data = generate_csv_data(num_rows, 10);
  BenchmarkBuffer buffer(csv_data);

  // Parse and convert once outside the benchmark loop
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  libvroom::ArrowConvertOptions opts;
  opts.infer_types = true;
  libvroom::ArrowConverter converter(opts);
  auto convert_result = converter.convert(buffer.data, buffer.len, idx);

  if (!convert_result.ok()) {
    state.SkipWithError(convert_result.error_message.c_str());
    return;
  }

  std::string tmp_path = "/tmp/benchmark_output.parquet";
  libvroom::ParquetWriteOptions parquet_opts;
  parquet_opts.compression = compression;

  for (auto _ : state) {
    auto result = libvroom::write_parquet(convert_result.table, tmp_path, parquet_opts);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(tmp_path);

  state.SetBytesProcessed(static_cast<int64_t>(buffer.len * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);

  // Label compression type
  const char* compression_names[] = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4"};
  state.SetLabel(compression_names[static_cast<int>(compression)]);
}

BENCHMARK(BM_ArrowToParquet)
    ->Args({10000, 0})  // UNCOMPRESSED
    ->Args({10000, 1})  // SNAPPY
    ->Args({10000, 2})  // GZIP
    ->Args({10000, 3})  // ZSTD
    ->Args({10000, 4})  // LZ4
    ->Args({100000, 1}) // SNAPPY with more rows
    ->Unit(benchmark::kMillisecond);
#endif // LIBVROOM_ENABLE_PARQUET

// End-to-end benchmark: CSV file to Feather file
static void BM_CSVToFeatherEndToEnd(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));

  std::string csv_data = generate_csv_data(num_rows, 10);

  // Write CSV to temp file
  std::string csv_path = "/tmp/benchmark_input.csv";
  std::string feather_path = "/tmp/benchmark_output.feather";
  {
    std::ofstream out(csv_path);
    out << csv_data;
  }

  for (auto _ : state) {
    auto result = libvroom::csv_to_feather(csv_path, feather_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(csv_path);
  std::filesystem::remove(feather_path);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
}

BENCHMARK(BM_CSVToFeatherEndToEnd)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);

#ifdef LIBVROOM_ENABLE_PARQUET
// End-to-end benchmark: CSV file to Parquet file
static void BM_CSVToParquetEndToEnd(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));

  std::string csv_data = generate_csv_data(num_rows, 10);

  // Write CSV to temp file
  std::string csv_path = "/tmp/benchmark_input.csv";
  std::string parquet_path = "/tmp/benchmark_output.parquet";
  {
    std::ofstream out(csv_path);
    out << csv_data;
  }

  for (auto _ : state) {
    auto result = libvroom::csv_to_parquet(csv_path, parquet_path);
    benchmark::DoNotOptimize(result);
  }

  // Clean up
  std::filesystem::remove(csv_path);
  std::filesystem::remove(parquet_path);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
}

BENCHMARK(BM_CSVToParquetEndToEnd)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);
#endif // LIBVROOM_ENABLE_PARQUET

// =============================================================================
// String Building Benchmarks: Compare different approaches for Arrow string columns
// =============================================================================

namespace {

// Generate CSV with controlled string characteristics
std::string generate_string_csv(size_t num_rows, double quote_ratio, double escape_ratio) {
  std::ostringstream oss;
  std::mt19937 rng(42);
  std::uniform_real_distribution<> dist(0.0, 1.0);

  // Header
  oss << "col0\n";

  // Data rows - all strings
  for (size_t r = 0; r < num_rows; ++r) {
    bool quoted = dist(rng) < quote_ratio;
    bool escaped = quoted && dist(rng) < escape_ratio;

    if (escaped) {
      oss << "\"value" << r << " \"\"with\"\" escapes\"\n";
    } else if (quoted) {
      oss << "\"value" << r << " quoted\"\n";
    } else {
      oss << "value" << r << "_unquoted\n";
    }
  }

  return oss.str();
}

// Unescape into a provided buffer (avoids allocation per field)
void unescape_into(std::string& out, std::string_view field, char quote_char) {
  out.clear();
  if (field.size() < 2)
    return;

  const char* p = field.data() + 1;                  // skip opening quote
  const char* end = field.data() + field.size() - 1; // skip closing quote
  out.reserve(end - p);

  while (p < end) {
    if (*p == quote_char && p + 1 < end && *(p + 1) == quote_char) {
      out += quote_char;
      p += 2;
    } else {
      out += *p++;
    }
  }
}

// Check if a specific field has escape sequences
bool field_has_escape(std::string_view field, char quote_char) {
  if (field.size() < 4 || field.front() != quote_char)
    return false;

  const char* p = field.data() + 1;
  const char* end = field.data() + field.size() - 1;
  while (p + 1 < end) {
    if (*p == quote_char && *(p + 1) == quote_char) {
      return true;
    }
    ++p;
  }
  return false;
}

// Extract field ranges from parsed index (simplified version for benchmarks)
struct FieldRange {
  size_t start;
  size_t end;
};

std::vector<FieldRange> extract_column_ranges(const uint8_t* buf, size_t len,
                                              const libvroom::ParseIndex& idx) {
  std::vector<FieldRange> ranges;

  // Get total separators and sort them
  std::vector<uint64_t> all_positions;
  for (uint16_t t = 0; t < idx.n_threads; ++t) {
    for (size_t i = 0; i < idx.n_indexes[t]; ++i) {
      uint64_t pos = idx.indexes[t + i * idx.n_threads];
      if (pos < len)
        all_positions.push_back(pos);
    }
  }
  std::sort(all_positions.begin(), all_positions.end());

  if (all_positions.empty())
    return ranges;

  // Find number of columns from first row
  size_t num_columns = 0;
  for (size_t i = 0; i < all_positions.size(); ++i) {
    num_columns++;
    if (buf[all_positions[i]] == '\n')
      break;
  }

  // Extract first column (col 0) ranges, skipping header
  size_t field_start = 0;
  size_t current_col = 0;
  bool in_header = true;

  for (size_t i = 0; i < all_positions.size(); ++i) {
    size_t field_end = all_positions[i];
    char sep_char = static_cast<char>(buf[field_end]);

    if (!in_header && current_col == 0) {
      ranges.push_back({field_start, field_end});
    }

    if (sep_char == '\n') {
      if (in_header)
        in_header = false;
      current_col = 0;
    } else {
      current_col++;
    }
    field_start = field_end + 1;
  }

  return ranges;
}

// Extract field with quote stripping (like ArrowConverter::extract_field)
std::string_view extract_field(const uint8_t* buf, size_t start, size_t end, char quote_char) {
  if (start >= end)
    return std::string_view(reinterpret_cast<const char*>(buf + start), 0);

  const char* field_start = reinterpret_cast<const char*>(buf + start);
  size_t len = end - start;

  if (len >= 2 && field_start[0] == quote_char && field_start[len - 1] == quote_char) {
    field_start++;
    len -= 2;
  }
  return std::string_view(field_start, len);
}

} // namespace

/**
 * @brief Current approach: builder.Append(std::string(cell))
 * Creates intermediate string, then copies to Arrow.
 */
static void BM_StringBuild_CurrentApproach(benchmark::State& state) {
  double quote_ratio = state.range(0) / 100.0;
  double escape_ratio = state.range(1) / 100.0;
  size_t num_rows = 100000;

  std::string csv_data = generate_string_csv(num_rows, quote_ratio, escape_ratio);
  BenchmarkBuffer buffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  auto ranges = extract_column_ranges(buffer.data, buffer.len, idx);
  if (ranges.empty()) {
    state.SkipWithError("No data extracted");
    return;
  }

  char quote_char = '"';

  for (auto _ : state) {
    arrow::StringBuilder builder;
    builder.Reserve(static_cast<int64_t>(ranges.size())).ok();

    for (const auto& range : ranges) {
      auto cell = extract_field(buffer.data, range.start, range.end, quote_char);
      // Current approach: create std::string, then append
      builder.Append(std::string(cell)).ok();
    }

    auto result = builder.Finish();
    benchmark::DoNotOptimize(result);
  }

  state.counters["Rows"] = static_cast<double>(ranges.size());
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_StringBuild_CurrentApproach)
    ->Args({0, 0})    // 0% quoted
    ->Args({30, 0})   // 30% quoted, 0% escaped
    ->Args({30, 10})  // 30% quoted, 10% escaped
    ->Args({100, 0})  // 100% quoted, 0% escaped
    ->Args({100, 10}) // 100% quoted, 10% escaped
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Direct approach: builder.Append(cell.data(), cell.size())
 * Single copy directly to Arrow buffer.
 */
static void BM_StringBuild_DirectAppend(benchmark::State& state) {
  double quote_ratio = state.range(0) / 100.0;
  double escape_ratio = state.range(1) / 100.0;
  size_t num_rows = 100000;

  std::string csv_data = generate_string_csv(num_rows, quote_ratio, escape_ratio);
  BenchmarkBuffer buffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  auto ranges = extract_column_ranges(buffer.data, buffer.len, idx);
  if (ranges.empty()) {
    state.SkipWithError("No data extracted");
    return;
  }

  char quote_char = '"';

  for (auto _ : state) {
    arrow::StringBuilder builder;
    builder.Reserve(static_cast<int64_t>(ranges.size())).ok();

    for (const auto& range : ranges) {
      auto cell = extract_field(buffer.data, range.start, range.end, quote_char);
      // Direct approach: append string_view directly (no intermediate string)
      builder.Append(cell.data(), static_cast<int64_t>(cell.size())).ok();
    }

    auto result = builder.Finish();
    benchmark::DoNotOptimize(result);
  }

  state.counters["Rows"] = static_cast<double>(ranges.size());
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_StringBuild_DirectAppend)
    ->Args({0, 0})
    ->Args({30, 0})
    ->Args({30, 10})
    ->Args({100, 0})
    ->Args({100, 10})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Optimized with escape info: direct for no-escape, scratch buffer for escapes.
 * Uses reusable scratch buffer to avoid per-field allocation.
 */
static void BM_StringBuild_WithEscapeInfo(benchmark::State& state) {
  double quote_ratio = state.range(0) / 100.0;
  double escape_ratio = state.range(1) / 100.0;
  size_t num_rows = 100000;

  std::string csv_data = generate_string_csv(num_rows, quote_ratio, escape_ratio);
  BenchmarkBuffer buffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  auto ranges = extract_column_ranges(buffer.data, buffer.len, idx);
  if (ranges.empty()) {
    state.SkipWithError("No data extracted");
    return;
  }

  char quote_char = '"';

  // Pre-scan to determine if column has any escapes (simulating escape info)
  bool column_has_escapes = false;
  for (const auto& range : ranges) {
    std::string_view raw(reinterpret_cast<const char*>(buffer.data + range.start),
                         range.end - range.start);
    if (field_has_escape(raw, quote_char)) {
      column_has_escapes = true;
      break;
    }
  }

  for (auto _ : state) {
    arrow::StringBuilder builder;
    builder.Reserve(static_cast<int64_t>(ranges.size())).ok();
    std::string scratch; // Reused across all rows

    for (const auto& range : ranges) {
      std::string_view raw(reinterpret_cast<const char*>(buffer.data + range.start),
                           range.end - range.start);

      if (raw.empty()) {
        builder.Append("", 0).ok();
      } else if (raw.front() != quote_char) {
        // Unquoted: direct append
        builder.Append(raw.data(), static_cast<int64_t>(raw.size())).ok();
      } else if (!column_has_escapes) {
        // Quoted but column has no escapes: strip quotes, direct append
        builder.Append(raw.data() + 1, static_cast<int64_t>(raw.size() - 2)).ok();
      } else {
        // Column has escapes: check this field and unescape if needed
        if (!field_has_escape(raw, quote_char)) {
          builder.Append(raw.data() + 1, static_cast<int64_t>(raw.size() - 2)).ok();
        } else {
          unescape_into(scratch, raw, quote_char);
          builder.Append(scratch.data(), static_cast<int64_t>(scratch.size())).ok();
        }
      }
    }

    auto result = builder.Finish();
    benchmark::DoNotOptimize(result);
  }

  state.counters["Rows"] = static_cast<double>(ranges.size());
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["EscapeRatio"] = escape_ratio;
  state.counters["ColHasEscapes"] = column_has_escapes ? 1.0 : 0.0;
}

BENCHMARK(BM_StringBuild_WithEscapeInfo)
    ->Args({0, 0})
    ->Args({30, 0})
    ->Args({30, 10})
    ->Args({100, 0})
    ->Args({100, 10})
    ->Unit(benchmark::kMillisecond);

/**
 * @brief Per-field optimistic: check each field for escapes inline.
 * No pre-scan, but avoids unnecessary work for clean fields.
 */
static void BM_StringBuild_PerFieldOptimistic(benchmark::State& state) {
  double quote_ratio = state.range(0) / 100.0;
  double escape_ratio = state.range(1) / 100.0;
  size_t num_rows = 100000;

  std::string csv_data = generate_string_csv(num_rows, quote_ratio, escape_ratio);
  BenchmarkBuffer buffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.len, 1);
  parser.parse(buffer.data, idx, buffer.len);

  auto ranges = extract_column_ranges(buffer.data, buffer.len, idx);
  if (ranges.empty()) {
    state.SkipWithError("No data extracted");
    return;
  }

  char quote_char = '"';

  for (auto _ : state) {
    arrow::StringBuilder builder;
    builder.Reserve(static_cast<int64_t>(ranges.size())).ok();
    std::string scratch;

    for (const auto& range : ranges) {
      std::string_view raw(reinterpret_cast<const char*>(buffer.data + range.start),
                           range.end - range.start);

      if (raw.empty()) {
        builder.Append("", 0).ok();
      } else if (raw.front() != quote_char) {
        // Unquoted: direct
        builder.Append(raw.data(), static_cast<int64_t>(raw.size())).ok();
      } else if (!field_has_escape(raw, quote_char)) {
        // Quoted, no escapes: strip quotes, direct
        builder.Append(raw.data() + 1, static_cast<int64_t>(raw.size() - 2)).ok();
      } else {
        // Has escapes: unescape into scratch
        unescape_into(scratch, raw, quote_char);
        builder.Append(scratch.data(), static_cast<int64_t>(scratch.size())).ok();
      }
    }

    auto result = builder.Finish();
    benchmark::DoNotOptimize(result);
  }

  state.counters["Rows"] = static_cast<double>(ranges.size());
  state.counters["QuoteRatio"] = quote_ratio;
  state.counters["EscapeRatio"] = escape_ratio;
}

BENCHMARK(BM_StringBuild_PerFieldOptimistic)
    ->Args({0, 0})
    ->Args({30, 0})
    ->Args({30, 10})
    ->Args({100, 0})
    ->Args({100, 10})
    ->Unit(benchmark::kMillisecond);

#endif // LIBVROOM_ENABLE_ARROW
