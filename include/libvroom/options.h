#pragma once

#include "cache.h"
#include "error.h"
#include "types.h"

#include <optional>
#include <string>
#include <vector>

namespace libvroom {

// CSV parsing options
struct CsvOptions {
  char separator = ',';
  char quote = '"';
  char escape = '\\';
  char comment = '\0'; // No comment char by default
  bool has_header = true;
  bool skip_empty_rows = true;
  std::string null_values = "NA,null,NULL,"; // Comma-separated
  std::string true_values = "true,TRUE,True,yes,YES,Yes";
  std::string false_values = "false,FALSE,False,no,NO,No";

  // Performance tuning
  size_t sample_rows = 1000; // Rows to sample for type inference
  size_t chunk_size = 0;     // 0 = auto-detect based on file size and width
  size_t num_threads = 0;    // 0 = auto-detect (hardware_concurrency)

  // Column selection (empty = all columns)
  std::vector<std::string> columns;
  std::vector<size_t> column_indices;

  // Error handling (DISABLED = no collection for max performance)
  ErrorMode error_mode = ErrorMode::DISABLED;
  size_t max_errors = ErrorCollector::DEFAULT_MAX_ERRORS;

  // Index caching (nullopt = disabled)
  std::optional<CacheConfig> cache;
  bool force_cache_refresh = false;
};

// Parquet writing options
struct ParquetOptions {
  Compression compression = Compression::ZSTD;
  int compression_level = 3; // zstd default level

  size_t row_group_size = 1'000'000; // Rows per row group
  size_t page_size = 1'048'576;      // 1MB page size
  size_t dictionary_page_size = 1'048'576;

  bool write_statistics = true;
  bool enable_dictionary = false; // Disabled by default until performance is optimized

  // Dictionary heuristics (from Polars)
  // Only create dictionary if cardinality < 75% of length
  double dictionary_ratio_threshold = 0.75;
};

// Thread pool options
struct ThreadOptions {
  size_t num_threads = 0; // 0 = auto-detect

  // Polars formula for chunk sizing
  // n_chunks * n_cols <= ALLOCATION_BUDGET
  static constexpr size_t ALLOCATION_BUDGET = 500'000;

  // Chunk size bounds
  // Smaller chunks improve parallelism for both CSV parsing and Parquet writing:
  // - More chunks = better thread utilization during parsing
  // - More row groups = more parallel column encoding opportunities
  // For numeric data, row group batching combines chunks into ~262K row groups
  // For string data, each chunk becomes a row group (merging is expensive)
  static constexpr size_t MIN_CHUNK_SIZE = 1 * 1024 * 1024; // 1MB - optimal balance for parallelism
  static constexpr size_t MAX_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
};

// Combined options for the entire conversion
struct VroomOptions {
  CsvOptions csv;
  ParquetOptions parquet;
  ThreadOptions threads;

  std::string input_path;
  std::string output_path;

  bool verbose = false;
  bool progress = false;
};

// Calculate optimal chunk size based on file size and column count
inline size_t calculate_chunk_size(size_t file_size, size_t n_cols, size_t n_threads) {
  // Polars formula: prevent memory explosion on wide files
  size_t max_chunks = ThreadOptions::ALLOCATION_BUDGET / std::max(n_cols, size_t(1));
  size_t n_parts = std::min(n_threads * 16, max_chunks);

  if (n_parts == 0)
    n_parts = 1;

  size_t chunk_size = file_size / n_parts;

  // Clamp to reasonable bounds
  if (chunk_size < ThreadOptions::MIN_CHUNK_SIZE) {
    chunk_size = ThreadOptions::MIN_CHUNK_SIZE;
  }
  if (chunk_size > ThreadOptions::MAX_CHUNK_SIZE) {
    chunk_size = ThreadOptions::MAX_CHUNK_SIZE;
  }

  return chunk_size;
}

} // namespace libvroom
