#pragma once

#include "dialect.h"
#include "io_util.h"
#include "options.h"
#include "types.h"

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

// Include ArrowColumnBuilder header for full class definition
// (needed for unique_ptr destruction in user code)
#include "arrow_column_builder.h"

namespace libvroom {

// Forward declarations
class Table;
class MmapSource;
class ColumnBuilder; // Legacy, kept for existing code that hasn't migrated
class ParquetWriter;

// Parsed chunks from parallel CSV parsing
// Each chunk becomes a separate Parquet row group (like Polars ChunkedArray)
struct ParsedChunks {
  std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> chunks; // One vector per chunk
  size_t total_rows = 0;
  bool used_cache = false; // True if index was loaded from cache
  std::string cache_path;  // Path to cache file (empty if disabled)
};

// CSV Reader - orchestrates the parsing
class CsvReader {
public:
  explicit CsvReader(const CsvOptions& options);
  ~CsvReader();

  // Open a CSV file
  Result<bool> open(const std::string& path);

  // Open from a pre-loaded buffer (e.g., stdin)
  // Takes ownership of the buffer
  Result<bool> open_from_buffer(AlignedBuffer buffer);

  // Get detected schema after opening
  const std::vector<ColumnSchema>& schema() const;

  // Parse the file into column builders
  // Returns ParsedChunks with one vector of ArrowColumnBuilders per chunk
  // Each chunk can be written as a separate Parquet row group
  Result<ParsedChunks> read_all();

  // Streaming API: parse chunks on background threads, consume one at a time.
  // Call open() first, then start_streaming() to begin, then next_chunk() in a loop.
  // start_streaming() runs SIMD analysis (phases 1-2) synchronously, then
  // dispatches chunk parsing to the thread pool.
  Result<bool> start_streaming();

  // Returns the next parsed chunk in order, or nullopt when all chunks are consumed.
  // Blocks if the next sequential chunk hasn't finished parsing yet.
  std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> next_chunk();

  // Get total number of rows (only valid after read_all() is called)
  size_t row_count() const;

  // Get detected encoding (valid after open/open_from_buffer)
  const EncodingResult& encoding() const;

  // Get collected errors (only populated when error_mode != DISABLED)
  const std::vector<ParseError>& errors() const;

  // Check if any errors were collected
  bool has_errors() const;

  // Get detected dialect (valid after open/open_from_buffer if auto-detection ran)
  std::optional<DetectionResult> detected_dialect() const;

private:
  // Serial implementation for small files or fallback
  Result<ParsedChunks> read_all_serial();
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Memory-mapped file source
class MmapSource {
public:
  MmapSource();
  ~MmapSource();

  // Open a file for reading
  Result<bool> open(const std::string& path);

  // Get data pointer and size
  const char* data() const;
  size_t size() const;

  // Check if file is open
  bool is_open() const;

  // Close the file
  void close();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Abstract column builder - accumulates values during parsing
// Uses chunked storage for O(1) merge_from() performance (like Polars ChunkedArray)
class ColumnBuilder {
public:
  virtual ~ColumnBuilder() = default;

  // Append a field value (parsed from CSV)
  virtual void append(std::string_view value) = 0;

  // Append a null value
  virtual void append_null() = 0;

  // Get the data type
  virtual DataType type() const = 0;

  // Get number of values
  virtual size_t size() const = 0;

  // Reserve capacity
  virtual void reserve(size_t capacity) = 0;

  // Get statistics
  virtual ColumnStatistics statistics() const = 0;

  // Finalize the current chunk (must be called before accessing chunks)
  virtual void finalize() = 0;

  // ========================================================================
  // Chunked data access (preferred for efficiency)
  // ========================================================================

  // Get number of chunks
  virtual size_t num_chunks() const = 0;

  // Get chunk size
  virtual size_t chunk_size(size_t chunk_idx) const = 0;

  // Get chunk data (type-erased pointer to the values vector)
  virtual const void* chunk_raw_values(size_t chunk_idx) const = 0;

  // Get chunk null bitmap
  virtual const std::vector<bool>& chunk_null_bitmap(size_t chunk_idx) const = 0;

  // ========================================================================
  // Legacy contiguous access (for backwards compatibility)
  // These may require concatenation if data is chunked
  // ========================================================================

  // Type-erased data accessors for Parquet writing
  // WARNING: These may be slow if data is chunked - prefer chunk_* methods
  virtual const void* raw_values() const = 0;
  virtual const std::vector<bool>& null_bitmap() const = 0;

  // Mutable accessors for devirtualized fast path
  virtual void* raw_values_mutable() = 0;
  virtual std::vector<bool>& null_bitmap_mutable() = 0;

  // Merge another column builder into this one (for parallel processing)
  // The other builder must be of the same type
  // O(1) operation - just moves chunk pointers, no data copying!
  virtual void merge_from(ColumnBuilder& other) = 0;

  // Clone this builder (create an empty builder of the same type)
  virtual std::unique_ptr<ColumnBuilder> clone_empty() const = 0;

  // Factory methods
  static std::unique_ptr<ColumnBuilder> create(DataType type);
  static std::unique_ptr<ColumnBuilder> create_string();
  static std::unique_ptr<ColumnBuilder> create_int32();
  static std::unique_ptr<ColumnBuilder> create_int64();
  static std::unique_ptr<ColumnBuilder> create_float64();
  static std::unique_ptr<ColumnBuilder> create_bool();
  static std::unique_ptr<ColumnBuilder> create_date();
  static std::unique_ptr<ColumnBuilder> create_timestamp();
};

// Parquet writer
class ParquetWriter {
public:
  explicit ParquetWriter(const ParquetOptions& options);
  ~ParquetWriter();

  // Open a file for writing
  Result<bool> open(const std::string& path);

  // Set schema (must be called before writing)
  void set_schema(const std::vector<ColumnSchema>& schema);

  // Write columns to the file using Arrow-style buffers
  Result<bool> write(const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns);

  // Pipelined writing API (overlaps encoding with I/O)
  // Call start_pipeline(), then submit_row_group() for each chunk, then finish_pipeline()
  Result<bool> start_pipeline();
  Result<bool> submit_row_group(std::vector<std::unique_ptr<ArrowColumnBuilder>> columns);
  Result<bool> finish_pipeline();

  // Close and finalize the file
  Result<bool> close();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Chunk boundary finder
class ChunkFinder {
public:
  explicit ChunkFinder(char separator = ',', char quote = '"');

  // Find all chunk boundaries in the data
  std::vector<ChunkBoundary> find_chunks(const char* data, size_t size, size_t target_chunk_size);

  // Find the end of the current row (respecting quotes)
  // Returns offset of first byte after row terminator
  size_t find_row_end(const char* data, size_t size, size_t start = 0);

  // Count rows using SIMD acceleration
  // Returns (row_count, offset_after_last_complete_row)
  std::pair<size_t, size_t> count_rows(const char* data, size_t size);

private:
  char separator_;
  char quote_;
};

// SIMD-accelerated row counting functions
// Returns (row_count, offset_after_last_complete_row)
std::pair<size_t, size_t> count_rows_simd(const char* data, size_t size, char quote_char = '"');

// Scalar row counting (for verification and small data)
std::pair<size_t, size_t> count_rows_scalar(const char* data, size_t size, char quote_char = '"');

// Analyze chunk with known starting quote state
// Returns (row_count, last_row_end_offset, ends_inside_quote)
std::tuple<size_t, size_t, bool> analyze_chunk_simd(const char* data, size_t size,
                                                    char quote_char = '"',
                                                    bool start_inside_quote = false);

// Dual-state chunk analysis result (like Polars LineStats[2])
struct DualStateChunkStats {
  // Stats for starting outside quotes (state 0)
  size_t row_count_outside = 0;
  size_t last_row_end_outside = 0;

  // Stats for starting inside quotes (state 1)
  size_t row_count_inside = 0;
  size_t last_row_end_inside = 0;

  // Ending quote state (same for both - determined by total quote parity)
  // If chunk ends inside a quote:
  //   - state 0 (started outside) ended inside
  //   - state 1 (started inside) ended outside
  bool ends_inside_quote_from_outside = false;
};

// Single-pass dual-state chunk analysis (Polars algorithm)
// Computes stats for BOTH starting states simultaneously using SIMD
// This is the key optimization: one pass instead of two
DualStateChunkStats analyze_chunk_dual_state_simd(const char* data, size_t size,
                                                  char quote_char = '"');

// SIMD-accelerated find_row_end
// Returns offset of first byte after row terminator, starting from 'start'
size_t find_row_end_simd(const char* data, size_t size, size_t start = 0, char quote_char = '"');

// Scalar find_row_end (for verification and small data)
size_t find_row_end_scalar(const char* data, size_t size, size_t start = 0, char quote_char = '"');

// Line parser - parses fields directly to column builders
class LineParser {
public:
  explicit LineParser(const CsvOptions& options);

  // Parse a single line, appending to column builders
  // Returns number of fields parsed
  size_t parse_line(const char* data, size_t size,
                    std::vector<std::unique_ptr<ColumnBuilder>>& columns);

  // Parse header line, returning column names
  std::vector<std::string> parse_header(const char* data, size_t size);

private:
  void init_null_values();
  bool is_null_value(std::string_view value) const;

  // Hash functor supporting heterogeneous lookup (no allocation for string_view)
  struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const noexcept {
      return std::hash<std::string_view>{}(sv);
    }
  };

  // Equality functor supporting heterogeneous lookup
  struct StringEqual {
    using is_transparent = void;
    bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
      return lhs == rhs;
    }
  };

  CsvOptions options_;
  std::unordered_set<std::string, StringHash, StringEqual> null_value_set_;
  size_t max_null_length_ = 0;
  bool empty_is_null_ = false;
};

// Type inference
class TypeInference {
public:
  explicit TypeInference(const CsvOptions& options);

  // Infer type of a single field
  DataType infer_field(std::string_view value);

  // Infer types from sample data
  std::vector<DataType> infer_from_sample(const char* data, size_t size, size_t n_columns,
                                          size_t max_rows = 1000);

private:
  CsvOptions options_;
};

// Field splitting functions
// Main dispatcher: uses SIMD for lines >= 64 bytes, scalar otherwise
std::vector<FieldView> split_fields(const char* data, size_t size, char separator = ',',
                                    char quote = '"');

// SIMD-optimized field splitting (best for lines >= 64 bytes)
std::vector<FieldView> split_fields_simd(const char* data, size_t size, char separator = ',',
                                         char quote = '"');

// Scalar field splitting (reference/fallback implementation)
std::vector<FieldView> split_fields_scalar(const char* data, size_t size, char separator = ',',
                                           char quote = '"');

// Buffer-reusing versions (avoid allocation per call)
// Main dispatcher: uses SIMD for lines >= 64 bytes, scalar otherwise
void split_fields_into(const char* data, size_t size, char separator, char quote,
                       std::vector<FieldView>& fields // Output: cleared and populated
);

// SIMD-optimized field splitting with buffer reuse
void split_fields_simd_into(const char* data, size_t size, char separator, char quote,
                            std::vector<FieldView>& fields // Output: cleared and populated
);

// Scalar field splitting with buffer reuse
void split_fields_scalar_into(const char* data, size_t size, char separator, char quote,
                              std::vector<FieldView>& fields // Output: cleared and populated
);

// ============================================================================
// Date/Time parsing functions
// ============================================================================

// Parse ISO8601 date (YYYY-MM-DD or YYYY/MM/DD) to days since Unix epoch
// Returns true on success, false on parse error
bool parse_date(std::string_view value, int32_t& days_since_epoch);

// Parse ISO8601 timestamp to microseconds since Unix epoch (UTC)
// Supports formats:
//   YYYY-MM-DDTHH:MM:SS
//   YYYY-MM-DD HH:MM:SS
//   YYYY-MM-DDTHH:MM:SS.ffffff (fractional seconds)
//   YYYY-MM-DDTHH:MM:SSZ (UTC)
//   YYYY-MM-DDTHH:MM:SS+HH:MM (timezone offset)
//   YYYY-MM-DDTHH:MM:SS-HH:MM (timezone offset)
// Returns true on success, false on parse error
bool parse_timestamp(std::string_view value, int64_t& micros_since_epoch);

} // namespace libvroom
