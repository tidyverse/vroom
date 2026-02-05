#include "libvroom/arrow_column_builder.h"
#include "libvroom/cache.h"
#include "libvroom/dialect.h"
#include "libvroom/encoding.h"
#include "libvroom/error.h"
#include "libvroom/parse_utils.h"
#include "libvroom/parsed_chunk_queue.h"
#include "libvroom/split_fields.h"
#include "libvroom/vroom.h"

#include "BS_thread_pool.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <unordered_set>
#include <vector>

namespace libvroom {

// Structure to hold dual-state analysis results (lightweight, no parsing)
struct ChunkAnalysisResult {
  // Row counts for each starting state (computed via SIMD in single pass)
  size_t row_count_outside = 0;
  size_t row_count_inside = 0;

  // Whether parsing ends inside a quote (if started outside)
  bool ends_inside_starting_outside = false;
};

// Structure to hold parsing results for a single chunk (single state only)
struct ChunkParseResult {
  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
  size_t row_count = 0;
};

// Parse a chunk of data with a specific starting quote state
// Returns (row_count, ends_inside_quote)
// error_collector is optional (nullptr = no error checking)
// base_byte_offset is the offset of this chunk within the full file (for error locations)
std::pair<size_t, bool> parse_chunk_with_state(
    const char* data, size_t size, const CsvOptions& options, const NullChecker& null_checker,
    std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns, bool start_inside_quote,
    ErrorCollector* error_collector = nullptr, size_t base_byte_offset = 0) {
  if (size == 0 || columns.empty()) {
    return {0, start_inside_quote};
  }

  // Create FastArrowContext for each column - eliminates virtual dispatch
  std::vector<FastArrowContext> fast_contexts;
  fast_contexts.reserve(columns.size());
  for (auto& col : columns) {
    fast_contexts.push_back(col->create_context());
  }

  bool in_quote = start_inside_quote;
  size_t offset = 0;
  size_t row_count = 0;
  const bool check_errors = error_collector != nullptr;

  // If we start inside a quote, we need to find where the quote ends
  // and skip the partial field
  if (start_inside_quote) {
    // Find the closing quote
    while (offset < size) {
      char c = data[offset];
      if (c == options.quote) {
        // Check for escaped quote
        if (offset + 1 < size && data[offset + 1] == options.quote) {
          offset += 2;
          continue;
        }
        // Found the closing quote
        in_quote = false;
        offset++;
        break;
      }
      offset++;
    }

    // Now skip to the end of this partial row (we can't use it)
    while (offset < size) {
      char c = data[offset];
      if (c == options.quote) {
        if (in_quote && offset + 1 < size && data[offset + 1] == options.quote) {
          offset += 2;
          continue;
        }
        in_quote = !in_quote;
      } else if (!in_quote && (c == '\n' || c == '\r')) {
        offset++;
        // Handle CRLF
        if (c == '\r' && offset < size && data[offset] == '\n') {
          offset++;
        }
        break;
      }
      offset++;
    }
  }

  // Now parse complete rows using Polars-style SplitFields iterator
  // Key optimization: no separate find_row_end call - iterator handles EOL
  const char quote = options.quote;
  const char sep = options.separator;
  const size_t num_cols = columns.size();

  while (offset < size) {
    // Skip empty lines (when enabled)
    if (options.skip_empty_rows) {
      while (offset < size) {
        char c = data[offset];
        if (c == '\n') {
          offset++;
          continue;
        }
        if (c == '\r') {
          offset++;
          if (offset < size && data[offset] == '\n') {
            offset++;
          }
          continue;
        }
        break;
      }
    }

    if (offset >= size)
      break;

    // Skip comment lines (handle \n, \r\n, and bare \r)
    if (options.comment != '\0' && data[offset] == options.comment) {
      while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
        offset++;
      }
      if (offset < size && data[offset] == '\r') {
        offset++;
        if (offset < size && data[offset] == '\n') {
          offset++; // CRLF
        }
      } else if (offset < size && data[offset] == '\n') {
        offset++;
      }
      continue;
    }

    // Create iterator for remaining data - it stops at EOL
    size_t row_start_offset = offset;
    size_t start_remaining = size - offset;
    SplitFields iter(data + offset, start_remaining, sep, quote, '\n');

    const char* field_data;
    size_t field_len;
    bool needs_escaping;
    size_t col_idx = 0;

    while (iter.next(field_data, field_len, needs_escaping)) {
      // Strip trailing \r if present
      if (field_len > 0 && field_data[field_len - 1] == '\r') {
        field_len--;
      }

      std::string_view field_view(field_data, field_len);

      // Error detection within fields
      // Note: row number is 0 in multi-threaded path because computing absolute
      // row offsets per chunk would require tracking the cumulative row count from
      // all preceding chunks, which isn't available during parallel parsing.
      if (check_errors) [[unlikely]] {
        // Null byte detection
        if (std::memchr(field_data, '\0', field_len)) {
          for (size_t i = 0; i < field_len; ++i) {
            if (field_data[i] == '\0') {
              size_t byte_off = base_byte_offset + static_cast<size_t>(field_data - data) + i;
              error_collector->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE, 0,
                                         col_idx + 1, byte_off, "Unexpected null byte in data");
              if (error_collector->should_stop())
                goto done_chunk;
            }
          }
        }

        // Quote in unquoted field
        if (!needs_escaping && field_len > 0 && std::memchr(field_data, quote, field_len)) {
          size_t byte_off = base_byte_offset + static_cast<size_t>(field_data - data);
          error_collector->add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE,
                                     0, col_idx + 1, byte_off, "Quote character in unquoted field");
          if (error_collector->should_stop())
            goto done_chunk;
        }
      }

      if (col_idx >= num_cols) {
        col_idx++;
        continue;
      }

      if (null_checker.is_null(field_view)) {
        // Devirtualized append_null call
        fast_contexts[col_idx].append_null();
      } else if (needs_escaping) {
        // Strip outer quotes
        if (field_len >= 2 && field_data[0] == quote && field_data[field_len - 1] == quote) {
          field_view = std::string_view(field_data + 1, field_len - 2);
        }
        bool has_invalid_escape = false;
        std::string unescaped =
            unescape_quotes(field_view, quote, check_errors ? &has_invalid_escape : nullptr);

        // Invalid quote escape detection
        if (has_invalid_escape) [[unlikely]] {
          size_t byte_off = base_byte_offset + static_cast<size_t>(field_data - data);
          error_collector->add_error(ErrorCode::INVALID_QUOTE_ESCAPE, ErrorSeverity::RECOVERABLE, 0,
                                     col_idx + 1, byte_off, "Invalid quote escape sequence");
          if (error_collector->should_stop())
            goto done_chunk;
        }

        // Devirtualized append call
        fast_contexts[col_idx].append(unescaped);
      } else {
        // Devirtualized append call
        fast_contexts[col_idx].append(field_view);
      }
      col_idx++;
    }

    // Error: inconsistent field count
    if (check_errors && col_idx != num_cols) [[unlikely]] {
      error_collector->add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 0,
                                 0, base_byte_offset + row_start_offset,
                                 "Expected " + std::to_string(num_cols) + " fields, got " +
                                     std::to_string(col_idx));
      if (error_collector->should_stop())
        goto done_chunk;
    }

    // Fill remaining columns with nulls
    for (; col_idx < num_cols; ++col_idx) {
      fast_contexts[col_idx].append_null();
    }

    row_count++;
    // Advance offset by consumed bytes
    offset += start_remaining - iter.remaining();
  }

done_chunk: // Early exit target for should_stop() (FAIL_FAST error mode)
  // Note: ending quote state is already computed during the analysis phase
  // so we return false here since the caller ignores it anyway
  return {row_count, false};
}

struct CsvReader::Impl {
  CsvOptions options;
  MmapSource source;              // For file-based reading
  AlignedBuffer owned_buffer;     // For stdin/buffer-based reading
  const char* data_ptr = nullptr; // Points to either source.data() or owned_buffer.data()
  size_t data_size = 0;           // Size of data
  std::vector<ColumnSchema> schema;
  size_t row_count = 0;
  size_t header_end_offset = 0;
  size_t num_threads = 0;
  bool file_has_quotes = false; // Detected during sampling
  ErrorCollector error_collector;
  std::string file_path;                                  // Stored from open() for caching
  EncodingResult detected_encoding;                       // Character encoding detection result
  std::optional<DetectionResult> detected_dialect_result; // From DialectDetector

  // Streaming state
  std::unique_ptr<ParsedChunkQueue> streaming_queue;
  std::unique_ptr<BS::thread_pool> streaming_pool;
  std::vector<ErrorCollector> streaming_error_collectors;
  std::vector<ChunkAnalysisResult> streaming_analysis;
  std::vector<bool> streaming_use_inside;
  std::vector<std::pair<size_t, size_t>> streaming_chunk_ranges;
  bool streaming_active = false;

  ~Impl() {
    // Ensure safe shutdown of streaming state:
    // 1. Close the queue to unblock any producers blocked on push()
    // 2. Drain the thread pool (waits for detached tasks to finish)
    // 3. Then remaining members are destroyed in reverse declaration order
    if (streaming_queue) {
      streaming_queue->close();
    }
    streaming_pool.reset();
  }

  Impl(const CsvOptions& opts) : options(opts), error_collector(opts.error_mode, opts.max_errors) {
    // Use options.num_threads if specified, otherwise auto-detect
    if (options.num_threads > 0) {
      num_threads = options.num_threads;
    } else {
      num_threads = std::thread::hardware_concurrency();
      if (num_threads == 0)
        num_threads = 4;
    }
  }

  // Auto-detect dialect if separator is the sentinel value ('\0').
  // Must be called after encoding detection/transcoding sets data_ptr/data_size.
  void auto_detect_dialect() {
    if (options.separator != '\0')
      return;

    DialectDetector detector;
    auto detected = detector.detect(reinterpret_cast<const uint8_t*>(data_ptr), data_size);

    if (detected.success()) {
      options.separator = detected.dialect.delimiter;
      options.quote = detected.dialect.quote_char;
      // Only override has_header from detection if user didn't explicitly disable it
      if (options.has_header) {
        options.has_header = detected.has_header;
      }
      if (detected.dialect.comment_char != '\0') {
        options.comment = detected.dialect.comment_char;
      }
      detected_dialect_result = detected;
    } else {
      // Fall back to comma if detection fails
      options.separator = ',';
    }
  }
};

// Skip leading comment lines in the data. Returns offset past all leading comment lines.
// A comment line starts with the comment character (at column 0) and ends at newline.
static size_t skip_leading_comment_lines(const char* data, size_t size, char comment_char) {
  if (comment_char == '\0' || size == 0) {
    return 0;
  }

  size_t offset = 0;
  while (offset < size) {
    // Check if current line starts with comment char
    if (data[offset] != comment_char) {
      break; // Not a comment line, stop
    }

    // Skip to end of this comment line (handle \n, \r\n, and bare \r)
    while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
      offset++;
    }
    // Skip past the line ending
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n') {
        offset++; // CRLF
      }
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
  }
  return offset;
}

CsvReader::CsvReader(const CsvOptions& options) : impl_(std::make_unique<Impl>(options)) {}

CsvReader::~CsvReader() = default;

Result<bool> CsvReader::open(const std::string& path) {
  impl_->file_path = path;

  auto result = impl_->source.open(path);
  if (!result) {
    return result;
  }

  // Set data pointer from mmap source
  impl_->data_ptr = impl_->source.data();
  impl_->data_size = impl_->source.size();

  if (impl_->data_size == 0) {
    return Result<bool>::failure("Empty file");
  }

  // Detect encoding and transcode if needed
  {
    const auto* raw = reinterpret_cast<const uint8_t*>(impl_->data_ptr);
    size_t raw_size = impl_->data_size;

    if (impl_->options.encoding.has_value()) {
      // User-specified encoding
      impl_->detected_encoding.encoding = *impl_->options.encoding;
      // Detect BOM even when encoding is forced
      auto bom_result = detect_encoding(raw, raw_size);
      if (bom_result.encoding == *impl_->options.encoding ||
          (*impl_->options.encoding == CharEncoding::UTF8 &&
           bom_result.encoding == CharEncoding::UTF8_BOM)) {
        impl_->detected_encoding.bom_length = bom_result.bom_length;
      }
      impl_->detected_encoding.confidence = 1.0;
      impl_->detected_encoding.needs_transcoding =
          (*impl_->options.encoding != CharEncoding::UTF8 &&
           *impl_->options.encoding != CharEncoding::UTF8_BOM);
    } else {
      impl_->detected_encoding = detect_encoding(raw, raw_size);
    }

    if (impl_->detected_encoding.needs_transcoding) {
      impl_->owned_buffer = transcode_to_utf8(raw, raw_size, impl_->detected_encoding.encoding,
                                              impl_->detected_encoding.bom_length);
      impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
      impl_->data_size = impl_->owned_buffer.size();
    } else if (impl_->detected_encoding.bom_length > 0) {
      // UTF-8 BOM: skip past BOM bytes (no allocation/copy)
      impl_->data_ptr += impl_->detected_encoding.bom_length;
      impl_->data_size -= impl_->detected_encoding.bom_length;
    }
  }

  impl_->auto_detect_dialect();

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;

  ChunkFinder finder(impl_->options.separator, impl_->options.quote);
  LineParser parser(impl_->options);

  // Skip leading comment lines before header
  size_t comment_skip = skip_leading_comment_lines(data, size, impl_->options.comment);
  if (comment_skip > 0) {
    impl_->data_ptr += comment_skip;
    impl_->data_size -= comment_skip;
    data = impl_->data_ptr;
    size = impl_->data_size;
    if (size == 0) {
      return Result<bool>::failure("File contains only comment lines");
    }
  }

  // Parse header if present
  if (impl_->options.has_header) {
    size_t header_end = finder.find_row_end(data, size, 0);
    impl_->header_end_offset = header_end;

    auto header_names = parser.parse_header(data, header_end);

    // Validate header (only if error handling is enabled)
    if (impl_->error_collector.is_enabled()) [[unlikely]] {
      // Check for empty header
      if (header_names.empty() || (header_names.size() == 1 && header_names[0].empty())) {
        impl_->error_collector.add_error(ErrorCode::EMPTY_HEADER, ErrorSeverity::FATAL, 1, 1, 0,
                                         "Header row is empty");
        if (impl_->error_collector.should_stop()) {
          return Result<bool>::failure("Header row is empty");
        }
      }

      // Check for duplicate column names
      std::unordered_set<std::string> seen_names;
      for (size_t i = 0; i < header_names.size(); ++i) {
        const auto& name = header_names[i];
        if (!name.empty() && !seen_names.insert(name).second) {
          impl_->error_collector.add_error(ErrorCode::DUPLICATE_COLUMN_NAMES,
                                           ErrorSeverity::WARNING, 1, i + 1, 0,
                                           "Duplicate column name: '" + name + "'");
          // Warnings don't stop parsing
        }
      }
    }

    // Create schema from header
    for (size_t i = 0; i < header_names.size(); ++i) {
      ColumnSchema col;
      col.name = header_names[i];
      col.index = i;
      col.type = DataType::STRING; // Will be refined by type inference
      impl_->schema.push_back(std::move(col));
    }
  } else {
    // No header - count columns from first row
    size_t first_row_end = finder.find_row_end(data, size, 0);

    // Count separators in first row
    bool in_quote = false;
    size_t col_count = 1;
    for (size_t i = 0; i < first_row_end; ++i) {
      char c = data[i];
      if (c == impl_->options.quote) {
        if (in_quote && i + 1 < first_row_end && data[i + 1] == impl_->options.quote) {
          ++i;
        } else {
          in_quote = !in_quote;
        }
      } else if (c == impl_->options.separator && !in_quote) {
        ++col_count;
      }
    }

    // Create generic column names
    for (size_t i = 0; i < col_count; ++i) {
      ColumnSchema col;
      col.name = "V" + std::to_string(i + 1);
      col.index = i;
      col.type = DataType::STRING;
      impl_->schema.push_back(std::move(col));
    }

    impl_->header_end_offset = 0;
  }

  // Perform type inference on sample rows
  if (!impl_->schema.empty()) {
    TypeInference inference(impl_->options);
    auto inferred_types = inference.infer_from_sample(
        data + impl_->header_end_offset, size - impl_->header_end_offset, impl_->schema.size(),
        impl_->options.sample_rows);

    for (size_t i = 0; i < impl_->schema.size() && i < inferred_types.size(); ++i) {
      impl_->schema[i].type = inferred_types[i];
    }
  }

  // Row count will be computed during read_all() to avoid separate SIMD pass
  // (eliminates ~5.6% overhead from AnalyzeChunkSimdImpl)
  impl_->row_count = 0;

  return Result<bool>::success(true);
}

Result<bool> CsvReader::open_from_buffer(AlignedBuffer buffer) {
  // Take ownership of the buffer
  impl_->owned_buffer = std::move(buffer);

  // Set data pointer from owned buffer
  impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
  impl_->data_size = impl_->owned_buffer.size();

  if (impl_->data_size == 0) {
    return Result<bool>::failure("Empty file");
  }

  // Detect encoding and transcode if needed
  {
    const auto* raw = reinterpret_cast<const uint8_t*>(impl_->data_ptr);
    size_t raw_size = impl_->data_size;

    if (impl_->options.encoding.has_value()) {
      impl_->detected_encoding.encoding = *impl_->options.encoding;
      auto bom_result = detect_encoding(raw, raw_size);
      if (bom_result.encoding == *impl_->options.encoding ||
          (*impl_->options.encoding == CharEncoding::UTF8 &&
           bom_result.encoding == CharEncoding::UTF8_BOM)) {
        impl_->detected_encoding.bom_length = bom_result.bom_length;
      }
      impl_->detected_encoding.confidence = 1.0;
      impl_->detected_encoding.needs_transcoding =
          (*impl_->options.encoding != CharEncoding::UTF8 &&
           *impl_->options.encoding != CharEncoding::UTF8_BOM);
    } else {
      impl_->detected_encoding = detect_encoding(raw, raw_size);
    }

    if (impl_->detected_encoding.needs_transcoding) {
      // Transcode into a new buffer, replacing the owned buffer
      AlignedBuffer transcoded = transcode_to_utf8(raw, raw_size, impl_->detected_encoding.encoding,
                                                   impl_->detected_encoding.bom_length);
      impl_->owned_buffer = std::move(transcoded);
      impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
      impl_->data_size = impl_->owned_buffer.size();
    } else if (impl_->detected_encoding.bom_length > 0) {
      impl_->data_ptr += impl_->detected_encoding.bom_length;
      impl_->data_size -= impl_->detected_encoding.bom_length;
    }
  }

  // Auto-detect dialect if separator is the sentinel value
  impl_->auto_detect_dialect();

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;

  ChunkFinder finder(impl_->options.separator, impl_->options.quote);
  LineParser parser(impl_->options);

  // Skip leading comment lines before header
  size_t comment_skip = skip_leading_comment_lines(data, size, impl_->options.comment);
  if (comment_skip > 0) {
    impl_->data_ptr += comment_skip;
    impl_->data_size -= comment_skip;
    data = impl_->data_ptr;
    size = impl_->data_size;
    if (size == 0) {
      return Result<bool>::failure("File contains only comment lines");
    }
  }

  // Parse header if present
  if (impl_->options.has_header) {
    size_t header_end = finder.find_row_end(data, size, 0);
    impl_->header_end_offset = header_end;

    auto header_names = parser.parse_header(data, header_end);

    // Validate header (only if error handling is enabled)
    if (impl_->error_collector.is_enabled()) [[unlikely]] {
      // Check for empty header
      if (header_names.empty() || (header_names.size() == 1 && header_names[0].empty())) {
        impl_->error_collector.add_error(ErrorCode::EMPTY_HEADER, ErrorSeverity::FATAL, 1, 1, 0,
                                         "Header row is empty");
        if (impl_->error_collector.should_stop()) {
          return Result<bool>::failure("Header row is empty");
        }
      }

      // Check for duplicate column names
      std::unordered_set<std::string> seen_names;
      for (size_t i = 0; i < header_names.size(); ++i) {
        const auto& name = header_names[i];
        if (!name.empty() && !seen_names.insert(name).second) {
          impl_->error_collector.add_error(ErrorCode::DUPLICATE_COLUMN_NAMES,
                                           ErrorSeverity::WARNING, 1, i + 1, 0,
                                           "Duplicate column name: '" + name + "'");
          // Warnings don't stop parsing
        }
      }
    }

    // Create schema from header
    for (size_t i = 0; i < header_names.size(); ++i) {
      ColumnSchema col;
      col.name = header_names[i];
      col.index = i;
      col.type = DataType::STRING; // Will be refined by type inference
      impl_->schema.push_back(std::move(col));
    }
  } else {
    // No header - count columns from first row
    size_t first_row_end = finder.find_row_end(data, size, 0);

    // Count separators in first row
    bool in_quote = false;
    size_t col_count = 1;
    for (size_t i = 0; i < first_row_end; ++i) {
      char c = data[i];
      if (c == impl_->options.quote) {
        if (in_quote && i + 1 < first_row_end && data[i + 1] == impl_->options.quote) {
          ++i;
        } else {
          in_quote = !in_quote;
        }
      } else if (c == impl_->options.separator && !in_quote) {
        ++col_count;
      }
    }

    // Create generic column names
    for (size_t i = 0; i < col_count; ++i) {
      ColumnSchema col;
      col.name = "V" + std::to_string(i + 1);
      col.index = i;
      col.type = DataType::STRING;
      impl_->schema.push_back(std::move(col));
    }

    impl_->header_end_offset = 0;
  }

  // Perform type inference on sample rows
  if (!impl_->schema.empty()) {
    TypeInference inference(impl_->options);
    auto inferred_types = inference.infer_from_sample(
        data + impl_->header_end_offset, size - impl_->header_end_offset, impl_->schema.size(),
        impl_->options.sample_rows);

    for (size_t i = 0; i < impl_->schema.size() && i < inferred_types.size(); ++i) {
      impl_->schema[i].type = inferred_types[i];
    }
  }

  // Row count will be computed during read_all() to avoid separate SIMD pass
  impl_->row_count = 0;

  return Result<bool>::success(true);
}

const std::vector<ColumnSchema>& CsvReader::schema() const {
  return impl_->schema;
}

const EncodingResult& CsvReader::encoding() const {
  return impl_->detected_encoding;
}

size_t CsvReader::row_count() const {
  return impl_->row_count;
}

const std::vector<ParseError>& CsvReader::errors() const {
  return impl_->error_collector.errors();
}

bool CsvReader::has_errors() const {
  return impl_->error_collector.has_errors();
}

std::optional<DetectionResult> CsvReader::detected_dialect() const {
  return impl_->detected_dialect_result;
}

Result<ParsedChunks> CsvReader::read_all() {
  ParsedChunks result;

  if (impl_->schema.empty()) {
    return Result<ParsedChunks>::success(std::move(result));
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  size_t data_start = impl_->header_end_offset;
  size_t data_size = size - data_start;

  // ========================================================================
  // Cache check: try to load cached index
  // ========================================================================
  const bool cache_enabled = impl_->options.cache.has_value() && !impl_->file_path.empty() &&
                             !impl_->options.force_cache_refresh;
  std::string cache_path;

  if (cache_enabled) {
    cache_path = IndexCache::compute_path(impl_->file_path, *impl_->options.cache);
  }

  // Also compute cache_path for writing even if force_refresh (we still want to write)
  if (!cache_path.empty() && !impl_->options.force_cache_refresh) {
    auto load_result = IndexCache::load(cache_path, impl_->file_path);
    if (load_result.ok()) {
      // Cache hit! Use cached chunk boundaries and analysis
      const auto& cached = load_result.index;
      size_t num_chunks = cached.chunk_boundaries.size();

      if (num_chunks == 0) {
        // Degenerate case: no chunks cached
        return read_all_serial();
      }

      // Reconstruct analysis_results and use_inside_state from cache
      std::vector<ChunkAnalysisResult> analysis_results(num_chunks);
      std::vector<bool> use_inside_state(num_chunks, false);

      for (size_t i = 0; i < num_chunks; ++i) {
        auto& ar = analysis_results[i];
        ar.ends_inside_starting_outside = cached.chunk_analysis[i].ends_inside_starting_outside;
        // We only need the row count for the correct state
        uint32_t row_count = cached.chunk_analysis[i].row_count;
        ar.row_count_outside = row_count;
        ar.row_count_inside = row_count;
      }

      // Phase 2: Link chunks (same logic, using cached analysis)
      for (size_t i = 1; i < num_chunks; ++i) {
        bool prev_used_inside = use_inside_state[i - 1];
        bool prev_ends_inside;
        if (prev_used_inside) {
          prev_ends_inside = !analysis_results[i - 1].ends_inside_starting_outside;
        } else {
          prev_ends_inside = analysis_results[i - 1].ends_inside_starting_outside;
        }
        use_inside_state[i] = prev_ends_inside;
      }

      impl_->row_count = cached.total_rows;

      // Phase 3: Parse chunks with cached state
      const bool check_errors = impl_->error_collector.is_enabled();
      std::vector<ErrorCollector> thread_error_collectors;
      if (check_errors) {
        thread_error_collectors.reserve(num_chunks);
        for (size_t i = 0; i < num_chunks; ++i) {
          thread_error_collectors.emplace_back(impl_->error_collector.mode(),
                                               impl_->error_collector.max_errors());
        }
      }

      size_t pool_threads = std::min(impl_->num_threads, num_chunks);
      BS::thread_pool pool(pool_threads);
      const CsvOptions options = impl_->options;
      const std::vector<ColumnSchema> schema = impl_->schema;

      std::vector<ChunkParseResult> chunk_results(num_chunks);
      {
        std::vector<std::future<void>> futures;
        futures.reserve(num_chunks);

        for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
          size_t start_offset = cached.chunk_boundaries[chunk_idx].first;
          size_t end_offset = cached.chunk_boundaries[chunk_idx].second;
          bool start_inside = use_inside_state[chunk_idx];
          size_t expected_rows = cached.chunk_analysis[chunk_idx].row_count;
          ErrorCollector* chunk_error_collector =
              check_errors ? &thread_error_collectors[chunk_idx] : nullptr;

          futures.push_back(pool.submit_task([&chunk_results, data, size, chunk_idx, start_offset,
                                              end_offset, start_inside, expected_rows, options,
                                              &schema, chunk_error_collector]() {
            if (start_offset >= size || end_offset > size || start_offset >= end_offset)
              return;

            auto& cr = chunk_results[chunk_idx];
            NullChecker null_checker(options);

            for (const auto& col_schema : schema) {
              auto builder = ArrowColumnBuilder::create(col_schema.type);
              builder->reserve(expected_rows);
              cr.columns.push_back(std::move(builder));
            }

            auto [rows, ends_inside] = parse_chunk_with_state(
                data + start_offset, end_offset - start_offset, options, null_checker, cr.columns,
                start_inside, chunk_error_collector, start_offset);
            (void)ends_inside;
            cr.row_count = rows;
          }));
        }

        for (auto& f : futures)
          f.get();
      }

      if (check_errors) {
        const auto& last_analysis = analysis_results[num_chunks - 1];
        bool last_used_inside = use_inside_state[num_chunks - 1];
        bool last_ends_inside = last_used_inside ? !last_analysis.ends_inside_starting_outside
                                                 : last_analysis.ends_inside_starting_outside;
        if (last_ends_inside) {
          size_t last_start = cached.chunk_boundaries[num_chunks - 1].first;
          thread_error_collectors.back().add_error(ErrorCode::UNCLOSED_QUOTE,
                                                   ErrorSeverity::RECOVERABLE, 0, 0, last_start,
                                                   "Quoted field not closed before end of data");
        }
        impl_->error_collector.merge_sorted(thread_error_collectors);
      }

      result.total_rows = 0;
      for (auto& cr : chunk_results) {
        result.total_rows += cr.row_count;
        result.chunks.push_back(std::move(cr.columns));
      }
      result.used_cache = true;
      result.cache_path = cache_path;

      return Result<ParsedChunks>::success(std::move(result));
    }
    // Cache miss - fall through to normal parsing
  }

  // Compute cache_path for writing if cache is configured
  if (impl_->options.cache.has_value() && !impl_->file_path.empty() && cache_path.empty()) {
    cache_path = IndexCache::compute_path(impl_->file_path, *impl_->options.cache);
  }

  // For small files, use single-threaded parsing
  // Use a higher threshold (1MB) to avoid overhead for medium files
  constexpr size_t PARALLEL_THRESHOLD = 1024 * 1024; // 1MB
  if (data_size < PARALLEL_THRESHOLD) {
    auto serial_result = read_all_serial();
    // Write cache for small files too if enabled
    if (serial_result.ok && !cache_path.empty() && impl_->options.cache.has_value()) {
      CachedIndex cached_idx;
      cached_idx.header_end_offset = impl_->header_end_offset;
      cached_idx.num_columns = static_cast<uint32_t>(impl_->schema.size());
      cached_idx.total_rows = serial_result.value.total_rows;
      cached_idx.sample_interval = impl_->options.cache->sample_interval;
      cached_idx.schema = impl_->schema;
      // Single chunk boundary
      cached_idx.chunk_boundaries.emplace_back(data_start, size);
      ChunkMeta meta;
      meta.row_count = static_cast<uint32_t>(serial_result.value.total_rows);
      meta.ends_inside_starting_outside = false;
      cached_idx.chunk_analysis.push_back(meta);
      // Empty sampled offsets for serial (small files don't benefit much)
      cached_idx.sampled_offsets = EliasFano::encode({}, 0);
      IndexCache::write_atomic(cache_path, cached_idx, impl_->file_path);
      serial_result.value.cache_path = cache_path;
    }
    return serial_result;
  }

  // Calculate chunk size based on Polars formula
  size_t n_cols = impl_->schema.size();
  size_t chunk_size = calculate_chunk_size(data_size, n_cols, impl_->num_threads);

  // Create chunk boundaries
  std::vector<std::pair<size_t, size_t>> chunk_ranges; // (start_offset, end_offset)
  size_t offset = data_start;

  ChunkFinder finder(impl_->options.separator, impl_->options.quote);

  while (offset < size) {
    size_t target_end = std::min(offset + chunk_size, size);
    size_t chunk_end;

    if (target_end >= size) {
      chunk_end = size;
    } else {
      // Find a row boundary near the target end
      chunk_end = finder.find_row_end(data, size, target_end);
      // If we're stuck, extend further
      while (chunk_end == target_end && chunk_end < size) {
        target_end = std::min(target_end + chunk_size, size);
        chunk_end = finder.find_row_end(data, size, target_end);
      }
    }

    chunk_ranges.emplace_back(offset, chunk_end);
    offset = chunk_end;
  }

  size_t num_chunks = chunk_ranges.size();

  // For single chunk, use serial processing
  if (num_chunks <= 1) {
    auto serial_result = read_all_serial();
    if (serial_result.ok && !cache_path.empty() && impl_->options.cache.has_value()) {
      CachedIndex cached_idx;
      cached_idx.header_end_offset = impl_->header_end_offset;
      cached_idx.num_columns = static_cast<uint32_t>(impl_->schema.size());
      cached_idx.total_rows = serial_result.value.total_rows;
      cached_idx.sample_interval = impl_->options.cache->sample_interval;
      cached_idx.schema = impl_->schema;
      size_t start_off = chunk_ranges.empty() ? data_start : chunk_ranges[0].first;
      size_t end_off = chunk_ranges.empty() ? size : chunk_ranges[0].second;
      cached_idx.chunk_boundaries.emplace_back(start_off, end_off);
      ChunkMeta meta;
      meta.row_count = static_cast<uint32_t>(serial_result.value.total_rows);
      meta.ends_inside_starting_outside = false;
      cached_idx.chunk_analysis.push_back(meta);
      cached_idx.sampled_offsets = EliasFano::encode({}, 0);
      IndexCache::write_atomic(cache_path, cached_idx, impl_->file_path);
      serial_result.value.cache_path = cache_path;
    }
    return serial_result;
  }

  // Create thread pool - limit to reasonable number of threads
  // With the new dual-state analysis approach, we no longer need 2x column builders
  // per chunk, so memory pressure is reduced and we can use more threads
  size_t pool_threads = std::min(impl_->num_threads, num_chunks);
  BS::thread_pool pool(pool_threads);

  // Capture what we need by value to avoid any lifetime issues
  const CsvOptions options = impl_->options;
  const std::vector<ColumnSchema> schema = impl_->schema;

  // ========================================================================
  // POLARS ALGORITHM: Two-phase approach
  // Phase 1: Lightweight dual-state ANALYSIS (single SIMD pass per chunk)
  //          - Counts rows for BOTH starting states simultaneously
  //          - Determines ending quote state
  // Phase 2: Link chunks to determine correct starting state for each
  // Phase 3: PARSE each chunk ONCE with the correct state only
  // ========================================================================

  // Phase 1: Analyze all chunks in parallel using dual-state SIMD
  // This is MUCH faster than parsing - just counts rows and tracks quote state
  std::vector<ChunkAnalysisResult> analysis_results(num_chunks);
  {
    std::vector<std::future<void>> futures;
    futures.reserve(num_chunks);

    for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
      size_t start_offset = chunk_ranges[chunk_idx].first;
      size_t end_offset = chunk_ranges[chunk_idx].second;

      futures.push_back(pool.submit_task(
          [&analysis_results, data, size, chunk_idx, start_offset, end_offset, options]() {
            if (start_offset >= size || end_offset > size || start_offset >= end_offset) {
              return;
            }

            const char* chunk_data = data + start_offset;
            size_t chunk_size = end_offset - start_offset;

            // Single-pass dual-state analysis using SIMD
            auto stats = analyze_chunk_dual_state_simd(chunk_data, chunk_size, options.quote);

            auto& result = analysis_results[chunk_idx];
            result.row_count_outside = stats.row_count_outside;
            result.row_count_inside = stats.row_count_inside;
            result.ends_inside_starting_outside = stats.ends_inside_quote_from_outside;
          }));
    }

    for (auto& f : futures) {
      f.get();
    }
  }

  // Phase 2: Link chunks - determine which starting state each chunk should use
  std::vector<bool> use_inside_state(num_chunks, false);
  use_inside_state[0] = false; // First chunk always starts outside quotes

  for (size_t i = 1; i < num_chunks; ++i) {
    const auto& prev_analysis = analysis_results[i - 1];
    bool prev_used_inside = use_inside_state[i - 1];

    // Ending state depends on which starting state was used
    // If started outside and ended inside, OR started inside and ended outside
    bool prev_ends_inside;
    if (prev_used_inside) {
      // Started inside: ends_inside is the OPPOSITE of ends_inside_from_outside
      prev_ends_inside = !prev_analysis.ends_inside_starting_outside;
    } else {
      // Started outside: use the value directly
      prev_ends_inside = prev_analysis.ends_inside_starting_outside;
    }

    use_inside_state[i] = prev_ends_inside;
  }

  // Compute total row count from analysis results (eliminates separate count_rows_simd pass)
  size_t total_row_count = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    total_row_count += use_inside_state[i] ? analysis_results[i].row_count_inside
                                           : analysis_results[i].row_count_outside;
  }
  impl_->row_count = total_row_count;

  // Phase 3: Parse each chunk ONCE with the correct starting state
  // Create per-thread error collectors if error handling is enabled
  const bool check_errors = impl_->error_collector.is_enabled();
  std::vector<ErrorCollector> thread_error_collectors;
  if (check_errors) {
    thread_error_collectors.reserve(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
      thread_error_collectors.emplace_back(impl_->error_collector.mode(),
                                           impl_->error_collector.max_errors());
    }
  }

  std::vector<ChunkParseResult> chunk_results(num_chunks);
  {
    std::vector<std::future<void>> futures;
    futures.reserve(num_chunks);

    for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
      size_t start_offset = chunk_ranges[chunk_idx].first;
      size_t end_offset = chunk_ranges[chunk_idx].second;
      bool start_inside = use_inside_state[chunk_idx];

      // Get expected row count from analysis phase for capacity reservation
      size_t expected_rows = start_inside ? analysis_results[chunk_idx].row_count_inside
                                          : analysis_results[chunk_idx].row_count_outside;

      ErrorCollector* chunk_error_collector =
          check_errors ? &thread_error_collectors[chunk_idx] : nullptr;

      futures.push_back(pool.submit_task([&chunk_results, data, size, chunk_idx, start_offset,
                                          end_offset, start_inside, expected_rows, options, &schema,
                                          chunk_error_collector]() {
        if (start_offset >= size || end_offset > size || start_offset >= end_offset) {
          return;
        }

        auto& result = chunk_results[chunk_idx];
        const char* chunk_data = data + start_offset;
        size_t chunk_size = end_offset - start_offset;

        NullChecker null_checker(options);

        // Create column builders with pre-allocated capacity
        for (const auto& col_schema : schema) {
          auto builder = ArrowColumnBuilder::create(col_schema.type);
          builder->reserve(expected_rows);
          result.columns.push_back(std::move(builder));
        }

        // Parse ONCE with the correct starting state
        auto [rows, ends_inside] =
            parse_chunk_with_state(chunk_data, chunk_size, options, null_checker, result.columns,
                                   start_inside, chunk_error_collector, start_offset);
        (void)ends_inside; // Already computed in analysis phase
        result.row_count = rows;
      }));
    }

    for (auto& f : futures) {
      f.get();
    }
  }

  // Merge per-thread error collectors into the main error collector
  if (check_errors) {
    // Detect UNCLOSED_QUOTE from the last chunk's ending state
    const auto& last_analysis = analysis_results[num_chunks - 1];
    bool last_used_inside = use_inside_state[num_chunks - 1];
    bool last_ends_inside;
    if (last_used_inside) {
      last_ends_inside = !last_analysis.ends_inside_starting_outside;
    } else {
      last_ends_inside = last_analysis.ends_inside_starting_outside;
    }
    if (last_ends_inside) {
      size_t last_start = chunk_ranges[num_chunks - 1].first;
      thread_error_collectors.back().add_error(ErrorCode::UNCLOSED_QUOTE,
                                               ErrorSeverity::RECOVERABLE, 0, 0, last_start,
                                               "Quoted field not closed before end of data");
    }

    impl_->error_collector.merge_sorted(thread_error_collectors);
  }

  // Phase 4: Return chunks directly (NO MERGING)
  // Each chunk becomes a separate Parquet row group (like Polars ChunkedArray)
  result.total_rows = 0;
  for (auto& chunk_result : chunk_results) {
    result.total_rows += chunk_result.row_count;
    result.chunks.push_back(std::move(chunk_result.columns));
  }

  // ========================================================================
  // Cache write: persist chunk analysis for future reads
  // ========================================================================
  if (!cache_path.empty() && impl_->options.cache.has_value()) {
    CachedIndex cached_idx;
    cached_idx.header_end_offset = impl_->header_end_offset;
    cached_idx.num_columns = static_cast<uint32_t>(impl_->schema.size());
    cached_idx.total_rows = result.total_rows;
    cached_idx.sample_interval = impl_->options.cache->sample_interval;
    cached_idx.schema = impl_->schema;
    cached_idx.chunk_boundaries = chunk_ranges;

    cached_idx.chunk_analysis.resize(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
      cached_idx.chunk_analysis[i].row_count =
          static_cast<uint32_t>(use_inside_state[i] ? analysis_results[i].row_count_inside
                                                    : analysis_results[i].row_count_outside);
      cached_idx.chunk_analysis[i].ends_inside_starting_outside =
          analysis_results[i].ends_inside_starting_outside;
    }

    // Sampled offsets (placeholder: chunk start offsets for now)
    std::vector<uint64_t> sample_offsets;
    for (const auto& range : chunk_ranges) {
      sample_offsets.push_back(range.first);
    }
    uint64_t universe = size > 0 ? size : 1;
    cached_idx.sampled_offsets = EliasFano::encode(sample_offsets, universe);
    cached_idx.sample_quote_states.resize((sample_offsets.size() + 7) / 8, 0);

    IndexCache::write_atomic(cache_path, cached_idx, impl_->file_path);
    result.cache_path = cache_path;
  }

  return Result<ParsedChunks>::success(std::move(result));
}

// Serial implementation for small files or fallback
Result<ParsedChunks> CsvReader::read_all_serial() {
  ParsedChunks result;
  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;

  if (impl_->schema.empty()) {
    return Result<ParsedChunks>::success(std::move(result));
  }

  // Create Arrow column builders based on inferred types
  // For serial path (small files), we don't pre-compute row count to avoid extra pass
  // The builder will grow dynamically, which is fine for small data
  for (const auto& col_schema : impl_->schema) {
    auto builder = ArrowColumnBuilder::create(col_schema.type);
    columns.push_back(std::move(builder));
  }

  // Create FastArrowContext for each column - eliminates virtual dispatch
  std::vector<FastArrowContext> fast_contexts;
  fast_contexts.reserve(columns.size());
  for (auto& col : columns) {
    fast_contexts.push_back(col->create_context());
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  const CsvOptions& options = impl_->options;

  // Create null checker once for O(1) lookup
  NullChecker null_checker(options);

  // Parse all rows using Polars-style SplitFields iterator
  // Key optimization: no separate find_row_end call - iterator handles EOL
  size_t offset = impl_->header_end_offset;
  const char quote = options.quote;
  const char sep = options.separator;
  const size_t num_cols = columns.size();
  const bool check_errors = impl_->error_collector.is_enabled();
  // Row number is 1-indexed; row 1 is the header (if present)
  size_t row_number = impl_->options.has_header ? 2 : 1;

  while (offset < size) {
    // Skip empty lines (when enabled)
    if (impl_->options.skip_empty_rows) {
      while (offset < size) {
        char c = data[offset];
        if (c == '\n') {
          offset++;
          continue;
        }
        if (c == '\r') {
          offset++;
          if (offset < size && data[offset] == '\n') {
            offset++;
          }
          continue;
        }
        break;
      }
    }

    if (offset >= size)
      break;

    // Skip comment lines (handle \n, \r\n, and bare \r)
    if (impl_->options.comment != '\0' && data[offset] == impl_->options.comment) {
      while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
        offset++;
      }
      if (offset < size && data[offset] == '\r') {
        offset++;
        if (offset < size && data[offset] == '\n') {
          offset++; // CRLF
        }
      } else if (offset < size && data[offset] == '\n') {
        offset++;
      }
      continue;
    }

    // Create iterator for remaining data - it stops at EOL
    size_t row_start_offset = offset;
    size_t start_remaining = size - offset;
    SplitFields iter(data + offset, start_remaining, sep, quote, '\n');

    const char* field_data;
    size_t field_len;
    bool needs_escaping;
    size_t col_idx = 0;

    while (iter.next(field_data, field_len, needs_escaping)) {
      // Strip trailing \r if present
      if (field_len > 0 && field_data[field_len - 1] == '\r') {
        field_len--;
      }

      std::string_view field_view(field_data, field_len);

      // Error detection within fields (only when error collection is enabled)
      if (check_errors) [[unlikely]] {
        // Null byte detection
        if (std::memchr(field_data, '\0', field_len)) {
          // Count and report each null byte
          for (size_t i = 0; i < field_len; ++i) {
            if (field_data[i] == '\0') {
              size_t byte_off = static_cast<size_t>(field_data - data) + i;
              impl_->error_collector.add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE,
                                               row_number, col_idx + 1, byte_off,
                                               "Unexpected null byte in data");
              if (impl_->error_collector.should_stop())
                goto done_serial;
            }
          }
        }

        // Quote in unquoted field
        if (!needs_escaping && field_len > 0 && std::memchr(field_data, quote, field_len)) {
          size_t byte_off = static_cast<size_t>(field_data - data);
          impl_->error_collector.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD,
                                           ErrorSeverity::RECOVERABLE, row_number, col_idx + 1,
                                           byte_off, "Quote character in unquoted field");
          if (impl_->error_collector.should_stop())
            goto done_serial;
        }
      }

      if (col_idx >= num_cols) {
        col_idx++;
        continue;
      }

      if (null_checker.is_null(field_view)) {
        // Devirtualized append_null call
        fast_contexts[col_idx].append_null();
      } else if (needs_escaping) {
        // Strip outer quotes
        if (field_len >= 2 && field_data[0] == quote && field_data[field_len - 1] == quote) {
          field_view = std::string_view(field_data + 1, field_len - 2);
        }
        bool has_invalid_escape = false;
        std::string unescaped =
            unescape_quotes(field_view, quote, check_errors ? &has_invalid_escape : nullptr);

        // Invalid quote escape detection
        if (has_invalid_escape) [[unlikely]] {
          size_t byte_off = static_cast<size_t>(field_data - data);
          impl_->error_collector.add_error(ErrorCode::INVALID_QUOTE_ESCAPE,
                                           ErrorSeverity::RECOVERABLE, row_number, col_idx + 1,
                                           byte_off, "Invalid quote escape sequence");
          if (impl_->error_collector.should_stop())
            goto done_serial;
        }

        // Devirtualized append call
        fast_contexts[col_idx].append(unescaped);
      } else {
        // Devirtualized append call
        fast_contexts[col_idx].append(field_view);
      }
      col_idx++;
    }

    // Error: inconsistent field count
    if (check_errors && col_idx != num_cols) [[unlikely]] {
      impl_->error_collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT,
                                       ErrorSeverity::RECOVERABLE, row_number, 0, row_start_offset,
                                       "Expected " + std::to_string(num_cols) + " fields, got " +
                                           std::to_string(col_idx));
      if (impl_->error_collector.should_stop())
        goto done_serial;
    }

    // Fill remaining columns with nulls
    for (; col_idx < num_cols; ++col_idx) {
      fast_contexts[col_idx].append_null();
    }

    // Advance offset by consumed bytes
    offset += start_remaining - iter.remaining();

    // Unclosed quote detection: if the iterator finished inside a quote
    // on the very last row (no more data), report it after the main loop
    if (check_errors && iter.finished_inside_quote() && offset >= size) [[unlikely]] {
      impl_->error_collector.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::RECOVERABLE,
                                       row_number, 0, row_start_offset,
                                       "Quoted field not closed before end of data");
    }

    row_number++;
  }

done_serial: // Early exit target for should_stop() (FAIL_FAST error mode)
  // Return as a single chunk
  result.total_rows = columns.empty() ? 0 : columns[0]->size();
  impl_->row_count = result.total_rows; // Set row count after parsing
  result.chunks.push_back(std::move(columns));
  return Result<ParsedChunks>::success(std::move(result));
}

// ============================================================================
// Streaming API implementation
// ============================================================================

Result<bool> CsvReader::start_streaming() {
  if (impl_->schema.empty()) {
    return Result<bool>::failure("No schema - call open() first");
  }
  if (impl_->streaming_active) {
    return Result<bool>::failure("Streaming already started");
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  size_t data_start = impl_->header_end_offset;
  size_t data_size = size - data_start;

  // For small files, produce a single chunk via serial parsing
  constexpr size_t PARALLEL_THRESHOLD = 1024 * 1024; // 1MB
  if (data_size < PARALLEL_THRESHOLD) {
    auto serial_result = read_all_serial();
    if (!serial_result.ok) {
      return Result<bool>::failure(serial_result.error);
    }
    size_t num_chunks = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, 4);
    for (size_t i = 0; i < num_chunks; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Calculate chunk boundaries (same logic as read_all)
  size_t n_cols = impl_->schema.size();
  size_t chunk_size = calculate_chunk_size(data_size, n_cols, impl_->num_threads);
  auto& chunk_ranges = impl_->streaming_chunk_ranges;
  chunk_ranges.clear();
  size_t offset = data_start;
  ChunkFinder finder(impl_->options.separator, impl_->options.quote);

  while (offset < size) {
    size_t target_end = std::min(offset + chunk_size, size);
    size_t chunk_end;
    if (target_end >= size) {
      chunk_end = size;
    } else {
      chunk_end = finder.find_row_end(data, size, target_end);
      while (chunk_end == target_end && chunk_end < size) {
        target_end = std::min(target_end + chunk_size, size);
        chunk_end = finder.find_row_end(data, size, target_end);
      }
    }
    chunk_ranges.emplace_back(offset, chunk_end);
    offset = chunk_end;
  }

  size_t num_chunks = chunk_ranges.size();
  if (num_chunks <= 1) {
    auto serial_result = read_all_serial();
    if (!serial_result.ok) {
      return Result<bool>::failure(serial_result.error);
    }
    size_t n = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(n, 4);
    for (size_t i = 0; i < n; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Phase 1: Analyze all chunks (SIMD, parallel)
  size_t pool_threads = std::min(impl_->num_threads, num_chunks);
  impl_->streaming_pool = std::make_unique<BS::thread_pool>(pool_threads);
  auto& pool = *impl_->streaming_pool;
  const CsvOptions options = impl_->options;

  auto& analysis_results = impl_->streaming_analysis;
  analysis_results.resize(num_chunks);
  {
    std::vector<std::future<void>> futures;
    futures.reserve(num_chunks);
    for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
      size_t start_offset = chunk_ranges[chunk_idx].first;
      size_t end_offset = chunk_ranges[chunk_idx].second;
      futures.push_back(pool.submit_task(
          [&analysis_results, data, size, chunk_idx, start_offset, end_offset, options]() {
            if (start_offset >= size || end_offset > size || start_offset >= end_offset)
              return;
            auto stats = analyze_chunk_dual_state_simd(data + start_offset,
                                                       end_offset - start_offset, options.quote);
            auto& result = analysis_results[chunk_idx];
            result.row_count_outside = stats.row_count_outside;
            result.row_count_inside = stats.row_count_inside;
            result.ends_inside_starting_outside = stats.ends_inside_quote_from_outside;
          }));
    }
    for (auto& f : futures)
      f.get();
  }

  // Phase 2: Link chunks (serial)
  auto& use_inside_state = impl_->streaming_use_inside;
  use_inside_state.assign(num_chunks, false);
  use_inside_state[0] = false;
  for (size_t i = 1; i < num_chunks; ++i) {
    bool prev_used_inside = use_inside_state[i - 1];
    bool prev_ends_inside;
    if (prev_used_inside) {
      prev_ends_inside = !analysis_results[i - 1].ends_inside_starting_outside;
    } else {
      prev_ends_inside = analysis_results[i - 1].ends_inside_starting_outside;
    }
    use_inside_state[i] = prev_ends_inside;
  }

  // Compute total row count
  size_t total_row_count = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    total_row_count += use_inside_state[i] ? analysis_results[i].row_count_inside
                                           : analysis_results[i].row_count_outside;
  }
  impl_->row_count = total_row_count;

  // Set up error collectors
  const bool check_errors = impl_->error_collector.is_enabled();
  if (check_errors) {
    impl_->streaming_error_collectors.clear();
    impl_->streaming_error_collectors.reserve(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
      impl_->streaming_error_collectors.emplace_back(impl_->error_collector.mode(),
                                                     impl_->error_collector.max_errors());
    }
  }

  // Create the bounded queue
  impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, /*max_buffered=*/4);

  // Phase 3: Dispatch parse tasks (fire-and-forget -- they push to queue)
  const std::vector<ColumnSchema> schema = impl_->schema;
  auto* queue_ptr = impl_->streaming_queue.get();
  auto* error_collectors_ptr = check_errors ? &impl_->streaming_error_collectors : nullptr;

  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t start_offset = chunk_ranges[chunk_idx].first;
    size_t end_offset = chunk_ranges[chunk_idx].second;
    bool start_inside = use_inside_state[chunk_idx];
    size_t expected_rows = start_inside ? analysis_results[chunk_idx].row_count_inside
                                        : analysis_results[chunk_idx].row_count_outside;
    ErrorCollector* chunk_error_collector =
        check_errors ? &(*error_collectors_ptr)[chunk_idx] : nullptr;

    pool.detach_task([queue_ptr, data, size, chunk_idx, start_offset, end_offset, start_inside,
                      expected_rows, options, schema, chunk_error_collector]() {
      if (start_offset >= size || end_offset > size || start_offset >= end_offset) {
        std::vector<std::unique_ptr<ArrowColumnBuilder>> empty;
        queue_ptr->push(chunk_idx, std::move(empty));
        return;
      }

      NullChecker null_checker(options);
      std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
      for (const auto& col_schema : schema) {
        auto builder = ArrowColumnBuilder::create(col_schema.type);
        builder->reserve(expected_rows);
        columns.push_back(std::move(builder));
      }

      auto [rows, ends_inside] = parse_chunk_with_state(
          data + start_offset, end_offset - start_offset, options, null_checker, columns,
          start_inside, chunk_error_collector, start_offset);
      (void)ends_inside;
      (void)rows;

      queue_ptr->push(chunk_idx, std::move(columns));
    });
  }

  impl_->streaming_active = true;
  return Result<bool>::success(true);
}

std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> CsvReader::next_chunk() {
  if (!impl_->streaming_active || !impl_->streaming_queue) {
    return std::nullopt;
  }

  auto result = impl_->streaming_queue->pop();

  if (!result.has_value()) {
    // All chunks consumed -- finalize
    if (impl_->error_collector.is_enabled() && !impl_->streaming_error_collectors.empty()) {
      size_t num_chunks = impl_->streaming_analysis.size();
      if (num_chunks > 0) {
        bool last_used_inside = impl_->streaming_use_inside[num_chunks - 1];
        bool last_ends_inside =
            last_used_inside
                ? !impl_->streaming_analysis[num_chunks - 1].ends_inside_starting_outside
                : impl_->streaming_analysis[num_chunks - 1].ends_inside_starting_outside;
        if (last_ends_inside) {
          size_t last_start = impl_->streaming_chunk_ranges[num_chunks - 1].first;
          impl_->streaming_error_collectors.back().add_error(
              ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::RECOVERABLE, 0, 0, last_start,
              "Quoted field not closed before end of data");
        }
      }
      impl_->error_collector.merge_sorted(impl_->streaming_error_collectors);
      impl_->streaming_error_collectors.clear();
    }

    // Wait for thread pool tasks to complete before destroying it
    impl_->streaming_pool.reset();
    impl_->streaming_queue.reset();
    impl_->streaming_active = false;
  }

  return result;
}

} // namespace libvroom
