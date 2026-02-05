#include "libvroom/arrow_column_builder.h"
#include "libvroom/error.h"
#include "libvroom/parse_utils.h"
#include "libvroom/split_fields.h"
#include "libvroom/streaming.h"
#include "libvroom/table.h"
#include "libvroom/vroom.h"

#include <cstring>
#include <deque>
#include <vector>

namespace libvroom {

// =============================================================================
// StreamingParser::Impl
// =============================================================================

struct StreamingParser::Impl {
  StreamingOptions options;

  // Internal buffer for accumulating partial input
  std::vector<char> buffer;
  size_t consumed = 0; // Bytes already parsed from the front of buffer

  // Schema state
  std::vector<ColumnSchema> schema;
  bool schema_ready = false;
  bool schema_explicit = false; // Set by set_schema()
  bool header_parsed = false;
  bool batch_initialized = false;

  // Error handling
  ErrorCollector error_collector;

  // Current batch being built
  std::vector<std::unique_ptr<ArrowColumnBuilder>> current_columns;
  std::vector<FastArrowContext> fast_contexts;
  size_t current_batch_rows = 0;

  // Ready batches
  std::deque<StreamBatch> ready_batches;

  // Null checker (initialized once schema is ready)
  std::unique_ptr<NullChecker> null_checker;

  // Finished flag
  bool finished = false;

  explicit Impl(const StreamingOptions& opts)
      : options(opts), error_collector(opts.csv.error_mode, opts.csv.max_errors) {}

  // Compact the buffer by removing consumed bytes
  void compact_buffer() {
    if (consumed > 0) {
      size_t remaining = buffer.size() - consumed;
      if (remaining > 0) {
        std::memmove(buffer.data(), buffer.data() + consumed, remaining);
      }
      buffer.resize(remaining);
      consumed = 0;
    }
  }

  // Initialize column builders for the current batch
  void init_batch() {
    current_columns.clear();
    fast_contexts.clear();
    current_batch_rows = 0;

    for (const auto& col_schema : schema) {
      auto builder = ArrowColumnBuilder::create(col_schema.type);
      current_columns.push_back(std::move(builder));
    }

    fast_contexts.reserve(current_columns.size());
    for (auto& col : current_columns) {
      fast_contexts.push_back(col->create_context());
    }

    batch_initialized = true;
  }

  // Ensure null_checker and batch are initialized
  void ensure_initialized() {
    if (!null_checker) {
      null_checker = std::make_unique<NullChecker>(options.csv);
    }
    if (!batch_initialized) {
      init_batch();
    }
  }

  // Flush the current batch to ready_batches
  void flush_batch(bool is_last) {
    if (current_batch_rows == 0 && !is_last) {
      return;
    }

    if (current_batch_rows > 0 || is_last) {
      StreamBatch batch;
      batch.columns = std::move(current_columns);
      batch.num_rows = current_batch_rows;
      batch.is_last = is_last;
      ready_batches.push_back(std::move(batch));
    }

    if (!is_last) {
      init_batch();
    }
  }

  // Try to parse the header from buffered data
  // Returns true if header was successfully parsed
  bool try_parse_header() {
    if (header_parsed)
      return true;

    const char* data = buffer.data() + consumed;
    size_t avail = buffer.size() - consumed;

    if (avail == 0)
      return false;

    if (!options.csv.has_header) {
      // No header mode - infer column count from the first row
      // We need at least one complete row
      size_t row_end = find_row_end_in_buffer(data, avail);
      if (row_end == 0)
        return false; // No complete row yet

      // Count separators in first row (exclude the newline chars)
      size_t content_end = row_end;
      while (content_end > 0 && (data[content_end - 1] == '\n' || data[content_end - 1] == '\r')) {
        content_end--;
      }

      bool in_q = false;
      size_t col_count = 1;
      for (size_t i = 0; i < content_end; ++i) {
        char c = data[i];
        if (c == options.csv.quote) {
          if (in_q && i + 1 < content_end && data[i + 1] == options.csv.quote) {
            ++i;
          } else {
            in_q = !in_q;
          }
        } else if (c == options.csv.separator && !in_q) {
          ++col_count;
        }
      }

      // Create generic column names
      for (size_t i = 0; i < col_count; ++i) {
        ColumnSchema col;
        col.name = "V" + std::to_string(i + 1);
        col.index = i;
        col.type = DataType::STRING;
        schema.push_back(std::move(col));
      }

      header_parsed = true;
      // Don't consume any bytes - first row is data
      return true;
    }

    // Find end of header line
    size_t row_end = find_row_end_in_buffer(data, avail);
    if (row_end == 0)
      return false; // Header line not complete yet

    // Parse header
    LineParser parser(options.csv);
    auto header_names = parser.parse_header(data, row_end);

    for (size_t i = 0; i < header_names.size(); ++i) {
      ColumnSchema col;
      col.name = header_names[i];
      col.index = i;
      col.type = DataType::STRING; // Will be refined by type inference
      schema.push_back(std::move(col));
    }

    consumed += row_end;
    header_parsed = true;
    return true;
  }

  // Try to infer types from available data
  void try_infer_types() {
    if (schema.empty())
      return;

    const char* data = buffer.data() + consumed;
    size_t avail = buffer.size() - consumed;

    if (avail == 0)
      return;

    TypeInference inference(options.csv);
    auto inferred_types =
        inference.infer_from_sample(data, avail, schema.size(), options.csv.sample_rows);

    for (size_t i = 0; i < schema.size() && i < inferred_types.size(); ++i) {
      schema[i].type = inferred_types[i];
    }
  }

  // Find the end of the first complete row in the buffer.
  // Returns offset past the row terminator, or 0 if no complete row.
  size_t find_row_end_in_buffer(const char* data, size_t size) {
    bool in_q = false;
    for (size_t i = 0; i < size; ++i) {
      char c = data[i];
      if (c == options.csv.quote) {
        if (in_q && i + 1 < size && data[i + 1] == options.csv.quote) {
          ++i; // Skip escaped quote
        } else {
          in_q = !in_q;
        }
      } else if (!in_q && c == '\n') {
        return i + 1;
      } else if (!in_q && c == '\r') {
        if (i + 1 < size && data[i + 1] == '\n') {
          return i + 2;
        }
        return i + 1;
      }
    }
    return 0; // No complete row
  }

  // Find the end of the last complete row in a data range.
  // Returns offset past the last row terminator, or 0 if no complete row found.
  size_t find_last_row_end(const char* data, size_t size) {
    size_t last_end = 0;
    size_t pos = 0;
    while (pos < size) {
      size_t row_end = find_row_end_in_buffer(data + pos, size - pos);
      if (row_end == 0)
        break;
      pos += row_end;
      last_end = pos;
    }
    return last_end;
  }

  // Parse a known-complete region of the buffer (all rows have terminators).
  // data points to the start, parseable_size is the length of complete rows.
  void parse_rows(const char* data, size_t parseable_size) {
    if (schema.empty() || !batch_initialized || parseable_size == 0)
      return;

    const char quote = options.csv.quote;
    const char sep = options.csv.separator;
    const size_t num_cols = schema.size();
    const bool check_errors = error_collector.is_enabled();
    const size_t batch_size = options.batch_size;

    size_t offset = 0;

    while (offset < parseable_size) {
      // Skip empty lines
      while (offset < parseable_size) {
        char c = data[offset];
        if (c == '\n') {
          offset++;
          continue;
        }
        if (c == '\r') {
          offset++;
          if (offset < parseable_size && data[offset] == '\n') {
            offset++;
          }
          continue;
        }
        break;
      }

      if (offset >= parseable_size)
        break;

      // Parse one row using SplitFields
      size_t row_remaining = parseable_size - offset;
      SplitFields iter(data + offset, row_remaining, sep, quote, '\n');

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

        // Error detection
        if (check_errors) [[unlikely]] {
          if (std::memchr(field_data, '\0', field_len)) {
            for (size_t i = 0; i < field_len; ++i) {
              if (field_data[i] == '\0') {
                error_collector.add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE, 0,
                                          col_idx + 1, 0, "Unexpected null byte in data");
                if (error_collector.should_stop())
                  return;
              }
            }
          }

          if (!needs_escaping && field_len > 0 && std::memchr(field_data, quote, field_len)) {
            error_collector.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD,
                                      ErrorSeverity::RECOVERABLE, 0, col_idx + 1, 0,
                                      "Quote character in unquoted field");
            if (error_collector.should_stop())
              return;
          }
        }

        if (col_idx >= num_cols) {
          col_idx++;
          continue;
        }

        if (null_checker->is_null(field_view)) {
          fast_contexts[col_idx].append_null();
        } else if (needs_escaping) {
          // Strip outer quotes
          if (field_len >= 2 && field_data[0] == quote && field_data[field_len - 1] == quote) {
            field_view = std::string_view(field_data + 1, field_len - 2);
          }
          bool has_invalid_escape = false;
          std::string unescaped =
              unescape_quotes(field_view, quote, check_errors ? &has_invalid_escape : nullptr);

          if (has_invalid_escape) [[unlikely]] {
            error_collector.add_error(ErrorCode::INVALID_QUOTE_ESCAPE, ErrorSeverity::RECOVERABLE,
                                      0, col_idx + 1, 0, "Invalid quote escape sequence");
            if (error_collector.should_stop())
              return;
          }

          fast_contexts[col_idx].append(unescaped);
        } else {
          fast_contexts[col_idx].append(field_view);
        }
        col_idx++;
      }

      // Error: inconsistent field count
      if (check_errors && col_idx != num_cols) [[unlikely]] {
        error_collector.add_error(
            ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 0, 0, 0,
            "Expected " + std::to_string(num_cols) + " fields, got " + std::to_string(col_idx));
        if (error_collector.should_stop())
          return;
      }

      // Fill remaining columns with nulls
      for (; col_idx < num_cols; ++col_idx) {
        fast_contexts[col_idx].append_null();
      }

      current_batch_rows++;
      offset += row_remaining - iter.remaining();

      // Check if we should flush the batch
      if (batch_size > 0 && current_batch_rows >= batch_size) {
        flush_batch(false);
      }
    }
  }

  // Parse available complete rows from the buffer
  void parse_available_rows(bool is_final) {
    if (schema.empty() || !batch_initialized)
      return;

    const char* data = buffer.data() + consumed;
    size_t avail = buffer.size() - consumed;

    if (avail == 0)
      return;

    if (is_final) {
      // In final mode, parse everything (including partial final row)
      parse_rows(data, avail);
      consumed += avail;
    } else {
      // In non-final mode, only parse up to the last complete row
      size_t parseable = find_last_row_end(data, avail);
      if (parseable == 0)
        return; // No complete rows yet

      parse_rows(data, parseable);
      consumed += parseable;
    }
  }
};

// =============================================================================
// StreamingParser public interface
// =============================================================================

StreamingParser::StreamingParser(const StreamingOptions& options)
    : impl_(std::make_unique<Impl>(options)) {}

StreamingParser::~StreamingParser() = default;

StreamingParser::StreamingParser(StreamingParser&&) noexcept = default;
StreamingParser& StreamingParser::operator=(StreamingParser&&) noexcept = default;

Result<void> StreamingParser::feed(const char* data, size_t size) {
  if (impl_->finished) {
    return Result<void>::failure("Cannot feed after finish()");
  }

  if (size == 0)
    return Result<void>::success();

  // Compact buffer if needed
  if (impl_->consumed > impl_->buffer.size() / 2) {
    impl_->compact_buffer();
  }

  // Append new data
  impl_->buffer.insert(impl_->buffer.end(), data, data + size);

  // Try to parse header if not done yet
  if (!impl_->header_parsed) {
    if (!impl_->schema_explicit) {
      if (!impl_->try_parse_header()) {
        return Result<void>::success(); // Need more data for header
      }
    } else {
      // Schema was set explicitly, but we still need to skip the header line if present
      if (impl_->options.csv.has_header) {
        const char* buf_data = impl_->buffer.data() + impl_->consumed;
        size_t avail = impl_->buffer.size() - impl_->consumed;
        size_t row_end = impl_->find_row_end_in_buffer(buf_data, avail);
        if (row_end == 0) {
          return Result<void>::success(); // Need more data for header
        }
        impl_->consumed += row_end;
      }
      impl_->header_parsed = true;
    }
  }

  // Infer types if schema is not yet finalized
  if (!impl_->schema_ready) {
    impl_->try_infer_types();
    impl_->schema_ready = true;
  }

  // Ensure null_checker and batch are initialized
  impl_->ensure_initialized();

  // Parse available rows
  impl_->parse_available_rows(false);

  if (impl_->error_collector.should_stop()) {
    return Result<void>::failure("Parsing stopped due to errors");
  }

  return Result<void>::success();
}

std::optional<StreamBatch> StreamingParser::next_batch() {
  if (impl_->ready_batches.empty()) {
    return std::nullopt;
  }

  StreamBatch batch = std::move(impl_->ready_batches.front());
  impl_->ready_batches.pop_front();
  return batch;
}

Result<void> StreamingParser::finish() {
  if (impl_->finished) {
    return Result<void>::success();
  }
  impl_->finished = true;

  // If we never got any data, just return
  if (impl_->buffer.empty() && impl_->consumed == 0) {
    return Result<void>::success();
  }

  // If header wasn't parsed yet, try one more time
  if (!impl_->header_parsed) {
    if (!impl_->schema_explicit) {
      impl_->try_parse_header();
    } else {
      if (impl_->options.csv.has_header) {
        const char* buf_data = impl_->buffer.data() + impl_->consumed;
        size_t avail = impl_->buffer.size() - impl_->consumed;
        size_t row_end = impl_->find_row_end_in_buffer(buf_data, avail);
        if (row_end > 0) {
          impl_->consumed += row_end;
        }
      }
      impl_->header_parsed = true;
    }
  }

  // Initialize schema if not done
  if (!impl_->schema_ready && !impl_->schema.empty()) {
    impl_->try_infer_types();
    impl_->schema_ready = true;
  }

  // Ensure null_checker and batch are initialized
  if (impl_->schema_ready && !impl_->schema.empty()) {
    impl_->ensure_initialized();
  }

  // Parse remaining data (including partial final row)
  if (impl_->schema_ready && !impl_->schema.empty()) {
    impl_->parse_available_rows(true);
  }

  // Flush remaining batch
  if (impl_->current_batch_rows > 0 || impl_->ready_batches.empty()) {
    impl_->flush_batch(true);
  } else if (!impl_->ready_batches.empty()) {
    // Mark the last batch as is_last
    impl_->ready_batches.back().is_last = true;
  }

  if (impl_->error_collector.should_stop()) {
    return Result<void>::failure("Parsing stopped due to errors");
  }

  return Result<void>::success();
}

void StreamingParser::set_schema(const std::vector<ColumnSchema>& schema) {
  impl_->schema = schema;
  impl_->schema_explicit = true;
  impl_->schema_ready = true;
  // Don't initialize batch yet - wait until feed() is called so we know we have data
}

bool StreamingParser::schema_ready() const {
  return impl_->schema_ready;
}

const std::vector<ColumnSchema>& StreamingParser::schema() const {
  return impl_->schema;
}

bool StreamingParser::has_errors() const {
  return impl_->error_collector.has_errors();
}

const std::vector<ParseError>& StreamingParser::errors() const {
  return impl_->error_collector.errors();
}

const ErrorCollector& StreamingParser::error_collector() const {
  return impl_->error_collector;
}

// =============================================================================
// read_csv_stream convenience function
// =============================================================================

std::shared_ptr<Table> read_csv_stream(std::istream& input, const StreamingOptions& options) {
  StreamingParser parser(options);

  constexpr size_t READ_SIZE = 64 * 1024; // 64KB chunks
  std::vector<char> read_buffer(READ_SIZE);

  while (input.good()) {
    input.read(read_buffer.data(), static_cast<std::streamsize>(READ_SIZE));
    auto bytes_read = static_cast<size_t>(input.gcount());
    if (bytes_read == 0)
      break;

    auto result = parser.feed(read_buffer.data(), bytes_read);
    if (!result.ok) {
      return nullptr;
    }
  }

  auto finish_result = parser.finish();
  if (!finish_result.ok) {
    return nullptr;
  }

  // Collect all batches into ParsedChunks
  const auto& schema = parser.schema();
  if (schema.empty()) {
    return nullptr;
  }

  ParsedChunks parsed;
  while (auto batch = parser.next_batch()) {
    parsed.total_rows += batch->num_rows;
    parsed.chunks.push_back(std::move(batch->columns));
  }

  return Table::from_parsed_chunks(schema, std::move(parsed));
}

} // namespace libvroom
