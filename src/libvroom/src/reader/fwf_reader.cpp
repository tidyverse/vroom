#include "libvroom/arrow_column_builder.h"
#include "libvroom/encoding.h"
#include "libvroom/fast_arrow_context.h"
#include "libvroom/parse_utils.h"
#include "libvroom/parsed_chunk_queue.h"
#include "libvroom/vroom.h"

#include "BS_thread_pool.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>

namespace libvroom {

// Skip leading comment lines. Returns offset past all leading comment lines.
static size_t skip_leading_comment_lines_fwf(const char* data, size_t size, char comment_char) {
  if (comment_char == '\0' || size == 0) {
    return 0;
  }

  size_t offset = 0;
  while (offset < size) {
    if (data[offset] != comment_char) {
      break;
    }
    while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
      offset++;
    }
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n') {
        offset++;
      }
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
  }
  return offset;
}

// Skip N data lines (for the skip option). Returns offset past skipped lines.
static size_t skip_n_lines(const char* data, size_t size, size_t n) {
  if (n == 0 || size == 0) {
    return 0;
  }

  size_t offset = 0;
  size_t lines_skipped = 0;
  while (offset < size && lines_skipped < n) {
    // Scan to end of line
    while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
      offset++;
    }
    // Advance past line ending
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n') {
        offset++;
      }
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
    lines_skipped++;
  }
  return offset;
}

// Trim leading and trailing whitespace (spaces and tabs)
static std::string_view trim_whitespace(std::string_view sv) {
  size_t start = 0;
  while (start < sv.size() && (sv[start] == ' ' || sv[start] == '\t')) {
    start++;
  }
  size_t end = sv.size();
  while (end > start && (sv[end - 1] == ' ' || sv[end - 1] == '\t')) {
    end--;
  }
  return sv.substr(start, end - start);
}

// Parse a chunk of FWF data into column builders.
// Returns the number of rows parsed.
// max_rows: maximum rows to parse (-1 = unlimited)
static size_t parse_fwf_chunk(
    const char* data, size_t size, const FwfOptions& options, const NullChecker& null_checker,
    std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns, int64_t max_rows = -1) {
  if (size == 0 || columns.empty()) {
    return 0;
  }

  std::vector<FastArrowContext> fast_contexts;
  fast_contexts.reserve(columns.size());
  for (auto& col : columns) {
    fast_contexts.push_back(col->create_context());
  }

  const size_t num_cols = columns.size();
  size_t offset = 0;
  size_t row_count = 0;

  while (offset < size && (max_rows < 0 || static_cast<int64_t>(row_count) < max_rows)) {
    // Skip empty lines
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

    // Skip comment lines
    if (options.comment != '\0' && data[offset] == options.comment) {
      while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
        offset++;
      }
      if (offset < size && data[offset] == '\r') {
        offset++;
        if (offset < size && data[offset] == '\n') {
          offset++;
        }
      } else if (offset < size && data[offset] == '\n') {
        offset++;
      }
      continue;
    }

    // Find end of this line
    const char* line_start = data + offset;
    size_t line_offset = offset;
    while (line_offset < size && data[line_offset] != '\n' && data[line_offset] != '\r') {
      line_offset++;
    }
    size_t line_len = line_offset - offset;

    // Strip trailing \r
    if (line_len > 0 && line_start[line_len - 1] == '\r') {
      line_len--;
    }

    // Extract fixed-width fields
    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
      int col_start = options.col_starts[col_idx];
      int col_end = options.col_ends[col_idx];

      std::string_view field;

      if (static_cast<size_t>(col_start) >= line_len) {
        // Field starts past end of line -> empty
        field = std::string_view();
      } else if (col_end == -1) {
        // Ragged: extend to end of line
        field = std::string_view(line_start + col_start, line_len - col_start);
      } else {
        size_t end = std::min(static_cast<size_t>(col_end), line_len);
        field = std::string_view(line_start + col_start, end - col_start);
      }

      if (options.trim_ws) {
        field = trim_whitespace(field);
      }

      if (null_checker.is_null(field)) {
        fast_contexts[col_idx].append_null();
      } else {
        fast_contexts[col_idx].append(field);
      }
    }

    row_count++;

    // Advance past line ending
    offset = line_offset;
    if (offset < size && data[offset] == '\r') {
      offset++;
    }
    if (offset < size && data[offset] == '\n') {
      offset++;
    }
  }

  return row_count;
}

// Count newlines in a data region (for row estimation)
static size_t count_newlines(const char* data, size_t size) {
  size_t count = 0;
  for (size_t i = 0; i < size; ++i) {
    if (data[i] == '\n') {
      count++;
    }
  }
  // If data doesn't end with newline, count the last partial line
  if (size > 0 && data[size - 1] != '\n') {
    count++;
  }
  return count;
}

// FWF-specific type inference: sample rows by scanning for newlines
// and extracting fields at fixed positions, then call infer_field per field.
static std::vector<DataType> infer_fwf_types(
    const char* data, size_t size, const FwfOptions& options, size_t max_rows) {
  size_t num_cols = options.col_starts.size();
  std::vector<DataType> types(num_cols, DataType::UNKNOWN);

  if (size == 0 || num_cols == 0) {
    return types;
  }

  // Build a CsvOptions for TypeInference (it only uses null/true/false values)
  CsvOptions csv_opts;
  csv_opts.null_values = options.null_values;
  csv_opts.true_values = options.true_values;
  csv_opts.false_values = options.false_values;
  TypeInference inference(csv_opts);

  size_t offset = 0;
  size_t rows_sampled = 0;

  while (offset < size && rows_sampled < max_rows) {
    // Skip empty lines (if configured) and comment lines
    while (offset < size) {
      char c = data[offset];
      if (options.skip_empty_rows) {
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
      }
      if (options.comment != '\0' && c == options.comment) {
        while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
          offset++;
        }
        continue;
      }
      break;
    }

    if (offset >= size)
      break;

    // Find end of this line
    const char* line_start = data + offset;
    size_t line_offset = offset;
    while (line_offset < size && data[line_offset] != '\n' && data[line_offset] != '\r') {
      line_offset++;
    }
    size_t line_len = line_offset - offset;

    // Strip trailing \r
    if (line_len > 0 && line_start[line_len - 1] == '\r') {
      line_len--;
    }

    // Extract and infer each field
    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
      int col_start = options.col_starts[col_idx];
      int col_end = options.col_ends[col_idx];

      std::string_view field;
      if (static_cast<size_t>(col_start) >= line_len) {
        field = std::string_view();
      } else if (col_end == -1) {
        field = std::string_view(line_start + col_start, line_len - col_start);
      } else {
        size_t end = std::min(static_cast<size_t>(col_end), line_len);
        field = std::string_view(line_start + col_start, end - col_start);
      }

      if (options.trim_ws) {
        field = trim_whitespace(field);
      }

      DataType field_type = inference.infer_field(field);
      types[col_idx] = wider_type(types[col_idx], field_type);
    }

    // Advance past line ending
    offset = line_offset;
    if (offset < size && data[offset] == '\r') {
      offset++;
    }
    if (offset < size && data[offset] == '\n') {
      offset++;
    }
    rows_sampled++;
  }

  // Convert UNKNOWN to STRING
  for (auto& t : types) {
    if (t == DataType::UNKNOWN) {
      t = DataType::STRING;
    }
  }

  return types;
}

struct FwfReader::Impl {
  FwfOptions options;
  MmapSource source;
  AlignedBuffer owned_buffer; // For transcoded data
  const char* data_ptr = nullptr;
  size_t data_size = 0;
  std::vector<ColumnSchema> schema;
  size_t row_count = 0;
  size_t data_start_offset = 0;
  size_t num_threads = 0;
  EncodingResult detected_encoding;

  // Streaming state
  std::unique_ptr<ParsedChunkQueue> streaming_queue;
  std::unique_ptr<BS::thread_pool> streaming_pool;
  std::vector<std::pair<size_t, size_t>> streaming_chunk_ranges;
  bool streaming_active = false;

  ~Impl() {
    if (streaming_queue) {
      streaming_queue->close();
    }
    streaming_pool.reset();
  }

  Impl(const FwfOptions& opts) : options(opts) {
    if (options.num_threads > 0) {
      num_threads = options.num_threads;
    } else {
      num_threads = std::thread::hardware_concurrency();
      if (num_threads == 0)
        num_threads = 4;
    }
  }
};

FwfReader::FwfReader(const FwfOptions& options) : impl_(std::make_unique<Impl>(options)) {}

FwfReader::~FwfReader() = default;

// Shared initialization: encoding detection, comment/line skipping, schema building, type inference.
// Called after data_ptr/data_size are set by open() or open_from_buffer().
Result<bool> FwfReader::initialize_data() {
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
      impl_->owned_buffer = transcode_to_utf8(raw, raw_size, impl_->detected_encoding.encoding,
                                              impl_->detected_encoding.bom_length);
      impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
      impl_->data_size = impl_->owned_buffer.size();
    } else if (impl_->detected_encoding.bom_length > 0) {
      impl_->data_ptr += impl_->detected_encoding.bom_length;
      impl_->data_size -= impl_->detected_encoding.bom_length;
    }
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;

  // Skip leading comment lines
  size_t comment_skip = skip_leading_comment_lines_fwf(data, size, impl_->options.comment);
  if (comment_skip > 0) {
    impl_->data_ptr += comment_skip;
    impl_->data_size -= comment_skip;
    data = impl_->data_ptr;
    size = impl_->data_size;
    if (size == 0) {
      return Result<bool>::failure("Data contains only comment lines");
    }
  }

  // Skip N data lines (user-specified skip)
  if (impl_->options.skip > 0) {
    size_t line_skip = skip_n_lines(data, size, impl_->options.skip);
    impl_->data_ptr += line_skip;
    impl_->data_size -= line_skip;
    data = impl_->data_ptr;
    size = impl_->data_size;
  }

  impl_->data_start_offset = 0;

  // Build schema from col_names
  size_t num_cols = impl_->options.col_starts.size();
  for (size_t i = 0; i < num_cols; ++i) {
    ColumnSchema col;
    if (i < impl_->options.col_names.size()) {
      col.name = impl_->options.col_names[i];
    } else {
      col.name = "X" + std::to_string(i + 1);
    }
    col.index = i;
    col.type = DataType::STRING;
    impl_->schema.push_back(std::move(col));
  }

  // Type inference on sample rows (from data after skip)
  if (!impl_->schema.empty()) {
    auto inferred_types = infer_fwf_types(data, size, impl_->options, impl_->options.sample_rows);
    for (size_t i = 0; i < impl_->schema.size() && i < inferred_types.size(); ++i) {
      impl_->schema[i].type = inferred_types[i];
    }
  }

  impl_->row_count = 0;

  return Result<bool>::success(true);
}

Result<bool> FwfReader::open(const std::string& path) {
  auto result = impl_->source.open(path);
  if (!result) {
    return result;
  }

  impl_->data_ptr = impl_->source.data();
  impl_->data_size = impl_->source.size();

  if (impl_->data_size == 0) {
    return Result<bool>::failure("Empty file");
  }

  return initialize_data();
}

Result<bool> FwfReader::open_from_buffer(AlignedBuffer buffer) {
  impl_->owned_buffer = std::move(buffer);
  impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
  impl_->data_size = impl_->owned_buffer.size();

  if (impl_->data_size == 0) {
    return Result<bool>::failure("Empty buffer");
  }

  return initialize_data();
}

const std::vector<ColumnSchema>& FwfReader::schema() const {
  return impl_->schema;
}

void FwfReader::set_schema(const std::vector<ColumnSchema>& schema) {
  for (size_t i = 0; i < schema.size() && i < impl_->schema.size(); ++i) {
    if (schema[i].type != DataType::UNKNOWN) {
      impl_->schema[i].type = schema[i].type;
    }
  }
}

const EncodingResult& FwfReader::encoding() const {
  return impl_->detected_encoding;
}

size_t FwfReader::row_count() const {
  return impl_->row_count;
}

Result<ParsedChunks> FwfReader::read_all_serial() {
  ParsedChunks result;

  if (impl_->schema.empty()) {
    return Result<ParsedChunks>::success(std::move(result));
  }

  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
  for (const auto& col_schema : impl_->schema) {
    auto builder = ArrowColumnBuilder::create(col_schema.type);
    columns.push_back(std::move(builder));
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  NullChecker null_checker(impl_->options);

  size_t rows =
      parse_fwf_chunk(data, size, impl_->options, null_checker, columns, impl_->options.max_rows);

  result.total_rows = rows;
  impl_->row_count = rows;
  result.chunks.push_back(std::move(columns));
  return Result<ParsedChunks>::success(std::move(result));
}

Result<bool> FwfReader::start_streaming() {
  if (impl_->schema.empty()) {
    return Result<bool>::failure("No schema - call open() first");
  }
  if (impl_->streaming_active) {
    return Result<bool>::failure("Streaming already started");
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  size_t data_size = size;

  // Small files or row-limited reads: serial parsing
  // (row limits require global coordination, so use serial path)
  constexpr size_t PARALLEL_THRESHOLD = 1024 * 1024; // 1MB
  bool has_row_limit = impl_->options.max_rows >= 0;
  if (data_size < PARALLEL_THRESHOLD || has_row_limit) {
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

  // Large files: parallel chunking
  // FWF has no quoting, so chunk boundaries just need to find newlines
  size_t n_cols = impl_->schema.size();
  size_t chunk_size = calculate_chunk_size(data_size, n_cols, impl_->num_threads);

  auto& chunk_ranges = impl_->streaming_chunk_ranges;
  chunk_ranges.clear();
  size_t offset = 0;

  while (offset < size) {
    size_t target_end = std::min(offset + chunk_size, size);
    size_t chunk_end;

    if (target_end >= size) {
      chunk_end = size;
    } else {
      // Find the next newline at or after target_end (no quote awareness needed)
      chunk_end = target_end;
      while (chunk_end < size && data[chunk_end] != '\n') {
        chunk_end++;
      }
      if (chunk_end < size) {
        chunk_end++; // Include the newline in this chunk
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

  // Count rows per chunk for capacity pre-allocation
  std::vector<size_t> chunk_row_counts(num_chunks, 0);
  size_t total_rows = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    size_t start = chunk_ranges[i].first;
    size_t end = chunk_ranges[i].second;
    chunk_row_counts[i] = count_newlines(data + start, end - start);
    total_rows += chunk_row_counts[i];
  }
  impl_->row_count = total_rows;

  // Create thread pool and queue
  size_t pool_threads = std::min(impl_->num_threads, num_chunks);
  impl_->streaming_pool = std::make_unique<BS::thread_pool>(pool_threads);
  impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, /*max_buffered=*/4);

  const FwfOptions options = impl_->options;
  const std::vector<ColumnSchema> schema = impl_->schema;
  auto* queue_ptr = impl_->streaming_queue.get();

  // Dispatch parse tasks
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t start_offset = chunk_ranges[chunk_idx].first;
    size_t end_offset = chunk_ranges[chunk_idx].second;
    size_t expected_rows = chunk_row_counts[chunk_idx];

    impl_->streaming_pool->detach_task([queue_ptr, data, size, chunk_idx, start_offset, end_offset,
                                        expected_rows, options, schema]() {
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

      parse_fwf_chunk(data + start_offset, end_offset - start_offset, options, null_checker,
                       columns);

      queue_ptr->push(chunk_idx, std::move(columns));
    });
  }

  impl_->streaming_active = true;
  return Result<bool>::success(true);
}

std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> FwfReader::next_chunk() {
  if (!impl_->streaming_active || !impl_->streaming_queue) {
    return std::nullopt;
  }

  auto result = impl_->streaming_queue->pop();

  if (!result.has_value()) {
    impl_->streaming_pool.reset();
    impl_->streaming_queue.reset();
    impl_->streaming_active = false;
  }

  return result;
}

} // namespace libvroom
