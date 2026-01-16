/**
 * @file streaming.cpp
 * @brief Implementation of streaming CSV parser.
 */

#include "streaming.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace libvroom {

//-----------------------------------------------------------------------------
// Field implementation
//-----------------------------------------------------------------------------

std::string Field::unescaped(char quote_char) const {
  if (!is_quoted || data.empty()) {
    return std::string(data);
  }

  std::string result;
  result.reserve(data.size());

  size_t i = 0;
  while (i < data.size()) {
    char c = data[i];
    if (c == quote_char && i + 1 < data.size() && data[i + 1] == quote_char) {
      // Escaped quote: "" -> "
      result += quote_char;
      i += 2;
    } else {
      result += c;
      ++i;
    }
  }

  return result;
}

//-----------------------------------------------------------------------------
// Row implementation
//-----------------------------------------------------------------------------

const Field& Row::at(size_t index) const {
  if (index >= fields_.size()) {
    throw std::out_of_range("Field index out of range: " + std::to_string(index));
  }
  return fields_[index];
}

const Field& Row::operator[](const std::string& name) const {
  if (!column_map_) {
    throw std::out_of_range("Column name lookup requires header parsing");
  }
  auto it = column_map_->find(name);
  if (it == column_map_->end()) {
    throw std::out_of_range("Column not found: " + name);
  }
  return at(it->second);
}

//-----------------------------------------------------------------------------
// StreamParser implementation
//-----------------------------------------------------------------------------

/**
 * @brief Parser state machine states for streaming CSV parsing.
 */
enum class ParserState {
  RECORD_START,   ///< At the beginning of a new record (row)
  FIELD_START,    ///< At the beginning of a new field (after delimiter)
  UNQUOTED_FIELD, ///< Inside an unquoted field
  QUOTED_FIELD,   ///< Inside a quoted field
  QUOTED_END,     ///< Just saw a quote inside a quoted field
  AFTER_CR        ///< Just saw a CR, waiting for LF
};

struct StreamParser::Impl {
  StreamConfig config;
  RowCallback row_callback;
  ErrorCallback error_callback;
  ErrorCollector errors;

  // Parser state
  ParserState state = ParserState::RECORD_START;
  bool finished = false;
  bool stopped = false; // Set when callback returns false

  // Buffer for partial records spanning chunk boundaries
  std::vector<uint8_t> partial_buffer;

  // Current row being built - store field boundaries to avoid dangling pointers
  struct FieldBoundary {
    size_t start;
    size_t end;
    bool is_quoted;
  };
  std::vector<FieldBoundary> current_field_bounds;
  size_t field_start = 0;
  bool field_is_quoted = false;

  // Current row for pull model
  Row current_row;

  // Position tracking
  size_t total_bytes = 0;
  size_t row_count = 0;
  size_t current_row_start = 0;

  // Header information
  std::vector<std::string> header_names;
  std::unordered_map<std::string, size_t> column_map;
  bool header_parsed = false;

  // Pull model state
  std::vector<Row> pending_rows;
  size_t pending_index = 0;

  Impl(const StreamConfig& cfg) : config(cfg), errors(cfg.error_mode) {
    current_field_bounds.reserve(cfg.initial_field_capacity);
  }

  void reset() {
    state = ParserState::RECORD_START;
    finished = false;
    stopped = false;
    partial_buffer.clear();
    current_field_bounds.clear();
    current_row.clear();
    field_start = 0;
    field_is_quoted = false;
    total_bytes = 0;
    row_count = 0;
    current_row_start = 0;
    header_names.clear();
    column_map.clear();
    header_parsed = false;
    pending_rows.clear();
    pending_index = 0;
    errors.clear();
  }

  // Emit a completed field - stores boundaries, not actual data
  void emit_field(const uint8_t* /*data*/, size_t start, size_t end) {
    // Check max field size to prevent DoS (only if limit is enabled)
    size_t field_size = (end > start) ? (end - start) : 0;
    if (config.max_field_size > 0 && field_size > config.max_field_size) {
      std::string msg = "Field size " + std::to_string(field_size) + " bytes exceeds maximum " +
                        std::to_string(config.max_field_size) + " bytes";
      if (error_callback) {
        ParseError err(ErrorCode::FIELD_TOO_LARGE, ErrorSeverity::RECOVERABLE, row_count + 1,
                       current_field_bounds.size() + 1, total_bytes, msg);
        if (!error_callback(err)) {
          stopped = true;
        }
      }
      errors.add_error(ErrorCode::FIELD_TOO_LARGE, ErrorSeverity::RECOVERABLE, row_count + 1,
                       current_field_bounds.size() + 1, total_bytes, msg);
      return;
    }

    current_field_bounds.push_back({start, end, field_is_quoted});
  }

  // Emit a completed row
  // Returns false if parsing should stop (callback returned false or error)
  bool emit_row() {
    // Handle empty rows
    if (config.skip_empty_rows && current_field_bounds.empty()) {
      return true; // Continue parsing
    }

    // Build the row - convert boundaries to actual field data
    Row row;
    row.row_number_ = row_count + 1; // 1-based
    row.byte_offset_ = current_row_start;
    row.column_map_ = header_parsed ? &column_map : nullptr;

    // First pass: build field storage from boundaries
    row.field_storage_.reserve(current_field_bounds.size());
    const uint8_t* data = partial_buffer.data();
    for (const auto& bounds : current_field_bounds) {
      std::string field_data;
      if (bounds.end > bounds.start) {
        field_data = std::string(reinterpret_cast<const char*>(data + bounds.start),
                                 bounds.end - bounds.start);
      }
      row.field_storage_.push_back(std::move(field_data));
    }

    // Second pass: build fields with string_views into row's storage
    row.fields_.reserve(current_field_bounds.size());
    for (size_t i = 0; i < current_field_bounds.size(); ++i) {
      Field field;
      field.data = row.field_storage_[i];
      field.is_quoted = current_field_bounds[i].is_quoted;
      field.field_index = i;
      row.fields_.push_back(field);
    }

    current_field_bounds.clear();
    current_field_bounds.reserve(config.initial_field_capacity);

    // Handle header row
    if (config.parse_header && !header_parsed) {
      header_names.clear();
      column_map.clear();
      for (size_t i = 0; i < row.fields_.size(); ++i) {
        std::string name(row.fields_[i].data);
        header_names.push_back(name);
        column_map[name] = i;
      }
      header_parsed = true;
      return true; // Don't count header as a data row
    }

    ++row_count;

    // For push model: invoke callback
    if (row_callback) {
      bool should_continue = row_callback(row);
      if (!should_continue) {
        stopped = true;
        return false; // Stop parsing
      }
      return true;
    }

    // For pull model: store row
    pending_rows.push_back(std::move(row));
    return true;
  }

  // Process a single character, updating state
  // Returns: 1 if row completed and should continue, 0 if should stop, -1 if no row completed
  // LCOV_EXCL_BR_START - state machine branches covered by integration tests
  int process_char(uint8_t c, const uint8_t* data, size_t pos, size_t buffer_start) {
    char delim = config.dialect.delimiter;
    char quote = config.dialect.quote_char;

    // Handle AFTER_CR state first
    if (state == ParserState::AFTER_CR) {
      if (c == '\n') {
        // CRLF - just continue, row was already emitted at CR
        state = ParserState::RECORD_START;
        field_start = pos + 1;
        return -1;
      }
      // CR not followed by LF - treat as if we're at record start
      state = ParserState::RECORD_START;
      // Fall through to process this character
    }

    switch (state) {
    case ParserState::RECORD_START:
    case ParserState::FIELD_START:
      if (c == static_cast<uint8_t>(quote)) {
        state = ParserState::QUOTED_FIELD;
        field_is_quoted = true;
        field_start = pos + 1; // Start after the quote
      } else if (c == static_cast<uint8_t>(delim)) {
        // Empty field
        emit_field(data, pos, pos);
        state = ParserState::FIELD_START;
        field_is_quoted = false;
        field_start = pos + 1;
      } else if (c == '\n') {
        // Empty field at end of row, or empty row
        if (state == ParserState::FIELD_START || !current_field_bounds.empty()) {
          emit_field(data, pos, pos);
        }
        if (!emit_row())
          return 0;
        state = ParserState::RECORD_START;
        field_is_quoted = false;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else if (c == '\r') {
        // CR - emit row and wait for LF
        if (state == ParserState::FIELD_START || !current_field_bounds.empty()) {
          emit_field(data, pos, pos);
        }
        if (!emit_row())
          return 0;
        state = ParserState::AFTER_CR;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else {
        state = ParserState::UNQUOTED_FIELD;
        field_is_quoted = false;
        field_start = pos;
      }
      break;

    case ParserState::UNQUOTED_FIELD:
      if (c == static_cast<uint8_t>(delim)) {
        emit_field(data, field_start, pos);
        state = ParserState::FIELD_START;
        field_is_quoted = false;
        field_start = pos + 1;
      } else if (c == '\n') {
        emit_field(data, field_start, pos);
        if (!emit_row())
          return 0;
        state = ParserState::RECORD_START;
        field_is_quoted = false;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else if (c == '\r') {
        emit_field(data, field_start, pos);
        if (!emit_row())
          return 0;
        state = ParserState::AFTER_CR;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else if (c == static_cast<uint8_t>(quote)) {
        // Quote in unquoted field - error but continue
        if (errors.mode() != ErrorMode::BEST_EFFORT) {
          if (error_callback) {
            ParseError err(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE,
                           row_count + 1, current_field_bounds.size() + 1,
                           total_bytes + (pos - buffer_start), "Quote character in unquoted field");
            if (!error_callback(err)) {
              stopped = true;
            }
          }
          errors.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE,
                           row_count + 1, current_field_bounds.size() + 1,
                           total_bytes + (pos - buffer_start), "Quote character in unquoted field");
        }
      }
      break;

    case ParserState::QUOTED_FIELD:
      if (c == static_cast<uint8_t>(quote)) {
        state = ParserState::QUOTED_END;
      }
      // Otherwise stay in quoted field
      break;

    case ParserState::QUOTED_END:
      if (c == static_cast<uint8_t>(quote)) {
        // Escaped quote "" - continue in quoted field
        state = ParserState::QUOTED_FIELD;
      } else if (c == static_cast<uint8_t>(delim)) {
        // End of quoted field
        emit_field(data, field_start, pos - 1); // -1 to exclude closing quote
        state = ParserState::FIELD_START;
        field_is_quoted = false;
        field_start = pos + 1;
      } else if (c == '\n') {
        // End of quoted field and row
        emit_field(data, field_start, pos - 1);
        if (!emit_row())
          return 0;
        state = ParserState::RECORD_START;
        field_is_quoted = false;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else if (c == '\r') {
        // End of quoted field (CRLF)
        emit_field(data, field_start, pos - 1);
        if (!emit_row())
          return 0;
        state = ParserState::AFTER_CR;
        current_row_start = total_bytes + (pos - buffer_start) + 1;
        field_start = pos + 1;
        return 1;
      } else {
        // Invalid character after closing quote
        if (errors.mode() != ErrorMode::BEST_EFFORT) {
          if (error_callback) {
            ParseError err(ErrorCode::INVALID_QUOTE_ESCAPE, ErrorSeverity::RECOVERABLE,
                           row_count + 1, current_field_bounds.size() + 1,
                           total_bytes + (pos - buffer_start),
                           "Invalid character after closing quote");
            if (!error_callback(err)) {
              stopped = true;
            }
          }
          errors.add_error(ErrorCode::INVALID_QUOTE_ESCAPE, ErrorSeverity::RECOVERABLE,
                           row_count + 1, current_field_bounds.size() + 1,
                           total_bytes + (pos - buffer_start),
                           "Invalid character after closing quote");
        }
        // Recovery: treat as part of unquoted field
        state = ParserState::UNQUOTED_FIELD;
      }
      break;

    case ParserState::AFTER_CR:
      // This case is handled at the top of the function
      break;
    }

    return -1; // No row completed
  }
  // LCOV_EXCL_BR_STOP

  // Process a chunk of data
  StreamStatus process_chunk(const uint8_t* data, size_t len) {
    if (finished || stopped) {
      return stopped ? StreamStatus::OK : StreamStatus::END_OF_DATA;
    }

    // Append new data to partial buffer
    size_t start_pos = partial_buffer.size();
    partial_buffer.insert(partial_buffer.end(), data, data + len);

    const uint8_t* work_data = partial_buffer.data();
    size_t work_len = partial_buffer.size();

    size_t last_row_end = 0;

    // Process only from where we left off (start_pos)
    for (size_t i = start_pos; i < work_len; ++i) {
      int result = process_char(work_data[i], work_data, i, 0);
      if (result == 0) {
        // Callback returned false, stop parsing
        partial_buffer.clear();
        current_field_bounds.clear();
        return StreamStatus::OK;
      }
      if (result == 1) {
        // Row completed
        last_row_end = i + 1;
      }

      if (errors.should_stop()) {
        partial_buffer.clear();
        current_field_bounds.clear();
        return StreamStatus::STREAM_ERROR;
      }

      // Check if error callback requested stop
      if (stopped) {
        partial_buffer.clear();
        current_field_bounds.clear();
        return StreamStatus::OK;
      }
    }

    // Buffer any partial row at end of chunk
    if (last_row_end > 0) {
      // Erase processed data and adjust field_start
      partial_buffer.erase(partial_buffer.begin(),
                           partial_buffer.begin() + static_cast<long>(last_row_end));
      if (field_start >= last_row_end) {
        field_start -= last_row_end;
      } else {
        field_start = 0;
      }
      total_bytes += last_row_end;
    }
    // If no rows completed, keep all data in partial_buffer for next chunk

    return StreamStatus::OK;
  }

  // Finish parsing - process any remaining data
  StreamStatus finish_parsing() {
    if (finished) {
      return StreamStatus::END_OF_DATA;
    }

    finished = true;

    // Don't process remaining data if stopped by callback
    if (stopped) {
      partial_buffer.clear();
      return StreamStatus::OK;
    }

    // Process any remaining data in partial buffer
    if (!partial_buffer.empty() || state != ParserState::RECORD_START) {
      const uint8_t* data = partial_buffer.data();
      size_t len = partial_buffer.size();

      // If we're in the middle of a field, emit it
      if (state == ParserState::UNQUOTED_FIELD) {
        emit_field(data, field_start, len);
        emit_row();
      } else if (state == ParserState::QUOTED_FIELD) {
        // Unclosed quote at EOF
        errors.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, row_count + 1,
                         current_field_bounds.size() + 1, total_bytes,
                         "Unclosed quote at end of file");
        if (errors.mode() != ErrorMode::FAIL_FAST) {
          // Best effort: emit partial field
          emit_field(data, field_start, len);
          emit_row();
        }
      } else if (state == ParserState::QUOTED_END) {
        // Field ended with quote at EOF - valid
        emit_field(data, field_start, len > 0 ? len - 1 : 0);
        emit_row();
      } else if (state == ParserState::FIELD_START) {
        // Empty field at end
        emit_field(data, len, len);
        emit_row();
      } else if (!current_field_bounds.empty()) {
        // Have partial row data
        emit_row();
      }

      total_bytes += len;
      partial_buffer.clear();
    }

    return errors.has_fatal_errors() ? StreamStatus::STREAM_ERROR : StreamStatus::END_OF_DATA;
  }

  // Pull model: get next row from pending queue
  StreamStatus get_next_row() {
    // First, try to get from pending rows
    if (pending_index < pending_rows.size()) {
      current_row = std::move(pending_rows[pending_index]);
      current_row.column_map_ = header_parsed ? &column_map : nullptr;
      ++pending_index;

      // Clear processed rows periodically to free memory
      if (pending_index >= 100) {
        pending_rows.erase(pending_rows.begin(),
                           pending_rows.begin() + static_cast<long>(pending_index));
        pending_index = 0;
      }

      return StreamStatus::ROW_READY;
    }

    // No pending rows
    pending_rows.clear();
    pending_index = 0;

    if (finished) {
      return StreamStatus::END_OF_DATA;
    }

    return StreamStatus::NEED_MORE_DATA;
  }
};

StreamParser::StreamParser(const StreamConfig& config) : impl_(std::make_unique<Impl>(config)) {}

StreamParser::~StreamParser() = default;

StreamParser::StreamParser(StreamParser&&) noexcept = default;
StreamParser& StreamParser::operator=(StreamParser&&) noexcept = default;

const StreamConfig& StreamParser::config() const {
  return impl_->config;
}

void StreamParser::set_row_handler(RowCallback callback) {
  impl_->row_callback = std::move(callback);
}

void StreamParser::set_error_handler(ErrorCallback callback) {
  impl_->error_callback = std::move(callback);
}

StreamStatus StreamParser::parse_chunk(const uint8_t* data, size_t size) {
  return impl_->process_chunk(data, size);
}

StreamStatus StreamParser::finish() {
  return impl_->finish_parsing();
}

void StreamParser::reset() {
  impl_->reset();
}

StreamStatus StreamParser::next_row() {
  return impl_->get_next_row();
}

const Row& StreamParser::current_row() const {
  return impl_->current_row;
}

const std::vector<std::string>& StreamParser::header() const {
  return impl_->header_names;
}

int StreamParser::column_index(const std::string& name) const {
  auto it = impl_->column_map.find(name);
  if (it == impl_->column_map.end()) {
    return -1;
  }
  return static_cast<int>(it->second);
}

size_t StreamParser::rows_processed() const {
  return impl_->row_count;
}

size_t StreamParser::bytes_processed() const {
  return impl_->total_bytes;
}

const ErrorCollector& StreamParser::error_collector() const {
  return impl_->errors;
}

bool StreamParser::is_finished() const {
  return impl_->finished;
}

//-----------------------------------------------------------------------------
// StreamRowIterator implementation
//-----------------------------------------------------------------------------

StreamRowIterator::StreamRowIterator() : reader_(nullptr), at_end_(true) {}

StreamRowIterator::StreamRowIterator(StreamReader* reader) : reader_(reader), at_end_(false) {
  // Advance to first row
  if (reader_ && !reader_->next_row()) {
    at_end_ = true;
  }
}

StreamRowIterator::reference StreamRowIterator::operator*() const {
  return reader_->row();
}

StreamRowIterator::pointer StreamRowIterator::operator->() const {
  return &reader_->row();
}

StreamRowIterator& StreamRowIterator::operator++() {
  if (reader_ && !reader_->next_row()) {
    at_end_ = true;
  }
  return *this;
}

StreamRowIterator StreamRowIterator::operator++(int) {
  StreamRowIterator tmp = *this;
  ++(*this);
  return tmp;
}

bool StreamRowIterator::operator==(const StreamRowIterator& other) const {
  if (at_end_ && other.at_end_)
    return true;
  if (at_end_ || other.at_end_)
    return false;
  return reader_ == other.reader_;
}

bool StreamRowIterator::operator!=(const StreamRowIterator& other) const {
  return !(*this == other);
}

//-----------------------------------------------------------------------------
// StreamReader implementation
//-----------------------------------------------------------------------------

struct StreamReader::Impl {
  StreamParser parser;
  std::unique_ptr<std::ifstream> owned_file;
  std::istream* input;
  std::vector<char> read_buffer;
  bool eof = false;
  size_t total_bytes_read = 0;

  Impl(const StreamConfig& config) : parser(config), input(nullptr) {
    read_buffer.resize(config.chunk_size);
  }

  bool read_more_data() {
    if (!input || eof) {
      return false;
    }

    input->read(read_buffer.data(), static_cast<std::streamsize>(read_buffer.size()));
    std::streamsize bytes_read = input->gcount();

    if (bytes_read == 0) {
      eof = true;
      parser.finish();
      return false;
    }

    total_bytes_read += static_cast<size_t>(bytes_read);

    if (input->eof()) {
      eof = true;
    }

    parser.parse_chunk(read_buffer.data(), static_cast<size_t>(bytes_read));

    if (eof) {
      parser.finish();
    }

    return true;
  }
};

StreamReader::StreamReader(const std::string& filename, const StreamConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
  impl_->owned_file = std::make_unique<std::ifstream>(filename, std::ios::binary);
  if (!impl_->owned_file->is_open()) {
    throw std::runtime_error("Cannot open file: " + filename);
  }
  impl_->input = impl_->owned_file.get();
}

StreamReader::StreamReader(std::istream& input, const StreamConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
  impl_->input = &input;
}

StreamReader::~StreamReader() = default;

StreamReader::StreamReader(StreamReader&&) noexcept = default;
StreamReader& StreamReader::operator=(StreamReader&&) noexcept = default;

const StreamConfig& StreamReader::config() const {
  return impl_->parser.config();
}

bool StreamReader::next_row() {
  while (true) {
    StreamStatus status = impl_->parser.next_row();

    switch (status) {
    case StreamStatus::ROW_READY:
      return true;

    case StreamStatus::NEED_MORE_DATA:
      if (!impl_->read_more_data()) {
        // Check one more time after finish
        status = impl_->parser.next_row();
        return status == StreamStatus::ROW_READY;
      }
      break;

    case StreamStatus::END_OF_DATA:
    case StreamStatus::STREAM_ERROR:
      return false;

    default:
      return false;
    }
  }
}

const Row& StreamReader::row() const {
  return impl_->parser.current_row();
}

const std::vector<std::string>& StreamReader::header() const {
  return impl_->parser.header();
}

int StreamReader::column_index(const std::string& name) const {
  return impl_->parser.column_index(name);
}

const ErrorCollector& StreamReader::error_collector() const {
  return impl_->parser.error_collector();
}

size_t StreamReader::rows_read() const {
  return impl_->parser.rows_processed();
}

size_t StreamReader::bytes_read() const {
  return impl_->total_bytes_read;
}

bool StreamReader::eof() const {
  return impl_->eof && impl_->parser.is_finished();
}

StreamRowIterator StreamReader::begin() {
  return StreamRowIterator(this);
}

StreamRowIterator StreamReader::end() {
  return StreamRowIterator();
}

} // namespace libvroom
