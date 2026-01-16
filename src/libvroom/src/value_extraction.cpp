#include "value_extraction.h"

#include "two_pass.h"

#include <cassert>
#include <stdexcept>

namespace libvroom {

// Helper: Skip over comment lines starting at the given position.
// Returns the position after all consecutive comment lines, or the original
// position if no comment lines are present.
static size_t skip_comment_lines_from(const uint8_t* buf, size_t len, size_t pos,
                                      char comment_char) {
  if (comment_char == '\0' || pos >= len) {
    return pos;
  }

  while (pos < len) {
    // Skip any leading whitespace (spaces and tabs only)
    size_t line_start = pos;
    while (pos < len && (buf[pos] == ' ' || buf[pos] == '\t')) {
      ++pos;
    }

    // Check if this line starts with the comment character
    if (pos < len && buf[pos] == static_cast<uint8_t>(comment_char)) {
      // This is a comment line - skip to end of line
      while (pos < len && buf[pos] != '\n' && buf[pos] != '\r') {
        ++pos;
      }
      // Skip the line ending (LF, CR, or CRLF)
      if (pos < len) {
        if (buf[pos] == '\r') {
          ++pos;
          if (pos < len && buf[pos] == '\n') {
            ++pos;
          }
        } else if (buf[pos] == '\n') {
          ++pos;
        }
      }
      // Continue checking for more comment lines
    } else {
      // Not a comment line - revert to start of this line content
      return line_start;
    }
  }

  return pos;
}

ValueExtractor::ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx_ref,
                               const Dialect& dialect, const ExtractionConfig& config)
    : buf_(buf), len_(len), idx_ptr_(&idx_ref), dialect_(dialect), config_(config) {
  // Determine number of columns by finding the first newline separator.
  // Uses O(n_threads) iteration through thread data instead of sorted indexes.
  uint64_t total_indexes = idx().total_indexes();
  for (uint64_t i = 0; i < total_indexes; ++i) {
    FieldSpan span = idx().get_field_span(i);
    if (!span.is_valid() || span.end >= len_)
      continue;
    uint8_t c = buf_[span.end];
    if (c == '\n' || c == '\r') {
      num_columns_ = i + 1;
      break;
    }
  }
  recalculate_num_rows();
}

ValueExtractor::ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx_ref,
                               const Dialect& dialect, const ExtractionConfig& config,
                               const ColumnConfigMap& column_configs)
    : buf_(buf), len_(len), idx_ptr_(&idx_ref), dialect_(dialect), config_(config),
      column_configs_(column_configs) {
  // Determine number of columns by finding the first newline separator.
  // Uses O(n_threads) iteration through thread data instead of sorted indexes.
  uint64_t total_indexes = idx().total_indexes();
  for (uint64_t i = 0; i < total_indexes; ++i) {
    FieldSpan span = idx().get_field_span(i);
    if (!span.is_valid() || span.end >= len_)
      continue;
    uint8_t c = buf_[span.end];
    if (c == '\n' || c == '\r') {
      num_columns_ = i + 1;
      break;
    }
  }
  recalculate_num_rows();
  // Resolve any name-based column configs now that we have headers
  resolve_column_configs();
}

ValueExtractor::ValueExtractor(std::shared_ptr<const ParseIndex> shared_idx, const Dialect& dialect,
                               const ExtractionConfig& config)
    : buf_(nullptr), len_(0), idx_ptr_(nullptr), dialect_(dialect), config_(config),
      shared_idx_(std::move(shared_idx)) {
  if (!shared_idx_) {
    throw std::invalid_argument("shared_idx cannot be null");
  }
  if (!shared_idx_->has_buffer()) {
    throw std::invalid_argument("ParseIndex must have buffer set for shared ownership");
  }

  // Get buffer from the shared ParseIndex
  shared_buffer_ = shared_idx_->buffer();
  buf_ = shared_buffer_->data();
  len_ = shared_buffer_->size();

  // Determine number of columns by finding the first newline separator.
  // Uses O(n_threads) iteration through thread data instead of sorted indexes.
  uint64_t total_indexes = idx().total_indexes();
  for (uint64_t i = 0; i < total_indexes; ++i) {
    FieldSpan span = idx().get_field_span(i);
    if (!span.is_valid() || span.end >= len_)
      continue;
    uint8_t c = buf_[span.end];
    if (c == '\n' || c == '\r') {
      num_columns_ = i + 1;
      break;
    }
  }
  recalculate_num_rows();
}

std::string_view ValueExtractor::get_string_view(size_t row, size_t col) const {
  if (row >= num_rows_)
    throw std::out_of_range("Row index out of range");
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  return get_string_view_internal(row, col);
}

std::string_view ValueExtractor::get_string_view_internal(size_t row, size_t col) const {
  size_t field_idx = compute_field_index(row, col);
  // Use ParseIndex::get_field_span() for O(n_threads) field access
  FieldSpan span = idx().get_field_span(field_idx);
  // Return empty view with valid pointer to avoid undefined behavior when
  // converting to std::string
  if (!span.is_valid())
    return std::string_view(reinterpret_cast<const char*>(buf_), 0);
  size_t start = span.start;
  size_t end = span.end;
  if (end > len_)
    end = len_; // Bounds check
  if (start > len_)
    start = len_; // Bounds check

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  // that may exist between the end of the previous row and the start of this row.
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    // Get previous field's end position to check for newline
    FieldSpan prev_span = idx().get_field_span(field_idx - 1);
    if (prev_span.is_valid() && prev_span.end < len_ &&
        (buf_[prev_span.end] == '\n' || buf_[prev_span.end] == '\r')) {
      // Previous field ended at a row boundary - skip any comment lines
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  if (end > start && buf_[end - 1] == '\r')
    --end;
  if (end > start && buf_[start] == static_cast<uint8_t>(dialect_.quote_char))
    if (buf_[end - 1] == static_cast<uint8_t>(dialect_.quote_char)) {
      ++start;
      --end;
    }
  if (end < start)
    end = start;
  assert(end >= start && "Invalid range: end must be >= start");
  return std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start);
}

std::string ValueExtractor::get_string(size_t row, size_t col) const {
  size_t field_idx = compute_field_index(row, col);
  // Use ParseIndex::get_field_span() for O(n_threads) field access
  FieldSpan span = idx().get_field_span(field_idx);
  if (!span.is_valid())
    return std::string(); // Bounds check
  size_t start = span.start;
  size_t end = span.end;
  if (end > len_)
    end = len_; // Bounds check
  if (start > len_)
    start = len_; // Bounds check

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  // that may exist between the end of the previous row and the start of this row.
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    // Get previous field's end position to check for newline
    FieldSpan prev_span = idx().get_field_span(field_idx - 1);
    if (prev_span.is_valid() && prev_span.end < len_ &&
        (buf_[prev_span.end] == '\n' || buf_[prev_span.end] == '\r')) {
      // Previous field ended at a row boundary - skip any comment lines
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  if (end > start && buf_[end - 1] == '\r')
    --end;
  if (end < start)
    end = start; // Normalize range
  assert(end >= start && "Invalid range: end must be >= start");
  return unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start));
}

size_t ValueExtractor::compute_field_index(size_t row, size_t col) const {
  return (has_header_ ? row + 1 : row) * num_columns_ + col;
}

void ValueExtractor::recalculate_num_rows() {
  uint64_t total_indexes = idx().total_indexes();
  if (total_indexes > 0 && num_columns_ > 0) {
    size_t total_rows = total_indexes / num_columns_;
    num_rows_ = has_header_ ? (total_rows > 0 ? total_rows - 1 : 0) : total_rows;
  }
}

std::string ValueExtractor::unescape_field(std::string_view field) const {
  if (field.empty() || field.front() != dialect_.quote_char)
    return std::string(field);
  if (field.size() < 2 || field.back() != dialect_.quote_char)
    return std::string(field);
  std::string_view inner = field.substr(1, field.size() - 2);
  std::string result;
  result.reserve(inner.size());
  for (size_t i = 0; i < inner.size(); ++i) {
    char c = inner[i];
    if (c == dialect_.escape_char && i + 1 < inner.size() && inner[i + 1] == dialect_.quote_char) {
      result += dialect_.quote_char;
      ++i;
    } else
      result += c;
  }
  return result;
}

std::vector<std::string_view> ValueExtractor::extract_column_string_view(size_t col) const {
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  std::vector<std::string_view> result;
  result.reserve(num_rows_);
  for (size_t row = 0; row < num_rows_; ++row)
    result.push_back(get_string_view_internal(row, col));
  return result;
}

std::vector<std::string> ValueExtractor::extract_column_string(size_t col) const {
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  std::vector<std::string> result;
  result.reserve(num_rows_);
  for (size_t row = 0; row < num_rows_; ++row)
    result.push_back(get_string(row, col));
  return result;
}

std::vector<std::string> ValueExtractor::get_header() const {
  if (!has_header_)
    throw std::runtime_error("CSV has no header row");
  std::vector<std::string> headers;
  headers.reserve(num_columns_);
  for (size_t col = 0; col < num_columns_; ++col) {
    // Use ParseIndex::get_field_span() for O(n_threads) field access
    FieldSpan span = idx().get_field_span(col);
    if (!span.is_valid())
      break; // Bounds check
    size_t start = span.start;
    size_t end = span.end;
    if (end > len_)
      end = len_; // Bounds check
    if (start > len_)
      start = len_; // Bounds check

    // For the first header column (col == 0), skip any comment lines at the
    // beginning of the file
    if (col == 0 && dialect_.comment_char != '\0') {
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }

    if (end > start && buf_[end - 1] == '\r')
      --end;
    if (end < start)
      end = start; // Normalize range
    assert(end >= start && "Invalid range: end must be >= start");
    headers.push_back(
        unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start)));
  }
  return headers;
}

bool ValueExtractor::get_field_bounds(size_t row, size_t col, size_t& start, size_t& end) const {
  if (row >= num_rows_ || col >= num_columns_)
    return false;
  size_t field_idx = compute_field_index(row, col);
  // Use ParseIndex::get_field_span() for O(n_threads) field access
  FieldSpan span = idx().get_field_span(field_idx);
  if (!span.is_valid())
    return false;
  start = span.start;
  end = span.end;

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  // that may exist between the end of the previous row and the start of this row.
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    // Get previous field's end position to check for newline
    FieldSpan prev_span = idx().get_field_span(field_idx - 1);
    if (prev_span.is_valid() && prev_span.end < len_ &&
        (buf_[prev_span.end] == '\n' || buf_[prev_span.end] == '\r')) {
      // Previous field ended at a row boundary - skip any comment lines
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  assert(end >= start && "Invalid field bounds: end must be >= start");
  return true;
}

ValueExtractor::Location ValueExtractor::byte_offset_to_location(size_t byte_offset) const {
  // Handle edge cases
  uint64_t total_indexes = idx().total_indexes();
  if (total_indexes == 0 || num_columns_ == 0) {
    return {0, 0, false};
  }

  // Linear search through fields to find which one contains the byte offset.
  // This is O(n) in the number of fields, but avoids the O(n log n) sorting.
  // For most use cases (error reporting), this is called infrequently.
  uint64_t prev_end = 0;
  for (uint64_t i = 0; i < total_indexes; ++i) {
    FieldSpan span = idx().get_field_span(i);
    if (!span.is_valid())
      continue;

    // Check if byte_offset falls within this field's bounds
    // Field spans from prev_end (or span.start) to span.end
    if (byte_offset <= span.end) {
      // Found the field containing this byte offset
      size_t row = i / num_columns_;
      size_t col = i % num_columns_;
      return {row, col, true};
    }
    prev_end = span.end;
  }

  // Byte offset is beyond the last field
  return {0, 0, false};
}

} // namespace libvroom
