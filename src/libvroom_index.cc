#include "libvroom_index.h"

namespace vroom {

libvroom_index::libvroom_index(
    const char* filename,
    const char* delim,
    char quote,
    bool trim_ws,
    bool escape_double,
    bool escape_backslash,
    bool has_header,
    size_t skip,
    size_t n_max,
    const char* comment,
    bool skip_empty_rows,
    std::shared_ptr<vroom_errors> errors,
    size_t num_threads,
    bool progress)
    : filename_(filename),
      has_header_(has_header),
      quote_(quote),
      trim_ws_(trim_ws),
      escape_double_(escape_double),
      escape_backslash_(escape_backslash) {

  // Load file using libvroom
  buffer_ = libvroom::load_file(filename);
  if (!buffer_.valid()) {
    throw std::runtime_error("Failed to load file: " + std::string(filename));
  }

  // Also mmap for direct field access
  std::error_code ec;
  mmap_.map(filename, ec);
  if (ec) {
    throw std::runtime_error("Failed to mmap file: " + std::string(filename));
  }

  // Set up dialect
  libvroom::Dialect dialect;
  bool auto_detect = (delim == nullptr);

  if (!auto_detect) {
    // Create dialect from parameters using struct initialization
    dialect.delimiter = delim[0];
    dialect.quote_char = quote;
    dialect.escape_char = escape_double ? quote : '\\';
    dialect.double_quote = escape_double;
    delim_ = delim;
  }

  // Create parser
  libvroom::Parser parser(num_threads);

  // Set up parse options
  libvroom::ParseOptions opts;
  if (!auto_detect) {
    opts.dialect = dialect;
  }

  // TODO: Handle skip, n_max, comment, skip_empty_rows
  // These would need libvroom API support or post-processing
  (void)skip;
  (void)n_max;
  (void)comment;
  (void)skip_empty_rows;
  (void)progress;

  // Parse
  result_ = parser.parse(buffer_.data(), buffer_.size(), opts);

  if (!result_.successful) {
    // Collect errors if we have an error collector
    if (errors && result_.has_errors()) {
      for (const auto& err : result_.errors()) {
        errors->add_parse_error(err.byte_offset, 0);
      }
    }
  }

  // Store detected dialect if auto-detected
  if (auto_detect) {
    dialect = result_.dialect;
    delim_ = std::string(1, dialect.delimiter);
  }

  // Get dimensions from Result (which uses ValueExtractor internally)
  columns_ = result_.num_columns();

  // Calculate rows (excluding header if present)
  size_t total_rows = result_.num_rows();
  // num_rows() already excludes header, so we use it directly
  rows_ = total_rows;

  // Cache headers for efficient access
  if (has_header_) {
    headers_ = result_.header();
  }
}

std::string_view libvroom_index::get_field(size_t row, size_t col) const {
  // libvroom's result_.row() already handles header offset internally
  // row 0 in vroom data terms = row 0 in libvroom Result terms (first data row)
  auto result_row = result_.row(row);
  return result_row.get_string_view(col);
}

std::string libvroom_index::get_header_field(size_t col) const {
  if (col < headers_.size()) {
    return headers_[col];
  }
  return std::string();
}

string libvroom_index::get_processed_field(size_t row, size_t col) const {
  auto sv = get_field(row, col);

  // For now, return as-is - libvroom handles quote stripping
  // If the field needs escape processing (e.g., "" -> "), we'd need to
  // check and handle that here
  return string(sv.data(), sv.data() + sv.size());
}

string libvroom_index::get(size_t row, size_t col) const {
  return get_processed_field(row, col);
}

// Column iterator implementation
string libvroom_index::column_iterator::value() const {
  return idx_->get_processed_field(row_, column_);
}

string libvroom_index::column_iterator::at(ptrdiff_t n) const {
  return idx_->get_processed_field(n, column_);
}

size_t libvroom_index::column_iterator::position() const {
  // Position is used for error reporting and debug info
  // Return 0 for now - full byte offset calculation from interleaved index
  // would require more complex logic
  // TODO: Implement proper position calculation if needed for error messages
  return 0;
}

// Row iterator implementation
string libvroom_index::row_iterator::value() const {
  // Handle header row (row_ == -1 as size_t wraps to max value)
  if (row_ == static_cast<size_t>(-1)) {
    // Header row - use cached headers
    std::string hdr = idx_->get_header_field(col_);
    return string(std::move(hdr));
  }

  // Data row - use libvroom's result_.row() which handles indexing
  auto result_row = idx_->result_.row(row_);
  auto sv = result_row.get_string_view(col_);
  return string(sv.data(), sv.data() + sv.size());
}

string libvroom_index::row_iterator::at(ptrdiff_t n) const {
  // Handle header row (row_ == -1 as size_t wraps to max value)
  if (row_ == static_cast<size_t>(-1)) {
    // Header row - use cached headers
    std::string hdr = idx_->get_header_field(static_cast<size_t>(n));
    return string(std::move(hdr));
  }

  // Data row - use libvroom's result_.row() which handles indexing
  auto result_row = idx_->result_.row(row_);
  auto sv = result_row.get_string_view(n);
  return string(sv.data(), sv.data() + sv.size());
}

size_t libvroom_index::row_iterator::position() const {
  // Position is used for error reporting and debug info
  // Return 0 for now - full byte offset calculation from interleaved index
  // would require more complex logic
  // TODO: Implement proper position calculation if needed for error messages
  return 0;
}

} // namespace vroom
