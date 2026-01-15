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

  // Set up dialect
  libvroom::Dialect dialect;
  bool auto_detect = (delim == nullptr);

  if (!auto_detect) {
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
  (void)skip;
  (void)n_max;
  (void)comment;
  (void)skip_empty_rows;
  (void)progress;

  // Parse - this creates the index but doesn't build ValueExtractor yet
  result_ = parser.parse(buffer_.data(), buffer_.size(), opts);

  if (!result_.successful) {
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

  // Set has_header on the result (this will be used by ValueExtractor)
  result_.set_has_header(has_header);

  // Get dimensions from libvroom (this triggers lazy ValueExtractor creation once)
  columns_ = result_.num_columns();
  rows_ = result_.num_rows();

  // Cache headers
  if (has_header_ && columns_ > 0) {
    headers_ = result_.header();
  }
}

std::pair<size_t, size_t> libvroom_index::get_field_bounds(size_t row, size_t col) const {
  // Use libvroom's ValueExtractor to get field bounds
  size_t start, end;
  // Access the extractor through result_ - it handles lazy initialization
  // Note: This is a workaround since ValueExtractor::get_field_bounds isn't directly exposed
  // We'll get the string_view and compute bounds from that
  auto sv = result_.row(row).get_string_view(col);
  start = sv.data() - reinterpret_cast<const char*>(buffer_.data());
  end = start + sv.size();
  return {start, end};
}

string libvroom_index::get_trimmed_val(size_t row, size_t col, bool is_last) const {
  // Use libvroom's Result API directly - it handles quote stripping
  auto sv = result_.row(row).get_string_view(col);

  const char* begin = sv.data();
  const char* end = sv.data() + sv.size();

  // Check for windows newlines if the last column
  if (is_last && begin < end && *(end - 1) == '\r') {
    --end;
  }

  // Trim whitespace if enabled (libvroom may not have trimmed)
  if (trim_ws_) {
    while (begin < end && (*begin == ' ' || *begin == '\t')) ++begin;
    while (end > begin && (*(end - 1) == ' ' || *(end - 1) == '\t')) --end;
  }

  return string(begin, end);
}

std::string_view libvroom_index::get_field(size_t row, size_t col) const {
  return result_.row(row).get_string_view(col);
}

std::string libvroom_index::get_header_field(size_t col) const {
  if (col < headers_.size()) {
    return headers_[col];
  }
  return std::string();
}

string libvroom_index::get_processed_field(size_t row, size_t col) const {
  auto sv = get_field(row, col);
  return string(sv.data(), sv.data() + sv.size());
}

string libvroom_index::get(size_t row, size_t col) const {
  return get_processed_field(row, col);
}

// Column iterator implementation
string libvroom_index::column_iterator::value() const {
  return idx_->get_trimmed_val(current_row_, column_, is_last_);
}

string libvroom_index::column_iterator::at(ptrdiff_t n) const {
  return idx_->get_trimmed_val(static_cast<size_t>(n), column_, is_last_);
}

size_t libvroom_index::column_iterator::position() const {
  auto [start, end] = idx_->get_field_bounds(current_row_, column_);
  return start;
}

// Row iterator implementation
string libvroom_index::row_iterator::value() const {
  // Handle header row (row_ == -1 as size_t wraps to max value)
  if (row_ == static_cast<size_t>(-1)) {
    std::string hdr = idx_->get_header_field(col_);
    return string(std::move(hdr));
  }

  auto sv = idx_->get_field(row_, col_);
  return string(sv.data(), sv.data() + sv.size());
}

string libvroom_index::row_iterator::at(ptrdiff_t n) const {
  if (row_ == static_cast<size_t>(-1)) {
    std::string hdr = idx_->get_header_field(static_cast<size_t>(n));
    return string(std::move(hdr));
  }

  auto sv = idx_->get_field(row_, n);
  return string(sv.data(), sv.data() + sv.size());
}

size_t libvroom_index::row_iterator::position() const {
  if (row_ == static_cast<size_t>(-1)) {
    return 0;
  }

  auto [start, end] = idx_->get_field_bounds(row_, col_);
  return start;
}

} // namespace vroom
