#include "libvroom_index.h"

#include <algorithm>

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

  // Build the cached linear index for O(1) field access
  // This is the key optimization - we linearize once at construction time
  // instead of going through libvroom's ValueExtractor on every access
  // We do this BEFORE calling result_.num_columns() etc to avoid creating ValueExtractor
  build_linear_index();

  // Now compute dimensions from our linear index (avoid ValueExtractor overhead)
  // Find the first newline to determine column count
  if (!linear_idx_.empty()) {
    columns_ = 0;
    for (size_t i = 0; i < linear_idx_.size(); ++i) {
      if (linear_idx_[i] < buffer_.size()) {
        uint8_t c = buffer_.data()[linear_idx_[i]];
        if (c == '\n' || c == '\r') {
          columns_ = i + 1;
          break;
        }
      }
    }
    if (columns_ == 0) {
      columns_ = linear_idx_.size();  // Single row
    }

    // Calculate rows from total fields
    size_t total_rows_in_file = linear_idx_.size() / columns_;
    rows_ = has_header_ ? (total_rows_in_file > 0 ? total_rows_in_file - 1 : 0) : total_rows_in_file;
  } else {
    columns_ = 0;
    rows_ = 0;
  }

  // Cache headers by reading directly from buffer (avoid ValueExtractor)
  if (has_header_ && columns_ > 0) {
    headers_.reserve(columns_);
    for (size_t col = 0; col < columns_; ++col) {
      size_t start = (col == 0) ? 0 : linear_idx_[col - 1] + 1;
      size_t end = linear_idx_[col];

      // Handle CR before LF
      if (end > start && buffer_.data()[end - 1] == '\r') {
        --end;
      }

      // Strip quotes
      if (end > start) {
        if (buffer_.data()[start] == static_cast<uint8_t>(quote_) &&
            buffer_.data()[end - 1] == static_cast<uint8_t>(quote_)) {
          ++start;
          --end;
        }
      }

      if (end < start) end = start;
      headers_.emplace_back(reinterpret_cast<const char*>(buffer_.data() + start), end - start);
    }
  }
}

void libvroom_index::build_linear_index() {
  // Get the interleaved ParseIndex from libvroom
  const auto& idx = result_.idx;

  if (idx.n_indexes == nullptr || idx.indexes == nullptr) {
    return;  // Empty or invalid index
  }

  // Count total indexes across all threads
  uint64_t total_indexes = 0;
  for (uint16_t t = 0; t < idx.n_threads; ++t) {
    total_indexes += idx.n_indexes[t];
  }

  if (total_indexes == 0) {
    return;
  }

  // Reserve space for the linear index
  linear_idx_.reserve(total_indexes);

  // De-interleave the indexes from thread-striped to linear order
  // libvroom stores: thread 0 at indices 0, n_threads, 2*n_threads, ...
  //                  thread 1 at indices 1, n_threads+1, 2*n_threads+1, ...
  for (uint16_t t = 0; t < idx.n_threads; ++t) {
    for (uint64_t j = 0; j < idx.n_indexes[t]; ++j) {
      linear_idx_.push_back(idx.indexes[t + (j * idx.n_threads)]);
    }
  }

  // Sort to get positions in file order
  std::sort(linear_idx_.begin(), linear_idx_.end());
}

std::pair<size_t, size_t> libvroom_index::get_cell(size_t i) const {
  // Direct cell access by linear index - optimized for iteration
  if (i >= linear_idx_.size()) {
    return {buffer_.size(), buffer_.size()};
  }

  size_t end = linear_idx_[i];
  // First cell starts at 0, all others start after previous separator
  size_t start = (i == 0) ? 0 : linear_idx_[i - 1] + 1;

  // Bounds checking
  if (end > buffer_.size()) end = buffer_.size();
  if (start > buffer_.size()) start = buffer_.size();

  return {start, end};
}

std::pair<size_t, size_t> libvroom_index::get_field_bounds(size_t row, size_t col) const {
  // Compute the linear index position for this field
  // If has_header is true, row 0 in vroom terms is actually row 1 in the file
  // (row 0 in the file is the header)
  size_t file_row = has_header_ ? row + 1 : row;
  size_t field_idx = file_row * columns_ + col;

  return get_cell(field_idx);
}

string libvroom_index::get_trimmed_val(size_t i, bool is_last) const {
  auto [start_p, end_p] = get_cell(i);

  if (start_p >= buffer_.size() || end_p > buffer_.size()) {
    return string(std::string());
  }

  const char* begin = reinterpret_cast<const char*>(buffer_.data()) + start_p;
  const char* end = reinterpret_cast<const char*>(buffer_.data()) + end_p;

  // Check for windows newlines if the last column
  if (is_last && begin < end && *(end - 1) == '\r') {
    --end;
  }

  // Trim whitespace if enabled
  if (trim_ws_) {
    while (begin < end && (*begin == ' ' || *begin == '\t')) ++begin;
    while (end > begin && (*(end - 1) == ' ' || *(end - 1) == '\t')) --end;
  }

  // Strip quotes if present
  if (quote_ != '\0' && begin < end && *begin == quote_) {
    ++begin;
    if (end > begin && *(end - 1) == quote_) {
      --end;
    }
    // Trim whitespace inside quotes if enabled
    if (trim_ws_) {
      while (begin < end && (*begin == ' ' || *begin == '\t')) ++begin;
      while (end > begin && (*(end - 1) == ' ' || *(end - 1) == '\t')) --end;
    }
  }

  return string(begin, end);
}

std::string_view libvroom_index::get_field(size_t row, size_t col) const {
  auto [start, end] = get_field_bounds(row, col);

  // Handle CR before LF (CRLF line endings)
  if (end > start && buffer_.data()[end - 1] == '\r') {
    --end;
  }

  // Strip quotes if present
  if (end > start) {
    const uint8_t* data = buffer_.data();
    if (data[start] == static_cast<uint8_t>(quote_) &&
        data[end - 1] == static_cast<uint8_t>(quote_)) {
      ++start;
      --end;
    }
  }

  if (end < start) end = start;

  return std::string_view(reinterpret_cast<const char*>(buffer_.data() + start), end - start);
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
  return idx_->get_trimmed_val(i_, is_last_);
}

string libvroom_index::column_iterator::at(ptrdiff_t n) const {
  size_t i = ((n + idx_->has_header_) * idx_->columns_) + column_;
  return idx_->get_trimmed_val(i, is_last_);
}

size_t libvroom_index::column_iterator::position() const {
  auto [start, end] = idx_->get_cell(i_);
  return start;
}

// Row iterator implementation
string libvroom_index::row_iterator::value() const {
  // Handle header row (row_ == -1 as size_t wraps to max value)
  if (row_ == static_cast<size_t>(-1)) {
    // Header row - use cached headers
    std::string hdr = idx_->get_header_field(col_);
    return string(std::move(hdr));
  }

  // Data row - use cached linear index
  auto sv = idx_->get_field(row_, col_);
  return string(sv.data(), sv.data() + sv.size());
}

string libvroom_index::row_iterator::at(ptrdiff_t n) const {
  // Handle header row (row_ == -1 as size_t wraps to max value)
  if (row_ == static_cast<size_t>(-1)) {
    // Header row - use cached headers
    std::string hdr = idx_->get_header_field(static_cast<size_t>(n));
    return string(std::move(hdr));
  }

  // Data row - use cached linear index
  auto sv = idx_->get_field(row_, n);
  return string(sv.data(), sv.data() + sv.size());
}

size_t libvroom_index::row_iterator::position() const {
  // Handle header row
  if (row_ == static_cast<size_t>(-1)) {
    return 0;
  }

  // Data row - return byte offset from cached linear index
  auto [start, end] = idx_->get_field_bounds(row_, col_);
  return start;
}

} // namespace vroom
