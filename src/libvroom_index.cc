#include "libvroom_index.h"

#include <chrono>
#include <R.h>

namespace vroom {

// Timing helper
static bool timing_enabled = false;

#define TIME_BLOCK(name, code) \
  do { \
    auto t0 = std::chrono::high_resolution_clock::now(); \
    code; \
    auto t1 = std::chrono::high_resolution_clock::now(); \
    if (timing_enabled) { \
      auto ms = std::chrono::duration<double, std::milli>(t1 - t0).count(); \
      Rprintf("  %s: %.2f ms\n", name, ms); \
    } \
  } while(0)

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

  // Enable timing for debugging
  timing_enabled = (getenv("VROOM_TIMING") != nullptr);
  if (timing_enabled) Rprintf("libvroom_index construction:\n");

  // Load file using libvroom
  TIME_BLOCK("load_file", buffer_ = libvroom::load_file(filename));
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
  TIME_BLOCK("parser.parse", result_ = parser.parse(buffer_.data(), buffer_.size(), opts));

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
  TIME_BLOCK("num_columns", columns_ = result_.num_columns());
  TIME_BLOCK("num_rows", rows_ = result_.num_rows());

  // Cache headers
  if (has_header_ && columns_ > 0) {
    TIME_BLOCK("header", headers_ = result_.header());
  }

  if (timing_enabled) Rprintf("  rows: %zu, columns: %zu\n", rows_, columns_);
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

bool libvroom_index::needs_escape_processing(std::string_view sv) const {
  // Check if the field contains escaped quotes (doubled quotes or backslash escapes)
  // This determines whether we need to allocate for escape processing
  if (sv.empty()) {
    return false;
  }

  // Look for escape sequences that need processing
  // For double-quote escaping: look for "" within the field
  // For backslash escaping: look for \"
  char escape_char = escape_double_ ? quote_ : '\\';

  for (size_t i = 0; i + 1 < sv.size(); ++i) {
    if (sv[i] == escape_char && sv[i + 1] == quote_) {
      return true;
    }
  }

  return false;
}

string libvroom_index::get_trimmed_val(size_t row, size_t col, bool is_last) const {
  // Use libvroom's Result API directly - it handles quote stripping
  // This is the zero-copy fast path for most fields
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

  // Check if escape processing is needed
  // If so, we need to allocate and process escapes
  std::string_view trimmed(begin, end - begin);
  if (needs_escape_processing(trimmed)) {
    // Slow path: allocate and process escapes
    char escape_char = escape_double_ ? quote_ : '\\';
    std::string result;
    result.reserve(trimmed.size());

    for (size_t i = 0; i < trimmed.size(); ++i) {
      if (i + 1 < trimmed.size() && trimmed[i] == escape_char && trimmed[i + 1] == quote_) {
        // Skip the escape character, output the quote
        result += quote_;
        ++i;
      } else {
        result += trimmed[i];
      }
    }
    return string(std::move(result));
  }

  // Fast path: zero-copy return using pointer pair
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

string libvroom_index::get(size_t row, size_t col) const {
  // Use the optimized get_trimmed_val for consistent behavior
  bool is_last = (col == columns_ - 1);
  return get_trimmed_val(row, col, is_last);
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
