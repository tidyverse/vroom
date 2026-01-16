#ifndef LIBVROOM_VALUE_EXTRACTION_H
#define LIBVROOM_VALUE_EXTRACTION_H

#include "common_defs.h"
#include "dialect.h"
#include "extraction_config.h"
#include "two_pass.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace libvroom {

// Forward declarations
class ValueExtractor;
class LazyColumnIterator;

/**
 * @brief Lazy column accessor for ALTREP-style deferred field parsing.
 *
 * LazyColumn provides per-column lazy access to CSV data without loading
 * or parsing the entire file upfront. This enables R's ALTREP pattern
 * where columns are only parsed when accessed.
 *
 * Key features:
 * - **Random access**: O(n_threads) access to any row via operator[]
 * - **Byte range access**: get_bounds() returns raw byte ranges for deferred parsing
 * - **Iterator support**: Range-based for loops via begin()/end()
 * - **Zero-copy views**: Returns string_view into the original buffer
 *
 * The class holds lightweight references to the buffer, index, and dialect.
 * It does NOT copy or sort the index, making construction O(1).
 *
 * @note The underlying buffer and ParseIndex must remain valid for the lifetime
 *       of the LazyColumn. This is typically managed by the Parser::Result object.
 *
 * @example
 * @code
 * // Create lazy column from parser result
 * auto result = parser.parse(buf, len);
 * LazyColumn col = result.get_column(0);
 *
 * // Random access - parses only row 100
 * std::string_view value = col[100];
 *
 * // Get byte bounds for deferred parsing
 * FieldSpan span = col.get_bounds(100);
 * // Parse later: parse_integer<int64_t>(buf + span.start, span.length())
 *
 * // Iterate (parses each row on access)
 * for (std::string_view row_value : col) {
 *     process(row_value);
 * }
 * @endcode
 *
 * @see ValueExtractor for eager column extraction
 * @see ParseIndex::get_field_span() for the underlying field lookup
 */
class LazyColumn {
public:
  /**
   * @brief Construct a lazy column accessor with bounds validation.
   *
   * @param buf Pointer to the CSV data buffer
   * @param len Length of the buffer in bytes
   * @param idx Reference to the parsed index
   * @param col Column index (0-based)
   * @param has_header Whether the CSV has a header row (affects row indexing)
   * @param dialect CSV dialect for quote handling
   * @param config Extraction configuration for parsing
   * @param validate_bounds If true (default), validates col < idx.columns
   *
   * @throws std::out_of_range if validate_bounds is true and col >= idx.columns
   */
  LazyColumn(const uint8_t* buf, size_t len, const ParseIndex& idx, size_t col, bool has_header,
             const Dialect& dialect = Dialect::csv(),
             const ExtractionConfig& config = ExtractionConfig::defaults(),
             bool validate_bounds = true)
      : buf_(buf), len_(len), idx_(&idx), col_(col), has_header_(has_header), dialect_(dialect),
        config_(config) {
    // Validate column bounds if requested
    if (validate_bounds && col >= idx_->columns) {
      throw std::out_of_range("LazyColumn: column index " + std::to_string(col) +
                              " out of range (columns = " + std::to_string(idx_->columns) + ")");
    }

    // Compute number of rows
    if (idx_->columns > 0) {
      uint64_t total_fields = idx_->total_indexes();
      uint64_t total_rows = total_fields / idx_->columns;
      num_rows_ = has_header_ ? (total_rows > 0 ? total_rows - 1 : 0) : total_rows;
    }
  }

  /**
   * @brief Get the number of data rows in the column.
   *
   * @return Number of rows (excludes header if has_header is true)
   */
  size_t size() const { return num_rows_; }

  /**
   * @brief Check if the column is empty.
   */
  bool empty() const { return num_rows_ == 0; }

  /**
   * @brief Get the column index.
   */
  size_t column_index() const { return col_; }

  /**
   * @brief Random access to a row's string value.
   *
   * Returns a string_view into the original buffer. The view is valid as long
   * as the underlying buffer remains valid. Quote characters are included in
   * the returned view for quoted fields.
   *
   * @param row Row index (0-based, excludes header)
   * @return String view of the field content
   * @throws std::out_of_range if row >= size()
   *
   * @note Complexity: O(n_threads) due to ParseIndex::get_field_span()
   */
  std::string_view operator[](size_t row) const {
    if (row >= num_rows_) {
      throw std::out_of_range("LazyColumn: row index out of range");
    }
    FieldSpan span = get_bounds(row);
    if (!span.is_valid()) {
      return std::string_view();
    }
    return get_field_content(span);
  }

  /**
   * @brief Get raw byte boundaries for a row.
   *
   * Returns the byte range in the source buffer for deferred parsing.
   * This enables the ALTREP pattern where parsing happens only when
   * the value is actually needed.
   *
   * @param row Row index (0-based, excludes header)
   * @return FieldSpan with start/end byte positions, or invalid span if out of bounds
   *
   * @note Complexity: O(n_threads) due to ParseIndex::get_field_span()
   *
   * @example
   * @code
   * FieldSpan span = col.get_bounds(row);
   * if (span.is_valid()) {
   *     // Deferred parsing - only parse when value is needed
   *     auto result = parse_integer<int64_t>(
   *         reinterpret_cast<const char*>(buf + span.start),
   *         span.length()
   *     );
   * }
   * @endcode
   */
  FieldSpan get_bounds(size_t row) const {
    // Adjust row for header
    size_t actual_row = has_header_ ? row + 1 : row;
    return idx_->get_field_span(actual_row, col_);
  }

  /**
   * @brief Get a typed value from a row.
   *
   * Parses the field content to the requested type using the configured
   * ExtractionConfig.
   *
   * @tparam T The type to extract (int32_t, int64_t, double, bool)
   * @param row Row index (0-based, excludes header)
   * @return ExtractResult containing the parsed value or error/NA status
   * @throws std::out_of_range if row >= size()
   */
  template <typename T> ExtractResult<T> get(size_t row) const;

  /**
   * @brief Get string value with unescaping applied.
   *
   * Unlike operator[], this method handles escape sequences (e.g., doubled
   * quotes) and returns a clean string. This involves a copy.
   *
   * @param row Row index (0-based, excludes header)
   * @return Unescaped string content
   * @throws std::out_of_range if row >= size()
   */
  std::string get_string(size_t row) const;

  // Iterator support
  class Iterator;
  Iterator begin() const;
  Iterator end() const;

  /**
   * @brief Iterator for lazy column traversal.
   *
   * Provides input iterator semantics for range-based for loops.
   * Each dereference accesses the field via the ParseIndex, so
   * iterating is O(n * n_threads) total.
   */
  class Iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using pointer = const std::string_view*;
    using reference = std::string_view;

    Iterator(const LazyColumn* col, size_t row) : col_(col), row_(row) {}

    reference operator*() const { return (*col_)[row_]; }

    Iterator& operator++() {
      ++row_;
      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      ++row_;
      return tmp;
    }

    bool operator==(const Iterator& other) const { return row_ == other.row_; }

    bool operator!=(const Iterator& other) const { return row_ != other.row_; }

    size_t row() const { return row_; }

  private:
    const LazyColumn* col_;
    size_t row_;
  };

  /**
   * @brief Get the extraction configuration.
   */
  const ExtractionConfig& config() const { return config_; }

  /**
   * @brief Get the dialect.
   */
  const Dialect& dialect() const { return dialect_; }

  /**
   * @brief Check if the column has a header.
   */
  bool has_header() const { return has_header_; }

private:
  const uint8_t* buf_;
  size_t len_;
  const ParseIndex* idx_;
  size_t col_;
  bool has_header_;
  Dialect dialect_;
  ExtractionConfig config_;
  size_t num_rows_ = 0;

  /**
   * @brief Extract field content from a span, handling quotes.
   */
  std::string_view get_field_content(const FieldSpan& span) const {
    if (!span.is_valid() || span.start >= len_) {
      return std::string_view();
    }

    size_t start = static_cast<size_t>(span.start);
    size_t end = std::min(static_cast<size_t>(span.end), len_);

    // Handle CR in CRLF endings
    if (end > start && buf_[end - 1] == '\r') {
      --end;
    }

    // Handle quoted fields - strip outer quotes
    if (end > start && buf_[start] == static_cast<uint8_t>(dialect_.quote_char)) {
      if (buf_[end - 1] == static_cast<uint8_t>(dialect_.quote_char)) {
        ++start;
        --end;
      }
    }

    if (end < start) {
      end = start;
    }

    return std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start);
  }
};

// LazyColumn iterator methods (defined after the class)
inline LazyColumn::Iterator LazyColumn::begin() const {
  return Iterator(this, 0);
}

inline LazyColumn::Iterator LazyColumn::end() const {
  return Iterator(this, num_rows_);
}

/**
 * @brief Factory function to create a LazyColumn from a ParseIndex with bounds validation.
 *
 * Validates that the column index is within bounds before construction,
 * providing a clear error message if the column index is invalid.
 *
 * @param buf Pointer to the CSV data buffer
 * @param len Length of the buffer
 * @param idx Reference to the parsed index
 * @param col Column index (0-based)
 * @param has_header Whether the CSV has a header row
 * @param dialect CSV dialect settings
 * @param config Extraction configuration
 * @return LazyColumn for the specified column
 * @throws std::out_of_range if col >= idx.columns
 *
 * @see make_lazy_column_unchecked() for performance-critical scenarios where
 *      bounds checking has already been performed
 */
inline LazyColumn make_lazy_column(const uint8_t* buf, size_t len, const ParseIndex& idx,
                                   size_t col, bool has_header = true,
                                   const Dialect& dialect = Dialect::csv(),
                                   const ExtractionConfig& config = ExtractionConfig::defaults()) {
  return LazyColumn(buf, len, idx, col, has_header, dialect, config, true);
}

/**
 * @brief Factory function to create a LazyColumn without bounds validation.
 *
 * This variant skips column bounds validation for performance-critical scenarios
 * where the caller has already validated the column index. Using an invalid
 * column index results in undefined behavior (typically empty/invalid spans).
 *
 * @param buf Pointer to the CSV data buffer
 * @param len Length of the buffer
 * @param idx Reference to the parsed index
 * @param col Column index (0-based)
 * @param has_header Whether the CSV has a header row
 * @param dialect CSV dialect settings
 * @param config Extraction configuration
 * @return LazyColumn for the specified column
 *
 * @note Only use this when you have already validated col < idx.columns
 *
 * @see make_lazy_column() for the validated version
 */
inline LazyColumn
make_lazy_column_unchecked(const uint8_t* buf, size_t len, const ParseIndex& idx, size_t col,
                           bool has_header = true, const Dialect& dialect = Dialect::csv(),
                           const ExtractionConfig& config = ExtractionConfig::defaults()) {
  return LazyColumn(buf, len, idx, col, has_header, dialect, config, false);
}

template <typename IntType>
really_inline ExtractResult<IntType>
parse_integer(const char* str, size_t len,
              const ExtractionConfig& config = ExtractionConfig::defaults()) {
  static_assert(std::is_integral_v<IntType>, "IntType must be an integral type");

  if (len == 0)
    return {std::nullopt, nullptr};

  const char* ptr = str;
  const char* end = str + len;

  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return {std::nullopt, nullptr};
  }

  std::string_view sv(ptr, end - ptr);
  for (const auto& na : config.na_values) {
    if (sv == na)
      return {std::nullopt, nullptr};
  }

  bool negative = false;
  // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
  if constexpr (std::is_signed_v<IntType>) {
    if (*ptr == '-') {
      negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }
  } else {
    if (*ptr == '+')
      ++ptr;
    if (*ptr == '-')
      return {std::nullopt, "Negative value for unsigned type"};
  }
  // LCOV_EXCL_BR_STOP

  if (ptr == end)
    return {std::nullopt, "Invalid integer: no digits"};

  size_t digit_count = end - ptr;
  if (digit_count > config.max_integer_digits)
    return {std::nullopt, "Integer too large"};

  // Check for leading zeros if not allowed
  // LCOV_EXCL_BR_START - compound condition branches covered by tests
  if (!config.allow_leading_zeros && digit_count > 1 && *ptr == '0')
    return {std::nullopt, "Leading zeros not allowed"};
  // LCOV_EXCL_BR_STOP

  using UnsignedType = std::make_unsigned_t<IntType>;
  UnsignedType result = 0;
  constexpr UnsignedType max_before_mul = std::numeric_limits<UnsignedType>::max() / 10;

  while (ptr < end) {
    char c = *ptr++;
    if (c < '0' || c > '9')
      return {std::nullopt, "Invalid character in integer"};
    unsigned digit = c - '0';
    if (result > max_before_mul)
      return {std::nullopt, "Integer overflow"};
    result *= 10;
    if (result > std::numeric_limits<UnsignedType>::max() - digit)
      return {std::nullopt, "Integer overflow"};
    result += digit;
  }

  // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
  if constexpr (std::is_signed_v<IntType>) {
    if (negative) {
      constexpr UnsignedType max_negative =
          static_cast<UnsignedType>(std::numeric_limits<IntType>::max()) + 1;
      if (result > max_negative)
        return {std::nullopt, "Integer underflow"};
      // Safe: negate the unsigned value, then cast. This avoids UB for INT_MIN.
      return {static_cast<IntType>(-result), nullptr};
    } else {
      if (result > static_cast<UnsignedType>(std::numeric_limits<IntType>::max()))
        return {std::nullopt, "Integer overflow"};
      return {static_cast<IntType>(result), nullptr};
    }
  } else {
    return {result, nullptr};
  }
  // LCOV_EXCL_BR_STOP
}

really_inline ExtractResult<double>
parse_double(const char* str, size_t len,
             const ExtractionConfig& config = ExtractionConfig::defaults()) {
  if (len == 0)
    return {std::nullopt, nullptr};

  const char* ptr = str;
  const char* end = str + len;

  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return {std::nullopt, nullptr};
  }

  size_t remaining = end - ptr;
  if (remaining >= 3) {
    if ((ptr[0] == 'N' || ptr[0] == 'n') && (ptr[1] == 'a' || ptr[1] == 'A') &&
        (ptr[2] == 'N' || ptr[2] == 'n') && remaining == 3) {
      return {std::numeric_limits<double>::quiet_NaN(), nullptr};
    }
    if ((ptr[0] == 'I' || ptr[0] == 'i') && (ptr[1] == 'N' || ptr[1] == 'n') &&
        (ptr[2] == 'F' || ptr[2] == 'f')) {
      if (remaining == 3)
        return {std::numeric_limits<double>::infinity(), nullptr};
      if (remaining == 8 && (ptr[3] == 'I' || ptr[3] == 'i') && (ptr[4] == 'N' || ptr[4] == 'n') &&
          (ptr[5] == 'I' || ptr[5] == 'i') && (ptr[6] == 'T' || ptr[6] == 't') &&
          (ptr[7] == 'Y' || ptr[7] == 'y')) {
        return {std::numeric_limits<double>::infinity(), nullptr};
      }
    }
    if (ptr[0] == '-' && remaining >= 4 && (ptr[1] == 'I' || ptr[1] == 'i') &&
        (ptr[2] == 'N' || ptr[2] == 'n') && (ptr[3] == 'F' || ptr[3] == 'f')) {
      if (remaining == 4)
        return {-std::numeric_limits<double>::infinity(), nullptr};
      if (remaining == 9 && (ptr[4] == 'I' || ptr[4] == 'i') && (ptr[5] == 'N' || ptr[5] == 'n') &&
          (ptr[6] == 'I' || ptr[6] == 'i') && (ptr[7] == 'T' || ptr[7] == 't') &&
          (ptr[8] == 'Y' || ptr[8] == 'y')) {
        return {-std::numeric_limits<double>::infinity(), nullptr};
      }
    }
  }

  bool negative = false;
  if (*ptr == '-') {
    negative = true;
    ++ptr;
  } else if (*ptr == '+') {
    ++ptr;
  }
  if (ptr == end)
    return {std::nullopt, "Invalid number: no digits"};

  uint64_t mantissa = 0;
  int64_t exponent = 0;
  int digit_count = 0;
  bool seen_digit = false;

  while (ptr < end && *ptr >= '0' && *ptr <= '9') {
    seen_digit = true;
    if (digit_count < 19) {
      mantissa = mantissa * 10 + (*ptr - '0');
      ++digit_count;
    } else {
      ++exponent;
    }
    ++ptr;
  }

  if (ptr < end && *ptr == '.') {
    ++ptr;
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
      seen_digit = true;
      if (digit_count < 19) {
        mantissa = mantissa * 10 + (*ptr - '0');
        ++digit_count;
        --exponent;
      }
      ++ptr;
    }
  }

  if (!seen_digit)
    return {std::nullopt, "Invalid number: no digits"};

  if (ptr < end && (*ptr == 'e' || *ptr == 'E')) {
    ++ptr;
    if (ptr == end)
      return {std::nullopt, "Invalid number: incomplete exponent"};
    bool exp_negative = false;
    if (*ptr == '-') {
      exp_negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }
    if (ptr == end || *ptr < '0' || *ptr > '9')
      return {std::nullopt, "Invalid number: missing exponent digits"};
    int64_t exp_value = 0;
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
      exp_value = exp_value * 10 + (*ptr - '0');
      ++ptr;
      if (exp_value > 400) {
        // Consume remaining exponent digits after overflow
        while (ptr < end && *ptr >= '0' && *ptr <= '9')
          ++ptr;
        break;
      }
    }
    if (exp_negative)
      exp_value = -exp_value;
    exponent += exp_value;
  }

  if (ptr != end)
    return {std::nullopt, "Invalid number: unexpected characters"};
  if (mantissa == 0)
    return {negative ? -0.0 : 0.0, nullptr};

  double result = static_cast<double>(mantissa) * std::pow(10.0, static_cast<double>(exponent));
  if (std::isinf(result))
    return {negative ? -std::numeric_limits<double>::infinity()
                     : std::numeric_limits<double>::infinity(),
            nullptr};
  return {negative ? -result : result, nullptr};
}

// Forward declare SIMD parsers (defined in simd_number_parsing.h)
// Default arguments MUST be here (in the first declaration), not in the definitions
template <typename IntType>
ExtractResult<IntType>
parse_integer_simd(const char* str, size_t len,
                   const ExtractionConfig& config = ExtractionConfig::defaults());
ExtractResult<double>
parse_double_simd(const char* str, size_t len,
                  const ExtractionConfig& config = ExtractionConfig::defaults());

class ValueExtractor {
public:
  ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx,
                 const Dialect& dialect = Dialect::csv(),
                 const ExtractionConfig& config = ExtractionConfig::defaults());

  /**
   * Constructor with per-column configuration support.
   *
   * @param buf Pointer to the CSV data buffer
   * @param len Length of the buffer
   * @param idx Parsed index containing field positions
   * @param dialect CSV dialect settings
   * @param config Global extraction configuration (fallback for columns without overrides)
   * @param column_configs Per-column configuration overrides
   *
   * @note Name-based column configs are resolved automatically after the header is read.
   */
  ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx, const Dialect& dialect,
                 const ExtractionConfig& config, const ColumnConfigMap& column_configs);

  /**
   * Constructor with shared ParseIndex ownership for buffer lifetime safety.
   *
   * This constructor accepts a shared_ptr to a ParseIndex, enabling safe sharing
   * of the underlying data between multiple consumers. The buffer data is obtained
   * from the ParseIndex's shared buffer reference.
   *
   * Use this constructor when:
   * - The ValueExtractor may outlive the original ParseIndex
   * - Multiple consumers need concurrent access to the same parsed data
   * - Implementing lazy column access (e.g., R's ALTREP)
   *
   * @param shared_idx Shared pointer to a ParseIndex with buffer set
   * @param dialect CSV dialect settings
   * @param config Global extraction configuration
   *
   * @throws std::invalid_argument if shared_idx is null or has no buffer
   *
   * @example
   * @code
   * // Parse and set up shared buffer
   * auto buffer = std::make_shared<std::vector<uint8_t>>(data, data + len);
   * auto result = parser.parse(buffer->data(), buffer->size());
   * result.idx.set_buffer(buffer);
   *
   * // Create shared index and extractor
   * auto shared_idx = result.idx.share();
   * ValueExtractor extractor(shared_idx, dialect, config);
   *
   * // Original ParseIndex can be moved/destroyed safely
   * result = {};  // Destroy original
   *
   * // Extractor still works - it holds shared references
   * auto value = extractor.get<double>(0, 0);
   * @endcode
   */
  ValueExtractor(std::shared_ptr<const ParseIndex> shared_idx,
                 const Dialect& dialect = Dialect::csv(),
                 const ExtractionConfig& config = ExtractionConfig::defaults());

  size_t num_rows() const { return num_rows_; }
  size_t num_columns() const { return num_columns_; }
  bool has_header() const { return has_header_; }
  void set_has_header(bool has_header) {
    if (has_header_ != has_header) {
      has_header_ = has_header;
      recalculate_num_rows();
    }
  }

  // =========================================================================
  // Buffer and Index accessors (for LazyColumn factory)
  // =========================================================================

  /**
   * @brief Get the underlying data buffer pointer.
   */
  const uint8_t* buffer() const { return buf_; }

  /**
   * @brief Get the buffer length.
   */
  size_t buffer_length() const { return len_; }

  /**
   * @brief Get the parse index reference.
   */
  const ParseIndex& index() const { return idx(); }

  /**
   * @brief Get the dialect.
   */
  const Dialect& dialect() const { return dialect_; }

  /**
   * @brief Create a LazyColumn for the specified column.
   *
   * Factory method to create a LazyColumn that provides lazy per-row access
   * to a single column. This is useful for R ALTREP integration where columns
   * are only parsed when accessed.
   *
   * @param col Column index (0-based)
   * @return LazyColumn for the specified column
   * @throws std::out_of_range if col >= num_columns()
   */
  LazyColumn get_lazy_column(size_t col) const {
    if (col >= num_columns_) {
      throw std::out_of_range("Column index out of range");
    }
    return LazyColumn(buf_, len_, idx(), col, has_header_, dialect_, config_);
  }

  std::string_view get_string_view(size_t row, size_t col) const;
  std::string get_string(size_t row, size_t col) const;

  /**
   * Get a typed value from a cell, using per-column config if available.
   *
   * This method checks for per-column configuration overrides and applies them
   * when extracting and parsing the value. If no override exists for the column,
   * the global config is used.
   *
   * @tparam T The type to extract (int32_t, int64_t, double, bool)
   * @param row Row index (0-based, excludes header if has_header is true)
   * @param col Column index (0-based)
   * @return ExtractResult containing the parsed value or error/NA status
   */
  template <typename T> ExtractResult<T> get(size_t row, size_t col) const {
    auto sv = get_string_view_internal(row, col);
    const ExtractionConfig& effective_config = get_effective_config(col);

    // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
    if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>) {
      return parse_integer_simd<T>(sv.data(), sv.size(), effective_config);
    } else if constexpr (std::is_same_v<T, double>) {
      return parse_double_simd(sv.data(), sv.size(), effective_config);
    } else if constexpr (std::is_same_v<T, bool>) {
      return parse_bool(sv.data(), sv.size(), effective_config);
    } else {
      static_assert(!std::is_same_v<T, T>, "Unsupported type");
    }
    // LCOV_EXCL_BR_STOP
  }

  std::vector<std::string_view> extract_column_string_view(size_t col) const;
  std::vector<std::string> extract_column_string(size_t col) const;

  template <typename T> std::vector<std::optional<T>> extract_column(size_t col) const {
    std::vector<std::optional<T>> result;
    result.reserve(num_rows_);
    for (size_t row = 0; row < num_rows_; ++row)
      result.push_back(get<T>(row, col).value);
    return result;
  }

  template <typename T> std::vector<T> extract_column_or(size_t col, T default_value) const {
    std::vector<T> result;
    result.reserve(num_rows_);
    for (size_t row = 0; row < num_rows_; ++row)
      result.push_back(get<T>(row, col).get_or(default_value));
    return result;
  }

  std::vector<std::string> get_header() const;
  bool get_field_bounds(size_t row, size_t col, size_t& start, size_t& end) const;
  const ExtractionConfig& config() const { return config_; }
  void set_config(const ExtractionConfig& config) { config_ = config; }

  // =========================================================================
  // Per-column configuration API
  // =========================================================================

  /**
   * Get the column configuration map.
   */
  const ColumnConfigMap& column_configs() const { return column_configs_; }

  /**
   * Set the column configuration map.
   * Name-based configs are resolved when headers are available.
   */
  void set_column_configs(const ColumnConfigMap& configs) {
    column_configs_ = configs;
    resolved_configs_.clear(); // Clear cache when configs change
    resolve_column_configs();
  }

  /**
   * Set configuration for a specific column by index.
   * @param col_index 0-based column index
   * @param config Configuration to apply
   */
  void set_column_config(size_t col_index, const ColumnConfig& config) {
    column_configs_.set(col_index, config);
    // Update resolved config cache for this column
    if (config.has_overrides()) {
      resolved_configs_[col_index] = config.merge_with(config_);
    } else {
      resolved_configs_.erase(col_index);
    }
  }

  /**
   * Set configuration for a specific column by name.
   * The name is resolved to an index using the header row.
   * @param col_name Column name (case-sensitive)
   * @param config Configuration to apply
   */
  void set_column_config(const std::string& col_name, const ColumnConfig& config) {
    column_configs_.set(col_name, config);
    resolve_column_configs(); // Re-resolve to update the index
  }

  /**
   * Get the per-column configuration for a specific column.
   * @param col_index 0-based column index
   * @return Pointer to column config, or nullptr if no override exists
   */
  const ColumnConfig* get_column_config(size_t col_index) const {
    return column_configs_.get(col_index);
  }

  /**
   * Get the type hint for a specific column.
   * @param col_index 0-based column index
   * @return The type hint, or TypeHint::AUTO if none is set
   */
  TypeHint get_type_hint(size_t col_index) const {
    const ColumnConfig* config = column_configs_.get(col_index);
    if (config && config->type_hint.has_value()) {
      return *config->type_hint;
    }
    return TypeHint::AUTO;
  }

  /**
   * Check if a column should be skipped during extraction.
   * @param col_index 0-based column index
   * @return true if the column has TypeHint::SKIP
   */
  bool should_skip_column(size_t col_index) const {
    return get_type_hint(col_index) == TypeHint::SKIP;
  }

  /**
   * @brief Result of a byte offset to (row, column) lookup.
   *
   * Location represents the result of finding which CSV cell contains
   * a given byte offset. This enables efficient error reporting by
   * converting internal byte positions to human-readable row/column
   * coordinates.
   */
  struct Location {
    size_t row;    ///< 0-based row index (data rows, header is row 0 if present)
    size_t column; ///< 0-based column index
    bool found;    ///< true if byte offset is within valid CSV data

    /// @return true if the location is valid (found == true)
    explicit operator bool() const { return found; }
  };

  /**
   * @brief Convert a byte offset to (row, column) coordinates.
   *
   * Uses binary search on the internal index for O(log n) lookup instead of
   * O(n) linear scan. This is useful for error reporting when you have a
   * byte offset from parsing and need to display the location to users.
   *
   * The row number returned is 0-based and includes the header row (if present).
   * So row 0 is the header (if has_header() is true), row 1 is the first data row.
   * If has_header() is false, row 0 is the first data row.
   *
   * @param byte_offset Byte offset into the CSV buffer
   * @return Location with row/column if found, or {0, 0, false} if offset is
   *         out of range or no data exists
   *
   * @note Complexity: O(log n) where n is the number of fields in the CSV
   *
   * @example
   * @code
   * auto result = parser.parse(buf, len);
   * ValueExtractor extractor(buf, len, result.idx);
   *
   * // Convert byte offset 150 to row/column
   * auto loc = extractor.byte_offset_to_location(150);
   * if (loc) {
   *     std::cout << "Row " << loc.row << ", Column " << loc.column << std::endl;
   * }
   * @endcode
   */
  Location byte_offset_to_location(size_t byte_offset) const;

private:
  const uint8_t* buf_;
  size_t len_;
  const ParseIndex* idx_ptr_; // Non-owning pointer (from reference constructor)
  Dialect dialect_;
  ExtractionConfig config_;
  ColumnConfigMap column_configs_;
  size_t num_rows_ = 0;
  size_t num_columns_ = 0;
  bool has_header_ = true;

  // Shared ownership members for buffer lifetime safety
  std::shared_ptr<const ParseIndex> shared_idx_;              // Owns ParseIndex when shared
  std::shared_ptr<const std::vector<uint8_t>> shared_buffer_; // Owns buffer when shared

  // Cache of resolved configs (merged with global config) for fast lookup
  mutable std::unordered_map<size_t, ExtractionConfig> resolved_configs_;

  // Helper to get the ParseIndex (works for both ownership modes)
  const ParseIndex& idx() const { return shared_idx_ ? *shared_idx_ : *idx_ptr_; }

  std::string_view get_string_view_internal(size_t row, size_t col) const;
  size_t compute_field_index(size_t row, size_t col) const;
  std::string unescape_field(std::string_view field) const;
  void recalculate_num_rows();

  /**
   * Get the effective extraction config for a column.
   * Returns the merged per-column config if one exists, otherwise the global config.
   */
  const ExtractionConfig& get_effective_config(size_t col) const {
    // Check cache first
    auto it = resolved_configs_.find(col);
    if (it != resolved_configs_.end()) {
      return it->second;
    }

    // Check if there's a per-column config
    const ColumnConfig* col_config = column_configs_.get(col);
    if (col_config && col_config->has_overrides()) {
      // Merge and cache
      resolved_configs_[col] = col_config->merge_with(config_);
      return resolved_configs_[col];
    }

    // No override, use global config
    return config_;
  }

  /**
   * Resolve name-based column configs to indices using header names.
   */
  void resolve_column_configs() {
    if (!has_header_ || column_configs_.by_name().empty()) {
      return;
    }

    // Build name-to-index map from headers
    std::unordered_map<std::string, size_t> name_to_index;
    auto headers = get_header();
    for (size_t i = 0; i < headers.size(); ++i) {
      name_to_index[headers[i]] = i;
    }

    // Resolve names to indices
    column_configs_.resolve_names(name_to_index);

    // Clear resolved config cache since indices may have changed
    resolved_configs_.clear();
  }
};

class RowIterator {
public:
  struct Row {
    size_t row_index;
    const ValueExtractor* extractor;
    std::string_view get_string_view(size_t col) const {
      return extractor->get_string_view(row_index, col);
    }
    std::string get_string(size_t col) const { return extractor->get_string(row_index, col); }
    template <typename T> ExtractResult<T> get(size_t col) const {
      return extractor->get<T>(row_index, col);
    }
    size_t num_columns() const { return extractor->num_columns(); }
  };
  RowIterator(const ValueExtractor* extractor, size_t row) : extractor_(extractor), row_(row) {}
  Row operator*() const { return {row_, extractor_}; }
  RowIterator& operator++() {
    ++row_;
    return *this;
  }
  bool operator!=(const RowIterator& other) const { return row_ != other.row_; }

private:
  const ValueExtractor* extractor_;
  size_t row_;
};

inline RowIterator begin(const ValueExtractor& ve) {
  return RowIterator(&ve, 0);
}
inline RowIterator end(const ValueExtractor& ve) {
  return RowIterator(&ve, ve.num_rows());
}

// LazyColumn::get<T>() implementation is defined after simd_number_parsing.h

} // namespace libvroom

// Include SIMD number parsing after all types are defined
// This provides the implementations for parse_integer_simd and parse_double_simd
#include "simd_number_parsing.h"

// LazyColumn template method implementations (need SIMD parsers)
namespace libvroom {

template <typename T> ExtractResult<T> LazyColumn::get(size_t row) const {
  if (row >= num_rows_) {
    throw std::out_of_range("LazyColumn: row index out of range");
  }

  std::string_view sv = (*this)[row];

  // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
  if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>) {
    return parse_integer_simd<T>(sv.data(), sv.size(), config_);
  } else if constexpr (std::is_same_v<T, double>) {
    return parse_double_simd(sv.data(), sv.size(), config_);
  } else if constexpr (std::is_same_v<T, bool>) {
    return parse_bool(sv.data(), sv.size(), config_);
  } else {
    static_assert(!std::is_same_v<T, T>, "Unsupported type");
  }
  // LCOV_EXCL_BR_STOP
}

inline std::string LazyColumn::get_string(size_t row) const {
  if (row >= num_rows_) {
    throw std::out_of_range("LazyColumn: row index out of range");
  }

  FieldSpan span = get_bounds(row);
  if (!span.is_valid() || span.start >= len_) {
    return std::string();
  }

  size_t start = static_cast<size_t>(span.start);
  size_t end = std::min(static_cast<size_t>(span.end), len_);

  // Handle CR in CRLF endings
  if (end > start && buf_[end - 1] == '\r') {
    --end;
  }

  if (end < start) {
    end = start;
  }

  std::string_view raw(reinterpret_cast<const char*>(buf_ + start), end - start);

  // Handle quoted fields - unescape
  if (raw.empty() || raw.front() != dialect_.quote_char) {
    return std::string(raw);
  }

  if (raw.size() < 2 || raw.back() != dialect_.quote_char) {
    return std::string(raw);
  }

  // Strip quotes and unescape doubled quotes
  std::string_view inner = raw.substr(1, raw.size() - 2);
  std::string result;
  result.reserve(inner.size());

  for (size_t i = 0; i < inner.size(); ++i) {
    char c = inner[i];
    if (c == dialect_.escape_char && i + 1 < inner.size() && inner[i + 1] == dialect_.quote_char) {
      result += dialect_.quote_char;
      ++i;
    } else {
      result += c;
    }
  }

  return result;
}

/**
 * @brief Create a LazyColumn from a ValueExtractor.
 *
 * Factory function to create a LazyColumn from an existing ValueExtractor,
 * inheriting its buffer, index, dialect, and config.
 *
 * @param extractor Source ValueExtractor
 * @param col Column index (0-based)
 * @return LazyColumn for the specified column
 */
inline LazyColumn get_lazy_column(const ValueExtractor& extractor, size_t col) {
  return extractor.get_lazy_column(col);
}

} // namespace libvroom

#endif // LIBVROOM_VALUE_EXTRACTION_H
