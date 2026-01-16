#ifndef LIBVROOM_VALUE_EXTRACTION_H
#define LIBVROOM_VALUE_EXTRACTION_H

#include "common_defs.h"
#include "dialect.h"
#include "extraction_config.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace libvroom {

class ParseIndex;

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

  size_t num_rows() const { return num_rows_; }
  size_t num_columns() const { return num_columns_; }
  bool has_header() const { return has_header_; }
  void set_has_header(bool has_header) {
    if (has_header_ != has_header) {
      has_header_ = has_header;
      recalculate_num_rows();
    }
  }

  std::string_view get_string_view(size_t row, size_t col) const;
  std::string get_string(size_t row, size_t col) const;

  template <typename T> ExtractResult<T> get(size_t row, size_t col) const {
    auto sv = get_string_view_internal(row, col);
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

private:
  const uint8_t* buf_;
  size_t len_;
  const ParseIndex& idx_;
  Dialect dialect_;
  ExtractionConfig config_;
  size_t num_rows_ = 0;
  size_t num_columns_ = 0;
  bool has_header_ = true;
  std::vector<uint64_t> linear_indexes_;

  std::string_view get_string_view_internal(size_t row, size_t col) const;
  size_t compute_field_index(size_t row, size_t col) const;
  std::string unescape_field(std::string_view field) const;
  void recalculate_num_rows() {
    size_t total_indexes = linear_indexes_.size();
    if (total_indexes > 0 && num_columns_ > 0) {
      size_t total_rows = total_indexes / num_columns_;
      num_rows_ = has_header_ ? (total_rows > 0 ? total_rows - 1 : 0) : total_rows;
    }
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

} // namespace libvroom

// Include SIMD number parsing after all types are defined
// This provides the implementations for parse_integer_simd and parse_double_simd
#include "simd_number_parsing.h"

#endif // LIBVROOM_VALUE_EXTRACTION_H
