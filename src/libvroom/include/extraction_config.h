#ifndef LIBVROOM_EXTRACTION_CONFIG_H
#define LIBVROOM_EXTRACTION_CONFIG_H

#include "common_defs.h"

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace libvroom {

/**
 * Result structure for value extraction operations.
 * Contains either a successfully parsed value or an error indicator.
 */
template <typename T> struct ExtractResult {
  std::optional<T> value;
  const char* error = nullptr;

  bool ok() const { return value.has_value(); }
  bool is_na() const { return !value.has_value() && error == nullptr; }

  T get() const {
    if (!value.has_value()) {
      throw std::runtime_error(error ? error : "Value is NA");
    }
    return *value;
  }

  T get_or(T default_value) const { return value.value_or(default_value); }
};

/**
 * Configuration for value extraction behavior.
 * Controls NA detection, boolean parsing, and whitespace handling.
 *
 * @note **Field Usage by Parser Type**:
 *
 * | Field               | Integer Parsers | Double Parsers | Boolean Parser |
 * |---------------------|-----------------|----------------|----------------|
 * | na_values           | Yes             | No*            | Yes            |
 * | true_values         | No              | No             | Yes            |
 * | false_values        | No              | No             | Yes            |
 * | trim_whitespace     | Yes             | Yes            | Yes            |
 * | allow_leading_zeros | Yes             | N/A            | No             |
 * | max_integer_digits  | Yes             | No             | No             |
 *
 * *Double parsers (parse_double_simd, SIMDDoubleParser) do NOT check na_values
 * because floating-point has valid special values (NaN, Inf) that overlap with
 * common NA strings. Use is_na() before parsing doubles if you need NA detection.
 *
 * @see parse_integer_simd() - uses na_values, trim_whitespace, max_integer_digits
 * @see parse_double_simd() - uses only trim_whitespace (see its docs for rationale)
 * @see parse_bool() - uses na_values, true_values, false_values, trim_whitespace
 * @see is_na() - uses na_values, trim_whitespace
 */
struct ExtractionConfig {
  /// Strings recognized as NA/missing values. Used by integer and boolean
  /// parsers. NOT used by double parsers (see class documentation).
  std::vector<std::string_view> na_values = {"", "NA", "N/A", "NaN", "null", "NULL", "None"};
  /// Strings recognized as boolean true. Used only by parse_bool().
  std::vector<std::string_view> true_values = {"true", "True", "TRUE", "1",
                                               "yes",  "Yes",  "YES",  "T"};
  /// Strings recognized as boolean false. Used only by parse_bool().
  std::vector<std::string_view> false_values = {"false", "False", "FALSE", "0",
                                                "no",    "No",    "NO",    "F"};
  /// Whether to trim leading/trailing whitespace. Used by all parsers.
  bool trim_whitespace = true;
  /// Whether to allow leading zeros in integers. Used by integer parsers.
  bool allow_leading_zeros = true;
  /// Maximum digits allowed in an integer. Used by integer parsers.
  size_t max_integer_digits = 20;

  static ExtractionConfig defaults() { return ExtractionConfig{}; }
};

/**
 * Parse a boolean value from a string.
 * Checks against configurable true/false/NA values.
 */
really_inline ExtractResult<bool>
parse_bool(const char* str, size_t len,
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

  std::string_view sv(ptr, end - ptr);
  for (const auto& tv : config.true_values)
    if (sv == tv)
      return {true, nullptr};
  for (const auto& fv : config.false_values)
    if (sv == fv)
      return {false, nullptr};
  for (const auto& na : config.na_values)
    if (sv == na)
      return {std::nullopt, nullptr};
  return {std::nullopt, "Invalid boolean value"};
}

/**
 * Check if a string value represents NA/missing.
 */
really_inline bool is_na(const char* str, size_t len,
                         const ExtractionConfig& config = ExtractionConfig::defaults()) {
  if (len == 0)
    return true;
  const char* ptr = str;
  const char* end = str + len;
  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return true;
  }
  std::string_view sv(ptr, end - ptr);
  for (const auto& na : config.na_values)
    if (sv == na)
      return true;
  return false;
}

} // namespace libvroom

#endif // LIBVROOM_EXTRACTION_CONFIG_H
