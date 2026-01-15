/**
 * @file libvroom_types.h
 * @brief Optional field type detection for CSV data.
 *
 * This header is optional and can be excluded from builds to reduce compilation
 * time and binary size. Type detection operates independently from core parsing
 * logic and can be disabled via CMake option LIBVROOM_ENABLE_TYPE_DETECTION=OFF.
 *
 * Features:
 * - Field type enumeration (BOOLEAN, INTEGER, FLOAT, DATE, STRING, EMPTY)
 * - Multi-format date detection (ISO, US, EU, compact formats)
 * - Configurable date format preference for ambiguous dates
 * - Boolean variant recognition (true/false, yes/no, on/off, 0/1)
 * - SIMD-accelerated digit classification
 * - Column type inference with configurable confidence thresholds
 * - Type hint system for overriding auto-detected types
 *
 * @note Date format detection has inherent ambiguity between US (MM/DD/YYYY)
 * and EU (DD/MM/YYYY) formats. Use DateFormatPreference in TypeDetectionOptions
 * to control how ambiguous dates are interpreted.
 */

#ifndef LIBVROOM_TYPES_H
#define LIBVROOM_TYPES_H

#include "common_defs.h"
#include "simd_highway.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace libvroom {

enum class FieldType : uint8_t {
  BOOLEAN = 0,
  INTEGER = 1,
  FLOAT = 2,
  DATE = 3,
  STRING = 4,
  EMPTY = 5
};

inline const char* field_type_to_string(FieldType type) {
  switch (type) {
  case FieldType::BOOLEAN:
    return "boolean";
  case FieldType::INTEGER:
    return "integer";
  case FieldType::FLOAT:
    return "float";
  case FieldType::DATE:
    return "date";
  case FieldType::STRING:
    return "string";
  case FieldType::EMPTY:
    return "empty";
  }
  return "unknown";
}

/**
 * Preference for interpreting ambiguous date formats.
 *
 * Dates like "01/02/2024" can be interpreted as either:
 * - US format: January 2nd, 2024 (MM/DD/YYYY)
 * - EU format: February 1st, 2024 (DD/MM/YYYY)
 *
 * This enum controls which interpretation is preferred when both are valid.
 */
enum class DateFormatPreference : uint8_t {
  AUTO = 0,     ///< Default behavior: check US format first, then EU
  US_FIRST = 1, ///< Explicitly prefer MM/DD/YYYY for ambiguous dates
  EU_FIRST = 2, ///< Prefer DD/MM/YYYY for ambiguous dates
  ISO_ONLY = 3  ///< Accept only YYYY-MM-DD (or YYYY/MM/DD) and YYYYMMDD formats
};

struct TypeDetectionOptions {
  bool bool_as_int = true;
  bool trim_whitespace = true;
  bool allow_exponential = true;
  bool allow_thousands_sep = false;
  char thousands_sep = ',';
  char decimal_point = '.';
  double confidence_threshold = 0.9;
  DateFormatPreference date_format_preference = DateFormatPreference::AUTO;

  static TypeDetectionOptions defaults() { return TypeDetectionOptions(); }
};

struct ColumnTypeStats {
  size_t total_count = 0;
  size_t empty_count = 0;
  size_t boolean_count = 0;
  size_t integer_count = 0;
  size_t float_count = 0;
  size_t date_count = 0;
  size_t string_count = 0;

  FieldType dominant_type(double threshold = 0.9) const {
    size_t non_empty = total_count - empty_count;
    if (non_empty == 0)
      return FieldType::EMPTY;

    // Check each type independently - highest specific count that meets threshold wins
    // Priority order: BOOLEAN > INTEGER > FLOAT > DATE > STRING

    // Check if booleans dominate
    if (static_cast<double>(boolean_count) / non_empty >= threshold)
      return FieldType::BOOLEAN;

    // Check if integers dominate (integers can include booleans like 0/1)
    // But only if booleans don't already dominate
    // Include boolean count since 0/1 are valid integers
    if (static_cast<double>(integer_count + boolean_count) / non_empty >= threshold)
      return FieldType::INTEGER;

    // Check if floats dominate (floats include integers which are valid floats)
    // Use cumulative count: floats + integers + booleans (0/1 are valid floats)
    if (static_cast<double>(float_count + integer_count + boolean_count) / non_empty >= threshold)
      return FieldType::FLOAT;

    // Check if dates dominate
    if (static_cast<double>(date_count) / non_empty >= threshold)
      return FieldType::DATE;

    return FieldType::STRING;
  }

  void add(FieldType type) {
    ++total_count;
    switch (type) {
    case FieldType::EMPTY:
      ++empty_count;
      break;
    case FieldType::BOOLEAN:
      ++boolean_count;
      break;
    case FieldType::INTEGER:
      ++integer_count;
      break;
    case FieldType::FLOAT:
      ++float_count;
      break;
    case FieldType::DATE:
      ++date_count;
      break;
    case FieldType::STRING:
      ++string_count;
      break;
    }
  }
};

class TypeDetector {
public:
  // Primary detection method - implemented in libvroom_types.cpp
  static FieldType detect_field(const uint8_t* data, size_t length,
                                const TypeDetectionOptions& options = TypeDetectionOptions());

  // Convenience overloads - implemented in libvroom_types.cpp
  static FieldType detect_field(const std::string& value,
                                const TypeDetectionOptions& options = TypeDetectionOptions());

  static FieldType detect_field(const char* value,
                                const TypeDetectionOptions& options = TypeDetectionOptions());

  // Type checking methods - implemented in libvroom_types.cpp
  static bool is_boolean(const uint8_t* data, size_t length,
                         const TypeDetectionOptions& options = TypeDetectionOptions());

  static bool is_integer(const uint8_t* data, size_t length,
                         const TypeDetectionOptions& options = TypeDetectionOptions());

  static bool is_float(const uint8_t* data, size_t length,
                       const TypeDetectionOptions& options = TypeDetectionOptions());

  /**
   * Detect if a field contains a date value.
   *
   * Supports the following formats:
   * - ISO: YYYY-MM-DD or YYYY/MM/DD
   * - US: MM/DD/YYYY or MM-DD-YYYY
   * - EU: DD/MM/YYYY or DD-MM-YYYY
   * - Compact: YYYYMMDD
   *
   * The date_format_preference option controls ambiguity resolution:
   * - AUTO/US_FIRST: Check US format before EU (default)
   * - EU_FIRST: Check EU format before US
   * - ISO_ONLY: Accept only ISO and compact formats
   */
  static bool is_date(const uint8_t* data, size_t length,
                      const TypeDetectionOptions& options = TypeDetectionOptions());

private:
  // Performance-critical inline helpers
  really_inline static bool is_whitespace(uint8_t c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  }

  really_inline static bool is_digit(uint8_t c) { return c >= '0' && c <= '9'; }

  really_inline static uint8_t to_lower(uint8_t c) { return (c >= 'A' && c <= 'Z') ? (c + 32) : c; }

  // Helper functions - implemented in libvroom_types.cpp
  static bool is_leap_year(int year);
  static int days_in_month(int year, int month);
  static bool is_valid_date(int year, int month, int day);
  static bool is_bool_string(const uint8_t* data, size_t length);
  static bool is_special_float(const uint8_t* data, size_t length);
  static bool is_date_iso(const uint8_t* data, size_t length);
  static bool is_date_us(const uint8_t* data, size_t length);
  static bool is_date_eu(const uint8_t* data, size_t length);
  static bool is_date_compact(const uint8_t* data, size_t length);
};

/**
 * SIMDTypeDetector provides SIMD-accelerated batch processing for type detection.
 *
 * Uses Highway SIMD operations to accelerate digit classification, following the
 * cmp_mask_against_input pattern. The key operations are:
 * - classify_digits: Returns a 64-bit mask where each bit indicates if the
 *   corresponding byte is a digit ('0'-'9')
 * - all_digits: Returns true if all bytes in the input are digits
 *
 * Implementation uses Highway's Ge/Le/And operations for range checking,
 * similar to the SIMDIntegerParser::validate_digits_simd pattern.
 */
class SIMDTypeDetector {
public:
  /**
   * Classify which bytes in the input are ASCII digits ('0'-'9').
   *
   * Uses SIMD comparison operations to check if each byte is in the range
   * ['0', '9'] and returns a 64-bit mask where bit i is set if data[i] is
   * a digit.
   *
   * @param data Pointer to input data (should have at least 64 bytes accessible
   *             for SIMD, padded with zeros if shorter)
   * @param length Actual length of valid data (0-64)
   * @return 64-bit mask where bit i indicates if data[i] is a digit
   */
  static HWY_ATTR uint64_t classify_digits(const uint8_t* data, size_t length) {
    if (length == 0)
      return 0;

    // Namespace alias for Highway operations
    namespace hn = hwy::HWY_NAMESPACE;

    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    // Set comparison values for range check
    const auto zero_char = hn::Set(d, '0');
    const auto nine_char = hn::Set(d, '9');

    uint64_t result = 0;

    // Process data in Highway vector-sized chunks
    size_t i = 0;
    size_t max_len = std::min(length, size_t(64));

    for (; i + N <= max_len; i += N) {
      auto vec = hn::LoadU(d, data + i);

      // Check if bytes are >= '0' and <= '9'
      auto ge_zero = hn::Ge(vec, zero_char);
      auto le_nine = hn::Le(vec, nine_char);
      auto is_digit = hn::And(ge_zero, le_nine);

      // Convert mask to bits
      uint64_t bits = hn::BitsFromMask(d, is_digit);
      result |= (bits << i);
    }

    // Handle remaining bytes with scalar code
    for (; i < max_len; ++i) {
      if (data[i] >= '0' && data[i] <= '9') {
        result |= (1ULL << i);
      }
    }

    // Apply length mask if length < 64
    if (length < 64) {
      uint64_t mask = blsmsk_u64(1ULL << length);
      result &= mask;
    }

    return result;
  }

  /**
   * Check if all bytes in the input are ASCII digits ('0'-'9').
   *
   * Uses SIMD to validate entire vectors at once, with scalar fallback
   * for the remainder. This is significantly faster than a byte-by-byte
   * loop for longer inputs.
   *
   * @param data Pointer to input data
   * @param length Length of input data
   * @return true if all bytes are digits, false otherwise
   */
  static HWY_ATTR bool all_digits(const uint8_t* data, size_t length) {
    if (length == 0)
      return false;

    // Namespace alias for Highway operations
    namespace hn = hwy::HWY_NAMESPACE;

    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    // Set comparison values for range check
    const auto zero_char = hn::Set(d, '0');
    const auto nine_char = hn::Set(d, '9');

    size_t i = 0;

    // Process full vectors using SIMD
    for (; i + N <= length; i += N) {
      auto vec = hn::LoadU(d, data + i);

      // Check if bytes are >= '0' and <= '9'
      auto ge_zero = hn::Ge(vec, zero_char);
      auto le_nine = hn::Le(vec, nine_char);
      auto is_digit = hn::And(ge_zero, le_nine);

      // If any byte is not a digit, return false
      if (!hn::AllTrue(d, is_digit)) {
        return false;
      }
    }

    // Scalar fallback for remainder
    for (; i < length; ++i) {
      if (data[i] < '0' || data[i] > '9') {
        return false;
      }
    }

    return true;
  }

  // Batch detection - implemented in libvroom_types.cpp
  static void detect_batch(const uint8_t** fields, const size_t* lengths, size_t count,
                           FieldType* results,
                           const TypeDetectionOptions& options = TypeDetectionOptions());
};

class ColumnTypeInference {
public:
  explicit ColumnTypeInference(size_t num_columns = 0,
                               const TypeDetectionOptions& options = TypeDetectionOptions());

  void set_options(const TypeDetectionOptions& options);
  void add_row(const std::vector<std::string>& fields);
  void add_field(size_t column, const uint8_t* data, size_t length);
  std::vector<FieldType> infer_types() const;
  const ColumnTypeStats& column_stats(size_t column) const;
  const std::vector<ColumnTypeStats>& all_stats() const;
  size_t num_columns() const;
  size_t num_rows() const;
  void reset();
  void merge(const ColumnTypeInference& other);

  /**
   * Check if all columns have confirmed types based on the confidence threshold.
   *
   * A column's type is "confirmed" when:
   * 1. It has at least min_samples non-empty values
   * 2. A single type dominates with >= confidence_threshold ratio
   *
   * @param min_samples Minimum samples per column before type can be confirmed (default: 100)
   * @return true if all columns have confirmed types, enabling early termination
   */
  bool all_types_confirmed(size_t min_samples = 100) const;

  /**
   * Check if a specific column has a confirmed type.
   *
   * @param column Column index to check
   * @param min_samples Minimum samples required for confirmation
   * @return true if this column's type is confirmed
   */
  bool is_column_type_confirmed(size_t column, size_t min_samples = 100) const;

private:
  std::vector<ColumnTypeStats> stats_;
  TypeDetectionOptions options_;
};

/**
 * TypeHints allows users to override auto-detected types for specific columns.
 *
 * Uses std::unordered_map for O(1) average-case column lookups, which scales
 * well for CSVs with many columns.
 */
struct TypeHints {
  std::unordered_map<std::string, FieldType> column_types;

  void add(const std::string& column, FieldType type) { column_types[column] = type; }

  FieldType get(const std::string& column) const {
    auto it = column_types.find(column);
    return it != column_types.end() ? it->second : FieldType::STRING;
  }

  bool has_hint(const std::string& column) const {
    return column_types.find(column) != column_types.end();
  }
};

} // namespace libvroom

#endif // LIBVROOM_TYPES_H
