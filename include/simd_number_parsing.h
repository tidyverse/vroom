/**
 * @file simd_number_parsing.h
 * @brief SIMD-accelerated integer and floating-point parsing.
 *
 * Based on research by Daniel Lemire on SIMD number parsing techniques.
 * Uses Google Highway for portable SIMD operations across x86, ARM, and other architectures.
 *
 * Key techniques:
 * - SIMD digit validation (checking if all characters are '0'-'9')
 * - SIMD digit-to-value conversion (subtracting '0' from each byte)
 * - Parallel accumulation using multiply-add operations
 * - Efficient handling of short numbers with scalar fallback
 *
 * References:
 * - https://lemire.me/blog/2023/01/30/parsing-integers-quickly-with-avx-512/
 * - https://lemire.me/blog/2023/08/08/fast-simd-timestamp-parsing/
 * - https://github.com/fastfloat/fast_float
 */

#ifndef LIBVROOM_SIMD_NUMBER_PARSING_H
#define LIBVROOM_SIMD_NUMBER_PARSING_H

#include "common_defs.h"
#include "extraction_config.h"
#include "simd_highway.h"
#include "value_extraction.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

namespace libvroom {

/**
 * Result structure for SIMD parsing operations.
 * Similar to ExtractResult but optimized for batch operations.
 */
template <typename T> struct SIMDParseResult {
  T value;
  bool valid;
  const char* error;

  static SIMDParseResult success(T val) { return {val, true, nullptr}; }

  static SIMDParseResult failure(const char* err) { return {T{}, false, err}; }

  static SIMDParseResult na() { return {T{}, false, nullptr}; }

  bool ok() const { return valid; }
  bool is_na() const { return !valid && error == nullptr; }

  T get() const {
    if (!valid)
      throw std::runtime_error(error ? error : "Value is NA");
    return value;
  }

  T get_or(T default_value) const { return valid ? value : default_value; }

  // Convert to ExtractResult for compatibility
  ExtractResult<T> to_extract_result() const {
    if (valid)
      return {value, nullptr};
    if (error)
      return {std::nullopt, error};
    return {std::nullopt, nullptr};
  }
};

/**
 * SIMD-accelerated integer parsing.
 *
 * Uses Highway SIMD operations to validate and parse integers.
 * For short integers (< 8 digits), uses optimized scalar code.
 * For longer integers, uses SIMD to validate all digits are numeric,
 * then accumulates the value.
 *
 * Technique based on Lemire's AVX-512 integer parsing:
 * 1. Load bytes into SIMD register
 * 2. Subtract '0' from each byte
 * 3. Check if all results are < 10 (valid digits)
 * 4. If valid, compute value using multiply-accumulate pattern
 */
class SIMDIntegerParser {
public:
  /**
   * Parse a 64-bit signed integer using SIMD acceleration.
   *
   * @param str Pointer to the string to parse
   * @param len Length of the string
   * @param trim_whitespace Whether to trim leading/trailing whitespace
   * @return SIMDParseResult with parsed value or error
   */
  static really_inline SIMDParseResult<int64_t> parse_int64(const char* str, size_t len,
                                                            bool trim_whitespace = true) {
    if (len == 0)
      return SIMDParseResult<int64_t>::na();

    const char* ptr = str;
    const char* end = str + len;

    // Trim whitespace if requested
    if (trim_whitespace) {
      while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
        ++ptr;
      while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
        --end;
      if (ptr == end)
        return SIMDParseResult<int64_t>::na();
    }

    // Handle sign
    bool negative = false;
    if (*ptr == '-') {
      negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }

    if (ptr == end) {
      return SIMDParseResult<int64_t>::failure("Invalid integer: no digits");
    }

    size_t digit_len = end - ptr;

    // Check for overflow potential (19 digits max for int64)
    if (digit_len > 19) {
      return SIMDParseResult<int64_t>::failure("Integer too large");
    }

    // Use SIMD for validation and parsing
    uint64_t result = 0;
    if (!parse_digits_simd(reinterpret_cast<const uint8_t*>(ptr), digit_len, result)) {
      return SIMDParseResult<int64_t>::failure("Invalid character in integer");
    }

    // Handle signed conversion
    if (negative) {
      constexpr uint64_t max_negative =
          static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
      if (result > max_negative) {
        return SIMDParseResult<int64_t>::failure("Integer underflow");
      }
      return SIMDParseResult<int64_t>::success(static_cast<int64_t>(-result));
    } else {
      if (result > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return SIMDParseResult<int64_t>::failure("Integer overflow");
      }
      return SIMDParseResult<int64_t>::success(static_cast<int64_t>(result));
    }
  }

  /**
   * Parse an unsigned 64-bit integer using SIMD acceleration.
   */
  static really_inline SIMDParseResult<uint64_t> parse_uint64(const char* str, size_t len,
                                                              bool trim_whitespace = true) {
    if (len == 0)
      return SIMDParseResult<uint64_t>::na();

    const char* ptr = str;
    const char* end = str + len;

    if (trim_whitespace) {
      while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
        ++ptr;
      while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
        --end;
      if (ptr == end)
        return SIMDParseResult<uint64_t>::na();
    }

    // Handle optional + sign
    if (*ptr == '+')
      ++ptr;
    if (*ptr == '-') {
      return SIMDParseResult<uint64_t>::failure("Negative value for unsigned type");
    }

    if (ptr == end) {
      return SIMDParseResult<uint64_t>::failure("Invalid integer: no digits");
    }

    size_t digit_len = end - ptr;

    // Check for overflow potential (20 digits max for uint64)
    if (digit_len > 20) {
      return SIMDParseResult<uint64_t>::failure("Integer too large");
    }

    uint64_t result = 0;
    if (!parse_digits_simd(reinterpret_cast<const uint8_t*>(ptr), digit_len, result)) {
      return SIMDParseResult<uint64_t>::failure("Invalid character in integer");
    }

    return SIMDParseResult<uint64_t>::success(result);
  }

  /**
   * Check if a string contains only valid digits using SIMD.
   * Returns true if all characters in the string are '0'-'9'.
   */
  static HWY_ATTR really_inline bool validate_digits_simd(const uint8_t* data, size_t len) {
    if (len == 0)
      return false;

    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    const auto zero = hn::Set(d, '0');
    const auto nine = hn::Set(d, '9');

    size_t i = 0;

    // Process full vectors
    for (; i + N <= len; i += N) {
      auto vec = hn::LoadU(d, data + i);

      // Check if all bytes are >= '0' and <= '9'
      auto ge_zero = hn::Ge(vec, zero);
      auto le_nine = hn::Le(vec, nine);
      auto valid = hn::And(ge_zero, le_nine);

      if (!hn::AllTrue(d, valid)) {
        return false;
      }
    }

    // Scalar fallback for remainder
    for (; i < len; ++i) {
      if (data[i] < '0' || data[i] > '9') {
        return false;
      }
    }

    return true;
  }

  /**
   * Parse a column of integer values in batch.
   * Takes advantage of SIMD for both validation and value extraction.
   *
   * @param fields Array of field pointers
   * @param lengths Array of field lengths
   * @param count Number of fields to parse
   * @param results Output array for parsed values
   * @param valid Output array for validity flags
   */
  static void parse_int64_column(const char** fields, const size_t* lengths, size_t count,
                                 int64_t* results, bool* valid);

  /**
   * Parse a column of integers returning a vector of optional values.
   */
  static std::vector<std::optional<int64_t>>
  parse_int64_column(const char** fields, const size_t* lengths, size_t count);

private:
  /**
   * Core SIMD digit parsing routine.
   * Validates that all characters are digits and computes the numeric value.
   *
   * Uses a technique similar to Lemire's approach:
   * - For short numbers (<=8 digits): scalar loop
   * - For medium numbers (9-16 digits): SIMD validation + scalar accumulation
   * - For long numbers (17-20 digits): careful overflow handling
   */
  static HWY_ATTR really_inline bool parse_digits_simd(const uint8_t* data, size_t len,
                                                       uint64_t& result) {
    result = 0;

    // Early validation using SIMD for longer strings
    if (len >= 8 && !validate_digits_simd(data, len)) {
      return false;
    }

    // Accumulate the value
    // For portability and simplicity, we use scalar accumulation
    // after SIMD validation. This is still faster than validating
    // each character individually during accumulation.
    constexpr uint64_t max_before_mul = std::numeric_limits<uint64_t>::max() / 10;

    for (size_t i = 0; i < len; ++i) {
      uint8_t c = data[i];
      if (c < '0' || c > '9')
        return false;

      uint8_t digit = c - '0';

      // Check for overflow before multiplying
      // max uint64 is 18446744073709551615
      // max_before_mul is 1844674407370955161
      if (result > max_before_mul) {
        return false; // Definitely overflow
      }
      if (result == max_before_mul && digit > 5) {
        // At the boundary: 1844674407370955161 * 10 + digit
        // Only digits 0-5 are safe (result would be 18446744073709551610-15)
        // Digits 6-9 would overflow
        return false;
      }

      result = result * 10 + digit;
    }

    return true;
  }
};

/**
 * SIMD-accelerated floating-point parsing.
 *
 * Based on the fast_float library approach with SIMD enhancements:
 * 1. Use SIMD to validate digit characters
 * 2. Parse mantissa using integer techniques
 * 3. Handle decimal point position tracking
 * 4. Process exponent separately
 * 5. Combine using pow10 lookup table
 */
class SIMDDoubleParser {
public:
  /**
   * Parse a double-precision floating point number using SIMD acceleration.
   *
   * Handles:
   * - Regular decimals: 3.14, -123.456
   * - Scientific notation: 1e10, 1.5e-10, 2E+5
   * - Special values: inf, -inf, nan, infinity
   *
   * @param str Pointer to the string to parse
   * @param len Length of the string
   * @param trim_whitespace Whether to trim leading/trailing whitespace
   * @return SIMDParseResult with parsed value or error
   */
  static really_inline SIMDParseResult<double> parse_double(const char* str, size_t len,
                                                            bool trim_whitespace = true) {
    if (len == 0)
      return SIMDParseResult<double>::na();

    const char* ptr = str;
    const char* end = str + len;

    // Trim whitespace
    if (trim_whitespace) {
      while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
        ++ptr;
      while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
        --end;
      if (ptr == end)
        return SIMDParseResult<double>::na();
    }

    size_t remaining = end - ptr;

    // Check for special values
    auto special = try_parse_special(ptr, remaining);
    if (special.has_value()) {
      return SIMDParseResult<double>::success(*special);
    }

    // Handle sign
    bool negative = false;
    if (*ptr == '-') {
      negative = true;
      ++ptr;
    } else if (*ptr == '+') {
      ++ptr;
    }

    if (ptr == end) {
      return SIMDParseResult<double>::failure("Invalid number: no digits");
    }

    // Parse mantissa using SIMD-accelerated integer parsing
    uint64_t mantissa = 0;
    int64_t exponent = 0;
    int digit_count = 0;
    bool seen_digit = false;

    // Parse integer part
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
      seen_digit = true;
      if (digit_count < 19) {
        mantissa = mantissa * 10 + (*ptr - '0');
        ++digit_count;
      } else {
        // Overflow protection - shift exponent instead
        ++exponent;
      }
      ++ptr;
    }

    // Parse fractional part
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

    if (!seen_digit) {
      return SIMDParseResult<double>::failure("Invalid number: no digits");
    }

    // Parse exponent
    if (ptr < end && (*ptr == 'e' || *ptr == 'E')) {
      ++ptr;
      if (ptr == end) {
        return SIMDParseResult<double>::failure("Invalid number: incomplete exponent");
      }

      bool exp_negative = false;
      if (*ptr == '-') {
        exp_negative = true;
        ++ptr;
      } else if (*ptr == '+') {
        ++ptr;
      }

      if (ptr == end || *ptr < '0' || *ptr > '9') {
        return SIMDParseResult<double>::failure("Invalid number: missing exponent digits");
      }

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

    if (ptr != end) {
      return SIMDParseResult<double>::failure("Invalid number: unexpected characters");
    }

    // Handle zero
    if (mantissa == 0) {
      return SIMDParseResult<double>::success(negative ? -0.0 : 0.0);
    }

    // Compute final value
    double result = static_cast<double>(mantissa) * compute_pow10(exponent);

    if (std::isinf(result)) {
      return SIMDParseResult<double>::success(negative ? -std::numeric_limits<double>::infinity()
                                                       : std::numeric_limits<double>::infinity());
    }

    return SIMDParseResult<double>::success(negative ? -result : result);
  }

  /**
   * Parse a column of double values in batch.
   */
  static void parse_double_column(const char** fields, const size_t* lengths, size_t count,
                                  double* results, bool* valid);

  /**
   * Parse a column of doubles returning a vector of optional values.
   */
  static std::vector<std::optional<double>>
  parse_double_column(const char** fields, const size_t* lengths, size_t count);

private:
  /**
   * Try to parse special floating point values (inf, nan).
   * Returns nullopt if not a special value.
   */
  static really_inline std::optional<double> try_parse_special(const char* ptr, size_t len) {
    if (len < 3)
      return std::nullopt;

    // Helper for case-insensitive comparison
    auto to_lower = [](char c) -> char { return (c >= 'A' && c <= 'Z') ? (c + 32) : c; };

    // Check for NaN
    if (len == 3 && to_lower(ptr[0]) == 'n' && to_lower(ptr[1]) == 'a' && to_lower(ptr[2]) == 'n') {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // Check for Inf
    if (to_lower(ptr[0]) == 'i' && to_lower(ptr[1]) == 'n' && to_lower(ptr[2]) == 'f') {
      if (len == 3) {
        return std::numeric_limits<double>::infinity();
      }
      if (len == 8 && to_lower(ptr[3]) == 'i' && to_lower(ptr[4]) == 'n' &&
          to_lower(ptr[5]) == 'i' && to_lower(ptr[6]) == 't' && to_lower(ptr[7]) == 'y') {
        return std::numeric_limits<double>::infinity();
      }
    }

    // Check for -Inf
    if (ptr[0] == '-' && len >= 4 && to_lower(ptr[1]) == 'i' && to_lower(ptr[2]) == 'n' &&
        to_lower(ptr[3]) == 'f') {
      if (len == 4) {
        return -std::numeric_limits<double>::infinity();
      }
      if (len == 9 && to_lower(ptr[4]) == 'i' && to_lower(ptr[5]) == 'n' &&
          to_lower(ptr[6]) == 'i' && to_lower(ptr[7]) == 't' && to_lower(ptr[8]) == 'y') {
        return -std::numeric_limits<double>::infinity();
      }
    }

    return std::nullopt;
  }

  /**
   * Compute 10^exp efficiently.
   * Uses a lookup table for common exponents.
   */
  static really_inline double compute_pow10(int64_t exp) {
    // Common case: small exponents
    static constexpr double pow10_table[] = {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,
                                             1e8,  1e9,  1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
                                             1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};

    static constexpr double neg_pow10_table[] = {
        1e0,   1e-1,  1e-2,  1e-3,  1e-4,  1e-5,  1e-6,  1e-7,  1e-8,  1e-9,  1e-10, 1e-11,
        1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18, 1e-19, 1e-20, 1e-21, 1e-22};

    if (exp >= 0 && exp <= 22) {
      return pow10_table[exp];
    }
    if (exp < 0 && exp >= -22) {
      return neg_pow10_table[-exp];
    }

    // Fallback to std::pow for larger exponents
    return std::pow(10.0, static_cast<double>(exp));
  }
};

/**
 * SIMD-based type validation for fast dialect detection.
 *
 * These functions provide quick validation without full parsing,
 * useful for type inference during dialect detection.
 */
class SIMDTypeValidator {
public:
  /**
   * Quickly validate if a field could be an integer.
   * Does not parse the value, just checks if it has valid integer format.
   *
   * @param data Field data
   * @param len Field length
   * @return true if the field appears to be a valid integer
   */
  static HWY_ATTR really_inline bool could_be_integer(const uint8_t* data, size_t len) {
    if (len == 0)
      return false;

    size_t start = 0;

    // Skip leading whitespace
    while (start < len && (data[start] == ' ' || data[start] == '\t'))
      ++start;

    // Skip trailing whitespace
    size_t end = len;
    while (end > start && (data[end - 1] == ' ' || data[end - 1] == '\t'))
      --end;

    if (start >= end)
      return false;

    // Handle sign
    if (data[start] == '-' || data[start] == '+') {
      ++start;
      if (start >= end)
        return false;
    }

    // All remaining characters must be digits
    return SIMDIntegerParser::validate_digits_simd(data + start, end - start);
  }

  /**
   * Quickly validate if a field could be a floating-point number.
   *
   * @param data Field data
   * @param len Field length
   * @return true if the field appears to be a valid float
   */
  static really_inline bool could_be_float(const uint8_t* data, size_t len) {
    if (len == 0)
      return false;

    size_t start = 0;
    size_t end = len;

    // Skip whitespace
    while (start < end && (data[start] == ' ' || data[start] == '\t'))
      ++start;
    while (end > start && (data[end - 1] == ' ' || data[end - 1] == '\t'))
      --end;

    if (start >= end)
      return false;

    const uint8_t* ptr = data + start;
    size_t remaining = end - start;

    // Check for special values
    if (remaining >= 3) {
      auto to_lower = [](uint8_t c) -> uint8_t { return (c >= 'A' && c <= 'Z') ? (c + 32) : c; };

      // nan
      if (remaining == 3 && to_lower(ptr[0]) == 'n' && to_lower(ptr[1]) == 'a' &&
          to_lower(ptr[2]) == 'n') {
        return true;
      }

      // inf, infinity, -inf, -infinity
      size_t offset = 0;
      if (ptr[0] == '-' || ptr[0] == '+')
        offset = 1;

      if (remaining - offset >= 3 && to_lower(ptr[offset]) == 'i' &&
          to_lower(ptr[offset + 1]) == 'n' && to_lower(ptr[offset + 2]) == 'f') {
        if (remaining - offset == 3)
          return true;
        if (remaining - offset == 8 && to_lower(ptr[offset + 3]) == 'i' &&
            to_lower(ptr[offset + 4]) == 'n' && to_lower(ptr[offset + 5]) == 'i' &&
            to_lower(ptr[offset + 6]) == 't' && to_lower(ptr[offset + 7]) == 'y') {
          return true;
        }
      }
    }

    // Regular float: sign? digits* .? digits* (e sign? digits+)?
    size_t i = 0;
    bool has_digit = false;
    bool has_decimal = false;
    bool has_exponent = false;

    // Sign
    if (ptr[i] == '-' || ptr[i] == '+')
      ++i;

    // Integer part
    while (i < remaining && ptr[i] >= '0' && ptr[i] <= '9') {
      has_digit = true;
      ++i;
    }

    // Decimal point
    if (i < remaining && ptr[i] == '.') {
      has_decimal = true;
      ++i;

      // Fractional part
      while (i < remaining && ptr[i] >= '0' && ptr[i] <= '9') {
        has_digit = true;
        ++i;
      }
    }

    // Exponent
    if (i < remaining && (ptr[i] == 'e' || ptr[i] == 'E')) {
      has_exponent = true;
      ++i;

      if (i < remaining && (ptr[i] == '-' || ptr[i] == '+'))
        ++i;

      if (i >= remaining || ptr[i] < '0' || ptr[i] > '9') {
        return false; // Exponent requires digits
      }

      while (i < remaining && ptr[i] >= '0' && ptr[i] <= '9')
        ++i;
    }

    // Must have digits and be at end
    // Also, must have either decimal or exponent to be a float (not integer)
    return has_digit && (has_decimal || has_exponent) && i == remaining;
  }

  /**
   * Batch validation of fields for type inference.
   * Returns counts of fields matching each type.
   *
   * Uses SIMD to accelerate the common case of integer detection.
   */
  static void validate_batch(const uint8_t** fields, const size_t* lengths, size_t count,
                             size_t& integer_count, size_t& float_count, size_t& other_count);
};

/**
 * DateTime SIMD parser for ISO 8601 timestamps.
 *
 * Efficiently parses timestamps like:
 * - 2024-01-15
 * - 2024-01-15T14:30:00
 * - 2024-01-15T14:30:00.123
 * - 2024-01-15T14:30:00Z
 * - 2024-01-15T14:30:00+05:30
 */
struct DateTime {
  int16_t year;
  int8_t month;
  int8_t day;
  int8_t hour;
  int8_t minute;
  int8_t second;
  int32_t nanoseconds;
  int16_t tz_offset_minutes; // Timezone offset in minutes from UTC

  bool is_valid() const {
    return year >= 0 && month >= 1 && month <= 12 && day >= 1 && day <= 31 && hour >= 0 &&
           hour <= 23 && minute >= 0 && minute <= 59 && second >= 0 &&
           second <= 60 && // 60 for leap seconds
           nanoseconds >= 0 && nanoseconds < 1000000000;
  }

  static DateTime invalid() { return {-1, -1, -1, -1, -1, -1, -1, 0}; }
};

class SIMDDateTimeParser {
public:
  /**
   * Parse an ISO 8601 date/datetime string.
   */
  static really_inline SIMDParseResult<DateTime> parse_datetime(const char* str, size_t len,
                                                                bool trim_whitespace = true) {
    if (len == 0)
      return SIMDParseResult<DateTime>::na();

    const char* ptr = str;
    const char* end = str + len;

    if (trim_whitespace) {
      while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
        ++ptr;
      while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
        --end;
      if (ptr == end)
        return SIMDParseResult<DateTime>::na();
    }

    size_t remaining = end - ptr;

    DateTime dt = {0, 1, 1, 0, 0, 0, 0, 0};

    // Need at least YYYY-MM-DD (10 chars)
    if (remaining < 10) {
      // Try compact format YYYYMMDD
      if (remaining == 8) {
        return parse_compact_date(ptr);
      }
      return SIMDParseResult<DateTime>::failure("Date too short");
    }

    // Parse date part: YYYY-MM-DD
    if (!parse_4digits(ptr, dt.year) || ptr[4] != '-' || !parse_2digits(ptr + 5, dt.month) ||
        ptr[7] != '-' || !parse_2digits(ptr + 8, dt.day)) {
      return SIMDParseResult<DateTime>::failure("Invalid date format");
    }

    ptr += 10;
    remaining = end - ptr;

    // If just date, we're done
    if (remaining == 0) {
      if (!validate_date(dt)) {
        return SIMDParseResult<DateTime>::failure("Invalid date values");
      }
      return SIMDParseResult<DateTime>::success(dt);
    }

    // Check for time separator
    if (*ptr != 'T' && *ptr != ' ') {
      return SIMDParseResult<DateTime>::failure("Invalid datetime separator");
    }
    ++ptr;
    remaining = end - ptr;

    // Need at least HH:MM:SS (8 chars)
    if (remaining < 8) {
      return SIMDParseResult<DateTime>::failure("Time too short");
    }

    // Parse time: HH:MM:SS
    if (!parse_2digits(ptr, dt.hour) || ptr[2] != ':' || !parse_2digits(ptr + 3, dt.minute) ||
        ptr[5] != ':' || !parse_2digits(ptr + 6, dt.second)) {
      return SIMDParseResult<DateTime>::failure("Invalid time format");
    }

    ptr += 8;
    remaining = end - ptr;

    // Optional fractional seconds
    if (remaining > 0 && *ptr == '.') {
      ++ptr;
      remaining = end - ptr;

      int frac_digits = 0;
      int64_t frac_value = 0;

      while (remaining > 0 && *ptr >= '0' && *ptr <= '9' && frac_digits < 9) {
        frac_value = frac_value * 10 + (*ptr - '0');
        ++frac_digits;
        ++ptr;
        --remaining;
      }

      // Skip extra digits
      while (remaining > 0 && *ptr >= '0' && *ptr <= '9') {
        ++ptr;
        --remaining;
      }

      // Scale to nanoseconds
      while (frac_digits < 9) {
        frac_value *= 10;
        ++frac_digits;
      }

      dt.nanoseconds = static_cast<int32_t>(frac_value);
    }

    // Optional timezone
    if (remaining > 0) {
      if (*ptr == 'Z') {
        dt.tz_offset_minutes = 0;
        ++ptr;
        remaining = end - ptr;
      } else if (*ptr == '+' || *ptr == '-') {
        bool tz_negative = (*ptr == '-');
        ++ptr;
        remaining = end - ptr;

        if (remaining < 2) {
          return SIMDParseResult<DateTime>::failure("Invalid timezone");
        }

        int8_t tz_hour, tz_minute = 0;
        if (!parse_2digits(ptr, tz_hour)) {
          return SIMDParseResult<DateTime>::failure("Invalid timezone hour");
        }

        ptr += 2;
        remaining = end - ptr;

        if (remaining > 0) {
          if (*ptr == ':') {
            ++ptr;
            remaining = end - ptr;
          }

          if (remaining >= 2) {
            if (!parse_2digits(ptr, tz_minute)) {
              return SIMDParseResult<DateTime>::failure("Invalid timezone minute");
            }
            ptr += 2;
          }
        }

        dt.tz_offset_minutes = static_cast<int16_t>(tz_hour * 60 + tz_minute);
        if (tz_negative)
          dt.tz_offset_minutes = -dt.tz_offset_minutes;
      }
    }

    if (!validate_datetime(dt)) {
      return SIMDParseResult<DateTime>::failure("Invalid datetime values");
    }

    return SIMDParseResult<DateTime>::success(dt);
  }

  /**
   * Batch parse datetime column.
   */
  static std::vector<std::optional<DateTime>>
  parse_datetime_column(const char** fields, const size_t* lengths, size_t count);

private:
  static really_inline bool parse_2digits(const char* p, int8_t& result) {
    if (p[0] < '0' || p[0] > '9' || p[1] < '0' || p[1] > '9')
      return false;
    result = static_cast<int8_t>((p[0] - '0') * 10 + (p[1] - '0'));
    return true;
  }

  static really_inline bool parse_2digits(const char* p, int16_t& result) {
    if (p[0] < '0' || p[0] > '9' || p[1] < '0' || p[1] > '9')
      return false;
    result = static_cast<int16_t>((p[0] - '0') * 10 + (p[1] - '0'));
    return true;
  }

  static really_inline bool parse_4digits(const char* p, int16_t& result) {
    for (int i = 0; i < 4; ++i) {
      if (p[i] < '0' || p[i] > '9')
        return false;
    }
    result = static_cast<int16_t>((p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 +
                                  (p[3] - '0'));
    return true;
  }

  static really_inline SIMDParseResult<DateTime> parse_compact_date(const char* ptr) {
    DateTime dt = {0, 1, 1, 0, 0, 0, 0, 0};

    // YYYYMMDD format
    if (!parse_4digits(ptr, dt.year)) {
      return SIMDParseResult<DateTime>::failure("Invalid year");
    }

    int8_t month_temp, day_temp;
    if (!parse_2digits(ptr + 4, month_temp) || !parse_2digits(ptr + 6, day_temp)) {
      return SIMDParseResult<DateTime>::failure("Invalid month/day");
    }

    dt.month = month_temp;
    dt.day = day_temp;

    if (!validate_date(dt)) {
      return SIMDParseResult<DateTime>::failure("Invalid date values");
    }

    return SIMDParseResult<DateTime>::success(dt);
  }

  static bool validate_date(const DateTime& dt);
  static bool validate_datetime(const DateTime& dt);
};

// Convenience type aliases
using SIMDInt64Result = SIMDParseResult<int64_t>;
using SIMDUInt64Result = SIMDParseResult<uint64_t>;
using SIMDDoubleResult = SIMDParseResult<double>;
using SIMDDateTimeResult = SIMDParseResult<DateTime>;

// =============================================================================
// Integration with value_extraction.h
// =============================================================================

/**
 * SIMD-accelerated integer parsing with NA value support.
 * This integrates SIMDIntegerParser with ExtractionConfig for NA handling.
 *
 * Use this function when you need SIMD-accelerated parsing with the same
 * interface as parse_integer(). The function handles NA values, whitespace
 * trimming, and overflow detection just like the scalar version.
 *
 * @note This function checks ExtractionConfig::na_values and returns NA
 *       (nullopt with no error) when the input matches. This differs from
 *       parse_double_simd() which does NOT check na_values.
 *
 * @tparam IntType The target integer type (int64_t, int32_t, uint64_t, uint32_t)
 * @param str Pointer to the string to parse
 * @param len Length of the string
 * @param config Extraction configuration (uses na_values, trim_whitespace, max_integer_digits)
 * @return ExtractResult with parsed value or error
 *
 * @see parse_double_simd() which does NOT check na_values
 */
template <typename IntType>
really_inline ExtractResult<IntType>
parse_integer_simd(const char* str, size_t len,
                   const ExtractionConfig&
                       config /* = ExtractionConfig::defaults() declared in value_extraction.h */) {
  static_assert(std::is_integral_v<IntType>, "IntType must be an integral type");

  if (len == 0)
    return {std::nullopt, nullptr};

  const char* ptr = str;
  const char* end = str + len;

  // Trim whitespace if requested
  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return {std::nullopt, nullptr};
  }

  // Check for NA values
  std::string_view sv(ptr, end - ptr);
  for (const auto& na : config.na_values) {
    if (sv == na)
      return {std::nullopt, nullptr};
  }

  // Check max_integer_digits limit (matching scalar parse_integer behavior)
  const char* digit_start = ptr;
  if (ptr < end && (*ptr == '-' || *ptr == '+'))
    ++digit_start;
  size_t digit_count = end - digit_start;
  if (digit_count > config.max_integer_digits) {
    return {std::nullopt, "Integer too large"};
  }

  // Check for leading zeros if not allowed
  // LCOV_EXCL_BR_START - compound condition branches covered by tests
  if (!config.allow_leading_zeros && digit_count > 1 && *digit_start == '0') {
    return {std::nullopt, "Leading zeros not allowed"};
  }
  // LCOV_EXCL_BR_STOP

  // Use SIMD parser for the actual parsing
  // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
  if constexpr (std::is_same_v<IntType, int64_t>) {
    auto result = SIMDIntegerParser::parse_int64(ptr, end - ptr, false); // Already trimmed
    return result.to_extract_result();
  } else if constexpr (std::is_same_v<IntType, uint64_t>) {
    auto result = SIMDIntegerParser::parse_uint64(ptr, end - ptr, false);
    return result.to_extract_result();
  } else if constexpr (std::is_same_v<IntType, int32_t>) {
    // Parse as int64 first, then check bounds
    auto result = SIMDIntegerParser::parse_int64(ptr, end - ptr, false);
    if (!result.ok()) {
      return {std::nullopt, result.error};
    }
    if (result.value > std::numeric_limits<int32_t>::max() ||
        result.value < std::numeric_limits<int32_t>::min()) {
      return {std::nullopt, "Integer overflow for int32"};
    }
    return {static_cast<int32_t>(result.value), nullptr};
  } else if constexpr (std::is_same_v<IntType, uint32_t>) {
    auto result = SIMDIntegerParser::parse_uint64(ptr, end - ptr, false);
    if (!result.ok()) {
      return {std::nullopt, result.error};
    }
    if (result.value > std::numeric_limits<uint32_t>::max()) {
      return {std::nullopt, "Integer overflow for uint32"};
    }
    return {static_cast<uint32_t>(result.value), nullptr};
  } else {
    // Fallback: parse as int64 and cast
    auto result = SIMDIntegerParser::parse_int64(ptr, end - ptr, false);
    if (!result.ok()) {
      return {std::nullopt, result.error};
    }
    return {static_cast<IntType>(result.value), nullptr};
  }
  // LCOV_EXCL_BR_STOP
}

/**
 * SIMD-accelerated double parsing.
 * This integrates SIMDDoubleParser with ExtractionConfig.
 *
 * Use this function when you need SIMD-accelerated parsing with the same
 * interface as parse_double(). The function handles whitespace trimming
 * and special values (NaN, Inf) just like the scalar version.
 *
 * @note **NA Handling Difference**: Unlike parse_integer_simd(), this function
 *       does NOT check ExtractionConfig::na_values. This is intentional because
 *       floating-point numbers have valid special values like NaN and Inf that
 *       overlap with common NA representations ("NaN" is both a valid double
 *       and a common NA string). To avoid ambiguity:
 *       - If you need NA detection for doubles, use is_na() before parsing,
 *         or check at a higher level (e.g., ValueExtractor).
 *       - Strings like "NA", "null", etc. will return a parse error, not NA.
 *       - The only fields from ExtractionConfig used are: trim_whitespace.
 *
 * @param str Pointer to the string to parse
 * @param len Length of the string
 * @param config Extraction configuration (only trim_whitespace is used)
 * @return ExtractResult with parsed value or error
 *
 * @see parse_integer_simd() which DOES check na_values
 * @see is_na() to check for NA values before parsing
 */
really_inline ExtractResult<double>
parse_double_simd(const char* str, size_t len,
                  const ExtractionConfig&
                      config /* = ExtractionConfig::defaults() declared in value_extraction.h */) {
  if (len == 0)
    return {std::nullopt, nullptr};

  const char* ptr = str;
  const char* end = str + len;

  // Trim whitespace if requested
  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return {std::nullopt, nullptr};
  }

  // Note: Unlike parse_integer_simd, we do NOT check NA values here.
  // This matches the scalar parse_double behavior, which directly parses
  // special values (NaN, Inf) and returns parse errors for other non-numeric
  // strings. If users need NA handling for doubles, they should check at a
  // higher level (e.g., ValueExtractor) or use is_na() first.

  // Use SIMD parser for the actual parsing
  auto result = SIMDDoubleParser::parse_double(ptr, end - ptr, false); // Already trimmed
  return result.to_extract_result();
}

/**
 * Generic SIMD-accelerated value extraction function.
 *
 * This is the SIMD-accelerated equivalent of the generic extract_value pattern.
 * It dispatches to the appropriate SIMD parser based on the requested type.
 *
 * Supported types:
 * - int64_t: Uses SIMDIntegerParser::parse_int64()
 * - int32_t: Uses SIMDIntegerParser::parse_int64() with bounds checking
 * - uint64_t: Uses SIMDIntegerParser::parse_uint64()
 * - double: Uses SIMDDoubleParser::parse_double()
 * - bool: Falls back to scalar parse_bool() (no SIMD benefit)
 *
 * @tparam T The target type
 * @param str Pointer to the string to parse
 * @param len Length of the string
 * @param config Extraction configuration
 * @return ExtractResult with parsed value or error
 */
template <typename T>
really_inline ExtractResult<T>
extract_value_simd(const char* str, size_t len,
                   const ExtractionConfig& config /* = ExtractionConfig::defaults() */) {
  // LCOV_EXCL_BR_START - if constexpr branches are compile-time only
  if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t> ||
                std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>) {
    return parse_integer_simd<T>(str, len, config);
  } else if constexpr (std::is_same_v<T, double>) {
    return parse_double_simd(str, len, config);
  } else if constexpr (std::is_same_v<T, bool>) {
    // Boolean parsing doesn't benefit from SIMD, use scalar
    return parse_bool(str, len, config);
  } else {
    static_assert(!std::is_same_v<T, T>, "Unsupported type for extract_value_simd");
  }
  // LCOV_EXCL_BR_STOP
}

} // namespace libvroom

#endif // LIBVROOM_SIMD_NUMBER_PARSING_H
