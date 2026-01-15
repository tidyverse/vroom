/**
 * @file simd_number_parsing.cpp
 * @brief Implementation of non-performance-critical SIMD number parsing functions.
 *
 * This file contains the implementations of batch parsing and validation functions
 * that are not performance-critical (they loop over the inline parsing functions).
 * The inline parsing functions remain in the header for performance.
 */

#include "simd_number_parsing.h"

namespace libvroom {

// =============================================================================
// SIMDIntegerParser batch parsing implementations
// =============================================================================

void SIMDIntegerParser::parse_int64_column(const char** fields, const size_t* lengths, size_t count,
                                           int64_t* results, bool* valid) {

  for (size_t i = 0; i < count; ++i) {
    auto result = parse_int64(fields[i], lengths[i]);
    results[i] = result.value;
    valid[i] = result.valid;
  }
}

std::vector<std::optional<int64_t>>
SIMDIntegerParser::parse_int64_column(const char** fields, const size_t* lengths, size_t count) {

  std::vector<std::optional<int64_t>> results;
  results.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    auto result = parse_int64(fields[i], lengths[i]);
    if (result.valid) {
      results.push_back(result.value);
    } else {
      results.push_back(std::nullopt);
    }
  }

  return results;
}

// =============================================================================
// SIMDDoubleParser batch parsing implementations
// =============================================================================

void SIMDDoubleParser::parse_double_column(const char** fields, const size_t* lengths, size_t count,
                                           double* results, bool* valid) {

  for (size_t i = 0; i < count; ++i) {
    auto result = parse_double(fields[i], lengths[i]);
    results[i] = result.value;
    valid[i] = result.valid;
  }
}

std::vector<std::optional<double>>
SIMDDoubleParser::parse_double_column(const char** fields, const size_t* lengths, size_t count) {

  std::vector<std::optional<double>> results;
  results.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    auto result = parse_double(fields[i], lengths[i]);
    if (result.valid) {
      results.push_back(result.value);
    } else {
      results.push_back(std::nullopt);
    }
  }

  return results;
}

// =============================================================================
// SIMDTypeValidator batch validation implementation
// =============================================================================

void SIMDTypeValidator::validate_batch(const uint8_t** fields, const size_t* lengths, size_t count,
                                       size_t& integer_count, size_t& float_count,
                                       size_t& other_count) {

  integer_count = 0;
  float_count = 0;
  other_count = 0;

  for (size_t i = 0; i < count; ++i) {
    if (lengths[i] == 0) {
      // Empty field - count as other
      ++other_count;
    } else if (could_be_integer(fields[i], lengths[i])) {
      ++integer_count;
    } else if (could_be_float(fields[i], lengths[i])) {
      ++float_count;
    } else {
      ++other_count;
    }
  }
}

// =============================================================================
// SIMDDateTimeParser batch parsing implementation
// =============================================================================

std::vector<std::optional<DateTime>>
SIMDDateTimeParser::parse_datetime_column(const char** fields, const size_t* lengths,
                                          size_t count) {

  std::vector<std::optional<DateTime>> results;
  results.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    auto result = parse_datetime(fields[i], lengths[i]);
    if (result.valid) {
      results.push_back(result.value);
    } else {
      results.push_back(std::nullopt);
    }
  }

  return results;
}

// =============================================================================
// DateTime validation functions (moved from header)
// =============================================================================

bool SIMDDateTimeParser::validate_date(const DateTime& dt) {
  if (dt.year < 1 || dt.year > 9999)
    return false;
  if (dt.month < 1 || dt.month > 12)
    return false;
  if (dt.day < 1)
    return false;

  // Days per month
  static constexpr int8_t days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  int max_day = days_in_month[dt.month];

  // February leap year check
  if (dt.month == 2) {
    bool leap = (dt.year % 4 == 0 && dt.year % 100 != 0) || (dt.year % 400 == 0);
    if (leap)
      max_day = 29;
  }

  return dt.day <= max_day;
}

bool SIMDDateTimeParser::validate_datetime(const DateTime& dt) {
  if (!validate_date(dt))
    return false;
  if (dt.hour < 0 || dt.hour > 23)
    return false;
  if (dt.minute < 0 || dt.minute > 59)
    return false;
  if (dt.second < 0 || dt.second > 60)
    return false; // Allow 60 for leap seconds
  if (dt.nanoseconds < 0 || dt.nanoseconds >= 1000000000)
    return false;
  if (dt.tz_offset_minutes < -720 || dt.tz_offset_minutes > 840)
    return false; // UTC-12 to UTC+14
  return true;
}

} // namespace libvroom
