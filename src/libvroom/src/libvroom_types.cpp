/**
 * @file libvroom_types.cpp
 * @brief Implementation of type detection functionality.
 *
 * This file contains the non-inline implementations for type detection,
 * moved from the header to reduce compilation time.
 */

#include "libvroom_types.h"

#include <cstring>

namespace libvroom {

// ============================================================================
// TypeDetector implementations
// ============================================================================

FieldType TypeDetector::detect_field(const uint8_t* data, size_t length,
                                     const TypeDetectionOptions& options) {
  if (length == 0)
    return FieldType::EMPTY;

  size_t start = 0;
  size_t end = length;
  if (options.trim_whitespace) {
    while (start < end && is_whitespace(data[start]))
      ++start;
    while (end > start && is_whitespace(data[end - 1]))
      --end;
  }

  if (start >= end)
    return FieldType::EMPTY;

  const uint8_t* field = data + start;
  assert(end >= start && "Invalid range: end must be >= start");
  size_t len = end - start;

  // Check for NA values early (before type-specific checks)
  if (options.detect_na && is_na(field, len))
    return FieldType::NA;

  // Check date first for compact format (8 digits like YYYYMMDD)
  // to avoid misdetecting as integer
  if (is_date(field, len, options))
    return FieldType::DATE;
  if (is_boolean(field, len, options))
    return FieldType::BOOLEAN;
  if (is_integer(field, len, options))
    return FieldType::INTEGER;
  if (is_float(field, len, options))
    return FieldType::FLOAT;

  return FieldType::STRING;
}

FieldType TypeDetector::detect_field(const std::string& value,
                                     const TypeDetectionOptions& options) {
  return detect_field(reinterpret_cast<const uint8_t*>(value.data()), value.size(), options);
}

FieldType TypeDetector::detect_field(const char* value, const TypeDetectionOptions& options) {
  return detect_field(reinterpret_cast<const uint8_t*>(value), std::strlen(value), options);
}

bool TypeDetector::is_boolean(const uint8_t* data, size_t length,
                              const TypeDetectionOptions& options) {
  if (length == 0)
    return false;

  if (options.bool_as_int && length == 1) {
    if (data[0] == '0' || data[0] == '1')
      return true;
  }

  return is_bool_string(data, length);
}

bool TypeDetector::is_integer(const uint8_t* data, size_t length,
                              const TypeDetectionOptions& options) {
  if (length == 0)
    return false;

  size_t i = 0;

  if (data[i] == '+' || data[i] == '-') {
    ++i;
    if (i >= length)
      return false;
  }

  if (!is_digit(data[i]))
    return false;

  if (!options.allow_thousands_sep) {
    // Simple case: just digits
    while (i < length) {
      if (!is_digit(data[i]))
        return false;
      ++i;
    }
    return true;
  }

  // With thousands separator: validate proper grouping
  // First group can be 1-3 digits, subsequent groups must be exactly 3 digits
  size_t first_group_digits = 0;

  // Count first group (1-3 digits before first separator or end)
  while (i < length && is_digit(data[i])) {
    ++first_group_digits;
    ++i;
  }

  if (first_group_digits == 0)
    return false;

  // If no separator found, it's valid
  if (i >= length)
    return true;

  // First group must be 1-3 digits if followed by separator
  if (first_group_digits > 3)
    return false;

  // Process remaining groups (must be exactly 3 digits each)
  while (i < length) {
    if (data[i] != static_cast<uint8_t>(options.thousands_sep)) {
      return false; // Invalid character
    }
    ++i; // Skip separator

    // Must have exactly 3 digits after separator
    if (i + 3 > length)
      return false;
    if (!is_digit(data[i]) || !is_digit(data[i + 1]) || !is_digit(data[i + 2])) {
      return false;
    }
    i += 3;
  }

  return true;
}

bool TypeDetector::is_float(const uint8_t* data, size_t length,
                            const TypeDetectionOptions& options) {
  if (length == 0)
    return false;

  size_t i = 0;
  bool has_digit = false;
  bool has_decimal = false;
  bool has_exponent = false;

  if (data[i] == '+' || data[i] == '-') {
    ++i;
    if (i >= length)
      return false;
  }

  if (is_special_float(data + i, length - i))
    return true;

  while (i < length && is_digit(data[i])) {
    has_digit = true;
    ++i;
  }

  if (i < length && data[i] == static_cast<uint8_t>(options.decimal_point)) {
    has_decimal = true;
    ++i;
    while (i < length && is_digit(data[i])) {
      has_digit = true;
      ++i;
    }
  }

  if (options.allow_exponential && i < length && (data[i] == 'e' || data[i] == 'E')) {
    has_exponent = true;
    ++i;
    if (i < length && (data[i] == '+' || data[i] == '-'))
      ++i;
    if (i >= length || !is_digit(data[i]))
      return false;
    while (i < length && is_digit(data[i]))
      ++i;
  }

  return has_digit && (has_decimal || has_exponent) && i == length;
}

bool TypeDetector::is_date(const uint8_t* data, size_t length,
                           const TypeDetectionOptions& options) {
  if (length < 8)
    return false;

  // ISO format is always checked first (unambiguous)
  if (is_date_iso(data, length))
    return true;

  // Compact format is also unambiguous
  if (is_date_compact(data, length))
    return true;

  // For ISO_ONLY mode, skip US/EU format checking
  if (options.date_format_preference == DateFormatPreference::ISO_ONLY)
    return false;

  // Check US and EU based on preference
  if (options.date_format_preference == DateFormatPreference::EU_FIRST) {
    if (is_date_eu(data, length))
      return true;
    if (is_date_us(data, length))
      return true;
  } else {
    // AUTO and US_FIRST both check US first
    if (is_date_us(data, length))
      return true;
    if (is_date_eu(data, length))
      return true;
  }

  return false;
}

// ============================================================================
// TypeDetector private helper implementations
// ============================================================================

bool TypeDetector::is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

int TypeDetector::days_in_month(int year, int month) {
  static const int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month < 1 || month > 12)
    return 0;
  if (month == 2 && is_leap_year(year))
    return 29;
  return days[month];
}

bool TypeDetector::is_valid_date(int year, int month, int day) {
  if (year < 1000 || year > 9999)
    return false;
  if (month < 1 || month > 12)
    return false;
  if (day < 1 || day > days_in_month(year, month))
    return false;
  return true;
}

bool TypeDetector::is_bool_string(const uint8_t* data, size_t length) {
  switch (length) {
  case 1: {
    uint8_t c = to_lower(data[0]);
    return c == 't' || c == 'f' || c == 'y' || c == 'n';
  }
  case 2: {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    return (c0 == 'n' && c1 == 'o') || (c0 == 'o' && c1 == 'n');
  }
  case 3: {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    uint8_t c2 = to_lower(data[2]);
    return (c0 == 'y' && c1 == 'e' && c2 == 's') || (c0 == 'o' && c1 == 'f' && c2 == 'f');
  }
  case 4: {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    uint8_t c2 = to_lower(data[2]);
    uint8_t c3 = to_lower(data[3]);
    return c0 == 't' && c1 == 'r' && c2 == 'u' && c3 == 'e';
  }
  case 5: {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    uint8_t c2 = to_lower(data[2]);
    uint8_t c3 = to_lower(data[3]);
    uint8_t c4 = to_lower(data[4]);
    return c0 == 'f' && c1 == 'a' && c2 == 'l' && c3 == 's' && c4 == 'e';
  }
  default:
    return false;
  }
}

bool TypeDetector::is_special_float(const uint8_t* data, size_t length) {
  if (length == 3) {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    uint8_t c2 = to_lower(data[2]);
    return (c0 == 'i' && c1 == 'n' && c2 == 'f') || (c0 == 'n' && c1 == 'a' && c2 == 'n');
  }
  if (length == 8) {
    uint8_t buf[8];
    for (size_t i = 0; i < 8; ++i)
      buf[i] = to_lower(data[i]);
    return buf[0] == 'i' && buf[1] == 'n' && buf[2] == 'f' && buf[3] == 'i' && buf[4] == 'n' &&
           buf[5] == 'i' && buf[6] == 't' && buf[7] == 'y';
  }
  return false;
}

bool TypeDetector::is_na(const uint8_t* data, size_t length) {
  // Single character NA representations
  if (length == 1) {
    return data[0] == '-' || data[0] == '.';
  }

  // Two character: "NA"
  if (length == 2) {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    return c0 == 'n' && c1 == 'a';
  }

  // Three character: "N/A"
  if (length == 3) {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c2 = to_lower(data[2]);
    return c0 == 'n' && data[1] == '/' && c2 == 'a';
  }

  // Four characters: "null", "none"
  if (length == 4) {
    uint8_t c0 = to_lower(data[0]);
    uint8_t c1 = to_lower(data[1]);
    uint8_t c2 = to_lower(data[2]);
    uint8_t c3 = to_lower(data[3]);
    // "null"
    if (c0 == 'n' && c1 == 'u' && c2 == 'l' && c3 == 'l')
      return true;
    // "none"
    if (c0 == 'n' && c1 == 'o' && c2 == 'n' && c3 == 'e')
      return true;
  }

  return false;
}

bool TypeDetector::is_date_iso(const uint8_t* data, size_t length) {
  if (length != 10)
    return false;

  char sep = static_cast<char>(data[4]);
  if (sep != '-' && sep != '/')
    return false;
  if (data[7] != static_cast<uint8_t>(sep))
    return false;

  for (int i = 0; i < 4; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 5; i < 7; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 8; i < 10; ++i)
    if (!is_digit(data[i]))
      return false;

  int year =
      (data[0] - '0') * 1000 + (data[1] - '0') * 100 + (data[2] - '0') * 10 + (data[3] - '0');
  int month = (data[5] - '0') * 10 + (data[6] - '0');
  int day = (data[8] - '0') * 10 + (data[9] - '0');

  return is_valid_date(year, month, day);
}

bool TypeDetector::is_date_us(const uint8_t* data, size_t length) {
  if (length != 10)
    return false;

  char sep = static_cast<char>(data[2]);
  if (sep != '-' && sep != '/')
    return false;
  if (data[5] != static_cast<uint8_t>(sep))
    return false;

  for (int i = 0; i < 2; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 3; i < 5; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 6; i < 10; ++i)
    if (!is_digit(data[i]))
      return false;

  int month = (data[0] - '0') * 10 + (data[1] - '0');
  int day = (data[3] - '0') * 10 + (data[4] - '0');
  int year =
      (data[6] - '0') * 1000 + (data[7] - '0') * 100 + (data[8] - '0') * 10 + (data[9] - '0');

  return is_valid_date(year, month, day);
}

bool TypeDetector::is_date_eu(const uint8_t* data, size_t length) {
  if (length != 10)
    return false;

  char sep = static_cast<char>(data[2]);
  if (sep != '-' && sep != '/')
    return false;
  if (data[5] != static_cast<uint8_t>(sep))
    return false;

  for (int i = 0; i < 2; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 3; i < 5; ++i)
    if (!is_digit(data[i]))
      return false;
  for (int i = 6; i < 10; ++i)
    if (!is_digit(data[i]))
      return false;

  int day = (data[0] - '0') * 10 + (data[1] - '0');
  int month = (data[3] - '0') * 10 + (data[4] - '0');
  int year =
      (data[6] - '0') * 1000 + (data[7] - '0') * 100 + (data[8] - '0') * 10 + (data[9] - '0');

  return is_valid_date(year, month, day);
}

bool TypeDetector::is_date_compact(const uint8_t* data, size_t length) {
  if (length != 8)
    return false;

  for (int i = 0; i < 8; ++i)
    if (!is_digit(data[i]))
      return false;

  int year =
      (data[0] - '0') * 1000 + (data[1] - '0') * 100 + (data[2] - '0') * 10 + (data[3] - '0');
  int month = (data[4] - '0') * 10 + (data[5] - '0');
  int day = (data[6] - '0') * 10 + (data[7] - '0');

  return is_valid_date(year, month, day);
}

// ============================================================================
// SIMDTypeDetector implementations
// ============================================================================

void SIMDTypeDetector::detect_batch(const uint8_t** fields, const size_t* lengths, size_t count,
                                    FieldType* results, const TypeDetectionOptions& options) {
  for (size_t i = 0; i < count; ++i) {
    results[i] = TypeDetector::detect_field(fields[i], lengths[i], options);
  }
}

// ============================================================================
// ColumnTypeInference implementations
// ============================================================================

ColumnTypeInference::ColumnTypeInference(size_t num_columns, const TypeDetectionOptions& options)
    : options_(options) {
  if (num_columns > 0) {
    stats_.resize(num_columns);
  }
}

void ColumnTypeInference::set_options(const TypeDetectionOptions& options) {
  options_ = options;
}

void ColumnTypeInference::add_row(const std::vector<std::string>& fields) {
  if (fields.size() > stats_.size()) {
    stats_.resize(fields.size());
  }

  for (size_t i = 0; i < fields.size(); ++i) {
    FieldType type = TypeDetector::detect_field(fields[i], options_);
    stats_[i].add(type);
  }
}

void ColumnTypeInference::add_field(size_t column, const uint8_t* data, size_t length) {
  if (column >= stats_.size()) {
    stats_.resize(column + 1);
  }
  FieldType type = TypeDetector::detect_field(data, length, options_);
  stats_[column].add(type);
}

std::vector<FieldType> ColumnTypeInference::infer_types() const {
  std::vector<FieldType> types(stats_.size());
  for (size_t i = 0; i < stats_.size(); ++i) {
    types[i] = stats_[i].dominant_type(options_.confidence_threshold);
  }
  return types;
}

const ColumnTypeStats& ColumnTypeInference::column_stats(size_t column) const {
  return stats_.at(column);
}

const std::vector<ColumnTypeStats>& ColumnTypeInference::all_stats() const {
  return stats_;
}

size_t ColumnTypeInference::num_columns() const {
  return stats_.size();
}

size_t ColumnTypeInference::num_rows() const {
  if (stats_.empty())
    return 0;
  return stats_[0].total_count;
}

void ColumnTypeInference::reset() {
  for (auto& s : stats_) {
    s = ColumnTypeStats();
  }
}

void ColumnTypeInference::merge(const ColumnTypeInference& other) {
  if (other.stats_.size() > stats_.size()) {
    stats_.resize(other.stats_.size());
  }
  for (size_t i = 0; i < other.stats_.size(); ++i) {
    stats_[i].total_count += other.stats_[i].total_count;
    stats_[i].empty_count += other.stats_[i].empty_count;
    stats_[i].na_count += other.stats_[i].na_count;
    stats_[i].boolean_count += other.stats_[i].boolean_count;
    stats_[i].integer_count += other.stats_[i].integer_count;
    stats_[i].float_count += other.stats_[i].float_count;
    stats_[i].date_count += other.stats_[i].date_count;
    stats_[i].string_count += other.stats_[i].string_count;
  }
}

bool ColumnTypeInference::is_column_type_confirmed(size_t column, size_t min_samples) const {
  if (column >= stats_.size())
    return false;

  const auto& s = stats_[column];
  // Exclude both empty and NA values from the denominator (consistent with dominant_type())
  size_t non_empty = s.total_count - s.empty_count - s.na_count;

  // Need enough samples to be confident
  if (non_empty < min_samples)
    return false;

  // Check if any type meets the confidence threshold
  double threshold = options_.confidence_threshold;

  // Check each type - using the same logic as dominant_type()
  if (static_cast<double>(s.boolean_count) / non_empty >= threshold)
    return true;
  if (static_cast<double>(s.integer_count) / non_empty >= threshold)
    return true;
  if (static_cast<double>(s.float_count + s.integer_count) / non_empty >= threshold)
    return true;
  if (static_cast<double>(s.date_count) / non_empty >= threshold)
    return true;

  // STRING type is the fallback - it's "confirmed" if we have enough samples
  // but no other type dominates. However, for early termination purposes,
  // we should still return true since the type won't change with more data.
  // The only way it could change is if we see a lot more of a specific type.
  return true;
}

bool ColumnTypeInference::all_types_confirmed(size_t min_samples) const {
  if (stats_.empty())
    return false;

  for (size_t i = 0; i < stats_.size(); ++i) {
    if (!is_column_type_confirmed(i, min_samples))
      return false;
  }
  return true;
}

} // namespace libvroom
