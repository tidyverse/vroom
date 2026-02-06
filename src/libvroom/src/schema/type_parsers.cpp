#include "libvroom/vroom.h"

#include <fast_float/fast_float.h>

namespace libvroom {

// Helper function to check if a year is a leap year
static inline bool is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

// Days in each month (non-leap year)
static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// Get days in a specific month, accounting for leap years
static inline int get_days_in_month(int year, int month) {
  if (month == 2 && is_leap_year(year)) {
    return 29;
  }
  return days_in_month[month];
}

// Count leap years from year 1 through year Y-1 (exclusive of Y)
// Uses the Gregorian calendar rule: divisible by 4, except centuries unless divisible by 400
static inline int leap_years_before(int year) {
  if (year <= 1)
    return 0;
  int y = year - 1;
  return y / 4 - y / 100 + y / 400;
}

// Calculate days from 1970-01-01 to Y-01-01 using closed-form formula
// This is O(1) instead of O(|year - 1970|)
static inline int32_t days_from_epoch_to_year(int year) {
  // Number of leap years between 1970 and target year
  // leap_years_before(1970) = 1969/4 - 1969/100 + 1969/400 = 492 - 19 + 4 = 477
  constexpr int LEAP_YEARS_BEFORE_1970 = 477;
  int leap_years_diff = leap_years_before(year) - LEAP_YEARS_BEFORE_1970;
  return static_cast<int32_t>(year - 1970) * 365 + leap_years_diff;
}

// Parse ISO8601 date (YYYY-MM-DD or YYYY/MM/DD) to days since epoch
bool parse_date(std::string_view value, int32_t& days_since_epoch) {
  if (value.size() != 10) {
    return false;
  }

  // Check separator (- or /)
  char sep = value[4];
  if (sep != '-' && sep != '/') {
    return false;
  }
  if (value[7] != sep) {
    return false;
  }

  // Parse year, month, day
  int year = 0, month = 0, day = 0;

  for (int i = 0; i < 4; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    year = year * 10 + (value[i] - '0');
  }
  for (int i = 5; i < 7; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    month = month * 10 + (value[i] - '0');
  }
  for (int i = 8; i < 10; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    day = day * 10 + (value[i] - '0');
  }

  // Validate month and day
  if (month < 1 || month > 12) {
    return false;
  }
  int max_day = get_days_in_month(year, month);
  if (day < 1 || day > max_day) {
    return false;
  }

  // Convert to days since Unix epoch (1970-01-01)
  // Use closed-form formula for O(1) calculation
  int32_t days = days_from_epoch_to_year(year);

  // Add days for complete months
  for (int m = 1; m < month; ++m) {
    days += get_days_in_month(year, m);
  }

  // Add days
  days += day - 1; // -1 because day 1 is the first day

  days_since_epoch = days;
  return true;
}

// Parse timezone offset and return offset in minutes
// Returns true if a timezone was parsed, false otherwise
// offset_minutes is set to 0 for UTC (Z), positive for +HH:MM, negative for -HH:MM
static bool parse_timezone(std::string_view value, size_t start_pos, int& offset_minutes) {
  if (start_pos >= value.size()) {
    offset_minutes = 0;
    return false;
  }

  char first = value[start_pos];

  // UTC indicator
  if (first == 'Z') {
    offset_minutes = 0;
    return true;
  }

  // +HH:MM or -HH:MM or +HHMM or -HHMM
  if (first != '+' && first != '-') {
    return false;
  }

  bool negative = (first == '-');
  size_t pos = start_pos + 1;

  // Need at least 2 digits for hours
  if (pos + 2 > value.size()) {
    return false;
  }

  int tz_hour = 0;
  for (size_t i = 0; i < 2; ++i) {
    if (value[pos + i] < '0' || value[pos + i] > '9')
      return false;
    tz_hour = tz_hour * 10 + (value[pos + i] - '0');
  }
  pos += 2;

  int tz_minute = 0;

  // Check for colon separator or direct minute digits
  if (pos < value.size()) {
    if (value[pos] == ':') {
      pos++;
    }

    // Parse minutes if present (must be exactly 2 digits if present)
    if (pos + 2 <= value.size() && value[pos] >= '0' && value[pos] <= '9' &&
        value[pos + 1] >= '0' && value[pos + 1] <= '9') {
      tz_minute = (value[pos] - '0') * 10 + (value[pos + 1] - '0');
    } else if (pos < value.size() && value[pos] >= '0' && value[pos] <= '9') {
      // Single digit after colon is invalid (e.g., "+05:3")
      return false;
    }
  }

  if (tz_hour > 14 || tz_minute > 59) {
    return false;
  }

  offset_minutes = tz_hour * 60 + tz_minute;
  if (negative) {
    offset_minutes = -offset_minutes;
  }

  return true;
}

// Parse ISO8601 timestamp to microseconds since epoch (UTC)
// Supports formats:
//   YYYY-MM-DDTHH:MM:SS
//   YYYY-MM-DD HH:MM:SS
//   YYYY-MM-DDTHH:MM:SS.ffffff
//   YYYY-MM-DDTHH:MM:SSZ
//   YYYY-MM-DDTHH:MM:SS+HH:MM
//   YYYY-MM-DDTHH:MM:SS-HH:MM
//   YYYY-MM-DDTHH:MM:SS.ffffffZ
//   YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM
bool parse_timestamp(std::string_view value, int64_t& micros_since_epoch) {
  if (value.size() < 19) {
    return false;
  }

  // Parse date part (allow both - and / separators)
  int32_t days;
  if (!parse_date(value.substr(0, 10), days)) {
    return false;
  }

  // Check separator (T or space)
  if (value[10] != 'T' && value[10] != ' ') {
    return false;
  }

  // Parse time part (HH:MM:SS)
  if (value[13] != ':' || value[16] != ':') {
    return false;
  }

  int hour = 0, minute = 0, second = 0;
  for (int i = 11; i < 13; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    hour = hour * 10 + (value[i] - '0');
  }
  for (int i = 14; i < 16; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    minute = minute * 10 + (value[i] - '0');
  }
  for (int i = 17; i < 19; ++i) {
    if (value[i] < '0' || value[i] > '9')
      return false;
    second = second * 10 + (value[i] - '0');
  }

  if (hour > 23 || minute > 59 || second > 59) {
    return false;
  }

  // Parse fractional seconds and timezone
  int64_t micros = 0;
  size_t tz_start = 19;

  if (value.size() > 19 && value[19] == '.') {
    // Parse up to 6 digits of fractional seconds
    size_t frac_start = 20;
    size_t frac_end = value.size();

    // Find end of fractional part (start of timezone or end of string)
    for (size_t i = frac_start; i < value.size(); ++i) {
      if (value[i] == 'Z' || value[i] == '+' || value[i] == '-') {
        frac_end = i;
        tz_start = i;
        break;
      }
    }

    int64_t frac = 0;
    int digits = 0;
    for (size_t i = frac_start; i < frac_end && digits < 6; ++i) {
      if (value[i] >= '0' && value[i] <= '9') {
        frac = frac * 10 + (value[i] - '0');
        ++digits;
      } else {
        // Invalid character in fractional part
        return false;
      }
    }

    // Pad to microseconds
    while (digits < 6) {
      frac *= 10;
      ++digits;
    }

    micros = frac;
    if (tz_start == 19) {
      tz_start = frac_end;
    }
  }

  // Parse timezone offset
  int tz_offset_minutes = 0;
  if (tz_start < value.size()) {
    if (!parse_timezone(value, tz_start, tz_offset_minutes)) {
      return false;
    }
  }

  // Convert to microseconds since epoch (UTC)
  micros_since_epoch = static_cast<int64_t>(days) * 24LL * 60LL * 60LL * 1000000LL +
                       static_cast<int64_t>(hour) * 60LL * 60LL * 1000000LL +
                       static_cast<int64_t>(minute) * 60LL * 1000000LL +
                       static_cast<int64_t>(second) * 1000000LL + micros;

  // Apply timezone offset (subtract to convert to UTC)
  micros_since_epoch -= static_cast<int64_t>(tz_offset_minutes) * 60LL * 1000000LL;

  return true;
}

} // namespace libvroom
