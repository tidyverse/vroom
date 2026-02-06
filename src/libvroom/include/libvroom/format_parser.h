#pragma once

#include "format_locale.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace libvroom {

// Parsed datetime components — no R dependencies.
struct ParsedDateTime {
  int year = -1, month = 1, day = 1;
  int hour = 0, minute = 0, second = 0;
  double partial_second = 0.0;
  int sign = 1;   // For %h (unrestricted hours): +1 or -1
  int am_pm = -1;  // -1 = unset, 0 = AM, 1 = PM

  int tz_offset_hours = 0;
  int tz_offset_minutes = 0;
  std::string tz_name;

  // Check if the date components represent a valid calendar date
  bool is_valid_date() const;

  // Convert parsed fields to days since Unix epoch (1970-01-01)
  int32_t to_days_since_epoch() const;

  // Convert parsed fields to microseconds since Unix epoch (UTC)
  int64_t to_micros_since_epoch() const;

  // Convert parsed fields to seconds since midnight (for TIME type)
  double to_seconds_since_midnight() const;

  // Compute the effective hour after applying AM/PM
  int effective_hour() const;
};

// Format-string-based datetime parser.
// Thread-safe: parse() is const, all state is in ParsedDateTime.
class FormatParser {
public:
  explicit FormatParser(const FormatLocale& locale = FormatLocale::english());

  // Parse a value using a strptime-style format string.
  // Returns true on success, filling `out`.
  bool parse(std::string_view value, const std::string& format, ParsedDateTime& out) const;

  // Parse ISO8601 datetime (flexible: handles compact time like HHMM, HH:MM without seconds)
  bool parse_iso8601(std::string_view value, ParsedDateTime& out) const;

  // Parse ISO8601 date only (YYYY-MM-DD or YYYY/MM/DD)
  bool parse_iso8601_date(std::string_view value, ParsedDateTime& out) const;

  // Parse auto-detect time (HH:MM:SS with optional AM/PM)
  bool parse_auto_time(std::string_view value, ParsedDateTime& out) const;

  const FormatLocale& locale() const { return locale_; }

private:
  FormatLocale locale_;

  // Internal parsing helpers — operate on a cursor (pos) within value
  static bool consume_integer(std::string_view value, size_t& pos, int max_digits, int& out, bool exact = true);
  static bool consume_integer_with_space(std::string_view value, size_t& pos, int max_digits, int& out);
  static bool consume_double(std::string_view value, size_t& pos, double& out);
  static bool consume_seconds(std::string_view value, size_t& pos, int& sec, double& psec);
  static bool consume_hours(std::string_view value, size_t& pos, int& hour, int& sign);
  static bool consume_char(std::string_view value, size_t& pos, char expected);
  static bool consume_whitespace(std::string_view value, size_t& pos);
  static bool consume_non_digit(std::string_view value, size_t& pos);
  static bool consume_non_digits(std::string_view value, size_t& pos);
  static bool consume_tz_offset(std::string_view value, size_t& pos, int& hours, int& minutes);
  static bool consume_tz_name(std::string_view value, size_t& pos, std::string& name);
  bool consume_string_match(std::string_view value, size_t& pos,
                            const std::vector<std::string>& haystack, int& out_1indexed) const;
};

} // namespace libvroom
