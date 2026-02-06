#include "libvroom/format_parser.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fast_float/fast_float.h>

namespace libvroom {

// ============================================================================
// Date arithmetic helpers (reuse logic from type_parsers.cpp)
// ============================================================================

static inline bool is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static const int days_in_month_table[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static inline int get_days_in_month(int year, int month) {
  if (month == 2 && is_leap_year(year))
    return 29;
  return days_in_month_table[month];
}

static inline int leap_years_before(int year) {
  if (year <= 1)
    return 0;
  int y = year - 1;
  return y / 4 - y / 100 + y / 400;
}

static inline int32_t days_from_epoch_to_year(int year) {
  constexpr int LEAP_YEARS_BEFORE_1970 = 477;
  int leap_years_diff = leap_years_before(year) - LEAP_YEARS_BEFORE_1970;
  return static_cast<int32_t>(year - 1970) * 365 + leap_years_diff;
}

// ============================================================================
// ParsedDateTime
// ============================================================================

int ParsedDateTime::effective_hour() const {
  int h = hour;
  if (am_pm >= 0) {
    // am_pm: 0 = AM, 1 = PM
    if (h == 12) {
      h = (am_pm == 0) ? 0 : 12;
    } else if (am_pm == 1) {
      h += 12;
    }
  }
  return h;
}

bool ParsedDateTime::is_valid_date() const {
  if (year < 0 || month < 1 || month > 12)
    return false;
  int max_day = get_days_in_month(year, month);
  return day >= 1 && day <= max_day;
}

int32_t ParsedDateTime::to_days_since_epoch() const {
  if (!is_valid_date())
    return 0;

  int32_t days = days_from_epoch_to_year(year);
  for (int m = 1; m < month; ++m) {
    days += get_days_in_month(year, m);
  }
  days += day - 1;
  return days;
}

int64_t ParsedDateTime::to_micros_since_epoch() const {
  int32_t days = to_days_since_epoch();
  int h = effective_hour();

  int64_t micros = static_cast<int64_t>(days) * 24LL * 60LL * 60LL * 1000000LL +
                   static_cast<int64_t>(h) * 60LL * 60LL * 1000000LL +
                   static_cast<int64_t>(minute) * 60LL * 1000000LL +
                   static_cast<int64_t>(second) * 1000000LL +
                   static_cast<int64_t>(partial_second * 1000000.0);

  // Apply timezone offset (subtract to convert to UTC)
  int total_tz_minutes = tz_offset_hours * 60 + tz_offset_minutes;
  micros -= static_cast<int64_t>(total_tz_minutes) * 60LL * 1000000LL;

  return micros;
}

double ParsedDateTime::to_seconds_since_midnight() const {
  int h = effective_hour();
  return sign * (partial_second + second + minute * 60.0 + h * 3600.0);
}

// ============================================================================
// FormatParser
// ============================================================================

FormatParser::FormatParser(const FormatLocale& locale) : locale_(locale) {}

// ============================================================================
// Consumption helpers
// ============================================================================

bool FormatParser::consume_integer(std::string_view value, size_t& pos, int max_digits,
                                   int& out, bool exact) {
  if (pos >= value.size() || value[pos] == '-' || value[pos] == '+')
    return false;

  size_t start = pos;
  size_t end = std::min(pos + static_cast<size_t>(max_digits), value.size());
  int result = 0;

  while (pos < end) {
    unsigned char c = static_cast<unsigned char>(value[pos]) - '0';
    if (c > 9)
      break;
    result = result * 10 + c;
    pos++;
  }

  if (pos == start)
    return false;
  if (exact && static_cast<int>(pos - start) != max_digits)
    return false;

  out = result;
  return true;
}

bool FormatParser::consume_integer_with_space(std::string_view value, size_t& pos,
                                              int max_digits, int& out) {
  int n = max_digits;
  if (pos < value.size() && value[pos] == ' ') {
    pos++;
    n--;
  }
  return consume_integer(value, pos, n, out, true);
}

bool FormatParser::consume_double(std::string_view value, size_t& pos, double& out) {
  if (pos >= value.size() || value[pos] == '-' || value[pos] == '+')
    return false;

  const char* start = value.data() + pos;
  const char* end_ptr = value.data() + value.size();
  double result;
  auto [ptr, ec] = fast_float::from_chars(start, end_ptr, result);
  if (ec != std::errc() || ptr == start)
    return false;

  pos += static_cast<size_t>(ptr - start);
  out = result;
  return true;
}

bool FormatParser::consume_seconds(std::string_view value, size_t& pos,
                                   int& sec, double& psec) {
  double full_sec;
  if (!consume_double(value, pos, full_sec))
    return false;
  sec = static_cast<int>(full_sec);
  psec = full_sec - sec;
  return true;
}

bool FormatParser::consume_hours(std::string_view value, size_t& pos,
                                 int& hour, int& sign) {
  if (pos >= value.size())
    return false;

  sign = 1;
  if (value[pos] == '-') {
    sign = -1;
    pos++;
  } else if (value[pos] == '+') {
    pos++;
  }

  return consume_integer(value, pos, 10, hour, false);
}

bool FormatParser::consume_char(std::string_view value, size_t& pos, char expected) {
  if (pos >= value.size() || value[pos] != expected)
    return false;
  pos++;
  return true;
}

bool FormatParser::consume_whitespace(std::string_view value, size_t& pos) {
  while (pos < value.size() && std::isspace(static_cast<unsigned char>(value[pos])))
    pos++;
  return true;
}

bool FormatParser::consume_non_digit(std::string_view value, size_t& pos) {
  if (pos >= value.size() || std::isdigit(static_cast<unsigned char>(value[pos])))
    return false;
  pos++;
  return true;
}

bool FormatParser::consume_non_digits(std::string_view value, size_t& pos) {
  if (!consume_non_digit(value, pos))
    return false;
  while (pos < value.size() && !std::isdigit(static_cast<unsigned char>(value[pos])))
    pos++;
  return true;
}

bool FormatParser::consume_tz_offset(std::string_view value, size_t& pos,
                                     int& hours, int& minutes) {
  if (pos >= value.size())
    return false;

  if (value[pos] == 'Z') {
    pos++;
    hours = 0;
    minutes = 0;
    return true;
  }

  int mult = 1;
  if (value[pos] == '+' || value[pos] == '-') {
    mult = (value[pos] == '-') ? -1 : 1;
    pos++;
  }

  if (!consume_integer(value, pos, 2, hours, true))
    return false;

  // Optional colon and minutes
  if (pos < value.size() && value[pos] == ':')
    pos++;
  minutes = 0;
  consume_integer(value, pos, 2, minutes, true);

  hours *= mult;
  minutes *= mult;
  return true;
}

bool FormatParser::consume_tz_name(std::string_view value, size_t& pos, std::string& name) {
  size_t start = pos;
  while (pos < value.size() && !std::isspace(static_cast<unsigned char>(value[pos])))
    pos++;
  if (pos == start)
    return false;
  name.assign(value.data() + start, pos - start);
  return true;
}

bool FormatParser::consume_string_match(std::string_view value, size_t& pos,
                                        const std::vector<std::string>& haystack,
                                        int& out_1indexed) const {
  // Case-insensitive prefix match against haystack entries
  std::string_view remaining = value.substr(pos);

  for (size_t i = 0; i < haystack.size(); ++i) {
    const std::string& hay = haystack[i];
    if (remaining.size() < hay.size())
      continue;

    bool match = true;
    for (size_t j = 0; j < hay.size(); ++j) {
      if (std::tolower(static_cast<unsigned char>(remaining[j])) !=
          std::tolower(static_cast<unsigned char>(hay[j]))) {
        match = false;
        break;
      }
    }
    if (match) {
      out_1indexed = static_cast<int>(i) + 1;
      pos += hay.size();
      return true;
    }
  }
  return false;
}

// ============================================================================
// Main parse() — format string interpreter
// ============================================================================

bool FormatParser::parse(std::string_view value, const std::string& format,
                         ParsedDateTime& out) const {
  out = ParsedDateTime{}; // reset
  size_t pos = 0;

  consume_whitespace(value, pos); // always consume leading whitespace

  for (size_t fi = 0; fi < format.size(); ++fi) {
    char fc = format[fi];

    // Whitespace in format matches 0 or more whitespace in value
    if (std::isspace(static_cast<unsigned char>(fc))) {
      consume_whitespace(value, pos);
      continue;
    }

    // Non-% characters must match exactly
    if (fc != '%') {
      if (!consume_char(value, pos, fc))
        return false;
      continue;
    }

    // % specifier
    if (fi + 1 >= format.size())
      return false; // trailing %
    fi++;
    fc = format[fi];

    switch (fc) {
    case 'Y': // 4-digit year
      if (!consume_integer(value, pos, 4, out.year, true))
        return false;
      break;

    case 'y': // 2-digit year
      if (!consume_integer(value, pos, 2, out.year, true))
        return false;
      out.year += (out.year < 69) ? 2000 : 1900;
      break;

    case 'm': // month (01-12, leading zero optional)
      if (!consume_integer(value, pos, 2, out.month, false))
        return false;
      break;

    case 'b': // abbreviated month name
      if (!consume_string_match(value, pos, locale_.month_abbr, out.month))
        return false;
      break;

    case 'B': // full month name
      if (!consume_string_match(value, pos, locale_.month_full, out.month))
        return false;
      break;

    case 'd': // day (01-31, leading zero optional)
      if (!consume_integer(value, pos, 2, out.day, false))
        return false;
      break;

    case 'a': // abbreviated day of week (consumed but value ignored for date calc)
    {
      int dummy;
      if (!consume_string_match(value, pos, locale_.day_abbr, dummy))
        return false;
      break;
    }

    case 'e': // day with optional leading space
      if (!consume_integer_with_space(value, pos, 2, out.day))
        return false;
      break;

    case 'h': // hour, unrestricted (for durations)
      if (!consume_hours(value, pos, out.hour, out.sign))
        return false;
      break;

    case 'H': // hour 0-23
      if (!consume_integer(value, pos, 2, out.hour, false))
        return false;
      if (out.hour < 0 || out.hour > 23)
        return false;
      break;

    case 'I': // hour 1-12
      if (!consume_integer(value, pos, 2, out.hour, false))
        return false;
      if (out.hour < 1 || out.hour > 12)
        return false;
      out.hour %= 12; // Convert 12 -> 0 for AM/PM handling
      break;

    case 'M': // minute
      if (!consume_integer(value, pos, 2, out.minute, true))
        return false;
      break;

    case 'S': // seconds (integer)
    {
      double psec_ignored = 0;
      if (!consume_seconds(value, pos, out.second, psec_ignored))
        return false;
      break;
    }

    case 'O': // %OS — seconds with optional fractional part
      if (fi + 1 >= format.size() || format[fi + 1] != 'S')
        return false;
      fi++; // consume the 'S'
      if (!consume_seconds(value, pos, out.second, out.partial_second))
        return false;
      break;

    case 'p': // AM/PM
      if (!consume_string_match(value, pos, locale_.am_pm, out.am_pm))
        return false;
      out.am_pm--; // Convert from 1-indexed to 0-indexed (0=AM, 1=PM)
      break;

    case 'z': // timezone offset
      out.tz_name = "UTC";
      if (!consume_tz_offset(value, pos, out.tz_offset_hours, out.tz_offset_minutes))
        return false;
      break;

    case 'Z': // timezone name
      if (!consume_tz_name(value, pos, out.tz_name))
        return false;
      break;

    case 's': // unix timestamp (epoch seconds)
    {
      // Parse as double for fractional seconds support
      double epoch_secs;
      const char* start = value.data() + pos;
      const char* end_ptr = value.data() + value.size();

      // Handle optional sign
      int epoch_sign = 1;
      if (pos < value.size() && (value[pos] == '-' || value[pos] == '+')) {
        if (value[pos] == '-')
          epoch_sign = -1;
        start++;
        pos++;
      }

      auto [ptr, ec] = fast_float::from_chars(start, end_ptr, epoch_secs);
      if (ec != std::errc() || ptr == start)
        return false;
      pos += static_cast<size_t>(ptr - start);
      epoch_secs *= epoch_sign;

      // Convert epoch seconds to date/time components
      // We store as year=1970, and encode the raw seconds as special
      // Actually, we need to decompose into components
      int64_t total_secs = static_cast<int64_t>(epoch_secs);
      double frac = epoch_secs - total_secs;

      // Decompose total_secs into days + time-of-day
      int64_t day_secs = 24LL * 60LL * 60LL;
      int64_t days = total_secs / day_secs;
      int64_t remaining = total_secs % day_secs;
      if (remaining < 0) {
        days--;
        remaining += day_secs;
      }

      out.hour = static_cast<int>(remaining / 3600);
      remaining %= 3600;
      out.minute = static_cast<int>(remaining / 60);
      out.second = static_cast<int>(remaining % 60);
      out.partial_second = frac;

      // Convert days since epoch to Y-M-D
      // Simple algorithm: find year, then month
      int32_t d = static_cast<int32_t>(days);
      // Approximate year
      int year = 1970 + static_cast<int>(d / 365);
      int32_t year_start = days_from_epoch_to_year(year);
      while (year_start > d) {
        year--;
        year_start = days_from_epoch_to_year(year);
      }
      while (days_from_epoch_to_year(year + 1) <= d) {
        year++;
        year_start = days_from_epoch_to_year(year);
      }
      out.year = year;
      int remaining_days = d - year_start;

      int month = 1;
      while (month <= 12) {
        int dim = get_days_in_month(year, month);
        if (remaining_days < dim)
          break;
        remaining_days -= dim;
        month++;
      }
      out.month = month;
      out.day = remaining_days + 1;
      out.tz_name = "UTC";
      break;
    }

    // Extensions (readr-specific)
    case '.': // require exactly one non-digit
      if (!consume_non_digit(value, pos))
        return false;
      break;

    case '+': // require one or more non-digits
      if (!consume_non_digits(value, pos))
        return false;
      break;

    case '*': // consume zero or more non-digits
      consume_non_digits(value, pos);
      break;

    case 'A': // %AD (auto date) or %AT (auto time)
      if (fi + 1 >= format.size())
        return false;
      fi++;
      if (format[fi] == 'D') {
        if (!parse_iso8601_date(value.substr(pos), out))
          return false;
        // Advance pos past the date (YYYY-MM-DD = 10 chars, YYYY/MM/DD = 10)
        // Find where we are by re-scanning
        size_t date_start = pos;
        // Skip 4 digits
        pos += 4;
        if (pos < value.size() && (value[pos] == '-' || value[pos] == '/'))
          pos++;
        pos += 2;
        if (pos < value.size() && (value[pos] == '-' || value[pos] == '/'))
          pos++;
        pos += 2;
      } else if (format[fi] == 'T') {
        if (!parse_auto_time(value.substr(pos), out))
          return false;
        // Advance pos past the consumed time
        // Re-scan: HH:MM[:SS[.sss]] [AM/PM]
        size_t time_start = pos;
        // skip digits for hour
        while (pos < value.size() && std::isdigit(static_cast<unsigned char>(value[pos])))
          pos++;
        if (pos < value.size() && value[pos] == ':')
          pos++;
        while (pos < value.size() && std::isdigit(static_cast<unsigned char>(value[pos])))
          pos++;
        if (pos < value.size() && value[pos] == ':') {
          pos++;
          // seconds (possibly decimal)
          while (pos < value.size() && (std::isdigit(static_cast<unsigned char>(value[pos])) || value[pos] == '.'))
            pos++;
        }
        // Optional AM/PM
        consume_whitespace(value, pos);
        if (pos + 1 < value.size()) {
          char c1 = std::toupper(static_cast<unsigned char>(value[pos]));
          char c2 = std::toupper(static_cast<unsigned char>(value[pos + 1]));
          if ((c1 == 'A' || c1 == 'P') && c2 == 'M')
            pos += 2;
        }
        consume_whitespace(value, pos);
      } else {
        return false;
      }
      break;

    // Compound formats
    case 'D': // %m/%d/%y
    {
      ParsedDateTime sub;
      std::string sub_fmt = "%m/%d/%y";
      if (!parse(value.substr(pos), sub_fmt, sub))
        return false;
      out.month = sub.month;
      out.day = sub.day;
      out.year = sub.year;
      // Advance pos (MM/DD/YY = 8 chars)
      // Actually need to figure out how many chars were consumed
      size_t sub_pos = 0;
      // Re-parse to find consumed length
      ParsedDateTime dummy;
      // Just advance past the expected chars
      for (size_t k = 0; k < 8 && pos < value.size(); k++)
        pos++;
      break;
    }

    case 'F': // %Y-%m-%d
    {
      if (!consume_integer(value, pos, 4, out.year, true))
        return false;
      if (!consume_char(value, pos, '-'))
        return false;
      if (!consume_integer(value, pos, 2, out.month, true))
        return false;
      if (!consume_char(value, pos, '-'))
        return false;
      if (!consume_integer(value, pos, 2, out.day, true))
        return false;
      break;
    }

    case 'R': // %H:%M
      if (!consume_integer(value, pos, 2, out.hour, false))
        return false;
      if (!consume_char(value, pos, ':'))
        return false;
      if (!consume_integer(value, pos, 2, out.minute, true))
        return false;
      break;

    case 'X': // %H:%M:%S
    case 'T': // %H:%M:%S
      if (!consume_integer(value, pos, 2, out.hour, false))
        return false;
      if (!consume_char(value, pos, ':'))
        return false;
      if (!consume_integer(value, pos, 2, out.minute, true))
        return false;
      if (!consume_char(value, pos, ':'))
        return false;
      {
        double psec_ignored = 0;
        if (!consume_seconds(value, pos, out.second, psec_ignored))
          return false;
      }
      break;

    case 'x': // %y/%m/%d
      if (!consume_integer(value, pos, 2, out.year, true))
        return false;
      out.year += (out.year < 69) ? 2000 : 1900;
      if (!consume_char(value, pos, '/'))
        return false;
      if (!consume_integer(value, pos, 2, out.month, true))
        return false;
      if (!consume_char(value, pos, '/'))
        return false;
      if (!consume_integer(value, pos, 2, out.day, true))
        return false;
      break;

    default:
      return false; // Unsupported specifier
    }
  }

  consume_whitespace(value, pos); // always consume trailing whitespace

  return pos == value.size(); // must consume entire input
}

// ============================================================================
// ISO8601 parsing
// ============================================================================

bool FormatParser::parse_iso8601_date(std::string_view value, ParsedDateTime& out) const {
  if (value.size() < 10)
    return false;

  // Parse YYYY
  if (!std::isdigit(static_cast<unsigned char>(value[0])))
    return false;
  out.year = (value[0] - '0') * 1000 + (value[1] - '0') * 100 +
             (value[2] - '0') * 10 + (value[3] - '0');

  // Check separator
  char sep = value[4];
  bool compact = (sep != '-' && sep != '/');
  if (compact)
    return false; // ISO8601 date requires separator for this parser

  if (value[7] != sep)
    return false;

  // Parse MM
  out.month = (value[5] - '0') * 10 + (value[6] - '0');
  // Parse DD
  out.day = (value[8] - '0') * 10 + (value[9] - '0');

  return true;
}

bool FormatParser::parse_iso8601(std::string_view value, ParsedDateTime& out) const {
  out = ParsedDateTime{};

  if (value.size() < 10)
    return false;

  // Parse date part
  if (!parse_iso8601_date(value, out))
    return false;

  if (value.size() == 10)
    return true; // Date only

  // Separator: T or space
  if (value[10] != 'T' && value[10] != ' ')
    return false;

  // Parse time: flexible — handles HH, HHMM, HH:MM, HHMMSS, HH:MM:SS, HH:MM:SS.sss
  size_t pos = 11;

  // Hours (required)
  if (pos + 2 > value.size())
    return false;
  out.hour = (value[pos] - '0') * 10 + (value[pos + 1] - '0');
  pos += 2;

  if (pos >= value.size())
    return true; // Just HH

  // Check for timezone right after hours (e.g., "2015-02-01T01Z")
  if (value[pos] == 'Z' || value[pos] == '+' || value[pos] == '-') {
    out.tz_name = "UTC";
    return consume_tz_offset(value, pos, out.tz_offset_hours, out.tz_offset_minutes) &&
           pos == value.size();
  }

  bool has_colon = (value[pos] == ':');
  if (has_colon)
    pos++;

  // Minutes
  if (pos + 2 > value.size())
    return false;
  if (!std::isdigit(static_cast<unsigned char>(value[pos])))
    return false;
  out.minute = (value[pos] - '0') * 10 + (value[pos + 1] - '0');
  pos += 2;

  if (pos >= value.size())
    return true; // HH:MM or HHMM

  // Check for timezone after minutes
  if (value[pos] == 'Z' || value[pos] == '+' || value[pos] == '-') {
    out.tz_name = "UTC";
    return consume_tz_offset(value, pos, out.tz_offset_hours, out.tz_offset_minutes) &&
           pos == value.size();
  }

  // Seconds
  if (has_colon) {
    if (value[pos] != ':')
      return false;
    pos++;
  }

  if (pos + 2 > value.size())
    return false;
  out.second = (value[pos] - '0') * 10 + (value[pos + 1] - '0');
  pos += 2;

  // Fractional seconds
  if (pos < value.size() && value[pos] == '.') {
    pos++;
    int64_t frac = 0;
    int digits = 0;
    while (pos < value.size() && std::isdigit(static_cast<unsigned char>(value[pos])) && digits < 6) {
      frac = frac * 10 + (value[pos] - '0');
      digits++;
      pos++;
    }
    // Skip remaining fractional digits beyond 6
    while (pos < value.size() && std::isdigit(static_cast<unsigned char>(value[pos])))
      pos++;
    // Pad to microseconds
    while (digits < 6) {
      frac *= 10;
      digits++;
    }
    out.partial_second = static_cast<double>(frac) / 1000000.0;
  }

  if (pos >= value.size())
    return true;

  // Timezone
  if (value[pos] == 'Z' || value[pos] == '+' || value[pos] == '-') {
    out.tz_name = "UTC";
    if (!consume_tz_offset(value, pos, out.tz_offset_hours, out.tz_offset_minutes))
      return false;
  }

  return pos == value.size();
}

bool FormatParser::parse_auto_time(std::string_view value, ParsedDateTime& out) const {
  out = ParsedDateTime{};
  size_t pos = 0;

  // HH:MM[:SS[.sss]] [AM/PM]
  if (!consume_integer(value, pos, 2, out.hour, false))
    return false;
  if (!consume_char(value, pos, ':'))
    return false;
  if (!consume_integer(value, pos, 2, out.minute, true))
    return false;

  // Optional seconds
  if (pos < value.size() && value[pos] == ':') {
    pos++;
    consume_seconds(value, pos, out.second, out.partial_second);
  }

  // Optional whitespace + AM/PM
  consume_whitespace(value, pos);
  if (pos < value.size()) {
    int ap;
    if (consume_string_match(value, pos, locale_.am_pm, ap)) {
      out.am_pm = ap - 1; // 0=AM, 1=PM
    }
  }

  consume_whitespace(value, pos);
  return pos == value.size();
}

} // namespace libvroom
