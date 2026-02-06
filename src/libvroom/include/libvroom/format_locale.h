#pragma once

#include <string>
#include <vector>

namespace libvroom {

// Locale data for format-string-based datetime parsing.
// Pure data struct with no R dependencies — usable standalone from Python/CLI.
struct FormatLocale {
  std::vector<std::string> month_abbr; // 12 entries: Jan, Feb, ...
  std::vector<std::string> month_full; // 12 entries: January, February, ...
  std::vector<std::string> day_abbr;   // 7 entries: Sun, Mon, ...
  std::vector<std::string> am_pm;      // 2 entries: AM, PM
  std::string date_format = "%Y-%m-%d";
  std::string time_format = "%H:%M:%S";
  char decimal_mark = '.';
  std::string default_tz = "UTC";

  static FormatLocale english();
};

} // namespace libvroom
