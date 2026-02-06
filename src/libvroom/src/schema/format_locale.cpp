#include "libvroom/format_locale.h"

namespace libvroom {

FormatLocale FormatLocale::english() {
  FormatLocale loc;
  loc.month_abbr = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  loc.month_full = {"January", "February", "March",     "April",
                    "May",     "June",     "July",      "August",
                    "September", "October", "November", "December"};
  loc.day_abbr = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  loc.am_pm = {"AM", "PM"};
  loc.date_format = "%Y-%m-%d";
  loc.time_format = "%H:%M:%S";
  loc.decimal_mark = '.';
  loc.default_tz = "UTC";
  return loc;
}

} // namespace libvroom
