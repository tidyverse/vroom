// Utility functions that were previously in the legacy reading codepaths
// but are still needed by the R package.

#include <cpp11/doubles.hpp>
#include <cpp11/integers.hpp>
#include <cpp11/list.hpp>
#include <cpp11/logicals.hpp>
#include <cpp11/strings.hpp>

#include "DateTime.h"
#include "DateTimeParser.h"
#include "unicode_fopen.h"
#include "utils.h"

// ============================================================================
// has_trailing_newline — from former vroom.cc
// ============================================================================

[[cpp11::register]] bool has_trailing_newline(const cpp11::strings& filename) {
  std::FILE* f = unicode_fopen(CHAR(filename[0]), "rb");

  if (!f) {
    return true;
  }

  std::setvbuf(f, nullptr, _IONBF, 0);

  fseek(f, -1, SEEK_END);
  char c = fgetc(f);

  fclose(f);

  return c == '\n';
}

// ============================================================================
// utctime_ — from former vroom_dttm.cc
// ============================================================================

[[cpp11::register]] cpp11::writable::doubles utctime_(
    const cpp11::integers& year,
    const cpp11::integers& month,
    const cpp11::integers& day,
    const cpp11::integers& hour,
    const cpp11::integers& min,
    const cpp11::integers& sec,
    const cpp11::doubles& psec) {
  int n = year.size();
  if (month.size() != n || day.size() != n || hour.size() != n ||
      min.size() != n || sec.size() != n || psec.size() != n) {
    cpp11::stop("All inputs must be same length");
  }

  cpp11::writable::doubles out(n);

  for (int i = 0; i < n; ++i) {
    DateTime dt(
        year[i], month[i], day[i], hour[i], min[i], sec[i], psec[i], "UTC");
    out[i] = dt.datetime();
  }

  out.attr("class") = {"POSIXct", "POSIXt"};
  out.attr("tzone") = "UTC";

  return out;
}

// ============================================================================
// whitespace_columns_ — from former vroom_fwf.cc
// ============================================================================

template <typename Iterator>
static std::vector<bool>
find_empty_cols(Iterator begin, Iterator end, ptrdiff_t n) {

  std::vector<bool> is_white;

  size_t row = 0, col = 0;
  for (Iterator cur = begin; cur != end; ++cur) {
    if (n > 0 && row > static_cast<size_t>(n)) {
      break;
    }

    switch (*cur) {
    case '\n':
      col = 0;
      row++;
      break;
    case '\r':
    case ' ':
      col++;
      break;
    default:
      // Make sure there's enough room
      if (col >= is_white.size())
        is_white.resize(col + 1, true);
      is_white[col] = false;
      col++;
    }
  }

  return is_white;
}

[[cpp11::register]] cpp11::list whitespace_columns_(
    const std::string& filename,
    size_t skip,
    ptrdiff_t n,
    const std::string& comment) {

  std::error_code error;
  auto mmap = make_mmap_source(filename.c_str(), error);
  if (error) {
    REprintf("mapping error: %s", error.message().c_str());
    return cpp11::list();
  }

  size_t s = vroom::find_first_line(
      mmap,
      skip,
      comment.data(),
      /* skip_empty_rows */ true,
      /* embedded_nl */ false,
      /* quote */ '\0');

  std::vector<bool> empty = find_empty_cols(mmap.begin() + s, mmap.end(), n);
  std::vector<int> begin, end;

  bool in_col = false;

  for (size_t i = 0; i < empty.size(); ++i) {
    if (in_col && empty[i]) {
      end.push_back(i);
      in_col = false;
    } else if (!in_col && !empty[i]) {
      begin.push_back(i);
      in_col = true;
    }
  }

  if (in_col)
    end.push_back(empty.size());

  using namespace cpp11::literals;
  return cpp11::writable::list({"begin"_nm = begin, "end"_nm = end});
}

// ============================================================================
// Datetime/date/time parsing using the readr DateTimeParser
// ============================================================================

[[cpp11::register]] cpp11::writable::doubles parse_datetime_(
    const cpp11::strings& x,
    const std::string& format,
    const cpp11::list& locale) {

  LocaleInfo loc(locale);
  DateTimeParser parser(&loc);

  int n = x.size();
  cpp11::writable::doubles out(n);

  for (int i = 0; i < n; ++i) {
    SEXP str = x[i];
    if (str == NA_STRING) {
      out[i] = NA_REAL;
      continue;
    }
    const char* s = CHAR(str);
    int len = strlen(s);
    parser.setDate(s, s + len);

    bool ok;
    if (format.empty() || format == "%AD %AT" || format == "%ADT%AT") {
      ok = parser.parseISO8601();
    } else {
      ok = parser.parse(format);
    }

    if (ok) {
      DateTime dt = parser.makeDateTime();
      out[i] = dt.validDateTime() ? dt.datetime() : NA_REAL;
    } else {
      out[i] = NA_REAL;
    }
  }

  out.attr("class") = {"POSIXct", "POSIXt"};
  out.attr("tzone") = loc.tz_;

  return out;
}

[[cpp11::register]] cpp11::writable::doubles parse_date_(
    const cpp11::strings& x,
    const std::string& format,
    const cpp11::list& locale) {

  LocaleInfo loc(locale);
  DateTimeParser parser(&loc);

  int n = x.size();
  cpp11::writable::doubles out(n);

  for (int i = 0; i < n; ++i) {
    SEXP str = x[i];
    if (str == NA_STRING) {
      out[i] = NA_REAL;
      continue;
    }
    const char* s = CHAR(str);
    int len = strlen(s);
    parser.setDate(s, s + len);

    bool ok;
    if (format.empty()) {
      ok = parser.parseDate();
    } else if (format == "%AD") {
      ok = parser.parseDate();
    } else {
      ok = parser.parse(format);
    }

    if (ok) {
      DateTime dt = parser.makeDate();
      out[i] = dt.validDate() ? static_cast<double>(dt.date()) : NA_REAL;
    } else {
      out[i] = NA_REAL;
    }
  }

  out.attr("class") = "Date";

  return out;
}

[[cpp11::register]] cpp11::writable::doubles parse_time_(
    const cpp11::strings& x,
    const std::string& format,
    const cpp11::list& locale) {

  LocaleInfo loc(locale);
  DateTimeParser parser(&loc);

  int n = x.size();
  cpp11::writable::doubles out(n);

  for (int i = 0; i < n; ++i) {
    SEXP str = x[i];
    if (str == NA_STRING) {
      out[i] = NA_REAL;
      continue;
    }
    const char* s = CHAR(str);
    int len = strlen(s);
    parser.setDate(s, s + len);

    bool ok;
    if (format.empty() || format == "%AT") {
      ok = parser.parseTime();
    } else {
      ok = parser.parse(format);
    }

    if (ok) {
      DateTime dt = parser.makeTime();
      out[i] = dt.time();
    } else {
      out[i] = NA_REAL;
    }
  }

  out.attr("class") = {"hms", "difftime"};
  out.attr("units") = "secs";

  return out;
}
