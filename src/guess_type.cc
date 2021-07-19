#include <cpp11/list.hpp>
#include <cpp11/strings.hpp>

#include "DateTime.h"
#include "DateTimeParser.h"
#include "LocaleInfo.h"
#include "r_utils.h"

#include "vroom_dbl.h"
#include "vroom_lgl.h"
#include "vroom_num.h"

typedef bool (*canParseFun)(const std::string&, LocaleInfo* pLocale);

bool canParse(
    const cpp11::strings& x, const canParseFun& canParse, LocaleInfo* pLocale) {
  for (auto && i : x) {
    if (i == NA_STRING)
      continue;

    if (i.size() == 0)
      continue;

    if (!canParse(std::string(i), pLocale))
      return false;
  }
  return true;
}

bool allMissing(const cpp11::strings& x) {
  for (auto && i : x) {
    if (i != NA_STRING && i.size() > 0)
      return false;
  }
  return true;
}

bool isLogical(const std::string& x, LocaleInfo* /* pLocale */) {
  const char* const str = x.data();
  int res = parse_logical(str, str + x.size());
  return res != NA_LOGICAL;
}

bool isNumber(const std::string& x, LocaleInfo* pLocale) {
  // Leading zero not followed by decimal mark
  if (x[0] == '0' && x.size() > 1 &&
      !matches(x.data() + 1, x.data() + x.size(), pLocale->decimalMark_))
    return false;

  auto str = vroom::string(x);
  auto num = parse_num(str.begin(), str.end(), *pLocale, true);

  return !ISNA(num);
}

bool isInteger(const std::string& x, LocaleInfo* /* pLocale */) {
  // Leading zero
  if (x[0] == '0' && x.size() > 1)
    return false;

  double res = 0;
  std::string::const_iterator begin = x.begin(), end = x.end();

  return parseInt(begin, end, res) && begin == end;
}

bool isDouble(const std::string& x, LocaleInfo* pLocale) {
  // Leading zero not followed by decimal mark
  if (x[0] == '0' && x.size() > 1 && x[1] != pLocale->decimalMark_[0])
    return false;

  double res =
      bsd_strtod(x.data(), x.data() + x.size(), pLocale->decimalMark_[0]);

  return !ISNA(res);
}

bool isTime(const std::string& x, LocaleInfo* pLocale) {
  DateTimeParser parser(pLocale);

  parser.setDate(x.c_str(), x.c_str() + x.size());
  return parser.parseLocaleTime();
}

bool isDate(const std::string& x, LocaleInfo* pLocale) {
  DateTimeParser parser(pLocale);

  parser.setDate(x.c_str(), x.c_str() + x.size());
  return parser.parseLocaleDate();
}

static bool isDateTime(const std::string& x, LocaleInfo* pLocale) {
  DateTimeParser parser(pLocale);

  parser.setDate(x.c_str(), x.c_str() + x.size());
  bool ok = parser.parseISO8601();

  if (!ok)
    return false;

  DateTime dt = parser.makeDateTime();
  return dt.validDateTime();
}

std::string guess_type__(
    cpp11::writable::strings input,
    const cpp11::strings& na,
    LocaleInfo* pLocale,
    bool guess_integer = false) {

  if (input.size() == 0) {
    return "character";
  }

  if (allMissing(input)) {
    return "character";
  }

  for (auto && i : input) {
    for (auto && j : na) {
      if (i == j) {
        i = NA_STRING;
        break;
      }
    }
  }

  // Work from strictest to most flexible
  if (canParse(input, isLogical, pLocale))
    return "logical";
  if (guess_integer && canParse(input, isInteger, pLocale))
    return "integer";
  if (canParse(input, isDouble, pLocale))
    return "double";
  if (canParse(input, isNumber, pLocale))
    return "number";
  if (canParse(input, isTime, pLocale))
    return "time";
  if (canParse(input, isDate, pLocale))
    return "date";
  if (canParse(input, isDateTime, pLocale))
    return "datetime";

  // Otherwise can always parse as a character
  return "character";
}

[[cpp11::register]] std::string guess_type_(
    const cpp11::strings& input,
    const cpp11::strings& na,
    const cpp11::list& locale,
    bool guess_integer = false) {
  LocaleInfo locale_(locale);
  return guess_type__(input, na, &locale_, guess_integer);
}
