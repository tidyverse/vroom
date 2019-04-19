#include <Rcpp.h>
using namespace Rcpp;

#include "DateTime.h"
#include "DateTimeParser.h"
#include "LocaleInfo.h"
#include "r_utils.h"

#include "vroom_lgl.h"
#include "vroom_num.h"

typedef bool (*canParseFun)(const std::string&, LocaleInfo* pLocale);

bool canParse(
    CharacterVector x, const canParseFun& canParse, LocaleInfo* pLocale) {
  for (int i = 0; i < x.size(); ++i) {
    if (x[i] == NA_STRING)
      continue;

    if (x[i].size() == 0)
      continue;

    if (!canParse(std::string(x[i]), pLocale))
      return false;
  }
  return true;
}

bool allMissing(CharacterVector x) {
  for (int i = 0; i < x.size(); ++i) {
    if (x[i] != NA_STRING && x[i].size() > 0)
      return false;
  }
  return true;
}

bool isLogical(const std::string& x, LocaleInfo* pLocale) {
  const char* const str = x.data();
  int res = parse_logical(str, str + x.size());
  return res != NA_LOGICAL;
}

bool isNumber(const std::string& x, LocaleInfo* pLocale) {
  // Leading zero not followed by decimal mark
  if (x[0] == '0' && x.size() > 1 && x[1] != pLocale->decimalMark_)
    return false;

  auto str = vroom::string(x);
  auto num = parse_num(str, *pLocale, true);

  return !ISNA(num);
}

bool isInteger(const std::string& x, LocaleInfo* pLocale) {
  // Leading zero
  if (x[0] == '0' && x.size() > 1)
    return false;

  double res = 0;
  std::string::const_iterator begin = x.begin(), end = x.end();

  return parseInt(begin, end, res) && begin == end;
}

bool isDouble(const std::string& x, LocaleInfo* pLocale) {
  // Leading zero not followed by decimal mark
  if (x[0] == '0' && x.size() > 1 && x[1] != pLocale->decimalMark_)
    return false;

  double res = 0;
  std::string::const_iterator begin = x.begin(), end = x.end();

  bool ok = parseDouble(pLocale->decimalMark_, begin, end, res);

  return ok && begin == end;
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

  if (!parser.compactDate())
    return true;

  // Values like 00014567 are unlikely to be dates, so don't guess
  return parser.year() > 999;
}

// [[Rcpp::export]]
std::string
guess_type_(CharacterVector input, List locale, bool guess_integer = false) {
  LocaleInfo locale_(locale);

  if (input.size() == 0) {
    return "character";
  }

  if (allMissing(input)) {
    return "logical";
  }

  // Work from strictest to most flexible
  if (canParse(input, isLogical, &locale_))
    return "logical";
  if (guess_integer && canParse(input, isInteger, &locale_))
    return "integer";
  if (canParse(input, isDouble, &locale_))
    return "double";
  if (canParse(input, isNumber, &locale_))
    return "number";
  if (canParse(input, isTime, &locale_))
    return "time";
  if (canParse(input, isDate, &locale_))
    return "date";
  if (canParse(input, isDateTime, &locale_))
    return "datetime";

  // Otherwise can always parse as a character
  return "character";
}
