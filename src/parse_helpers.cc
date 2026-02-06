#include "parse_helpers.h"
#include "utils.h"

#include <ctype.h>
#include <cmath>
#include <cerrno>
#include <climits>
#include <cstdlib>

using namespace vroom;

// ============================================================================
// bsd_strtod — from former vroom_dbl.cc
// ============================================================================

/*
    An STL iterator-based string to floating point number conversion.
    This function was adapted from the C standard library of RetroBSD,
    which is based on Berkeley UNIX.
    This function and this function only is BSD license.

    https://retrobsd.googlecode.com/svn/stable/libc/stdlib/strtod.c
 */
double bsd_strtod(const char* begin, const char* end, const char decimalMark) {
  if (begin == end) {
    return NA_REAL;
  }
  int sign = 0, expSign = 0, i;
  double fraction, dblExp;
  const char* p = begin;
  char c;

  /* Exponent read from "EX" field. */
  int exp = 0;

  /* Exponent that derives from the fractional part.  Under normal
   * circumstances, it is the negative of the number of digits in F.
   * However, if I is very long, the last digits of I get dropped
   * (otherwise a long I with a large negative exponent could cause an
   * unnecessary overflow on I alone).  In this case, fracExp is
   * incremented one for each dropped digit. */
  int fracExp = 0;

  /* Number of digits in mantissa. */
  int mantSize;

  /* Number of mantissa digits BEFORE decimal point. */
  int decPt;

  /* Temporarily holds location of exponent in str. */
  const char* pExp;

  /* Largest possible base 10 exponent.
   * Any exponent larger than this will already
   * produce underflow or overflow, so there's
   * no need to worry about additional digits. */
  static int maxExponent = 307;

  /* Table giving binary powers of 10.
   * Entry is 10^2^i.  Used to convert decimal
   * exponents into floating-point numbers. */
  static double powersOf10[] = {
      1e1,
      1e2,
      1e4,
      1e8,
      1e16,
      1e32,
      1e64,
      1e128,
      1e256,
  };

  /*
   * check for a sign.
   */
  if (p != end && *p == '-') {
    sign = 1;
    ++p;
  } else if (p != end && *p == '+')
    ++p;

  /* NaN */
  if (end - p == 3 && tolower(p[0]) == 'n' && tolower(p[1]) == 'a' &&
      tolower(p[2]) == 'n') {
    return NAN;
  }
  /* Inf */
  if (end - p == 3 && tolower(p[0]) == 'i' && tolower(p[1]) == 'n' &&
      tolower(p[2]) == 'f') {
    return sign == 1 ? -HUGE_VAL : HUGE_VAL;
  }

  /* If we don't have a digit or decimal point something is wrong, so return
   * an NA */
  if (!(isdigit(*p) || *p == decimalMark)) {
    return NA_REAL;
  }

  /*
   * Count the number of digits in the mantissa (including the decimal
   * point), and also locate the decimal point.
   */
  decPt = -1;
  for (mantSize = 0; p != end; ++mantSize) {
    c = *p;
    if (!isdigit(c)) {
      if (c != decimalMark || decPt >= 0)
        break;
      decPt = mantSize;
    }
    ++p;
  }

  /*
   * Now suck up the digits in the mantissa.  Use two integers to
   * collect 9 digits each (this is faster than using floating-point).
   * If the mantissa has more than 18 digits, ignore the extras, since
   * they can't affect the value anyway.
   */
  pExp = p;
  p -= mantSize;
  if (decPt < 0)
    decPt = mantSize;
  else
    --mantSize; /* One of the digits was the point. */

  if (mantSize > 2 * 9)
    mantSize = 2 * 9;
  fracExp = decPt - mantSize;
  if (mantSize == 0) {
    fraction = 0.0;
    p = begin;
    goto done;
  } else {
    int frac1, frac2;

    for (frac1 = 0; mantSize > 9 && p != end; --mantSize) {
      c = *p++;
      if (c == decimalMark)
        c = *p++;
      frac1 = frac1 * 10 + (c - '0');
    }
    for (frac2 = 0; mantSize > 0 && p != end; --mantSize) {
      c = *p++;
      if (c == decimalMark)
        c = *p++;
      frac2 = frac2 * 10 + (c - '0');
    }
    fraction = (double)1000000000 * frac1 + frac2;
  }

  /*
   * Skim off the exponent.
   */
  p = pExp;
  if (p != end &&
      (*p == 'E' || *p == 'e' || *p == 'S' || *p == 's' || *p == 'F' ||
       *p == 'f' || *p == 'D' || *p == 'd' || *p == 'L' || *p == 'l')) {
    ++p;
    if (p != end && *p == '-') {
      expSign = 1;
      ++p;
    } else if (p != end && *p == '+')
      ++p;
    while (p != end && isdigit(*p))
      exp = exp * 10 + (*p++ - '0');
  }
  if (expSign)
    exp = fracExp - exp;
  else
    exp = fracExp + exp;

  /*
   * Generate a floating-point number that represents the exponent.
   * Do this by processing the exponent one bit at a time to combine
   * many powers of 2 of 10. Then combine the exponent with the
   * fraction.
   */
  if (exp < 0) {
    expSign = 1;
    exp = -exp;
  } else
    expSign = 0;
  if (exp > maxExponent)
    exp = maxExponent;
  dblExp = 1.0;
  for (i = 0; exp; exp >>= 1, ++i)
    if (exp & 01)
      dblExp *= powersOf10[i];
  if (expSign)
    fraction /= dblExp;
  else
    fraction *= dblExp;

done:
  if (p != end) {
    return NA_REAL;
  }
  return sign ? -fraction : fraction;
}

// ============================================================================
// parse_num — from former vroom_num.cc
// ============================================================================

enum NumberState { STATE_INIT, STATE_LHS, STATE_RHS, STATE_EXP, STATE_FIN };

// First and last are updated to point to first/last successfully parsed
// character
template <typename Iterator, typename Attr>
static bool parseNumber(
    const std::string& decimalMark,
    const std::string& groupingMark,
    Iterator& first,
    Iterator& last,
    Attr& res) {

  Iterator cur = first;

  // Advance to first non-character
  for (; cur != last; ++cur) {
    if (*cur == '-' || matches(cur, last, decimalMark) ||
        (*cur >= '0' && *cur <= '9'))
      break;
  }

  if (cur == last) {
    return false;
  } else { // Move first to start of number
    first = cur;
  }

  double sum = 0, denom = 1, exponent = 0;
  NumberState state = STATE_INIT;
  bool seenNumber = false, exp_init = true;
  double sign = 1.0, exp_sign = 1.0;

  for (; cur < last; ++cur) {
    if (state == STATE_FIN)
      break;

    switch (state) {
    case STATE_INIT:
      if (*cur == '-') {
        state = STATE_LHS;
        sign = -1.0;
      } else if (matches(cur, last, decimalMark)) {
        cur += decimalMark.size() - 1;
        state = STATE_RHS;
      } else if (*cur >= '0' && *cur <= '9') {
        seenNumber = true;
        state = STATE_LHS;
        sum = *cur - '0';
      } else {
        goto end;
      }
      break;
    case STATE_LHS:
      if (matches(cur, last, groupingMark)) {
        cur += groupingMark.size() - 1;
      } else if (matches(cur, last, decimalMark)) {
        cur += decimalMark.size() - 1;
        state = STATE_RHS;
      } else if (seenNumber && (*cur == 'e' || *cur == 'E')) {
        state = STATE_EXP;
      } else if (*cur >= '0' && *cur <= '9') {
        seenNumber = true;
        sum *= 10;
        sum += *cur - '0';
      } else {
        goto end;
      }
      break;
    case STATE_RHS:
      if (matches(cur, last, groupingMark)) {
        cur += groupingMark.size() - 1;
      } else if (seenNumber && (*cur == 'e' || *cur == 'E')) {
        state = STATE_EXP;
      } else if (*cur >= '0' && *cur <= '9') {
        seenNumber = true;
        denom *= 10;
        sum += (*cur - '0') / denom;
      } else {
        goto end;
      }
      break;
    case STATE_EXP:
      // negative/positive sign only allowed immediately after 'e' or 'E'
      if (*cur == '-' && exp_init) {
        exp_sign = -1.0;
        exp_init = false;
      } else if (*cur == '+' && exp_init) {
        // sign defaults to positive
        exp_init = false;
      } else if (*cur >= '0' && *cur <= '9') {
        exponent *= 10.0;
        exponent += *cur - '0';
        exp_init = false;
      } else {
        goto end;
      }
      break;
    case STATE_FIN:
      goto end;
    }
  }

end:

  // Set last to point to final character used
  last = cur;

  res = sign * sum;

  // If the number was in scientific notation, multiply by 10^exponent
  if (exponent) {
    res *= pow(10.0, exp_sign * exponent);
  }

  return seenNumber;
}

double parse_num(
    const char* start, const char* end, const LocaleInfo& loc, bool strict) {
  double ret;
  auto start_p = start;
  auto end_p = end;
  bool ok =
      parseNumber(loc.decimalMark_, loc.groupingMark_, start_p, end_p, ret);
  if (ok && (!strict || (start_p == start && end_p == end))) {
    return ret;
  }

  return NA_REAL;
}
