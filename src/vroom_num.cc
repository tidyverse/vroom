#include "vroom_num.h"
#include "Rcpp.h"

enum NumberState { STATE_INIT, STATE_LHS, STATE_RHS, STATE_EXP, STATE_FIN };

// First and last are updated to point to first/last successfully parsed
// character
template <typename Iterator, typename Attr>
bool parseNumber(
    char decimalMark,
    char groupingMark,
    Iterator& first,
    Iterator& last,
    Attr& res) {

  Iterator cur = first;

  // Advance to first non-character
  for (; cur != last; ++cur) {
    if (*cur == '-' || *cur == decimalMark || (*cur >= '0' && *cur <= '9'))
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

  for (; cur != last; ++cur) {
    if (state == STATE_FIN)
      break;

    switch (state) {
    case STATE_INIT:
      if (*cur == '-') {
        state = STATE_LHS;
        sign = -1.0;
      } else if (*cur == decimalMark) {
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
      if (*cur == groupingMark) {
        // do nothing
      } else if (*cur == decimalMark) {
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
      if (*cur == groupingMark) {
        // do nothing
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

double parse_num(const string& str, const LocaleInfo& loc, bool strict) {
  double ret;
  auto start = str.begin();
  auto end = str.end();
  bool ok = parseNumber(loc.decimalMark_, loc.groupingMark_, start, end, ret);
  if (ok && (!strict || (start == str.begin() && end == str.end()))) {
    return ret;
  }

  return NA_REAL;
}

Rcpp::NumericVector read_num(vroom_vec_info* info) {

  R_xlen_t n = info->column->size();

  Rcpp::NumericVector out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t id) {
        size_t i = start;
        auto col = info->column->slice(start, end);
        for (const auto& str : *col) {
          out[i++] = parse_num(str, *info->locale);
        }
      },
      info->num_threads);

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_num::class_t;

void init_vroom_num(DllInfo* dll) { vroom_num::Init(dll); }

#else
void init_vroom_num(DllInfo* dll) {}
#endif
