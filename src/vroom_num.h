#pragma once

#include "altrep.h"

#include "vroom_vec.h"
#include <Rcpp.h>

#include "parallel.h"

using namespace vroom;

enum NumberState { STATE_INIT, STATE_LHS, STATE_RHS, STATE_EXP, STATE_FIN };

// First and last are updated to point to first/last successfully parsed
// character
template <typename Iterator, typename Attr>
inline bool parseNumber(
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

double parse_num(const string& str, const LocaleInfo& loc) {
  double ret;
  auto start = str.begin();
  auto end = str.end();
  bool ok = parseNumber(loc.decimalMark_, loc.groupingMark_, start, end, ret);
  if (ok) {
    return ret;
  }

  return NA_REAL;
}

Rcpp::NumericVector read_num(vroom_vec_info* info) {

  R_xlen_t n = info->idx->num_rows();

  Rcpp::NumericVector out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t id) {
        size_t i = start;
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          out[i++] = parse_num(str, *info->locale);
        }
      },
      info->num_threads);

  return out;
}

#ifdef HAS_ALTREP

/* Vroom number */

class vroom_num : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(1);

    return res;
  }

  // ALTREP methods -------------------

  // What gets printed when .Internal(inspect()) is used
  static Rboolean Inspect(
      SEXP x,
      int pre,
      int deep,
      int pvec,
      void (*inspect_subtree)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_num (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTREAL methods -----------------

  // the element at the index `i`
  static double real_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return REAL(data2)[i];
    }

    auto str = vroom_vec::Get(vec, i);
    auto inf = vroom_vec::Info(vec);

    return parse_num(str, *inf.locale);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto out = read_num(&Info(vec));
    R_set_altrep_data2(vec, out);

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_num::class_t = R_make_altreal_class("vroom_num", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);

    // altinteger
    R_set_altreal_Elt_method(class_t, real_Elt);
  }
};

R_altrep_class_t vroom_num::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_num(DllInfo* dll) { vroom_num::Init(dll); }

#else
void init_vroom_num(DllInfo* dll) {}
#endif
