#pragma once

#include <cpp11/doubles.hpp>

#include "altrep.h"

constexpr long long NA_INTEGER64 = 0x8000000000000000LL;

#include "r_utils.h"
#include "vroom.h"

namespace cpp11 {
inline bool is_na(long long x) { return x == NA_INTEGER64; }
} // namespace cpp11

namespace vroom {
template <> inline long long na<long long>() { return NA_INTEGER64; }
} // namespace vroom

#include "vroom_vec.h"

long long vroom_strtoll(const char* begin, const char* end);

cpp11::doubles read_big_int(vroom_vec_info* info);

union vroom_big_int_t {
  long long ll;
  double dbl;
};

#ifdef HAS_ALTREP

class vroom_big_int : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    cpp11::sexp res = R_new_altrep(class_t, out, R_NilValue);

    res.attr("class") = {"integer64"};

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // ALTREP methods -------------------

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_big_int (len=%" R_PRIdXLEN_T ", materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTREAL methods -----------------

  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto out = read_big_int(&Info(vec));
    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the info
    Finalize(R_altrep_data1(vec));

    return out;
  }

  // the element at the index `i`
  static double real_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return REAL(data2)[i];
    }

    auto info = vroom_vec::Info(vec);

    vroom_big_int_t res;
    res.ll = parse_value<long long>(
        i, info.column, vroom_strtoll, info.errors, "a big integer", *info.na);

    info.errors->warn_for_errors();

    return res.dbl;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_big_int::class_t =
        R_make_altreal_class("vroom_big_int", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_big_int>);

    // altreal
    R_set_altreal_Elt_method(class_t, real_Elt);
  }
};
#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_big_int(DllInfo* dll);
