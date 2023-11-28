#include <cpp11/doubles.hpp>

#include "altrep.h"
#include "parallel.h"
#include "r_utils.h"
#include "vroom_vec.h"

double bsd_strtod(const char* begin, const char* end, const char decimalMark);

cpp11::doubles read_dbl(vroom_vec_info* info);

#ifdef HAS_ALTREP

/* Vroom Dbl */

class vroom_dbl : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // ALTREP methods -------------------

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_dbl (len=%" R_PRIdXLEN_T ", materialized=%s)\n",
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

    auto& info = vroom_vec::Info(vec);
    double out = parse_value<double>(
        i,
        info.column,
        [&](const char* begin, const char* end) -> double {
          return bsd_strtod(begin, end, info.locale->decimalMark_[0]);
        },
        info.errors,
        "a double",
        *info.na);
    info.errors->warn_for_errors();
    return out;
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto out = read_dbl(&Info(vec));
    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the info
    Finalize(R_altrep_data1(vec));

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_dbl::class_t = R_make_altreal_class("vroom_dbl", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_dbl>);

    // altinteger
    R_set_altreal_Elt_method(class_t, real_Elt);
  }
};

#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_dbl(DllInfo* dll);
