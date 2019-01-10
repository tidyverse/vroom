#include "altrep.h"

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include "vroom_vec.h"

#include <Rcpp.h>

using namespace Rcpp;

// inspired by Luke Tierney and the R Core Team
// https://github.com/ALTREP-examples/Rpkg-mutable/blob/master/src/mutable.c
// and Romain François
// https://purrple.cat/blog/2018/10/21/lazy-abs-altrep-cplusplus/ and Dirk

struct vroom_string : vroom_vec {

public:
  static R_altrep_class_t class_t;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    // make a new altrep object of class `vroom_string::class_t`
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
        "vroom_string (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING methods -----------------

  static SEXP Val(SEXP vec, R_xlen_t i) {
    auto inf = Info(vec);
    auto sep_locs = inf.idx;

    auto loc = Get(vec, i);

    auto val = Rf_mkCharLenCE(loc.begin, loc.end - loc.begin, CE_UTF8);
    val = check_na(vec, val);

    return val;
  }

  static SEXP check_na(SEXP vec, SEXP val) {
    auto inf = Info(vec);

    // Look for NAs
    for (const auto& v : *Info(vec).na) {
      // We can just compare the addresses directly because they should now
      // both be in the global string cache.
      if (v == val) {
        val = NA_STRING;
        break;
      }
    }
    return val;
  }

  // the element at the index `i`
  //
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return STRING_ELT(data2, i);
    }

    return Val(vec, i);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    // allocate a standard character vector for data2
    R_xlen_t n = Length(vec);
    data2 = PROTECT(Rf_allocVector(STRSXP, n));

    auto inf = Info(vec);
    auto sep_locs = inf.idx;

    auto na_len = inf.na->length();
    auto i = 0;

    for (const auto& loc : inf.idx->column(inf.column)) {

      auto val = Rf_mkCharLenCE(loc.begin, loc.end - loc.begin, CE_UTF8);

      // Look for NAs
      for (const auto& v : *Info(vec).na) {
        // We can just compare the addresses directly because they should now
        // both be in the global string cache.
        if (v == val) {
          val = NA_STRING;
          break;
        }
      }

      SET_STRING_ELT(data2, i++, val);
    }

    R_set_altrep_data2(vec, data2);
    UNPROTECT(1);
    return data2;
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("vroom_string", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);

    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
};

R_altrep_class_t vroom_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_string(DllInfo* dll) { vroom_string::Init(dll); }
