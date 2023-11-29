#pragma once

#include "altrep.h"
#include "r_utils.h"

#ifdef HAS_ALTREP

class vroom_rle {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(SEXP input) {

    SEXP res = R_new_altrep(class_t, input, R_NilValue);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // ALTREP methods -------------------
  // The length of the object
  static inline R_xlen_t Length(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return Rf_xlength(data2);
    }
    R_xlen_t sz = 0;
    SEXP rle = R_altrep_data1(vec);
    int* rle_p = INTEGER(rle);

    for (R_xlen_t i = 0; i < Rf_xlength(rle); ++i) {
      sz += rle_p[i];
    }

    return sz;
  }

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_rle (len=%" R_PRIdXLEN_T ", materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING methods -----------------

  // the element at the index `i`
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return STRING_ELT(data2, i);
    }

    SEXP rle = R_altrep_data1(vec);
    int* rle_p = INTEGER(rle);
    SEXP nms = Rf_getAttrib(rle, Rf_install("names"));

    R_xlen_t idx = 0;
    while (i >= 0 && idx < Rf_xlength(rle)) {
      i -= rle_p[idx++];
    }

    return STRING_ELT(nms, idx - 1);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    R_xlen_t sz = Length(vec);
    SEXP rle = R_altrep_data1(vec);
    int* rle_p = INTEGER(rle);

    SEXP out = PROTECT(Rf_allocVector(STRSXP, sz));

    R_xlen_t idx = 0;
    SEXP nms = Rf_getAttrib(rle, Rf_install("names"));
    for (R_xlen_t i = 0; i < Rf_xlength(rle); ++i) {
      for (R_xlen_t j = 0; j < rle_p[i]; ++j) {
        SET_STRING_ELT(out, idx++, STRING_ELT(nms, i));
      }
    }

    UNPROTECT(1);
    R_set_altrep_data2(vec, out);

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue)
      return nullptr;

    return STDVEC_DATAPTR(data2);
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_rle::class_t = R_make_altstring_class("vroom_rle", "vroom", dll);

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

#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_rle(DllInfo* dll);
