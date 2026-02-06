#pragma once

#include <cpp11/R.hpp>

#include "altrep.h"

// Dictionary-backed ALTREP string vector.
// Stores integer codes + pre-interned CHARSXP levels.
// Element access: two array lookups (codes[i] -> levels[code-1]).
// Materialization: SET_STRING_ELT with pre-interned CHARSXPs (no
// Rf_mkCharLenCE).

struct vroom_dict_chr {
  static R_altrep_class_t class_t;

  // Create an ALTREP string vector from integer codes and pre-interned levels.
  // codes: INTSXP with 1-based factor codes (NA_INTEGER for nulls)
  // levels: STRSXP with pre-interned level strings
  static SEXP Make(SEXP codes, SEXP levels) {
    SEXP info = PROTECT(Rf_allocVector(VECSXP, 2));
    SET_VECTOR_ELT(info, 0, codes);
    SET_VECTOR_ELT(info, 1, levels);

    SEXP res = R_new_altrep(class_t, info, R_NilValue);
    MARK_NOT_MUTABLE(res);

    UNPROTECT(1);
    return res;
  }

  // ALTREP methods

  static R_xlen_t Length(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return Rf_xlength(data2);
    }
    SEXP info = R_altrep_data1(vec);
    return Rf_xlength(VECTOR_ELT(info, 0));
  }

  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    SEXP info = R_altrep_data1(x);
    SEXP levels = VECTOR_ELT(info, 1);
    Rprintf(
        "vroom_dict_chr (len=%d, levels=%d, materialized=%s)\n",
        (int)Length(x),
        (int)Rf_xlength(levels),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING Elt: two array lookups, no R API allocation
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return STRING_ELT(data2, i);
    }
    SEXP info = R_altrep_data1(vec);
    int code = INTEGER(VECTOR_ELT(info, 0))[i];
    if (code == NA_INTEGER) {
      return NA_STRING;
    }
    return STRING_ELT(VECTOR_ELT(info, 1), code - 1);
  }

  // Materialize: SET_STRING_ELT with pre-interned CHARSXPs (no Rf_mkCharLenCE)
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    SEXP info = R_altrep_data1(vec);
    SEXP codes = VECTOR_ELT(info, 0);
    SEXP levels = VECTOR_ELT(info, 1);
    R_xlen_t n = Rf_xlength(codes);
    int* code_ptr = INTEGER(codes);

    SEXP result = PROTECT(Rf_allocVector(STRSXP, n));
    for (R_xlen_t i = 0; i < n; i++) {
      if (code_ptr[i] == NA_INTEGER) {
        SET_STRING_ELT(result, i, NA_STRING);
      } else {
        SET_STRING_ELT(result, i, STRING_ELT(levels, code_ptr[i] - 1));
      }
    }
    R_set_altrep_data2(vec, result);
    UNPROTECT(1);
    return result;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return DATAPTR_RW(Materialize(vec));
  }

  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue)
      return nullptr;
    return DATAPTR_RO(data2);
  }

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("vroom_dict_chr", "vroom", dll);

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

[[cpp11::init]] void init_vroom_dict_chr(DllInfo* dll);
