#include "altrep.h"

#include <cpp11/sexp.hpp>
#include <sstream>

[[cpp11::register]] SEXP vroom_convert(SEXP x) {
  SEXP out = PROTECT(Rf_allocVector(VECSXP, Rf_xlength(x)));
  SHALLOW_DUPLICATE_ATTRIB(out, x);

  for (R_xlen_t col = 0; col < Rf_xlength(x); ++col) {
    SEXP elt = VECTOR_ELT(x, col);
    if (!ALTREP(elt)) {
      SET_VECTOR_ELT(out, col, elt);
    } else {
      R_xlen_t nrow = Rf_xlength(elt);
      switch (TYPEOF(elt)) {
      case LGLSXP: {
        SET_VECTOR_ELT(out, col, Rf_allocVector(LGLSXP, nrow));
        int* out_p = LOGICAL(VECTOR_ELT(out, col));
        const int* in_p = LOGICAL_RO(elt);
        for (R_xlen_t row = 0; row < nrow; ++row) {
          out_p[row] = in_p[row];
        }
        break;
      }
      case INTSXP: {
        SET_VECTOR_ELT(out, col, Rf_allocVector(INTSXP, nrow));
        int* out_p = INTEGER(VECTOR_ELT(out, col));
        const int* in_p = INTEGER_RO(elt);
        for (R_xlen_t row = 0; row < nrow; ++row) {
          out_p[row] = in_p[row];
        }
        break;
      }
      case REALSXP: {
        SET_VECTOR_ELT(out, col, Rf_allocVector(REALSXP, nrow));
        double* out_p = REAL(VECTOR_ELT(out, col));
        const double* in_p = REAL_RO(elt);
        for (R_xlen_t row = 0; row < nrow; ++row) {
          out_p[row] = in_p[row];
        }
        break;
      }
      case STRSXP: {
        SET_VECTOR_ELT(out, col, Rf_allocVector(STRSXP, nrow));
        SEXP out_elt = VECTOR_ELT(out, col);
        const SEXP* in_p = STRING_PTR_RO(elt);
        for (R_xlen_t row = 0; row < nrow; ++row) {
          SET_STRING_ELT(out_elt, row, in_p[row]);
        }
        break;
      }
      }
      SHALLOW_DUPLICATE_ATTRIB(VECTOR_ELT(out, col), elt);
    }
  }
  UNPROTECT(1);
  return out;
}

// Force in-place materialization of any ALTREP columns in a data frame,
// without creating copies. For numeric types, accessing the data pointer
// triggers materialization. For strings, we touch each element.
[[cpp11::register]] SEXP vroom_materialize(SEXP x, bool replace) {
  for (R_xlen_t col = 0; col < Rf_xlength(x); ++col) {
    SEXP elt = VECTOR_ELT(x, col);
    if (ALTREP(elt)) {
      R_xlen_t n = Rf_xlength(elt);
      switch (TYPEOF(elt)) {
      case LGLSXP:
        LOGICAL(elt);
        break;
      case INTSXP:
        INTEGER(elt);
        break;
      case REALSXP:
        REAL(elt);
        break;
      case STRSXP:
        // STRING_PTR_RO triggers full materialization for STRSXP ALTREP
        STRING_PTR_RO(elt);
        break;
      default:
        // For other types, access each element to force materialization
        for (R_xlen_t i = 0; i < n; ++i) {
          VECTOR_ELT(elt, i);
        }
        break;
      }
    }
  }

  if (replace) {
    for (R_xlen_t col = 0; col < Rf_xlength(x); ++col) {
      SEXP elt = PROTECT(VECTOR_ELT(x, col));
      if (ALTREP(elt) && R_altrep_data2(elt) != R_NilValue) {
        SET_VECTOR_ELT(x, col, R_altrep_data2(elt));
        R_set_altrep_data2(elt, R_NilValue);
      }
      UNPROTECT(1);
    }
  }

  return x;
}

[[cpp11::register]] std::string vroom_str_(const cpp11::sexp& x) {
  std::stringstream ss;

  if (ALTREP(x)) {

    auto csym = R_altrep_class_name(x);
    auto psym = R_altrep_class_package(x);
    bool is_altrep = ALTREP(x);
    bool materialzied = R_altrep_data2(x) != R_NilValue;

    ss << std::boolalpha << "altrep:" << is_altrep << '\t'
       << "type:" << CHAR(PRINTNAME(psym)) << "::" << CHAR(PRINTNAME(csym));
    // We would have to dispatch to get the length of an object
    if (!Rf_isObject(x)) {
      ss << '\t' << "length:" << LENGTH(x);
    }
    ss << '\t' << "materialized:" << materialzied << '\n';
  } else {
    ss << std::boolalpha << "altrep:" << false << '\t'
       << "type: " << Rf_type2char(TYPEOF(x));
    if (!Rf_isObject(x)) {
      ss << '\t' << "length:" << LENGTH(x);
    }
    ss << '\n';
  }

  return ss.str();
}
