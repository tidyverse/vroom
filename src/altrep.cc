#include <Rcpp.h>

#include "altrep.h"
#include <thread>

using namespace Rcpp;

// [[Rcpp::export]]
void force_materialization(SEXP x) {
#ifdef HAS_ALTREP
  DATAPTR(x);
#endif
}

// [[Rcpp::export]]
void vroom_materialize(SEXP x, bool replace = false) {
#ifdef HAS_ALTREP
  for (R_xlen_t i = 0; i < Rf_xlength(x); ++i) {

    SEXP elt = VECTOR_ELT(x, i);
    // First materialize all of the non-character vectors
    if (ALTREP(elt)) {
      DATAPTR(elt);
    }
  }

  // If replace replace the altrep vectors with their materialized
  // vectors
  if (replace) {
    for (R_xlen_t i = 0; i < Rf_xlength(x); ++i) {
      SEXP elt = PROTECT(VECTOR_ELT(x, i));
      if (ALTREP(elt)) {
        SET_VECTOR_ELT(x, i, R_altrep_data2(elt));
        R_set_altrep_data2(elt, R_NilValue);
      }
      UNPROTECT(1);
    }
  }

#endif
}

// [[Rcpp::export]]
std::string vroom_str_(RObject x) {
  std::stringstream ss;

#ifdef HAS_ALTREP
  if (ALTREP(x)) {

    auto csym = CAR(ATTRIB(ALTREP_CLASS(x)));
    auto psym = CADR(ATTRIB(ALTREP_CLASS(x)));
    bool is_altrep = ALTREP(x);
    bool materialzied = R_altrep_data2(x) != R_NilValue;

    ss << std::boolalpha << "altrep:" << is_altrep << '\t'
       << "type:" << CHAR(PRINTNAME(psym)) << "::" << CHAR(PRINTNAME(csym))
       << '\t' << "length:" << LENGTH(x) << '\t'
       << "materialized:" << materialzied << '\n';
  }
#else
  if (false) {
  }
#endif
  else {
    ss << std::boolalpha << "altrep:" << false << '\t'
       << "type: " << Rf_type2char(TYPEOF(x)) << '\t' << "length:" << LENGTH(x)
       << '\n';
  }

  return ss.str();
}
