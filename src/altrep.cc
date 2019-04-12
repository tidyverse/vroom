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
void vroom_materialize(Rcpp::List x) {
#ifdef HAS_ALTREP
  std::vector<std::thread> t;
  for (int i = 0; i < x.length(); ++i) {

    // First materialize all of the non-character vectors
    if (TYPEOF(x[i]) == REALSXP || TYPEOF(x[i]) == INTSXP) {
      t.emplace_back(std::thread([&, i]() { DATAPTR(x[i]); }));
    }
  }

  // Then materialize the rest
  for (int i = 0; i < x.length(); ++i) {
    if (!(TYPEOF(x[i]) == REALSXP || TYPEOF(x[i]) == INTSXP)) {
      DATAPTR(x[i]);
    }
  }
  std::for_each(t.begin(), t.end(), std::mem_fn(&std::thread::join));
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
