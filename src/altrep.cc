#include "altrep.h"
#include "vroom_chr.h"
#include "vroom_date.h"
#include "vroom_dbl.h"
#include "vroom_dttm.h"
#include "vroom_fct.h"
#include "vroom_int.h"
#include "vroom_lgl.h"
#include "vroom_num.h"
#include "vroom_time.h"
#include <cpp11/sexp.hpp>
#include <sstream>
#include <thread>

[[cpp11::register]] void force_materialization(SEXP x) {
#ifdef HAS_ALTREP
  DATAPTR(x);
#endif
}

bool vroom_altrep(SEXP x) {
#ifdef HAS_ALTREP
  return R_altrep_inherits(x, vroom_chr::class_t) ||
         R_altrep_inherits(x, vroom_date::class_t) ||
         R_altrep_inherits(x, vroom_dbl::class_t) ||
         R_altrep_inherits(x, vroom_dttm::class_t) ||
         R_altrep_inherits(x, vroom_fct::class_t) ||
         R_altrep_inherits(x, vroom_int::class_t) ||
         // R_altrep_inherits(x, vroom_lgl::class_t) ||
         R_altrep_inherits(x, vroom_num::class_t) ||
         R_altrep_inherits(x, vroom_time::class_t);
#else
  return false;
#endif
}

[[cpp11::register]] SEXP vroom_materialize(SEXP x, bool replace) {
#ifdef HAS_ALTREP
  for (R_xlen_t i = 0; i < Rf_xlength(x); ++i) {

    SEXP elt = VECTOR_ELT(x, i);
    // First materialize all of the non-character vectors
    if (vroom_altrep(elt)) {
      DATAPTR(elt);
    }
  }

  // If replace replace the altrep vectors with their materialized
  // vectors
  if (replace) {
    for (R_xlen_t i = 0; i < Rf_xlength(x); ++i) {
      SEXP elt = PROTECT(VECTOR_ELT(x, i));
      if (vroom_altrep(elt)) {
        SET_VECTOR_ELT(x, i, R_altrep_data2(elt));
        R_set_altrep_data2(elt, R_NilValue);
      }
      UNPROTECT(1);
    }
  }

#endif

  return x;
}

[[cpp11::register]] std::string vroom_str_(cpp11::sexp x) {
  std::stringstream ss;

#ifdef HAS_ALTREP
  if (ALTREP(x)) {

    auto csym = CAR(ATTRIB(ALTREP_CLASS(x)));
    auto psym = CADR(ATTRIB(ALTREP_CLASS(x)));
    bool is_altrep = ALTREP(x);
    bool materialzied = R_altrep_data2(x) != R_NilValue;

    ss << std::boolalpha << "altrep:" << is_altrep << '\t'
       << "type:" << CHAR(PRINTNAME(psym)) << "::" << CHAR(PRINTNAME(csym));
    // We would have to dispatch to get the length of an object
    if (!Rf_isObject(x)) {
      ss << '\t' << "length:" << LENGTH(x);
    }
    ss << '\t' << "materialized:" << materialzied << '\n';
  }
#else
  if (false) {
  }
#endif
  else {
    ss << std::boolalpha << "altrep:" << false << '\t'
       << "type: " << Rf_type2char(TYPEOF(x));
    if (!Rf_isObject(x)) {
      ss << '\t' << "length:" << LENGTH(x);
    }
    ss << '\n';
  }

  return ss.str();
}
