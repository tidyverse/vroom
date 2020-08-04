#pragma once

#include <cpp11/doubles.hpp>

#include "vroom.h"
#include "vroom_dttm.h"

using namespace vroom;

double parse_time(
    const string& str, DateTimeParser& parser, const std::string& format);
cpp11::doubles read_time(vroom_vec_info* info);

#ifdef HAS_ALTREP

class vroom_time : public vroom_dttm {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    vroom_dttm_info* dttm_info = new vroom_dttm_info;
    dttm_info->info = info;
    dttm_info->parser =
        std::unique_ptr<DateTimeParser>(new DateTimeParser(info->locale.get()));

    SEXP out = PROTECT(R_MakeExternalPtr(dttm_info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_dttm::Finalize, FALSE);

    cpp11::sexp res = R_new_altrep(class_t, out, R_NilValue);

    res.attr("class") = {"hms", "difftime"};
    res.attr("units") = "secs";

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_time (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // the element at the index `i`
  static double time_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return REAL(data2)[i];
    }

    auto str = Get(vec, i);
    auto inf = Info(vec);

    return parse_time(str, *inf->parser, inf->info->format);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto inf = Info(vec);

    auto out = read_time(inf->info);

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
    vroom_time::class_t = R_make_altreal_class("vroom_time", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_time>);

    // altreal
    R_set_altreal_Elt_method(class_t, time_Elt);
  }
};
#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_time(DllInfo* dll);
