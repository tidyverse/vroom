#pragma once

#include <cpp11/strings.hpp>

#include "altrep.h"

#include "r_utils.h"
#include "vroom_vec.h"

cpp11::strings read_chr(vroom_vec_info* info);
SEXP check_na(SEXP na, SEXP val);

#ifdef HAS_ALTREP

struct vroom_chr : vroom_vec {

public:
  static R_altrep_class_t class_t;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    // make a new altrep object of class `vroom_chr::class_t`
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
        "vroom_chr (len=%" R_PRIdXLEN_T ", materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING methods -----------------

  static SEXP Val(SEXP vec, R_xlen_t i) {
    auto& info = Info(vec);

    auto&& col = info.column;
    auto str = col->at(i);

    auto val =
        PROTECT(info.locale->encoder_.makeSEXP(str.begin(), str.end(), true));

    if (Rf_xlength(val) < str.end() - str.begin()) {
      auto&& itr = info.column->begin();
      info.errors->add_error(
          itr.index(), col->get_index(), "", "embedded null", itr.filename());
    }

    val = check_na(*info.na, val);

    info.errors->warn_for_errors();

    UNPROTECT(1);

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
    SPDLOG_TRACE("{0:x}: vroom_chr string_Elt {1}", (size_t)vec, i);

    return Val(vec, i);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    SPDLOG_TRACE("{0:x}: vroom_chr Materialize", (size_t)vec);
    auto out = read_chr(&Info(vec));
    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the info
    Finalize(R_altrep_data1(vec));

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return DATAPTR_RW(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("vroom_chr", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_chr>);

    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
};

#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_chr(DllInfo* dll);
