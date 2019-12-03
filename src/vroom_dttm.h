#pragma once

#include "DateTimeParser.h"
#include "parallel.h"
#include "vroom.h"
#include "vroom_vec.h"

#ifdef VROOM_LOG
#include "spdlog/spdlog.h"
#endif

using namespace vroom;

double parse_dttm(
    const string& str, DateTimeParser& parser, const std::string& format);

Rcpp::NumericVector read_dttm(vroom_vec_info* info);

#ifdef HAS_ALTREP

/* Vroom dttm */

struct vroom_dttm_info {
  vroom_vec_info* info;
  std::unique_ptr<DateTimeParser> parser;
};

class vroom_dttm : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    vroom_dttm_info* dttm_info = new vroom_dttm_info;
    dttm_info->info = info;
    dttm_info->parser =
        std::unique_ptr<DateTimeParser>(new DateTimeParser(info->locale.get()));

    SEXP out = PROTECT(R_MakeExternalPtr(dttm_info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_dttm::Finalize, FALSE);

    Rcpp::RObject res = R_new_altrep(class_t, out, R_NilValue);

    res.attr("class") = Rcpp::CharacterVector::create("POSIXct", "POSIXt");
    res.attr("tzone") = info->locale->tz_;

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

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
        "vroom_dttm (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  static void Finalize(SEXP ptr) {
    if (ptr == nullptr || R_ExternalPtrAddr(ptr) == nullptr) {
      return;
    }
    auto info_p = static_cast<vroom_dttm_info*>(R_ExternalPtrAddr(ptr));
    delete info_p->info;
    delete info_p;
    info_p = nullptr;
    R_ClearExternalPtr(ptr);
  }

  static inline vroom_dttm_info* Info(SEXP x) {
    return static_cast<vroom_dttm_info*>(R_ExternalPtrAddr(R_altrep_data1(x)));
  }

  static inline R_xlen_t Length(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return Rf_xlength(data2);
    }

    auto inf = Info(vec);
    return inf->info->column->size();
  }

  static inline string Get(SEXP vec, R_xlen_t i) {
    auto inf = Info(vec);
    return inf->info->column->at(i);
  }

  // ALTREAL methods -----------------

  // the element at the index `i`
  static double dttm_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return REAL(data2)[i];
    }

    auto str = Get(vec, i);
    auto inf = Info(vec);

    return parse_dttm(str, *inf->parser, inf->info->format);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto inf = Info(vec);

    auto out = read_dttm(inf->info);

    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the info
    Finalize(R_altrep_data1(vec));

    return out;
  }

  template <typename T>
  static SEXP Extract_subset(SEXP x, SEXP indx, SEXP call) {
    SEXP data2 = R_altrep_data2(x);
    // If the vector is already materialized, just fall back to the default
    // implementation
    if (data2 != R_NilValue) {
      return nullptr;
    }

    Rcpp::IntegerVector in(indx);

    auto idx = std::make_shared<std::vector<size_t> >();

    std::transform(in.begin(), in.end(), std::back_inserter(*idx), [](int i) {
      return i - 1;
    });

    auto inf = Info(x);

    auto info = new vroom_vec_info{inf->info->column->subset(idx),
                                   inf->info->num_threads,
                                   inf->info->na,
                                   inf->info->locale,
                                   inf->info->format};

    return T::Make(info);
  }

  static SEXP Duplicate(SEXP x, Rboolean deep) {
    SEXP data2 = R_altrep_data2(x);

    SPDLOG_TRACE(
        "Duplicate dttm: deep = {0}, materialized={1}",
        deep,
        data2 != R_NilValue);

    /* If deep or already materialized, do the default behavior */
    if (deep || data2 != R_NilValue) {
      return nullptr;
    }

    /* otherwise copy the metadata */

    auto inf = Info(x);

    auto info = new vroom_vec_info{inf->info->column,
                                   inf->info->num_threads,
                                   inf->info->na,
                                   inf->info->locale,
                                   inf->info->format};
    return Make(info);
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_dttm::class_t = R_make_altreal_class("vroom_dttm", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);
    R_set_altrep_Duplicate_method(class_t, Duplicate);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_dttm>);

    // altreal
    R_set_altreal_Elt_method(class_t, dttm_Elt);
  }
};

#endif

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_dttm(DllInfo* dll);
