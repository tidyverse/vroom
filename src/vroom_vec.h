#pragma once

#include <cpp11/integers.hpp>
#include <cpp11/strings.hpp>

#include "LocaleInfo.h"
#include "altrep.h"
#include "index_collection.h"
#include "vroom_errors.h"

using namespace vroom;

struct vroom_vec_info {
  std::shared_ptr<vroom::index::column> column;
  size_t num_threads;
  std::shared_ptr<cpp11::strings> na;
  std::shared_ptr<LocaleInfo> locale;
  std::shared_ptr<vroom_errors> errors;
  std::string format;
};

namespace vroom {

inline bool is_explicit_na(SEXP na, const char* begin, const char* end) {
  R_xlen_t n = end - begin;
  for (R_xlen_t i = 0; i < Rf_xlength(na); ++i) {
    SEXP str = STRING_ELT(na, i);
    R_xlen_t str_n = Rf_xlength(str);
    const char* v = CHAR(STRING_ELT(na, i));
    if (n == str_n && strncmp(v, begin, n) == 0) {
      return true;
    }
  }
  return false;
}

template <typename V, typename F, typename I, typename C>
static auto parse_value(
    const I& itr,
    const C& col,
    F f,
    std::shared_ptr<vroom_errors>& errors,
    const char* expected,
    SEXP na) -> V {
  auto str = *itr;
  V out = f(str.begin(), str.end());
  if (cpp11::is_na(out) && !is_explicit_na(na, str.begin(), str.end())) {
    errors->add_error(
        itr.index(),
        col->get_index(),
        expected,
        std::string(str.begin(), str.end() - str.begin()),
        itr.filename());
  }
  return out;
}
} // namespace vroom

#ifdef HAS_ALTREP

class vroom_vec {

public:
  // finalizer for the external pointer
  static void Finalize(SEXP ptr) {
    if (ptr == nullptr || R_ExternalPtrAddr(ptr) == nullptr) {
      return;
    }
    auto info_p = static_cast<vroom_vec_info*>(R_ExternalPtrAddr(ptr));
    delete info_p;
    info_p = nullptr;
    R_ClearExternalPtr(ptr);
  }

  static inline vroom_vec_info& Info(SEXP x) {
    return *static_cast<vroom_vec_info*>(R_ExternalPtrAddr(R_altrep_data1(x)));
  }

  // ALTREP methods -------------------

  // The length of the object
  static inline R_xlen_t Length(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return Rf_xlength(data2);
    }

    auto& inf = Info(vec);
    return inf.column->size();
  }

  static inline string Get(SEXP vec, R_xlen_t i) {
    auto& inf = Info(vec);
    return inf.column->at(i);
  }

  // ALTVec methods -------------------

  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue)
      return nullptr;

    return STDVEC_DATAPTR(data2);
  }

  static std::shared_ptr<std::vector<size_t>>
  get_subset_index(SEXP indx, R_xlen_t x_len) {
    auto idx = std::make_shared<std::vector<size_t>>();
    R_xlen_t n = Rf_xlength(indx);
    idx->reserve(n);

    int i_val;
    double d_val;

    for (R_xlen_t i = 0; i < n; ++i) {
      switch (TYPEOF(indx)) {
      case INTSXP:
        i_val = INTEGER_ELT(indx, i);
        if (i_val == NA_INTEGER || i_val > x_len) {
          return nullptr;
        }
        idx->push_back(i_val - 1);
        break;
      case REALSXP:
        d_val = REAL_ELT(indx, i);
        if (ISNA(d_val) || d_val > x_len) {
          return nullptr;
        }
        idx->push_back(d_val - 1);
        break;
      default:
        Rf_error("Invalid index");
      }
    }
    return idx;
  }

  template <typename T> static SEXP Extract_subset(SEXP x, SEXP indx, SEXP) {
    SEXP data2 = R_altrep_data2(x);
    // If the vector is already materialized, just fall back to the default
    // implementation
    if (data2 != R_NilValue) {
      return nullptr;
    }

    // If there are no indices to subset fall back to default implementation.
    if (Rf_xlength(indx) == 0) {
      return nullptr;
    }

    vroom_vec_info* info;

    // This block is here to avoid a false positive from rchck
    {
      auto& inf = Info(x);

      auto idx = get_subset_index(indx, Rf_xlength(x));

      if (idx == nullptr) {
        return nullptr;
      }

      info = new vroom_vec_info{
          inf.column->subset(idx),
          inf.num_threads,
          inf.na,
          inf.locale,
          inf.errors,
          inf.format};
    }

    return T::Make(info);
  }
};

#endif
