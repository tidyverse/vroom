#pragma once

#include "altrep.h"

#include "index_collection.h"

#include "LocaleInfo.h"

#include <Rcpp.h>

using namespace vroom;

struct vroom_vec_info {
  std::shared_ptr<vroom::index::column> column;
  size_t num_threads;
  std::shared_ptr<Rcpp::CharacterVector> na;
  std::shared_ptr<LocaleInfo> locale;
  std::string format;
};

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

  template <typename T>
  static SEXP Extract_subset(SEXP x, SEXP indx, SEXP call) {
    SEXP data2 = R_altrep_data2(x);
    // If the vector is already materialized, just fall back to the default
    // implementation
    if (data2 != R_NilValue) {
      return nullptr;
    }

    vroom_vec_info* info;

    // This block is here to avoid a false positive from rchck
    {
      auto& inf = Info(x);

      Rcpp::IntegerVector in(indx);

      auto idx = std::make_shared<std::vector<size_t> >();

      std::transform(in.begin(), in.end(), std::back_inserter(*idx), [](int i) {
        return i - 1;
      });

      info = new vroom_vec_info{inf.column->subset(idx),
                                inf.num_threads,
                                inf.na,
                                inf.locale,
                                inf.format};
    }

    return T::Make(info);
  }
};

#endif
