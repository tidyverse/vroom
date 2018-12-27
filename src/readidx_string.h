#include "altrep.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
#pragma clang diagnostic pop

#include "readidx_vec.h"

using namespace Rcpp;

// inspired by Luke Tierney and the R Core Team
// https://github.com/ALTREP-examples/Rpkg-mutable/blob/master/src/mutable.c
// and Romain François
// https://purrple.cat/blog/2018/10/21/lazy-abs-altrep-cplusplus/ and Dirk

struct readidx_string : readidx_vec {

public:
  static R_altrep_class_t class_t;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(
      std::shared_ptr<std::vector<size_t> >* offsets,
      mio::shared_mmap_source* mmap,
      R_xlen_t column,
      R_xlen_t num_columns,
      R_xlen_t skip) {

    // `out` and `xp` needs protection because R_new_altrep allocates
    SEXP out = PROTECT(Rf_allocVector(VECSXP, 5));

    SEXP idx_xp = PROTECT(R_MakeExternalPtr(offsets, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(idx_xp, readidx_string::Finalize_Idx, TRUE);

    SEXP mmap_xp = PROTECT(R_MakeExternalPtr(mmap, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(mmap_xp, readidx_string::Finalize_Mmap, TRUE);

    SET_VECTOR_ELT(out, 0, idx_xp);
    SET_VECTOR_ELT(out, 1, mmap_xp);
    SET_VECTOR_ELT(out, 2, Rf_ScalarReal(column));
    SET_VECTOR_ELT(out, 3, Rf_ScalarReal(num_columns));
    SET_VECTOR_ELT(out, 4, Rf_ScalarReal(skip));

    // make a new altrep object of class `readidx_string::class_t`
    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(3);

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
        "readidx_string (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING methods -----------------

  // the element at the index `i`
  //
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return STRING_ELT(data2, i);
    }
    auto sep_locs = Idx(vec);
    auto column = Column(vec);
    auto num_columns = Num_Columns(vec);
    auto skip = Skip(vec);

    size_t idx = (i + skip) * num_columns + column;
    size_t cur_loc = (*sep_locs)[idx];
    size_t next_loc = (*sep_locs)[idx + 1] - 1;
    size_t len = next_loc - cur_loc;
    // Rcerr << cur_loc << ':' << next_loc << ':' << len << '\n';

    mio::shared_mmap_source* mmap = Mmap(vec);

    return Rf_mkCharLenCE(mmap->data() + cur_loc, len, CE_UTF8);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    // allocate a standard character vector for data2
    R_xlen_t n = Length(vec);
    data2 = PROTECT(Rf_allocVector(STRSXP, n));

    auto sep_locs = Idx(vec);
    auto column = Column(vec);
    auto num_columns = Num_Columns(vec);
    auto skip = Skip(vec);

    mio::shared_mmap_source* mmap = Mmap(vec);

    for (R_xlen_t i = 0; i < n; ++i) {
      size_t idx = (i + skip) * num_columns + column;
      size_t cur_loc = (*sep_locs)[idx];
      size_t next_loc = (*sep_locs)[idx + 1] - 1;
      size_t len = next_loc - cur_loc;

      auto val = Rf_mkCharLenCE(mmap->data() + cur_loc, len, CE_UTF8);
      SET_STRING_ELT(data2, i, val);
    }

    R_set_altrep_data2(vec, data2);
    UNPROTECT(1);
    return data2;
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("readidx_string", "readidx", dll);

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

R_altrep_class_t readidx_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_readidx_string(DllInfo* dll) { readidx_string::Init(dll); }
