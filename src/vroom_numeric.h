#include "altrep.h"

#include "parallel.h"
#include "vroom_vec.h"

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include <Rcpp.h>

using namespace Rcpp;

// inspired by Luke Tierney and the R Core Team
// https://github.com/ALTREP-examples/Rpkg-mutable/blob/master/src/mutable.c
// and Romain François
// https://purrple.cat/blog/2018/10/21/lazy-abs-altrep-cplusplus/ and Dirk

template <class TYPE> class vroom_numeric : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    // make a new altrep object of class `vroom_numeric::class_t`
    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(1);

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
        "vroom_numeric (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTREAL methods -----------------

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    // allocate a standard numeric vector for data2
    R_xlen_t n = Length(vec);
    TYPE out(n);

    auto p = out.begin();

    auto info = Info(vec);

    parallel_for(
        n,
        [&](int start, int end, int id) {
          // Need to copy to a temp buffer since we have no way to tell strtod
          // how long the buffer is.
          char buf[128];

          size_t i = start;
          for (const auto& str :
               info.idx->get_column(info.column, start, end)) {
            std::copy(str.begin(), str.end(), buf);
            buf[str.length()] = '\0';

            p[i++] = R_strtod(buf, NULL);
          }
        },
        info.num_threads,
        true);

    R_set_altrep_data2(vec, out);

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
};

typedef vroom_numeric<NumericVector> vroom_real;

template <> R_altrep_class_t vroom_real::class_t{};

// the element at the index `i`
double real_Elt(SEXP vec, R_xlen_t i) {
  SEXP data2 = R_altrep_data2(vec);
  if (data2 != R_NilValue) {
    return REAL(data2)[i];
  }

  auto str = vroom_vec::Get(vec, i);

  return R_strtod(str.c_str(), NULL);
}

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_real(DllInfo* dll) {
  vroom_real::class_t = R_make_altreal_class("vroom_real", "vroom", dll);

  // altrep
  R_set_altrep_Length_method(vroom_real::class_t, vroom_real::Length);
  R_set_altrep_Inspect_method(vroom_real::class_t, vroom_real::Inspect);

  // altvec
  R_set_altvec_Dataptr_method(vroom_real::class_t, vroom_real::Dataptr);
  R_set_altvec_Dataptr_or_null_method(
      vroom_real::class_t, vroom_real::Dataptr_or_null);

  // altreal
  R_set_altreal_Elt_method(vroom_real::class_t, real_Elt);
}

typedef vroom_numeric<IntegerVector> vroom_int;

template <> R_altrep_class_t vroom_int::class_t{};

// https://github.com/wch/r-source/blob/efed16c945b6e31f8e345d2f18e39a014d2a57ae/src/main/scan.c#L145-L157
static int Strtoi(const char* nptr, int base) {
  long res;
  char* endp;

  errno = 0;
  res = strtol(nptr, &endp, base);
  if (*endp != '\0')
    res = NA_INTEGER;
  /* next can happen on a 64-bit platform */
  if (res > INT_MAX || res < INT_MIN)
    res = NA_INTEGER;
  if (errno == ERANGE)
    res = NA_INTEGER;
  return (int)res;
}

// the element at the index `i`
int int_Elt(SEXP vec, R_xlen_t i) {
  SEXP data2 = R_altrep_data2(vec);
  if (data2 != R_NilValue) {
    return INTEGER(data2)[i];
  }

  auto str = vroom_vec::Get(vec, i);

  return Strtoi(str.c_str(), 10);
}

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_int(DllInfo* dll) {
  vroom_int::class_t = R_make_altinteger_class("vroom_int", "vroom", dll);

  // altrep
  R_set_altrep_Length_method(vroom_int::class_t, vroom_int::Length);
  R_set_altrep_Inspect_method(vroom_int::class_t, vroom_int::Inspect);

  // altvec
  R_set_altvec_Dataptr_method(vroom_int::class_t, vroom_int::Dataptr);
  R_set_altvec_Dataptr_or_null_method(
      vroom_int::class_t, vroom_int::Dataptr_or_null);

  // altinteger
  R_set_altinteger_Elt_method(vroom_int::class_t, int_Elt);
}

// Altrep for Logical vectors does not yet exist
// typedef vroom_numeric<LogicalVector> vroom_lgl;

// template <> R_altrep_class_t vroom_lgl::class_t{};

//// the element at the index `i`
// int lgl_Elt(SEXP vec, R_xlen_t i) {
// SEXP data2 = R_altrep_data2(vec);
// if (data2 != R_NilValue) {
// return INTEGER(data2)[i];
//}
// char buf[128];
// vroom_lgl::buf_Elt(vec, i, buf);

// return Rf_StringTrue(buf);
//}

//// Called the package is loaded (needs Rcpp 0.12.18.3)
//// [[Rcpp::init]]
// void init_vroom_lgl(DllInfo* dll) {
// vroom_lgl::class_t = R_make_altinteger_class("vroom_lgl", "vroom",
// dll);

//// altrep
// R_set_altrep_Length_method(vroom_lgl::class_t, vroom_lgl::Length);
// R_set_altrep_Inspect_method(vroom_lgl::class_t, vroom_lgl::Inspect);

//// altvec
// R_set_altvec_Dataptr_method(vroom_lgl::class_t, vroom_lgl::Dataptr);
// R_set_altvec_Dataptr_or_null_method(
// vroom_lgl::class_t, vroom_lgl::Dataptr_or_null);

//// altinteger
// R_set_altinteger_Elt_method(vroom_lgl::class_t, lgl_Elt);
//}
