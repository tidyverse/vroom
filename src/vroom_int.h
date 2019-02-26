#include "altrep.h"

#include "vroom_vec.h"

#include <Rcpp.h>

// https://github.com/wch/r-source/blob/efed16c945b6e31f8e345d2f18e39a014d2a57ae/src/main/scan.c#L145-L157
int Strtoi(const char* nptr, int base) {
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

// Normal reading of integer vectors
Rcpp::IntegerVector read_int(vroom_vec_info* info) {

  R_xlen_t n = info->idx->num_rows();

  Rcpp::IntegerVector out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t id) {
        size_t i = start;
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          out[i++] = Strtoi(str.c_str(), 10);
        }
      },
      info->num_threads);

  return out;
}

#ifdef HAS_ALTREP

class vroom_int : public vroom_vec {

public:
  static R_altrep_class_t class_t;

  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

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
        "vroom_int (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTINTEGER methods -----------------

  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    auto out = read_int(&Info(vec));
    R_set_altrep_data2(vec, out);

    return out;
  }

  // the element at the index `i`
  static int int_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return INTEGER(data2)[i];
    }

    auto str = vroom_vec::Get(vec, i);

    return Strtoi(str.c_str(), 10);
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above
  static void Init(DllInfo* dll) {
    vroom_int::class_t = R_make_altinteger_class("vroom_int", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);

    // altinteger
    R_set_altinteger_Elt_method(class_t, int_Elt);
  }
};

R_altrep_class_t vroom_int::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_vroom_int(DllInfo* dll) { vroom_int::Init(dll); }

#else
void init_vroom_int(DllInfo* dll) {}
#endif
