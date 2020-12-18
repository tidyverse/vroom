#include <cpp11/integers.hpp>
#include <cpp11/strings.hpp>

#include "altrep.h"
#include "vroom.h"
#include "vroom_vec.h"

#include <unordered_map>

using namespace vroom;

cpp11::integers
read_fct_explicit(vroom_vec_info* info, cpp11::strings levels, bool ordered);

cpp11::integers read_fct_implicit(vroom_vec_info* info, bool include_na);

int parse_factor(
    const char* begin,
    const char* end,
    const std::unordered_map<SEXP, size_t>& level_map,
    LocaleInfo& locale);

#ifdef HAS_ALTREP

struct vroom_factor_info {
  vroom_vec_info* info;
  std::unordered_map<SEXP, size_t> levels;
};

struct vroom_fct : vroom_vec {

public:
  static R_altrep_class_t class_t;

  // Make an altrep object of class `vroom_factor::class_t`
  static SEXP Make(vroom_vec_info* info, cpp11::strings levels, bool ordered) {

    vroom_factor_info* fct_info = new vroom_factor_info;
    fct_info->info = info;

    for (auto i = 0; i < levels.size(); ++i) {
      if (levels[i] == NA_STRING) {
        for (const auto& str : *info->na) {
          fct_info->levels[str] = i + 1;
        }
      } else {
        fct_info->levels[levels[i]] = i + 1;
      }
    }

    SEXP out = PROTECT(R_MakeExternalPtr(fct_info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, Finalize, FALSE);

    // make a new altrep object of class `vroom_factor::class_t`
    cpp11::sexp res = R_new_altrep(class_t, out, R_NilValue);

    res.attr("levels") = static_cast<SEXP>(levels);
    if (ordered) {
      res.attr("class") = {"ordered", "factor"};
    } else {
      res.attr("class") = "factor";
    }

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // ALTREP methods -------------------

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_factor (len=%d, materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  static void Finalize(SEXP ptr) {
    if (ptr == nullptr || R_ExternalPtrAddr(ptr) == nullptr) {
      return;
    }
    auto info_p = static_cast<vroom_factor_info*>(R_ExternalPtrAddr(ptr));
    delete info_p->info;
    delete info_p;
    info_p = nullptr;
    R_ClearExternalPtr(ptr);
  }

  static inline vroom_factor_info& Info(SEXP x) {
    return *static_cast<vroom_factor_info*>(
        R_ExternalPtrAddr(R_altrep_data1(x)));
  }

  static inline R_xlen_t Length(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return Rf_xlength(data2);
    }

    auto inf = Info(vec);
    return inf.info->column->size();
  }

  static inline string Get(SEXP vec, R_xlen_t i) {
    auto inf = Info(vec);
    return inf.info->column->at(i);
  }

  // ALTSTRING methods -----------------

  static int Val(SEXP vec, R_xlen_t i) {
    auto info = Info(vec);

    double out = parse_value<double>(
        info.info->column->begin() + i,
        info.info->column,
        [&](const char* begin, const char* end) -> double {
          return parse_factor(begin, end, info.levels, *info.info->locale);
        },
        info.info->errors,
        "value in level set",
        *info.info->na);

    info.info->errors->warn_for_errors();

    return out;
  }

  // the element at the index `i`
  //
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static int factor_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return INTEGER(data2)[i];
    }

    return Val(vec, i);
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    // allocate a standard character vector for data2
    R_xlen_t n = Length(vec);
    cpp11::writable::integers out(n);

    for (R_xlen_t i = 0; i < n; ++i) {
      out[i] = Val(vec, i);
    }

    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the info
    Finalize(R_altrep_data1(vec));

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return STDVEC_DATAPTR(Materialize(vec));
  }

  static SEXP Extract_subset(SEXP x, SEXP indx, SEXP) {
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

    cpp11::sexp x_(x);

    auto idx = get_subset_index(indx, Rf_xlength(x));

    if (idx == nullptr) {
      return nullptr;
    }

    auto inf = Info(x);

    auto info = new vroom_vec_info{
        inf.info->column->subset(idx),
        inf.info->num_threads,
        inf.info->na,
        inf.info->locale,
        inf.info->errors,
        inf.info->format};

    bool is_ordered = Rf_inherits(x_, "ordered");
    return Make(info, cpp11::strings(x_.attr("levels")), is_ordered);
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altinteger_class("vroom_fct", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset);

    // altinteger
    R_set_altinteger_Elt_method(class_t, factor_Elt);
  }
};
#endif

// Called the package is loaded
[[cpp11::init]] void init_vroom_fct(DllInfo* dll);
