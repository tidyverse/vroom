#include "Rcpp.h"

#if R_VERSION >= R_Version(3, 5, 0)
#define HAS_ALTREP
#endif

#ifdef HAS_ALTREP
#if R_VERSION < R_Version(3, 6, 0)

// workaround because R's <R_ext/Altrep.h> not so conveniently uses `class`
// as a variable name, and C++ is not happy about that
//
// SEXP R_new_altrep(R_altrep_class_t class, SEXP data1, SEXP data2);
//

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wkeyword-macro"
#define class klass
# pragma clang diagnostic pop
#else
#define class klass
#endif
// clang-format on

// Because functions declared in <R_ext/Altrep.h> have C linkage
extern "C" {
#include <R_ext/Altrep.h>
}

// undo the workaround
#undef class

#else
extern "C" {
#include <R_ext/Altrep.h>
}
#endif
#endif
