#pragma once

#include <cpp11/R.hpp>

#include <R_ext/Rdynload.h>

extern "C" {
#include <R_ext/Altrep.h>
}

// Backport DATAPTR_RW for R < 4.6.0 (as recommended in Writing R Extensions)
#if R_VERSION < R_Version(4, 6, 0)
#define DATAPTR_RW(x) DATAPTR(x)
#endif

// Backport ALTREP class accessors for R < 4.6.0
// These were introduced in R-devel:
// https://github.com/wch/r-source/commit/37eb29515a83672b53743cba11e88167ca063d06
#if R_VERSION < R_Version(4, 6, 0)
inline SEXP R_altrep_class_name(SEXP x) {
  return ALTREP(x) ? CAR(ATTRIB(ALTREP_CLASS(x))) : R_NilValue;
}
inline SEXP R_altrep_class_package(SEXP x) {
  return ALTREP(x) ? CADR(ATTRIB(ALTREP_CLASS(x))) : R_NilValue;
}
#endif
