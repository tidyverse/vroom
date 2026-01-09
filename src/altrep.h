#include <cpp11/R.hpp>

#include <R_ext/Rdynload.h>

extern "C" {
#include <R_ext/Altrep.h>
}

// Backport DATAPTR_RW for R < 4.6.0 (as recommended in Writing R Extensions)
#if R_VERSION < R_Version(4, 6, 0)
#define DATAPTR_RW(x) DATAPTR(x)
#endif
