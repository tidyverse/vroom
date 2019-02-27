#include <Rcpp.h>

#include "altrep.h"

//[[Rcpp::export]]
void force_materialization(SEXP x) {
#ifdef HAS_ALTREP
  DATAPTR(x);
#endif
}
