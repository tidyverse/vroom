#include <Rcpp.h>

//[[Rcpp::export]]
void force_materialization(SEXP x) { DATAPTR(x); }
