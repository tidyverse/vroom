#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "readidx_string.h"

// [[Rcpp::export]]
SEXP read_tsv_(const std::string& filename) {

  std::shared_ptr<std::vector<size_t> > idx;
  size_t columns;
  mio::shared_mmap_source mmap;

  std::tie(idx, columns, mmap) = create_index(filename);

  SEXP res = PROTECT(Rf_allocVector(VECSXP, columns));

  for (size_t i = 0; i < columns; ++i) {
    SET_VECTOR_ELT(
        res,
        i,
        readidx_string::Make(
            new std::shared_ptr<std::vector<size_t> >(idx),
            new mio::shared_mmap_source(mmap),
            i,
            columns));
  }

  UNPROTECT(1);

  return res;
}
