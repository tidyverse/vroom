#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "readidx_string.h"

// [[Rcpp::export]]
SEXP read_tsv_(const std::string& filename, R_xlen_t skip) {

  std::shared_ptr<std::vector<size_t> > idx;
  size_t columns;
  mio::shared_mmap_source mmap;

  std::tie(idx, columns, mmap) = create_index(filename);

  List res(columns);

  // Create column name vector
  CharacterVector nms(columns);

  for (size_t i = 0; i < columns; ++i) {
    size_t cur_loc = (*idx)[i];
    size_t next_loc = (*idx)[i + 1];
    size_t len = next_loc - cur_loc;
    nms[i] = Rf_mkCharLenCE(mmap.data() + cur_loc, len - 1, CE_UTF8);
    SET_VECTOR_ELT(
        res,
        i,
        readidx_string::Make(
            new std::shared_ptr<std::vector<size_t> >(idx),
            new mio::shared_mmap_source(mmap),
            i,
            columns,
            skip));
  }

  res.attr("names") = nms;

  return res;
}
