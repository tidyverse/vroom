#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "readidx_real.h"
#include "readidx_string.h"

enum column_type { character = 0, real = 1 };

inline int min(int a, int b) { return a < b ? a : b; }

// [[Rcpp::export]]
SEXP read_tsv_(const std::string& filename, R_xlen_t skip, int num_threads) {

  std::shared_ptr<std::vector<size_t> > readidx_idx;
  size_t num_columns;
  mio::shared_mmap_source mmap;

  std::tie(readidx_idx, num_columns, mmap) =
      create_index(filename, num_threads);

  List res(num_columns);

  // Create column name vector
  CharacterVector nms(num_columns);

  Rcpp::Environment readidx("package:readidx");
  Rcpp::Function guess_type = readidx["guess_type"];

  auto num_rows = (readidx_idx->size() / num_columns) - skip;
  auto num_guess = min(num_rows, 100);
  // Rcpp::Rcout << num_guess << "\n";

  for (size_t i = 0; i < num_columns; ++i) {
    // Set column header
    size_t cur_loc = (*readidx_idx)[i];
    size_t next_loc = (*readidx_idx)[i + 1] - 1;
    size_t len = next_loc - cur_loc;
    nms[i] = Rf_mkCharLenCE(mmap.data() + cur_loc, len, CE_UTF8);

    // Guess column type
    CharacterVector col(num_guess);
    for (auto j = 0; j < num_guess; ++j) {
      size_t idx = (j + skip) * num_columns + i;
      size_t cur_loc = (*readidx_idx)[idx];
      size_t next_loc = (*readidx_idx)[idx + 1] - 1;
      size_t len = next_loc - cur_loc;
      col[j] = Rf_mkCharLenCE(mmap.data() + cur_loc, len, CE_UTF8);
      // Rcpp::Rcout << "i:" << i << " j:" << j << " cur_loc:" << cur_loc
      //<< " next_loc:" << next_loc << " " << col[j] << "\n";
    }
    auto col_type = INTEGER(guess_type(col))[0];
    // Rcpp::Rcout << "i:" << i << " type:" << col_type << "\n";

    switch (col_type) {
    case real:
      SET_VECTOR_ELT(
          res,
          i,
          readidx_real::Make(
              new std::shared_ptr<std::vector<size_t> >(readidx_idx),
              new mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip,
              num_threads));
      break;
    case character:
      SET_VECTOR_ELT(
          res,
          i,
          readidx_string::Make(
              new std::shared_ptr<std::vector<size_t> >(readidx_idx),
              new mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip));
      break;
    }
  }

  res.attr("names") = nms;

  return res;
}
