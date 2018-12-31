#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "read_normal.h"
#include "vroom_numeric.h"
#include "vroom_string.h"

#include <Rcpp.h>

enum column_type { character = 0, real = 1, integer = 2, logical = 3 };

inline int min(int a, int b) { return a < b ? a : b; }

CharacterVector read_column_names(
    std::shared_ptr<std::vector<size_t> > idx,
    mio::shared_mmap_source mmap,
    size_t num_columns,
    R_xlen_t skip) {

  CharacterVector nms(num_columns);

  for (size_t col = 0; col < num_columns; ++col) {
    // Set column header
    auto i = skip * num_columns + col;
    size_t cur_loc = (*idx)[i];
    size_t next_loc = (*idx)[i + 1] - 1;
    size_t len = next_loc - cur_loc;
    nms[col] = Rf_mkCharLenCE(mmap.data() + cur_loc, len, CE_UTF8);
  }

  return nms;
}

// [[Rcpp::export]]
SEXP vroom_(
    const std::string& filename,
    const char delim,
    RObject col_names,
    R_xlen_t skip,
    CharacterVector na,
    int num_threads) {

  std::shared_ptr<std::vector<size_t> > vroom_idx;
  size_t num_columns;
  mio::shared_mmap_source mmap;

  std::tie(vroom_idx, num_columns, mmap) =
      create_index(filename, delim, num_threads);

  List res(num_columns);

  if (col_names.sexp_type() == STRSXP) {
    res.attr("names") = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    res.attr("names") = read_column_names(vroom_idx, mmap, num_columns, skip);
    ++skip;
  }

  auto vroom = Rcpp::Environment::namespace_env("vroom");
  Rcpp::Function guess_type = vroom["guess_type"];

  auto num_rows = (vroom_idx->size() / num_columns) - skip;
  auto num_guess = min(num_rows, 100);
  // Rcpp::Rcout << num_guess << "\n";

  for (size_t i = 0; i < num_columns; ++i) {
    // Guess column type
    // TODO: guess from rows interspersed throughout the data
    CharacterVector col(num_guess);
    for (auto j = 0; j < num_guess; ++j) {
      size_t idx = (j + skip) * num_columns + i;
      size_t cur_loc = (*vroom_idx)[idx];
      size_t next_loc = (*vroom_idx)[idx + 1] - 1;
      size_t len = next_loc - cur_loc;
      col[j] = Rf_mkCharLenCE(mmap.data() + cur_loc, len, CE_UTF8);
      // Rcpp::Rcout << "i:" << i << " j:" << j << " cur_loc:" << cur_loc
      //<< " next_loc:" << next_loc << " " << col[j] << "\n";
    }
    auto col_type = INTEGER(
        guess_type(col, Named("guess_integer") = true, Named("na") = na))[0];
    // Rcpp::Rcout << "i:" << i << " type:" << col_type << "\n";

    switch (col_type) {
    case real:
      SET_VECTOR_ELT(
          res,
          i,
          vroom_real::Make(
              std::shared_ptr<std::vector<size_t> >(vroom_idx),
              mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip,
              num_threads));
      break;
    case integer:
      SET_VECTOR_ELT(
          res,
          i,
          vroom_int::Make(
              std::shared_ptr<std::vector<size_t> >(vroom_idx),
              mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip,
              num_threads));
      break;
    case logical:
      SET_VECTOR_ELT(
          res,
          i,
          read_lgl(
              std::shared_ptr<std::vector<size_t> >(vroom_idx),
              mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip,
              num_threads));
      break;
    case character:
      SET_VECTOR_ELT(
          res,
          i,
          vroom_string::Make(
              std::shared_ptr<std::vector<size_t> >(vroom_idx),
              mio::shared_mmap_source(mmap),
              i,
              num_columns,
              skip,
              na));
      break;
    }
  }

  return res;
}
