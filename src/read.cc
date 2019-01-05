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
    size_t skip) {

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
    RObject file,
    const char delim,
    RObject col_names,
    size_t skip,
    CharacterVector na,
    size_t num_threads) {

  std::shared_ptr<std::vector<size_t> > vroom_idx;
  size_t num_columns;
  mio::shared_mmap_source mmap;

  Rcpp::CharacterVector tempfile;

  bool is_connection = file.sexp_type() != STRSXP;

  std::string filename;

  if (is_connection) {
    tempfile = as<Rcpp::Function>(Rcpp::Environment::base_env()["tempfile"])();
    filename = CHAR(STRING_ELT(tempfile, 0));
    std::tie(vroom_idx, num_columns, mmap) =
        create_index_connection(file, filename, delim, 1024 * 1024);
  } else {
    filename = CHAR(STRING_ELT(file, 0));
    std::tie(vroom_idx, num_columns, mmap) =
        create_index(filename.c_str(), delim, num_threads);
  }

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
  auto guess_num = min(num_rows, 100);

  // Guess based on values throughout the data
  auto guess_step = num_rows / guess_num;

  for (size_t col = 0; col < num_columns; ++col) {
    CharacterVector col_vals(guess_num);
    for (auto j = 0; j < guess_num; ++j) {
      auto row = j * guess_step + skip;
      size_t idx = row * num_columns + col;
      size_t cur_loc = (*vroom_idx)[idx];
      size_t next_loc = (*vroom_idx)[idx + 1] - 1;
      size_t len = next_loc - cur_loc;
      col_vals[j] = Rf_mkCharLenCE(mmap.data() + cur_loc, len, CE_UTF8);
      // Rcpp::Rcout << "row:" << row << " col:" << col << " cur_loc:" <<
      // cur_loc
      //<< " next_loc:" << next_loc << " " << col_vals[j] << "\n";
    }
    auto col_type = INTEGER(guess_type(
        col_vals, Named("guess_integer") = true, Named("na") = na))[0];
    // Rcpp::Rcout << "i:" << i << " type:" << col_type << "\n";

    // This is deleted in finalizes when the vectors are GC'd by R
    auto info = new vroom_vec_info{vroom_idx,
                                   mmap,
                                   filename,
                                   is_connection,
                                   col,
                                   num_columns,
                                   skip,
                                   num_threads,
                                   std::make_shared<Rcpp::CharacterVector>(na)};

    switch (col_type) {
    case real:
      SET_VECTOR_ELT(res, col, vroom_real::Make(info));
      break;
    case integer:
      SET_VECTOR_ELT(res, col, vroom_int::Make(info));
      break;
    case logical:
      SET_VECTOR_ELT(res, col, read_lgl(info));
      break;
    // case factor:
    //   SET_VECTOR_ELT(
    //       res,
    //       col,
    //       read_fctr(
    //           std::shared_ptr<std::vector<size_t> >(vroom_idx),
    //           mmap,
    //           col,
    //           num_columns,
    //           skip,
    //           num_threads));
    //   break;
    case character:
      SET_VECTOR_ELT(res, col, vroom_string::Make(info));
      break;
    }
  }

  res.attr("filename") = filename;

  return res;
}
