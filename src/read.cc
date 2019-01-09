#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "index_connection.h"
#include "read_normal.h"
#include "vroom_numeric.h"
#include "vroom_string.h"

#include <Rcpp.h>

enum column_type { character = 0, real = 1, integer = 2, logical = 3 };

inline int min(int a, int b) { return a < b ? a : b; }

CharacterVector read_column_names(std::shared_ptr<vroom::index> idx) {
  CharacterVector nms(idx->num_columns());

  auto col = 0;
  for (const auto& loc : idx->header()) {
    nms[col++] = Rf_mkCharLenCE(loc.begin, loc.end - loc.begin, CE_UTF8);
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

  Rcpp::CharacterVector tempfile;

  bool is_connection = file.sexp_type() != STRSXP;

  std::string filename;

  // if (is_connection) {
  // tempfile =
  // as<Rcpp::Function>(Rcpp::Environment::base_env()["tempfile"])(); filename
  // = CHAR(STRING_ELT(tempfile, 0)); auto idx = vroom::index_connection(file,
  // filename, delim, 1024 * 1024);
  //} else {
  filename = CHAR(STRING_ELT(file, 0));
  //}

  bool has_header =
      col_names.sexp_type() == STRSXP ||
      (col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]);
  auto idx = std::make_shared<vroom::index>(
      filename.c_str(), delim, has_header, skip, num_threads);

  auto num_columns = idx->num_columns();

  List res(num_columns);

  if (col_names.sexp_type() == STRSXP) {
    res.attr("names") = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    res.attr("names") = read_column_names(idx);
  }

  auto vroom = Rcpp::Environment::namespace_env("vroom");
  Rcpp::Function guess_type = vroom["guess_type"];

  auto num_rows = idx->num_rows();

  auto guess_num = min(num_rows, 100);

  // Guess based on values throughout the data
  auto guess_step = num_rows / guess_num;

  for (size_t col = 0; col < num_columns; ++col) {
    CharacterVector col_vals(guess_num);
    for (auto j = 0; j < guess_num; ++j) {
      auto row = j * guess_step;
      auto loc = idx->get(row, col);
      col_vals[j] = Rf_mkCharLenCE(loc.begin, loc.end - loc.begin, CE_UTF8);
    }
    auto col_type = INTEGER(guess_type(
        col_vals, Named("guess_integer") = true, Named("na") = na))[0];
    // Rcpp::Rcout << "i:" << i << " type:" << col_type << "\n";

    // This is deleted in finalizes when the vectors are GC'd by R
    auto info = new vroom_vec_info{
        idx, col, num_threads, std::make_shared<Rcpp::CharacterVector>(na)};

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
