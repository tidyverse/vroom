#include <mio/shared_mmap.hpp>

#include "index.h"
#include "index_collection.h"
#include "index_connection.h"
#include "read_normal.h"
#include "vroom_numeric.h"
#include "vroom_string.h"

#include <Rcpp.h>

enum column_type { character = 0, real = 1, integer = 2, logical = 3 };

inline int min(int a, int b) { return a < b ? a : b; }

CharacterVector
read_column_names(const std::shared_ptr<vroom::index_collection>& idx) {
  CharacterVector nms(idx->num_columns());

  auto col = 0;
  for (const auto& str : idx->header()) {
    nms[col++] = Rf_mkCharLenCE(str.c_str(), str.length(), CE_UTF8);
  }

  return nms;
}

// [[Rcpp::export]]
SEXP vroom_(
    List inputs,
    const char* delim,
    const char quote,
    bool trim_ws,
    bool escape_double,
    bool escape_backslash,
    const char comment,
    RObject col_names,
    RObject col_types,
    size_t skip,
    CharacterVector na,
    size_t num_threads,
    bool progress) {

  Rcpp::CharacterVector tempfile;

  bool has_header =
      col_names.sexp_type() == STRSXP ||
      (col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]);

  std::shared_ptr<vroom::index_collection> idx =
      std::make_shared<vroom::index_collection>(
          inputs,
          delim,
          quote,
          trim_ws,
          escape_double,
          escape_backslash,
          has_header,
          skip,
          comment,
          num_threads,
          progress);

  auto total_columns = idx->num_columns();

  List res(total_columns);

  CharacterVector col_nms;

  auto vroom = Rcpp::Environment::namespace_env("vroom");

  if (col_names.sexp_type() == STRSXP) {
    col_nms = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    col_nms = read_column_names(idx);
  } else {
    Rcpp::Function make_names = vroom["make_names"];
    col_nms = make_names(total_columns);
  }

  Rcpp::Function col_types_standardise = vroom["col_types_standardise"];
  col_types = col_types_standardise(col_types, col_nms);

  Rcpp::Function guess_type = vroom["guess_type"];

  auto num_rows = idx->num_rows();

  auto guess_num = min(num_rows, 100);

  // Guess based on values throughout the data
  auto guess_step = num_rows / guess_num;

  std::vector<std::string> res_nms;

  size_t i = 0;
  for (size_t col = 0; col < total_columns; ++col) {
    Rcpp::List collector =
        Rcpp::as<List>(Rcpp::as<List>(col_types)["cols"])[col];
    std::string col_type = Rcpp::as<std::string>(
        Rcpp::as<CharacterVector>(collector.attr("class"))[0]);

    if (col_type == "collector_skip") {
      continue;
    }

    if (col_type == "collector_guess") {
      CharacterVector col_vals(guess_num);
      for (auto j = 0; j < guess_num; ++j) {
        auto row = j * guess_step;
        auto str = idx->get(row, col);
        col_vals[j] = Rf_mkCharLenCE(str.c_str(), str.length(), CE_UTF8);
      }
      collector = guess_type(
          col_vals, Named("guess_integer") = false, Named("na") = na);
      col_type = Rcpp::as<std::string>(
          Rcpp::as<CharacterVector>(collector.attr("class"))[0]);
    }

    // This is deleted in the finalizers when the vectors are GC'd by R
    auto info = new vroom_vec_info{
        idx, col, num_threads, std::make_shared<Rcpp::CharacterVector>(na)};

    res_nms.push_back(Rcpp::as<std::string>(col_nms[col]));

    if (col_type == "collector_double") {
      SET_VECTOR_ELT(res, i, vroom_real::Make(info));
    } else if (col_type == "collector_integer") {
      SET_VECTOR_ELT(res, i, vroom_int::Make(info));
    } else if (col_type == "collector_logical") {
      SET_VECTOR_ELT(res, i, read_lgl(info));
    } else if (col_type == "collector_factor") {
      SET_VECTOR_ELT(res, i, read_fctr(info));
    } else {
      SET_VECTOR_ELT(res, i, vroom_string::Make(info));
    }

    ++i;
  }

  if (i < total_columns) {
    // Resize the list appropriately
    SETLENGTH(res, i);
    SET_TRUELENGTH(res, i);
  }

  res.attr("names") = res_nms;
  // res.attr("filename") = idx->filename();

  return res;
}
