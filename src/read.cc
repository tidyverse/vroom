#include <mio/shared_mmap.hpp>

#include "LocaleInfo.h"
#include "index.h"
#include "index_collection.h"
#include "index_connection.h"
#include "vroom_chr.h"
#include "vroom_dbl.h"
#include "vroom_dttm.h"
#include "vroom_fct.h"
#include "vroom_int.h"
#include "vroom_lgl.h"

#include <Rcpp.h>

enum column_type { character = 0, real = 1, integer = 2, logical = 3 };

inline int min(int a, int b) { return a < b ? a : b; }

CharacterVector read_column_names(
    std::shared_ptr<vroom::index_collection> idx,
    std::shared_ptr<LocaleInfo> locale) {
  CharacterVector nms(idx->num_columns());

  auto col = 0;
  for (const auto& str : idx->get_header()) {
    nms[col++] = locale->encoder_.makeSEXP(
        str.c_str(), str.c_str() + str.length(), false);
  }

  return nms;
}

CharacterVector generate_filename_column(
    List inputs, const std::vector<size_t> lengths, size_t rows) {
  std::vector<std::string> out;
  out.reserve(rows);

  if (static_cast<size_t>(inputs.size()) != lengths.size()) {
    Rcpp::stop("inputs and lengths inconsistent");
  }

  for (int i = 0; i < inputs.size(); ++i) {
    for (size_t j = 0; j < lengths[i]; ++j) {
      // CharacterVector filename = Rf_asChar(inputs[i]);
      out.push_back(inputs[i]);
    }
  }
  return Rcpp::wrap(out);
}

// [[Rcpp::export]]
SEXP vroom_(
    List inputs,
    SEXP delim,
    const char quote,
    bool trim_ws,
    bool escape_double,
    bool escape_backslash,
    const char comment,
    RObject col_names,
    RObject col_types,
    SEXP id,
    size_t skip,
    CharacterVector na,
    List locale,
    bool use_altrep,
    size_t num_threads,
    bool progress) {

  Rcpp::CharacterVector tempfile;

  bool has_header =
      col_names.sexp_type() == STRSXP ||
      (col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]);

  std::shared_ptr<vroom::index_collection> idx =
      std::make_shared<vroom::index_collection>(
          inputs,
          Rf_isNull(delim) ? nullptr : Rcpp::as<const char*>(delim),
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

  bool add_filename = !Rf_isNull(id);

  auto locale_info = std::make_shared<LocaleInfo>(locale);

  List res(total_columns + add_filename);

  CharacterVector col_nms;

  auto vroom = Rcpp::Environment::namespace_env("vroom");

  if (col_names.sexp_type() == STRSXP) {
    col_nms = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    col_nms = read_column_names(idx, locale_info);
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
        col_vals[j] = locale_info->encoder_.makeSEXP(
            str.c_str(), str.c_str() + str.length(), false);
      }
      collector = guess_type(
          col_vals, Named("guess_integer") = false, Named("na") = na);
      col_type = Rcpp::as<std::string>(
          Rcpp::as<CharacterVector>(collector.attr("class"))[0]);
    }

    // This is deleted in the finalizers when the vectors are GC'd by R
    auto info = new vroom_vec_info{idx,
                                   col,
                                   num_threads,
                                   std::make_shared<Rcpp::CharacterVector>(na),
                                   locale_info};

    res_nms.push_back(Rcpp::as<std::string>(col_nms[col]));

    if (col_type == "collector_double") {
      if (use_altrep) {
        res[i] = vroom_dbl::Make(info);
      } else {
        res[i] = read_dbl(info);
        delete info;
      }
    } else if (col_type == "collector_integer") {
      if (use_altrep) {
        res[i] = vroom_int::Make(info);
      } else {
        res[i] = read_int(info);
        delete info;
      }
    } else if (col_type == "collector_logical") {
      // No altrep for logicals as of R 3.5
      res[i] = read_lgl(info);
      delete info;
    } else if (col_type == "collector_factor") {
      auto levels = collector["levels"];
      if (Rf_isNull(levels)) {
        res[i] = read_fctr_implicit(info, collector["include_na"]);
        delete info;
      } else {
        if (use_altrep) {
          res[i] = vroom_factor::Make(info, levels, collector["ordered"]);
        } else {
          res[i] = read_fctr_explicit(info, levels, collector["ordered"]);
        }
      }
    } else if (col_type == "collector_date") {
      res[i] = read_date(info, collector["format"]);
      delete info;
    } else if (col_type == "collector_datetime") {
      res[i] = read_datetime(info, collector["format"]);
      delete info;
    } else if (col_type == "collector_time") {
      res[i] = read_time(info, collector["format"]);
      delete info;
    } else {
      if (use_altrep) {
        res[i] = vroom_string::Make(info);
      } else {
        res[i] = read_chr(info);
        delete info;
      }
    }

    ++i;
  }

  if (add_filename) {
    res[i++] =
        generate_filename_column(inputs, idx->row_sizes(), idx->num_rows());
    res_nms.push_back(Rcpp::as<std::string>(id));
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
