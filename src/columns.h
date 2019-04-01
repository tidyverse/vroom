#pragma once

#include "Rcpp.h"
#include "connection.h"
#include "index_collection.h"
#include "vroom.h"
#include "vroom_chr.h"
#include "vroom_date.h"
#include "vroom_dbl.h"
#include "vroom_dttm.h"
#include "vroom_fct.h"
#include "vroom_int.h"
#include "vroom_lgl.h"
#include "vroom_num.h"
#include "vroom_time.h"
#include "vroom_vec.h"

using namespace Rcpp;

namespace vroom {

CharacterVector read_column_names(
    std::shared_ptr<vroom::index_collection> idx,
    std::shared_ptr<LocaleInfo> locale) {
  CharacterVector nms(idx->num_columns());

  auto col = 0;
  auto header = idx->get_header();
  for (const auto& str : *header) {
    nms[col++] = locale->encoder_.makeSEXP(str.begin(), str.end(), false);
  }

  return nms;
}

std::vector<std::string> get_filenames(SEXP in) {
  auto n = Rf_xlength(in);
  std::vector<std::string> out;
  out.reserve(n);

  for (R_xlen_t i = 0; i < n; ++i) {
    SEXP x = VECTOR_ELT(in, i);
    if (TYPEOF(x) == STRSXP) {
      out.emplace_back(Rcpp::as<std::string>(x));
    } else {
      auto con = R_GetConnection(x);
      out.emplace_back(con->description);
    }
  }

  return out;
}

CharacterVector generate_filename_column(
    const std::vector<std::string>& filenames,
    const std::vector<size_t>& lengths,
    size_t rows) {
  std::vector<std::string> out;
  out.reserve(rows);

  if (static_cast<size_t>(filenames.size()) != lengths.size()) {
    stop("inputs and lengths inconsistent");
  }

  for (size_t i = 0; i < filenames.size(); ++i) {
    for (size_t j = 0; j < lengths[i]; ++j) {
      out.push_back(filenames[i]);
    }
  }
  return wrap(out);
}

List create_columns(
    std::shared_ptr<index_collection> idx,
    RObject col_names,
    RObject col_types,
    RObject col_keep,
    RObject col_skip,
    SEXP id,
    std::vector<std::string>& filenames,
    CharacterVector na,
    List locale,
    size_t altrep_opts,
    size_t guess_max,
    size_t num_threads) {

  auto num_col = idx->num_columns();

  auto locale_info = std::make_shared<LocaleInfo>(locale);

  CharacterVector col_nms;

  auto vroom = Rcpp::Environment::namespace_env("vroom");

  if (col_names.sexp_type() == STRSXP) {
    col_nms = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    col_nms = read_column_names(idx, locale_info);
  } else {
    Rcpp::Function make_names = vroom["make_names"];
    col_nms = make_names(num_col);
  }

  Rcpp::Function col_types_standardise = vroom["col_types_standardise"];
  col_types = col_types_standardise(col_types, col_nms, col_keep, col_skip);

  Rcpp::Function guess_type = vroom["guess_type"];

  auto num_rows = idx->num_rows();

  auto guess_num = std::min(num_rows, guess_max);

  // Guess based on values throughout the data
  auto guess_step = guess_num > 0 ? num_rows / guess_num : 0;

  std::vector<std::string> res_nms;

  size_t i = 0;

  bool add_filename = !Rf_isNull(id);

  List res(num_col + add_filename);

  if (add_filename) {
    res[i++] =
        generate_filename_column(filenames, idx->row_sizes(), idx->num_rows());
    res_nms.push_back(Rcpp::as<std::string>(id));
  }

  for (size_t col = 0; col < num_col; ++col) {
    Rcpp::List collector =
        Rcpp::as<List>(Rcpp::as<List>(col_types)["cols"])[col];
    std::string col_type = Rcpp::as<std::string>(
        Rcpp::as<CharacterVector>(collector.attr("class"))[0]);

    if (col_type == "collector_skip") {
      continue;
    }

    if (col_type == "collector_guess") {
      CharacterVector col_vals(guess_num);
      for (size_t j = 0; j < guess_num; ++j) {
        auto row = j * guess_step;
        auto str = idx->get(row, col);
        col_vals[j] =
            locale_info->encoder_.makeSEXP(str.begin(), str.end(), false);
      }
      collector = guess_type(
          col_vals, Named("guess_integer") = false, Named("na") = na);
      col_type = Rcpp::as<std::string>(
          Rcpp::as<CharacterVector>(collector.attr("class"))[0]);
    }

    // This is deleted in the finalizers when the vectors are GC'd by R
    auto info = new vroom_vec_info{idx->get_column(col),
                                   num_threads,
                                   std::make_shared<Rcpp::CharacterVector>(na),
                                   locale_info};

    res_nms.push_back(Rcpp::as<std::string>(col_nms[col]));

    if (col_type == "collector_double") {
      if (altrep_opts & column_type::DBL) {
#ifdef HAS_ALTREP
        res[i] = vroom_dbl::Make(info);
#endif
      } else {
        res[i] = read_dbl(info);
        delete info;
      }
    } else if (col_type == "collector_integer") {
      if (altrep_opts & column_type::INT) {
#ifdef HAS_ALTREP
        res[i] = vroom_int::Make(info);
#endif
      } else {
        res[i] = read_int(info);
        delete info;
      }
    } else if (col_type == "collector_number") {
      if (altrep_opts & column_type::NUM) {
#ifdef HAS_ALTREP
        res[i] = vroom_num::Make(info);
#endif
      } else {
        res[i] = read_num(info);
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
        if (altrep_opts & column_type::FCT) {
#ifdef HAS_ALTREP
          res[i] = vroom_fct::Make(info, levels, collector["ordered"]);
#endif
        } else {
          res[i] = read_fctr_explicit(info, levels, collector["ordered"]);
          delete info;
        }
      }
    } else if (col_type == "collector_date") {
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (altrep_opts & column_type::DATE) {
#ifdef HAS_ALTREP
        res[i] = vroom_date::Make(info);
#endif
      } else {
        res[i] = read_date(info);
        delete info;
      }
    } else if (col_type == "collector_datetime") {
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (altrep_opts & column_type::DTTM) {
#ifdef HAS_ALTREP
        res[i] = vroom_dttm::Make(info);
#endif
      } else {
        res[i] = read_dttm(info);
        delete info;
      }
    } else if (col_type == "collector_time") {
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (altrep_opts & column_type::TIME) {
#ifdef HAS_ALTREP
        res[i] = vroom_time::Make(info);
#endif
      } else {
        res[i] = read_time(info);
        delete info;
      }
    } else {
      if (altrep_opts & column_type::CHR) {
#ifdef HAS_ALTREP
        res[i] = vroom_chr::Make(info);
#endif
      } else {
        res[i] = read_chr(info);
        delete info;
      }
    }

    ++i;
  }

  if (i < num_col) {
    // Resize the list appropriately
    SETLENGTH(res, i);
    SET_TRUELENGTH(res, i);
  }

  res.attr("names") = res_nms;
  // res.attr("filename") = idx->filenames();

  return res;
}
} // namespace vroom
