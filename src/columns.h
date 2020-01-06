#pragma once

#include "Rcpp.h"
#include "vroom.h"
#include "vroom_big_int.h"
#include "vroom_chr.h"
#include "vroom_date.h"
#include "vroom_dbl.h"
#include "vroom_dttm.h"
#include "vroom_fct.h"
#include "vroom_int.h"
#include "vroom_lgl.h"
#include "vroom_num.h"
#include "vroom_rle.h"
#include "vroom_time.h"
#include "vroom_vec.h"

#include "connection.h"
#include "index_collection.h"

#include "collectors.h"

using namespace Rcpp;

namespace vroom {

inline std::vector<std::string> get_filenames(SEXP in) {
  auto n = Rf_xlength(in);
  std::vector<std::string> out;
  out.reserve(n);

  for (R_xlen_t i = 0; i < n; ++i) {
    SEXP x = VECTOR_ELT(in, i);
    if (TYPEOF(x) == STRSXP) {
      out.emplace_back(Rcpp::as<std::string>(x));
    } else {
      out.emplace_back(con_description(x));
    }
  }

  return out;
}

inline SEXP generate_filename_column(
    const std::vector<std::string>& filenames,
    const std::vector<size_t>& lengths,
    size_t rows) {
#ifdef HAS_ALTREP
  IntegerVector rle(filenames.size());
  for (size_t i = 0; i < lengths.size(); ++i) {
    rle[i] = lengths[i];
  }
  rle.names() = filenames;

  return vroom_rle::Make(rle);
#else
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
#endif
}

inline List create_columns(
    std::shared_ptr<index_collection> idx,
    RObject col_names,
    RObject col_types,
    RObject col_select,
    SEXP id,
    std::vector<std::string>& filenames,
    CharacterVector na,
    List locale,
    size_t altrep,
    size_t guess_max,
    size_t num_threads) {

  auto num_cols = idx->num_columns();
  auto num_rows = idx->num_rows();

  auto locale_info = std::make_shared<LocaleInfo>(locale);

  size_t i = 0;

  bool add_filename = !Rf_isNull(id);

  List res(num_cols + add_filename);

  CharacterVector res_nms(num_cols + add_filename);

  if (add_filename) {
    res[i] =
        generate_filename_column(filenames, idx->row_sizes(), idx->num_rows());
    res_nms[i] = Rcpp::as<Rcpp::CharacterVector>(id)[0];
    ++i;
  }

  auto my_collectors = resolve_collectors(
      col_names,
      col_types,
      col_select,
      idx,
      na,
      locale_info,
      guess_max,
      altrep);

  size_t to_parse = 0;
  for (size_t col = 0; col < num_cols; ++col) {
    auto collector = my_collectors[col];
    if (collector.use_altrep()) {
      to_parse += num_rows;
    }
  }
  // Rcpp::Rcerr << to_parse << '\n';

  for (size_t col = 0; col < num_cols; ++col) {
    auto collector = my_collectors[col];
    auto col_type = collector.type();

    if (col_type == column_type::Skip) {
      continue;
    }

    // This is deleted in the finalizers when the vectors are GC'd by R
    auto info = new vroom_vec_info{idx->get_column(col),
                                   num_threads,
                                   std::make_shared<Rcpp::CharacterVector>(na),
                                   locale_info,
                                   std::string()};

    res_nms[i] = collector.name();

    switch (collector.type()) {
    case column_type::Dbl:
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_dbl::Make(info);
#endif
      } else {
        res[i] = read_dbl(info);
        delete info;
      }
      break;
    case column_type::Int:
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_int::Make(info);
#endif
      } else {
        res[i] = read_int(info);
        delete info;
      }
      break;
    case column_type::BigInt:
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_big_int::Make(info);
#endif
      } else {
        res[i] = read_big_int(info);
        delete info;
      }
      break;
    case column_type::Num:
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_num::Make(info);
#endif
      } else {
        res[i] = read_num(info);
        delete info;
      }
      break;
    case column_type::Lgl:
      // No altrep for logicals as of R 3.5
      res[i] = read_lgl(info);
      delete info;
      break;
    case column_type::Fct: {
      auto levels = collector["levels"];
      if (Rf_isNull(levels)) {
        res[i] =
            read_fct_implicit(info, Rcpp::as<bool>(collector["include_na"]));
        delete info;
      } else {
        bool ordered = Rcpp::as<bool>(collector["ordered"]);
        if (collector.use_altrep()) {
#ifdef HAS_ALTREP
          res[i] = vroom_fct::Make(info, levels, ordered);
#endif
        } else {
          res[i] = read_fct_explicit(info, levels, ordered);
          delete info;
        }
      }
      break;
    }
    case column_type::Date:
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_date::Make(info);
#endif
      } else {
        res[i] = read_date(info);
        delete info;
      }
      break;
    case column_type::Dttm:
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_dttm::Make(info);
#endif
      } else {
        res[i] = read_dttm(info);
        delete info;
      }
      break;
    case column_type::Time:
      info->format = Rcpp::as<std::string>(collector["format"]);
      if (collector.use_altrep()) {
#ifdef HAS_ALTREP
        res[i] = vroom_time::Make(info);
#endif
      } else {
        res[i] = read_time(info);
        delete info;
      }
      break;
    default:
      if (collector.use_altrep()) {
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

  if (i < num_cols) {
    // Resize the list appropriately
    SETLENGTH(res, i);
    SET_TRUELENGTH(res, i);

    SETLENGTH(res_nms, i);
    SET_TRUELENGTH(res_nms, i);
  }

  res.attr("names") = res_nms;
  Rcpp::List spec = my_collectors.spec();
  spec["delim"] = idx->get_delim();
  spec.attr("class") = "col_spec";
  res.attr("spec") = spec;

  return res;
}
} // namespace vroom
