#pragma once

#include <cpp11/as.hpp>
#include <cpp11/list.hpp>
#include <cpp11/strings.hpp>

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

namespace vroom {

inline std::vector<std::string> get_filenames(SEXP in) {
  auto n = Rf_xlength(in);
  std::vector<std::string> out;
  out.reserve(n);

  for (R_xlen_t i = 0; i < n; ++i) {
    SEXP x = VECTOR_ELT(in, i);
    if (TYPEOF(x) == STRSXP) {
      out.emplace_back(cpp11::as_cpp<std::string>(x));
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
  // suppress compiler warning about unused parameter, as this is only used
  // without altrep.
  (void)rows;

  cpp11::writable::integers rle(filenames.size());
  for (R_xlen_t i = 0; i < R_xlen_t(lengths.size()); ++i) {
    rle[i] = lengths[i];
  }
  rle.names() = filenames;

  return vroom_rle::Make(rle);
#else
  std::vector<std::string> out;
  out.reserve(rows);

  if (static_cast<size_t>(filenames.size()) != lengths.size()) {
    cpp11::stop("inputs and lengths inconsistent");
  }

  for (size_t i = 0; i < filenames.size(); ++i) {
    for (size_t j = 0; j < lengths[i]; ++j) {
      out.push_back(filenames[i]);
    }
  }
  return cpp11::as_sexp(out);
#endif
}

inline cpp11::list create_columns(
    std::shared_ptr<index_collection> idx,
    cpp11::sexp col_names,
    cpp11::sexp col_types,
    cpp11::sexp col_select,
    cpp11::sexp name_repair,
    SEXP id,
    std::vector<std::string>& filenames,
    cpp11::strings na,
    cpp11::list locale,
    size_t altrep,
    size_t guess_max,
    size_t num_threads) {

  R_xlen_t num_cols = idx->num_columns();
  auto num_rows = idx->num_rows();

  auto locale_info = std::make_shared<LocaleInfo>(locale);

  R_xlen_t i = 0;

  bool add_filename = !Rf_isNull(id);

  cpp11::writable::list res(num_cols + add_filename);

  cpp11::writable::strings res_nms(num_cols + add_filename);

  if (add_filename) {
    res[i] =
        generate_filename_column(filenames, idx->row_sizes(), idx->num_rows());
    res_nms[i] = cpp11::strings(id)[0];
    ++i;
  }

  auto my_collectors = resolve_collectors(
      col_names,
      col_types,
      col_select,
      name_repair,
      idx,
      na,
      locale_info,
      guess_max,
      altrep);

  size_t to_parse = 0;
  for (R_xlen_t col = 0; col < num_cols; ++col) {
    auto collector = my_collectors[col];
    if (collector.use_altrep()) {
      to_parse += num_rows;
    }
  }

  for (R_xlen_t col = 0; col < num_cols; ++col) {
    auto collector = my_collectors[col];
    auto col_type = collector.type();

    if (col_type == column_type::Skip) {
      continue;
    }

    // This is deleted in the finalizers when the vectors are GC'd by R
    auto info = new vroom_vec_info{idx->get_column(col),
                                   num_threads,
                                   std::make_shared<cpp11::strings>(na),
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
        res[i] = read_fct_implicit(
            info, cpp11::as_cpp<bool>(collector["include_na"]));
        delete info;
      } else {
        bool ordered = cpp11::as_cpp<bool>(collector["ordered"]);
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
      info->format = cpp11::as_cpp<std::string>(collector["format"]);
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
      info->format = cpp11::as_cpp<std::string>(collector["format"]);
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
      info->format = cpp11::as_cpp<std::string>(collector["format"]);
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
  cpp11::writable::list spec(my_collectors.spec());
  spec["delim"] = cpp11::writable::strings({idx->get_delim().c_str()});
  spec.attr("class") = "col_spec";
  res.attr("spec") = spec;

  return res;
}
} // namespace vroom
