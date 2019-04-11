#pragma once

#include "Rcpp.h"

using namespace Rcpp;

class collector {
  const Rcpp::List data_;
  const std::string name_;
  const column_type type_;
  const size_t altrep_opts_;

private:
  column_type derive_type(const std::string& t) {
    if (t == "collector_skip") {
      return column_type::Skip;
    }
    if (t == "collector_double") {
      return column_type::Dbl;
    }
    if (t == "collector_integer") {
      return column_type::Int;
    }
    if (t == "collector_number") {
      return column_type::Num;
    }
    if (t == "collector_logical") {
      return column_type::Lgl;
    }
    if (t == "collector_factor") {
      return column_type::Fct;
    }
    if (t == "collector_date") {
      return column_type::Date;
    }
    if (t == "collector_datetime") {
      return column_type::Dttm;
    }
    if (t == "collector_time") {
      return column_type::Time;
    }

    return column_type::Chr;
  }

public:
  collector(Rcpp::List data, std::string name, size_t altrep_opts)
      : data_(data),
        name_(name),
        type_(derive_type(Rcpp::as<std::string>(
            Rcpp::as<Rcpp::CharacterVector>(data_.attr("class"))[0]))),
        altrep_opts_(altrep_opts) {}
  column_type type() const { return type_; }
  std::string name() const { return name_; }
  SEXP operator[](const char* nme) { return data_[nme]; }
  bool use_altrep() {
    switch (type()) {
    case column_type::Skip:
      return false;
    case column_type::Dbl:
      return altrep_opts_ & column_type::Dbl;
    case column_type::Int:
      return altrep_opts_ & column_type::Int;
    case column_type::Num:
      return altrep_opts_ & column_type::Num;
    case column_type::Fct:
      return altrep_opts_ & column_type::Fct;
    case column_type::Date:
      return altrep_opts_ & column_type::Date;
    case column_type::Dttm:
      return altrep_opts_ & column_type::Dttm;
    case column_type::Time:
      return altrep_opts_ & column_type::Time;
    case column_type::Chr:
      return altrep_opts_ & column_type::Chr;
    default:
      return false;
    }
  }
};

class collectors {
  Rcpp::List spec_;
  Rcpp::List collectors_;
  size_t altrep_opts_;

public:
  collectors(Rcpp::List col_types, size_t altrep_opts)
      : spec_(col_types),
        collectors_(Rcpp::as<Rcpp::List>(col_types["cols"])),
        altrep_opts_(altrep_opts) {}
  collector operator[](int i) {
    return {collectors_[i],
            Rcpp::as<std::string>(
                Rcpp::as<Rcpp::CharacterVector>(collectors_.attr("names"))[i]),
            altrep_opts_};
  }
  Rcpp::List spec() { return spec_; }
};

inline CharacterVector read_column_names(
    std::shared_ptr<vroom::index_collection> idx,
    std::shared_ptr<LocaleInfo> locale_info) {
  CharacterVector nms(idx->num_columns());

  auto col = 0;
  auto header = idx->get_header();
  for (const auto& str : *header) {
    nms[col++] = locale_info->encoder_.makeSEXP(str.begin(), str.end(), false);
  }

  return nms;
}

inline collectors resolve_collectors(
    RObject col_names,
    RObject col_types,
    RObject col_select,
    std::shared_ptr<index_collection> idx,
    CharacterVector na,
    std::shared_ptr<LocaleInfo> locale_info,
    size_t guess_max,
    size_t altrep_opts) {

  auto num_cols = idx->num_columns();
  auto num_rows = idx->num_rows();

  auto vroom = Rcpp::Environment::namespace_env("vroom");

  CharacterVector col_nms;

  Rcpp::Function make_names = vroom["make_names"];

  if (col_names.sexp_type() == STRSXP) {
    col_nms = make_names(col_names, num_cols);
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    col_nms = read_column_names(idx, locale_info);
  } else {
    col_nms = make_names(R_NilValue, num_cols);
  }

  Rcpp::Function col_types_standardise = vroom["col_types_standardise"];
  Rcpp::List col_types_std =
      col_types_standardise(col_types, col_nms, col_select);

  auto guess_num = std::min(num_rows, guess_max);

  auto guess_step = guess_num > 0 ? num_rows / guess_num : 0;

  Rcpp::Function guess_type = vroom["guess_type"];

  Rcpp::List my_collectors = col_types_std["cols"];

  for (size_t col = 0; col < num_cols; ++col) {
    Rcpp::List my_collector = my_collectors[col];
    std::string my_col_type = Rcpp::as<std::string>(
        Rcpp::as<Rcpp::CharacterVector>(my_collector.attr("class"))[0]);
    if (my_col_type == "collector_guess") {
      CharacterVector col_vals(guess_num);
      for (size_t j = 0; j < guess_num; ++j) {
        auto row = j * guess_step;
        auto str = idx->get(row, col);
        col_vals[j] =
            locale_info->encoder_.makeSEXP(str.begin(), str.end(), false);
      }
      my_collectors[col] = Rcpp::as<Rcpp::List>(guess_type(
          col_vals, Named("guess_integer") = false, Named("na") = na));
    }
  }

  return {col_types_std, altrep_opts};
}
