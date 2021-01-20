#pragma once

#include <cpp11/function.hpp>
#include <cpp11/list.hpp>
#include <cpp11/sexp.hpp>
#include <cpp11/strings.hpp>

class collector {
  const cpp11::list data_;
  const SEXP name_;
  const column_type type_;
  const size_t altrep_;

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
    if (t == "collector_big_integer") {
      return column_type::BigInt;
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
  collector(cpp11::list data, SEXP name, size_t altrep)
      : data_(data),
        name_(name),
        type_(derive_type(cpp11::strings(data_.attr("class"))[0])),
        altrep_(altrep) {}
  column_type type() const { return type_; }
  SEXP name() const { return name_; }
  SEXP operator[](const char* nme) { return data_[nme]; }
  bool use_altrep() {
    switch (type()) {
    case column_type::Skip:
      return false;
    case column_type::Dbl:
      return altrep_ & column_type::Dbl;
    case column_type::Int:
      return altrep_ & column_type::Int;
    case column_type::BigInt:
      return altrep_ & column_type::BigInt;
    case column_type::Num:
      return altrep_ & column_type::Num;
    case column_type::Fct:
      return altrep_ & column_type::Fct;
    case column_type::Date:
      return altrep_ & column_type::Date;
    case column_type::Dttm:
      return altrep_ & column_type::Dttm;
    case column_type::Time:
      return altrep_ & column_type::Time;
    case column_type::Chr:
      return altrep_ & column_type::Chr;
    default:
      return false;
    }
  }
};

class collectors {
  cpp11::list spec_;
  cpp11::list collectors_;
  size_t altrep_;

public:
  collectors(cpp11::list col_types, size_t altrep)
      : spec_(col_types), collectors_(col_types["cols"]), altrep_(altrep) {}
  collector operator[](int i) {
    return {
        collectors_[i], cpp11::strings(collectors_.attr("names"))[i], altrep_};
  }
  cpp11::list spec() { return spec_; }
};

inline cpp11::strings read_column_names(
    std::shared_ptr<vroom::index_collection> idx,
    std::shared_ptr<LocaleInfo> locale_info) {
  cpp11::writable::strings nms(idx->num_columns());

  auto col = 0;
  auto header = idx->get_header();
  for (const auto& str : *header) {
    nms[col++] = locale_info->encoder_.makeSEXP(str.begin(), str.end(), false);
  }

  return nms;
}

std::string guess_type__(
    cpp11::writable::strings input,
    cpp11::strings na,
    LocaleInfo* locale,
    bool guess_integer);

inline collectors resolve_collectors(
    cpp11::sexp col_names,
    cpp11::sexp col_types,
    cpp11::sexp col_select,
    cpp11::sexp name_repair,
    std::shared_ptr<index_collection> idx,
    cpp11::strings na,
    std::shared_ptr<LocaleInfo> locale_info,
    size_t guess_max,
    size_t altrep) {

  R_xlen_t num_cols = idx->num_columns();
  auto num_rows = idx->num_rows();

  auto vroom = cpp11::package("vroom");

  cpp11::writable::strings col_nms;

  auto make_names = vroom["make_names"];

  if (TYPEOF(col_names) == STRSXP) {
    col_nms = static_cast<SEXP>(make_names(col_names, num_cols));
  } else if (TYPEOF(col_names) == LGLSXP && cpp11::logicals(col_names)[0]) {
    col_nms = read_column_names(idx, locale_info);
  } else {
    col_nms = static_cast<SEXP>(make_names(R_NilValue, num_cols));
  }

  auto col_types_standardise = vroom["col_types_standardise"];
  cpp11::list col_types_std(
      col_types_standardise(col_types, col_nms, col_select, name_repair));

  R_xlen_t guess_num = std::min(num_rows, guess_max);

  auto guess_step = guess_num > 0 ? num_rows / guess_num : 0;

  cpp11::writable::list my_collectors(col_types_std["cols"]);

  for (R_xlen_t col = 0; col < num_cols; ++col) {
    cpp11::writable::list my_collector(my_collectors[col]);
    std::string my_col_type = cpp11::strings(my_collector.attr("class"))[0];

    if (my_col_type == "collector_guess") {
      cpp11::writable::strings col_vals(guess_num);
      for (R_xlen_t j = 0; j < guess_num; ++j) {
        auto row = j * guess_step;
        auto str = idx->get(row, col);
        col_vals[j] =
            locale_info->encoder_.makeSEXP(str.begin(), str.end(), true);
      }
      auto type =
          guess_type__(std::move(col_vals), na, locale_info.get(), false);
      auto fun_name = std::string("col_") + type;
      auto col_type = vroom[fun_name.c_str()];
      my_collectors[col] = col_type();
    }
  }

  return {col_types_std, altrep};
}
