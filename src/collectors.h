#pragma once

class collector {
  const Rcpp::List data_;
  const std::string name_;
  const std::string type_;

public:
  collector(Rcpp::List data, std::string name)
      : data_(data),
        name_(name),
        type_(Rcpp::as<Rcpp::CharacterVector>(data_.attr("class"))[0]) {}
  std::string type() const { return type_; }
  std::string name() const { return name_; }
  SEXP operator[](const char* nme) { return data_[nme]; }
};

class collectors {
  Rcpp::List collectors_;

public:
  collectors(Rcpp::List col_types)
      : collectors_(Rcpp::as<Rcpp::List>(col_types["cols"])) {}
  collector operator[](int i) {
    return {collectors_[i],
            Rcpp::as<std::string>(
                Rcpp::as<Rcpp::CharacterVector>(collectors_.attr("names"))[i])};
  }
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
    RObject col_keep,
    RObject col_skip,
    std::shared_ptr<index_collection> idx,
    CharacterVector na,
    std::shared_ptr<LocaleInfo> locale_info,
    size_t guess_max) {

  auto num_cols = idx->num_columns();
  auto num_rows = idx->num_rows();

  auto vroom = Rcpp::Environment::namespace_env("vroom");

  CharacterVector col_nms;

  if (col_names.sexp_type() == STRSXP) {
    col_nms = col_names;
  } else if (
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0]) {
    col_nms = read_column_names(idx, locale_info);
  } else {
    Rcpp::Function make_names = vroom["make_names"];
    col_nms = make_names(num_cols);
  }

  Rcpp::Function col_types_standardise = vroom["col_types_standardise"];
  Rcpp::List col_types_std =
      col_types_standardise(col_types, col_nms, col_keep, col_skip);

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

  return col_types_std;
}
