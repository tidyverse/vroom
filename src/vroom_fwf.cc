#include "fixed_width_index.h"

#include "LocaleInfo.h"

#include "vroom_chr.h"

using namespace Rcpp;

// [[Rcpp::export]]
List vroom_fwf_(
    List inputs,
    std::vector<int> col_starts,
    std::vector<int> col_ends,
    bool trim_ws,
    List locale) {
  auto idx = std::make_shared<vroom::index_collection>(
      inputs, col_starts, col_ends, trim_ws);

  size_t n_col = idx->num_columns();
  List out(n_col);

  auto locale_info = std::make_shared<LocaleInfo>(locale);

  for (size_t col = 0; col < n_col; ++col) {

    auto info = new vroom_vec_info{idx->get_column(col),
                                   1,
                                   std::make_shared<Rcpp::CharacterVector>(""),
                                   locale_info};
    out[col] = vroom_chr::Make(info);
    // CharacterVector c(n_row);
    // size_t row = 0;
    // for (const auto& str : *idx->get_column(col)) {
    // c[row++] = std::string(str.begin(), str.end());
    //}
    // out[col] = c;
  }

  return out;
}
