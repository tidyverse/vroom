#include "fixed_width_index.h"

using namespace Rcpp;

// [[Rcpp::export]]
List vroom_fwf(
    std::string filename,
    std::vector<size_t> col_starts,
    std::vector<size_t> col_ends) {
  vroom::fixed_width_index idx(filename.c_str(), col_starts, col_ends);

  size_t n_col = idx.num_columns();
  size_t n_row = idx.num_rows();
  List out(n_col);

  for (size_t col = 0; col < n_col; ++col) {
    CharacterVector c(n_row);
    for (size_t row = 0; row < n_row; ++row) {
      auto str = idx.get(row, col);
      c[row] = std::string(str.begin(), str.end());
    }
    out[col] = c;
  }

  return out;
}
