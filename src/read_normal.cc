#include "read_normal.h"
#include "parallel.h"

Rcpp::LogicalVector read_lgl(vroom_vec_info* info) {

  R_xlen_t n = info->idx->num_rows();

  Rcpp::LogicalVector out(n);

  auto p = out.begin();

  parallel_for(
      n,
      [&](int start, int end, int id) {
        // Need to copy to a temp buffer since we have no way to tell strtod how
        // long the buffer is.

        auto i = start;
        auto col = info->idx->get_column(info->column);
        auto it = col.begin();
        auto it_end = col.begin();
        it += start;
        it_end += end;
        for (; it != it_end; ++it) {
          const auto& str = *it;
          p[i++] = Rf_StringTrue(str.c_str());
        }
      },
      info->num_threads);

  delete info;

  return out;
}

Rcpp::IntegerVector read_fctr(vroom_vec_info* info) {
  R_xlen_t n = info->idx->num_rows();

  Rcpp::IntegerVector out(n);
  std::vector<std::string> levels;
  std::unordered_map<std::string, int> level_map;

  auto p = out.begin();

  int max_level = 1;

  auto start = 0;
  auto end = n;
  auto i = start;
  auto col = info->idx->get_column(info->column);
  auto it = col.begin();
  auto it_end = col.begin();
  it += start;
  it_end += end;
  for (; it != it_end; ++it) {
    const auto& str = *it;
    auto val = level_map.find(str);
    if (val != level_map.end()) {
      p[i++] = val->second;
    } else {
      p[i++] = max_level;
      level_map[str] = max_level++;
      levels.emplace_back(str);
    }
  }

  out.attr("levels") = levels;
  out.attr("class") = "factor";

  delete info;

  return out;
}
