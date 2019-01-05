#include "read_normal.h"
#include "parallel.h"

Rcpp::LogicalVector read_lgl(vroom_vec_info* info) {

  R_xlen_t n = info->idx->size() / info->num_columns - info->skip;

  Rcpp::LogicalVector out(n);

  auto p = out.begin();

  // Need to copy to a temp buffer since we have no way to tell strtod how
  // long the buffer is.
  char buf[128];

  parallel_for(
      n,
      [&](int start, int end, int id) {
        size_t idx = (start + info->skip) * info->num_columns + info->column;
        for (int i = start; i < end; ++i) {
          size_t cur_loc = (*info->idx)[idx];
          size_t next_loc = (*info->idx)[idx + 1] - 1;
          size_t len = next_loc - cur_loc;

          std::copy(
              info->mmap.data() + cur_loc, info->mmap.data() + next_loc, buf);
          buf[len] = '\0';

          // TODO: na values
          p[i] = Rf_StringTrue(buf);

          idx += info->num_columns;
        }
      },
      info->num_threads);

  delete info;

  return out;
}

Rcpp::IntegerVector read_fctr(
    std::shared_ptr<std::vector<size_t> > offsets,
    mio::shared_mmap_source mmap,
    R_xlen_t column,
    R_xlen_t num_columns,
    R_xlen_t skip,
    R_xlen_t num_threads) {

  R_xlen_t n = offsets->size() / num_columns - skip;

  Rcpp::IntegerVector out(n);
  std::vector<std::string> levels;
  std::unordered_map<std::string, int> level_map;

  auto p = out.begin();

  int max_level = 1;

  auto start = 0;
  auto end = n;
  size_t idx = (start + skip) * num_columns + column;
  for (int i = start; i < end; ++i) {
    size_t cur_loc = (*offsets)[idx];
    size_t next_loc = (*offsets)[idx + 1] - 1;
    size_t len = next_loc - cur_loc;

    auto str = std::string(mmap.data() + cur_loc, len);

    auto val = level_map.find(str);
    if (val != level_map.end()) {
      p[i] = val->second;
    } else {
      p[i] = max_level;
      level_map[str] = max_level++;
      levels.emplace_back(str);
    }
    idx += num_columns;
  }

  out.attr("levels") = levels;
  out.attr("class") = "factor";

  return out;
}
