#include "read_normal.h"
#include "parallel.h"

const static char* const true_values[] = {
    "T", "t", "True", "TRUE", "true", (char*)NULL};
const static char* const false_values[] = {
    "F", "f", "False", "FALSE", "false", (char*)NULL};

inline bool isTrue(const char* start, const char* end) {
  size_t len = end - start;

  for (int i = 0; true_values[i]; i++) {
    size_t true_len = strlen(true_values[i]);
    if (true_len == len && strncmp(start, true_values[i], len) == 0) {
      return true;
    }
  }
  return false;
}
inline bool isFalse(const char* start, const char* end) {
  size_t len = end - start;

  for (int i = 0; false_values[i]; i++) {
    if (strlen(false_values[i]) == len &&
        strncmp(start, false_values[i], len) == 0) {
      return true;
    }
  }
  return false;
}

inline int parse_logical(const char* start, const char* end) {
  auto len = end - start;

  if (isTrue(start, end) || (len == 1 && *start == '1')) {
    return true;
  }
  if (isFalse(start, end) || (len == 1 && *start == '0')) {
    return false;
  }
  return NA_LOGICAL;
}

Rcpp::LogicalVector read_lgl(vroom_vec_info* info) {

  R_xlen_t n = info->idx->num_rows();

  Rcpp::LogicalVector out(n);

  auto p = out.begin();

  parallel_for(
      n,
      [&](int start, int end, int id) {
        // Need to copy to a temp buffer since we have no way to tell strtod
        // how long the buffer is.

        auto i = start;
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          p[i++] = parse_logical(str.c_str(), str.c_str() + str.length());
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
  for (const auto& str : info->idx->get_column(info->column, start, end)) {
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
