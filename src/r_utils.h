#pragma once

#include "utils.h"

#include <Rcpp.h>

namespace vroom {

inline std::string
get_pb_format(const std::string& which, const std::string& filename = "") {
  Rcpp::Function fun = Rcpp::Environment::namespace_env(
      "vroom")[std::string("pb_") + which + "_format"];
  return Rcpp::as<std::string>(fun(filename));
}

inline int get_pb_width(const std::string& format) {
  Rcpp::Function fun = Rcpp::Environment::namespace_env("vroom")["pb_width"];
  return Rcpp::as<int>(fun(format));
}

template <typename T>
static char guess_delim(
    const T& source, size_t start, size_t guess_max = 5, size_t end = 0) {
  std::vector<std::string> lines;

  if (end == 0) {
    end = source.size();
  }

  auto nl = find_next_newline(source, start);
  while (nl > start && nl < end && guess_max > 0) {
    auto str = std::string(source.data() + start, nl - start);
    lines.push_back(str);
    start = nl + 1;
    nl = find_next_newline(source, start);
    --guess_max;
  }

  Rcpp::Function fun = Rcpp::Environment::namespace_env("vroom")["guess_delim"];
  return Rcpp::as<char>(fun(lines));
}

} // namespace vroom
