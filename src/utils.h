#pragma once

#include <Rcpp.h>

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
static size_t find_next_newline(const T& source, size_t start) {
  auto begin = source.data() + start;
  auto res =
      static_cast<const char*>(memchr(begin, '\n', source.size() - start));
  if (!res) {
    return start;
  }
  return res - source.data();
}

template <typename T>
static char guess_delim(const T& source, size_t start, size_t guess_max = 5) {
  std::vector<std::string> lines;

  auto nl = find_next_newline(source, start);
  while (nl > start && guess_max > 0) {
    lines.push_back(std::string(source.data() + start, nl - start));
    start = nl + 1;
    nl = find_next_newline(source, start);
    --guess_max;
  }

  Rcpp::Function fun = Rcpp::Environment::namespace_env("vroom")["guess_delim"];
  return Rcpp::as<char>(fun(lines));
}
