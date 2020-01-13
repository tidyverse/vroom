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

inline void rethrow_rcpp_eval_error(const Rcpp::eval_error& e) {
  std::string msg = e.what();
  // Remove "Evaluation error: "
  msg.erase(0, 18);
  // Remove trailing period
  msg.erase(msg.length() - 1);

  throw Rcpp::exception(msg.c_str(), false);
}

template <typename T>
static char guess_delim(
    const T& source, size_t start, size_t guess_max = 5, size_t end = 0) {
  std::vector<std::string> lines;

  if (end == 0) {
    end = source.size();
  }

  auto nl = find_next_newline(source, start);
  while (nl > start && nl <= end && guess_max > 0) {
    auto str = std::string(source.data() + start, nl - start);
    lines.push_back(str);
    start = nl + 1;
    nl = find_next_newline(source, start);
    --guess_max;
  }

  Rcpp::Function fun = Rcpp::Environment::namespace_env("vroom")["guess_delim"];

  char delim;
  try {
    delim = Rcpp::as<char>(fun(lines));
  } catch (const Rcpp::eval_error& e) {
    rethrow_rcpp_eval_error(e);
  }
  return delim;
}

} // namespace vroom
