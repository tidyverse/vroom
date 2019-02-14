#pragma once

#include <Rcpp.h>

static std::string get_pb_format(std::string which, std::string filename = "") {
  Rcpp::Function fun = Rcpp::Environment::namespace_env(
      "vroom")[std::string("pb_") + which + "_format"];
  return Rcpp::as<std::string>(fun(filename));
}
