#pragma once

#include "utils.h"

#include <cpp11/R.hpp>
#include <cpp11/as.hpp>
#include <cpp11/function.hpp>

#include <string>
#include <vector>

namespace vroom {

inline std::string
get_pb_format(const std::string& which, const std::string& filename = "") {
  auto fun_name = std::string("pb_") + which + "_format";
  auto fun = cpp11::package("vroom")[fun_name.c_str()];
  return cpp11::as_cpp<std::string>(fun(filename));
}

inline int get_pb_width(const std::string& format) {
  auto pb_width = cpp11::package("vroom")["pb_width"];
  return cpp11::as_cpp<int>(pb_width(format));
}

template <typename T>
static char guess_delim(
    const T& source, size_t start, size_t guess_max = 5, size_t end = 0) {
  std::vector<std::string> lines;

  if (end == 0) {
    end = source.size();
  }

  auto nl = find_next_newline(
      source,
      start,
      /* comment */ "",
      /* skip_empty_rows */ false,
      /* embedded_nl */ true);
  while (nl > start && nl <= end && guess_max > 0) {
    auto str = std::string(source.data() + start, nl - start);
    lines.push_back(str);
    start = nl + 1;
    nl = find_next_newline(
        source,
        start,
        /* comment */ "",
        /* skip_empty_rows */ false,
        /* embededd_nl */ true);
    --guess_max;
  }

  auto guess_delim = cpp11::package("vroom")["guess_delim"];

  char delim;
  delim = cpp11::as_cpp<char>(guess_delim(lines));
  return delim;
}

} // namespace vroom
