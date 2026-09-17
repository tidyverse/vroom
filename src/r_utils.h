#pragma once

#include "utils.h"

#include <cpp11/R.hpp>
#include <cpp11/as.hpp>
#include <cpp11/function.hpp>

#include <string>
#include <vector>

#ifndef R_PRIdXLEN_T
#  ifdef LONG_VECTOR_SUPPORT
#    define R_PRIdXLEN_T "td"
#  else
#    define R_PRIdXLEN_T "d"
#  endif
#endif

namespace vroom {

template <typename T>
static char guess_delim(
    const T& source,
    size_t start,
    size_t guess_max,
    size_t end,
    const char quote) {
  std::vector<std::string> lines;

  if (end == 0) {
    end = source.size();
  }

  size_t nl;
  newline_type nlt;
  std::tie(nl, nlt) = find_next_newline(
      source,
      start,
      /* comment */ "",
      /* skip_empty_rows */ false,
      /* embedded_nl */ true,
      /* quote */ quote);
  while (nl > start && nl <= end && guess_max > 0) {
    auto str = std::string(source.data() + start, nl - start);
    lines.push_back(str);
    start = nl + 1;
    std::tie(nl, nlt) = find_next_newline(
        source,
        start,
        /* comment */ "",
        /* skip_empty_rows */ false,
        /* embededd_nl */ true,
        quote);
    --guess_max;
  }

  auto guess_delim = cpp11::package("vroom")["guess_delim"];

  char delim;
  delim = cpp11::as_cpp<char>(guess_delim(lines));
  return delim;
}

} // namespace vroom
