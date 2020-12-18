#pragma once

#include <cpp11/logicals.hpp>

#include "parallel.h"
#include "vroom_vec.h"

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

inline int
parse_logical(const char* start, const char* end, bool strict = true) {
  auto len = end - start;

  if (isTrue(start, end) || (!strict && len == 1 && *start == '1')) {
    return TRUE;
  }
  if (isFalse(start, end) || (!strict && len == 1 && *start == '0')) {
    return FALSE;
  }
  return NA_LOGICAL;
}

inline cpp11::logicals read_lgl(vroom_vec_info* info) {

  R_xlen_t n = info->column->size();

  cpp11::writable::logicals out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t) {
        R_xlen_t i = start;
        auto col = info->column->slice(start, end);
        for (auto b = col->begin(), e = col->end(); b != e; ++b) {
          out[i++] = parse_value<int>(
              b,
              col,
              [&](const char* begin, const char* end) -> int {
                return parse_logical(begin, end, false);
              },
              info->errors,
              "1/0/T/F/TRUE/FALSE",
              *info->na);
        }
      },
      info->num_threads);

  info->errors->warn_for_errors();

  return out;
}
