#pragma once

#include <cpp11/R.hpp>
#include <cstring>
#include <string>

#include "LocaleInfo.h"

// --- Logical parsing (from former vroom_lgl.h) ---

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

// --- Double parsing (from former vroom_dbl.cc) ---

double bsd_strtod(const char* begin, const char* end, const char decimalMark);

// --- Number parsing (from former vroom_num.cc) ---

double parse_num(
    const char* start,
    const char* end,
    const LocaleInfo& loc,
    bool strict = false);
