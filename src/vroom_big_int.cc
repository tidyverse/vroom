#include "vroom_big_int.h"
#include "parallel.h"
#include <climits>

// A version of strtoll that doesn't need null terminated strings, to avoid
// needing to copy the data
long long vroom_strtoll(const char* begin, const char* end) {
  unsigned long long val = 0;
  bool is_neg = false;

  if (begin == end) {
    return NA_INTEGER64;
  }

  if (begin != end && *begin == '-') {
    is_neg = true;
    ++begin;
  }

  while (begin != end && isdigit(*begin)) {
    val = val * 10 + ((*begin++) - '0');
  }

  if (val > LLONG_MAX) {
    return NA_INTEGER64;
  }

  // If there is more than digits, return NA
  if (begin != end) {
    return NA_INTEGER64;
  }

  return is_neg ? -val : val;
}

// Normal reading of integer vectors
cpp11::doubles read_big_int(vroom_vec_info* info) {

  R_xlen_t n = info->column->size();

  cpp11::writable::doubles out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t) {
        R_xlen_t i = start;
        auto col = info->column->slice(start, end);
        for (auto b = col->begin(), e = col->end(); b != e; ++b) {
          vroom_big_int_t res;
          res.ll = parse_value<long long>(
              b, col, vroom_strtoll, info->errors, "a big integer", *info->na);
          out[i++] = res.dbl;
        }
      },
      info->num_threads);

  info->errors->warn_for_errors();

  out.attr("class") = {"integer64"};

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_big_int::class_t;

void init_vroom_big_int(DllInfo* dll) { vroom_big_int::Init(dll); }

#else
void init_vroom_big_int(DllInfo* dll) {}
#endif
