#include "vroom_int.h"
#include "parallel.h"

// A version of strtoi that doesn't need null terminated strings, to avoid
// needing to copy the data
int strtoi(const char* begin, const char* end) {
  double val = 0;
  bool is_neg = false;

  if (begin == end) {
    return NA_INTEGER;
  }

  if (begin != end && *begin == '-') {
    is_neg = true;
    ++begin;
  }

  while (begin != end && isdigit(*begin)) {
    val = val * 10 + ((*begin++) - '0');
  }

  // If there is more than digits, return NA
  if (begin != end) {
    return NA_INTEGER;
  }

  if (val > INT_MAX) {
    return NA_INTEGER;
  }

  return is_neg ? -val : val;
}

// Normal reading of integer vectors
cpp11::integers read_int(vroom_vec_info* info) {

  R_xlen_t n = info->column->size();

  cpp11::writable::integers out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t) {
        // Use bulk extraction for better performance
        auto col = info->column->slice(start, end);
        auto strings = col->extract_all();

        for (size_t j = 0; j < strings.size(); ++j) {
          R_xlen_t i = start + j;
          auto& str = strings[j];

          if (vroom::is_explicit_na(*info->na, str.begin(), str.end())) {
            out[i] = NA_INTEGER;
            continue;
          }

          int val = strtoi(str.begin(), str.end());
          if (cpp11::is_na(val)) {
            auto b = col->begin() + j;
            info->errors->add_error(
                b.index(),
                col->get_index(),
                "an integer",
                std::string(str.begin(), str.end() - str.begin()),
                b.filename());
          }
          out[i] = val;
        }
      },
      info->num_threads);

  info->errors->warn_for_errors();

  return out;
}

R_altrep_class_t vroom_int::class_t;

void init_vroom_int(DllInfo* dll) { vroom_int::Init(dll); }
