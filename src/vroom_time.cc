#include "vroom_time.h"

double parse_time(
    const char* begin,
    const char* end,
    DateTimeParser& parser,
    const std::string& format) {
  parser.setDate(begin, end);
  bool res = (format == "") ? parser.parseLocaleTime() : parser.parse(format);

  if (res) {
    DateTime dt = parser.makeTime();
    if (dt.validDuration()) {
      return dt.time();
    }
  }
  return NA_REAL;
}

cpp11::doubles read_time(vroom_vec_info* info) {
  R_xlen_t n = info->column->size();

  cpp11::writable::doubles out(n);

  auto err_msg = info->format.size() == 0
                     ? std::string("time in ISO8601")
                     : std::string("time like ") + info->format;

  try {
    parallel_for(
        n,
        [&](size_t start, size_t end, size_t) {
          R_xlen_t i = start;
          DateTimeParser parser(info->locale.get());
          auto col = info->column->slice(start, end);
          for (auto b = col->begin(), e = col->end(); b != e; ++b) {
            out[i++] = parse_value<double>(
                b,
                col,
                [&](const char* begin, const char* end) -> double {
                  return parse_time(begin, end, parser, info->format);
                },
                info->errors,
                err_msg.c_str(),
                *info->na);
          }
        },
        info->num_threads,
        true);
  } catch (const std::runtime_error& e) {
    Rf_errorcall(R_NilValue, "%s", e.what());
  }

  info->errors->warn_for_errors();

  out.attr("class") = {"hms", "difftime"};
  out.attr("units") = "secs";

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_time::class_t;

void init_vroom_time(DllInfo* dll) { vroom_time::Init(dll); }

#else
void init_vroom_time(DllInfo* dll) {}
#endif
