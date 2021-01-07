#include "vroom_date.h"

using namespace vroom;

double parse_date(
    const char* begin,
    const char* end,
    DateTimeParser& parser,
    const std::string& format) {
  parser.setDate(begin, end);
  bool res = (format == "") ? parser.parseLocaleDate() : parser.parse(format);

  if (res) {
    DateTime dt = parser.makeDate();
    if (dt.validDate()) {
      return dt.date();
    }
  }
  return NA_REAL;
}

cpp11::doubles read_date(vroom_vec_info* info) {
  R_xlen_t n = info->column->size();

  cpp11::writable::doubles out(n);

  auto err_msg = info->format.size() == 0
                     ? std::string("date in ISO8601")
                     : std::string("date like ") + info->format;

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
                  return parse_date(begin, end, parser, info->format);
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

  out.attr("class") = "Date";

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_date::class_t;

void init_vroom_date(DllInfo* dll) { vroom_date::Init(dll); }

#else
void init_vroom_date(DllInfo* dll) {}
#endif
