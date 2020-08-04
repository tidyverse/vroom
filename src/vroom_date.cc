#include "vroom_date.h"

using namespace vroom;

double parse_date(
    const string& str, DateTimeParser& parser, const std::string& format) {
  parser.setDate(str.begin(), str.end());
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

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t) {
        R_xlen_t i = start;
        DateTimeParser parser(info->locale.get());
        auto col = info->column->slice(start, end);
        for (const auto& str : *col) {
          out[i++] = parse_date(str, parser, info->format);
        }
      },
      info->num_threads,
      true);

  out.attr("class") = "Date";

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_date::class_t;

void init_vroom_date(DllInfo* dll) { vroom_date::Init(dll); }

#else
void init_vroom_date(DllInfo* dll) {}
#endif
