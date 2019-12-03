#include "vroom_dttm.h"

double parse_dttm(
    const string& str, DateTimeParser& parser, const std::string& format) {
  parser.setDate(str.begin(), str.end());
  bool res = (format == "") ? parser.parseISO8601() : parser.parse(format);

  if (res) {
    DateTime dt = parser.makeDateTime();
    if (dt.validDateTime()) {
      return dt.datetime();
    }
  }
  return NA_REAL;
}

Rcpp::NumericVector read_dttm(vroom_vec_info* info) {
  R_xlen_t n = info->column->size();

  Rcpp::NumericVector out(n);

  parallel_for(
      n,
      [&](size_t start, size_t end, size_t id) {
        R_xlen_t i = start;
        DateTimeParser parser(info->locale.get());
        auto col = info->column->slice(start, end);
        for (const auto& str : *col) {
          out[i++] = parse_dttm(str, parser, info->format);
        }
      },
      info->num_threads,
      true);

  out.attr("class") = Rcpp::CharacterVector::create("POSIXct", "POSIXt");
  out.attr("tzone") = info->locale->tz_;

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_dttm::class_t;

void init_vroom_dttm(DllInfo* dll) { vroom_dttm::Init(dll); }

#else
void init_vroom_dttm(DllInfo* dll) {}
#endif
