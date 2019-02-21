#include "DateTimeParser.h"
#include "parallel.h"

Rcpp::NumericVector
read_datetime(vroom_vec_info* info, Rcpp::List locale, std::string format) {
  R_xlen_t n = info->idx->num_rows();

  Rcpp::NumericVector out(n);

  LocaleInfo li(locale);

  parallel_for(
      n,
      [&](int start, int end, int id) {
        auto i = start;
        DateTimeParser parser(&li);
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          parser.setDate(str.c_str());
          bool res =
              (format == "") ? parser.parseISO8601() : parser.parse(format);

          if (res) {
            DateTime dt = parser.makeDateTime();
            if (!dt.validDateTime()) {
              out[i++] = NA_REAL;
            }
            out[i++] = dt.datetime();
          } else {
            out[i++] = NA_REAL;
          }
        }
      },
      info->num_threads,
      true);

  out.attr("class") = Rcpp::CharacterVector::create("POSIXct", "POSIXt");
  out.attr("tzone") = li.tz_;

  return out;
}

Rcpp::NumericVector
read_date(vroom_vec_info* info, Rcpp::List locale, std::string format) {
  R_xlen_t n = info->idx->num_rows();

  Rcpp::NumericVector out(n);

  LocaleInfo li(locale);

  parallel_for(
      n,
      [&](int start, int end, int id) {
        auto i = start;
        DateTimeParser parser(&li);
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          parser.setDate(str.c_str());
          bool res =
              (format == "") ? parser.parseLocaleDate() : parser.parse(format);

          if (res) {
            DateTime dt = parser.makeDate();
            if (!dt.validDate()) {
              out[i++] = NA_REAL;
            }
            out[i++] = dt.date();
          } else {
            out[i++] = NA_REAL;
          }
        }
      },
      info->num_threads,
      true);

  out.attr("class") = "Date";

  return out;
}

Rcpp::NumericVector
read_time(vroom_vec_info* info, Rcpp::List locale, std::string format) {
  R_xlen_t n = info->idx->num_rows();

  Rcpp::NumericVector out(n);

  LocaleInfo li(locale);

  parallel_for(
      n,
      [&](int start, int end, int id) {
        auto i = start;
        DateTimeParser parser(&li);
        for (const auto& str :
             info->idx->get_column(info->column, start, end)) {
          parser.setDate(str.c_str());
          bool res =
              (format == "") ? parser.parseLocaleTime() : parser.parse(format);

          if (res) {
            DateTime dt = parser.makeTime();
            if (!dt.validTime()) {
              out[i++] = NA_REAL;
            }
            out[i++] = dt.time();
          } else {
            out[i++] = NA_REAL;
          }
        }
      },
      info->num_threads,
      true);

  out.attr("class") = Rcpp::CharacterVector::create("hms", "difftime");
  out.attr("units") = "secs";

  return out;
}
