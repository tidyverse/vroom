#include "vroom_dttm.h"

double parse_dttm(
    const char* begin,
    const char* end,
    DateTimeParser& parser,
    const std::string& format) {
  if (format == "%s") {
    double out;
    bool ok = parseDouble('.', begin, end, out);
    if (!ok) {
      return NA_REAL;
    }
    return out;
  }
  parser.setDate(begin, end);
  bool res = (format == "") ? parser.parseISO8601() : parser.parse(format);

  if (res) {
    DateTime dt = parser.makeDateTime();
    if (dt.validDateTime()) {
      return dt.datetime();
    }
  }
  return NA_REAL;
}

cpp11::doubles read_dttm(vroom_vec_info* info) {
  R_xlen_t n = info->column->size();

  cpp11::writable::doubles out(n);
  auto err_msg = info->format.size() == 0
                     ? std::string("date in ISO8601")
                     : std::string("date like ") + info->format;

  try {
    parallel_for(
        n,
        [&](size_t start, size_t end, size_t) {
          // Use bulk extraction for better performance
          DateTimeParser parser(info->locale.get());
          auto col = info->column->slice(start, end);
          auto strings = col->extract_all();

          for (size_t j = 0; j < strings.size(); ++j) {
            R_xlen_t i = start + j;
            auto& str = strings[j];

            if (vroom::is_explicit_na(*info->na, str.begin(), str.end())) {
              out[i] = NA_REAL;
              continue;
            }

            double val = parse_dttm(str.begin(), str.end(), parser, info->format);
            if (cpp11::is_na(val)) {
              auto b = col->begin() + j;
              info->errors->add_error(
                  b.index(),
                  col->get_index(),
                  err_msg.c_str(),
                  std::string(str.begin(), str.end() - str.begin()),
                  b.filename());
            }
            out[i] = val;
          }
        },
        info->num_threads,
        true);
  } catch (const std::runtime_error& e) {
    Rf_errorcall(R_NilValue, "%s", e.what());
  }

  info->errors->warn_for_errors();

  out.attr("class") = {"POSIXct", "POSIXt"};
  out.attr("tzone") = info->locale->tz_;

  return out;
}

R_altrep_class_t vroom_dttm::class_t;

void init_vroom_dttm(DllInfo* dll) { vroom_dttm::Init(dll); }

[[cpp11::register]] cpp11::writable::doubles utctime_(
    const cpp11::integers& year,
    const cpp11::integers& month,
    const cpp11::integers& day,
    const cpp11::integers& hour,
    const cpp11::integers& min,
    const cpp11::integers& sec,
    const cpp11::doubles& psec) {
  int n = year.size();
  if (month.size() != n || day.size() != n || hour.size() != n ||
      min.size() != n || sec.size() != n || psec.size() != n) {
    cpp11::stop("All inputs must be same length");
  }

  cpp11::writable::doubles out(n);

  for (int i = 0; i < n; ++i) {
    DateTime dt(
        year[i], month[i], day[i], hour[i], min[i], sec[i], psec[i], "UTC");
    out[i] = dt.datetime();
  }

  out.attr("class") = {"POSIXct", "POSIXt"};
  out.attr("tzone") = "UTC";

  return out;
}
