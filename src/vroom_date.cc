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

            double val = parse_date(str.begin(), str.end(), parser, info->format);
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

  out.attr("class") = "Date";

  return out;
}

R_altrep_class_t vroom_date::class_t;

void init_vroom_date(DllInfo* dll) { vroom_date::Init(dll); }
