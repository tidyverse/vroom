#include "cpp11/as.hpp"
#include "cpp11/list.hpp"
#include "cpp11/strings.hpp"
#include <string>
#include <vector>

#include "LocaleInfo.h"

LocaleInfo::LocaleInfo(const cpp11::list& x)
    : encoding_(cpp11::as_cpp<std::string>(x["encoding"])),
      encoder_(Iconv(encoding_)) {
  std::string klass = cpp11::as_cpp<std::string>(x.attr("class"));
  if (klass != "locale")
    cpp11::stop("Invalid input: must be of class locale");

  cpp11::list date_names(x["date_names"]);
  mon_ = cpp11::as_cpp<std::vector<std::string>>(date_names["mon"]);
  monAb_ = cpp11::as_cpp<std::vector<std::string>>(date_names["mon_ab"]);
  day_ = cpp11::as_cpp<std::vector<std::string>>(date_names["day"]);
  dayAb_ = cpp11::as_cpp<std::vector<std::string>>(date_names["day_ab"]);
  amPm_ = cpp11::as_cpp<std::vector<std::string>>(date_names["am_pm"]);

  Iconv date_names_encoder("UTF-8", encoding_);
  auto encode_date_names = [&](std::vector<std::string>& names) {
    for (auto& name : names) {
      name = date_names_encoder.makeString(
          name.data(), name.data() + name.size());
    }
  };
  encode_date_names(mon_);
  encode_date_names(monAb_);
  encode_date_names(day_);
  encode_date_names(dayAb_);
  encode_date_names(amPm_);

  decimalMark_ = cpp11::as_cpp<std::string>(x["decimal_mark"]);
  groupingMark_ = cpp11::as_cpp<std::string>(x["grouping_mark"]);

  dateFormat_ = cpp11::as_cpp<std::string>(x["date_format"]);
  timeFormat_ = cpp11::as_cpp<std::string>(x["time_format"]);

  tz_ = cpp11::as_cpp<std::string>(x["tz"]);
}
