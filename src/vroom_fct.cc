#include "vroom_fct.h"
#include <unordered_map>

bool matches(const string& needle, const std::vector<std::string>& haystack) {
  for (auto& hay : haystack) {
    if (needle == hay) {
      return true;
    }
  }
  return false;
}

cpp11::integers
read_fct_explicit(vroom_vec_info* info, cpp11::strings levels, bool ordered) {
  R_xlen_t n = info->column->size();

  cpp11::writable::integers out(n);
  std::unordered_map<SEXP, size_t> level_map;

  for (auto i = 0; i < levels.size(); ++i) {
    level_map[levels[i]] = i + 1;
  }

  R_xlen_t i = 0;
  for (const auto& str : *info->column) {
    auto search = level_map.find(
        info->locale->encoder_.makeSEXP(str.begin(), str.end(), false));
    if (search != level_map.end()) {
      out[i++] = search->second;
    } else {
      out[i++] = NA_INTEGER;
    }
  }

  out.attr("levels") = static_cast<SEXP>(levels);
  if (ordered) {
    out.attr("class") = {"ordered", "factor"};
  } else {
    out.attr("class") = "factor";
  }

  return out;
}

cpp11::integers read_fct_implicit(vroom_vec_info* info, bool include_na) {
  R_xlen_t n = info->column->size();

  cpp11::writable::integers out(n);
  std::vector<std::string> levels;
  std::unordered_map<vroom::string, size_t> level_map;

  auto nas = cpp11::as_cpp<std::vector<std::string> >(*info->na);

  size_t max_level = 1;

  auto start = 0;
  auto end = n;
  auto i = start;
  auto col = info->column->slice(start, end);
  for (const auto& str : *col) {
    if (include_na && matches(str, nas)) {
      out[i++] = NA_INTEGER;
    } else {
      auto val = level_map.find(str);
      if (val != level_map.end()) {
        out[i++] = val->second;
      } else {
        out[i++] = max_level;
        level_map[str] = max_level++;
        levels.emplace_back(str.str());
      }
    }
  }

  cpp11::writable::strings out_lvls(levels.size());
  for (R_xlen_t i = 0; i < static_cast<R_xlen_t>(levels.size()); ++i) {
    out_lvls[i] = info->locale->encoder_.makeSEXP(
        levels[i].c_str(), levels[i].c_str() + levels[i].size(), false);
  }
  if (include_na) {
    out_lvls.push_back(NA_STRING);
  }

  out.attr("levels") = static_cast<SEXP>(out_lvls);
  out.attr("class") = "factor";

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_fct::class_t;

void init_vroom_fct(DllInfo* dll) { vroom_fct::Init(dll); }

#else
void init_vroom_fct(DllInfo* dll) {}
#endif
