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

cpp11::integers read_fct_explicit(
    vroom_vec_info* info, const cpp11::strings& levels, bool ordered) {
  R_xlen_t n = info->column->size();

  cpp11::writable::integers out(n);
  std::unordered_map<SEXP, size_t> level_map;

  for (auto i = 0; i < levels.size(); ++i) {
    if (levels[i] == NA_STRING) {
      for (const auto& str : *info->na) {
        level_map[str] = i + 1;
      }
    } else {
      level_map[levels[i]] = i + 1;
    }
  }

  auto col = info->column;
  R_xlen_t i = 0;
  for (auto b = col->begin(), e = col->end(); b != e; ++b) {
    auto str = *b;
    out[i++] =
        parse_factor(b, col, level_map, *info->locale, info->errors, *info->na);
  }

  info->errors->warn_for_errors();

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
  cpp11::writable::strings levels;
  std::unordered_map<std::string, size_t> level_map;

  auto nas = cpp11::as_cpp<std::vector<std::string>>(*info->na);

  size_t max_level = 1;

  auto start = 0;
  auto end = n;
  auto i = start;
  auto col = info->column->slice(start, end);
  int na_level = NA_INTEGER;
  for (const auto& str : *col) {
    auto val = level_map.find(str.str());
    if (val != level_map.end()) {
      out[i++] = val->second;
    } else {
      if (include_na && matches(str, nas)) {
        if (na_level == NA_INTEGER) {
          na_level = max_level++;
          levels.push_back(NA_STRING);
          out[i++] = na_level;
          level_map[str.str()] = na_level;
        }
      } else {
        out[i++] = max_level;
        level_map[str.str()] = max_level++;
        levels.push_back(
            info->locale->encoder_.makeSEXP(str.begin(), str.end(), false));
      }
    }
  }

  out.attr("levels") = static_cast<SEXP>(levels);
  out.attr("class") = "factor";

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_fct::class_t;

void init_vroom_fct(DllInfo* dll) { vroom_fct::Init(dll); }

#else
void init_vroom_fct(DllInfo* dll) {}
#endif
