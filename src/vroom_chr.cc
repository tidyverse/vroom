#include "vroom_chr.h"

Rcpp::CharacterVector read_chr(vroom_vec_info* info) {

  R_xlen_t n = info->column->size();

  Rcpp::CharacterVector out(n);

  auto i = 0;
  for (const auto& str : *info->column) {
    auto val = info->locale->encoder_.makeSEXP(str.begin(), str.end(), false);

    // Look for NAs
    for (const auto& v : *info->na) {
      // We can just compare the addresses directly because they should now
      // both be in the global string cache.
      if (v == val) {
        val = NA_STRING;
        break;
      }
    }

    out[i++] = val;
  }

  return out;
}

#ifdef HAS_ALTREP

R_altrep_class_t vroom_chr::class_t;

void init_vroom_chr(DllInfo* dll) { vroom_chr::Init(dll); }

#else
void init_vroom_chr(DllInfo* dll) {}
#endif
