#include "vroom_chr.h"

static void copy_cached_chr(SEXP cache, SEXP out);

SEXP check_na(SEXP na, SEXP val) {
  for (R_xlen_t i = 0; i < Rf_xlength(na); ++i) {
    SEXP v = STRING_ELT(na, i);
    // We can just compare the addresses directly because they should now
    // both be in the global string cache.
    if (v == val) {
      return NA_STRING;
    }
  }
  return val;
}

cpp11::strings read_chr(vroom_vec_info* info, SEXP cache) {

  R_xlen_t n = info->column->size();

  cpp11::writable::strings out(n);

  copy_cached_chr(cache, out);
  fill_chr(info, out);

  return out;
}

void fill_chr(vroom_vec_info* info, SEXP out) {
  SEXP nas = *info->na;

  cpp11::unwind_protect([&] {
    R_xlen_t i = 0;
    auto col = info->column;
    for (auto b = col->begin(), e = col->end(); b != e; ++b, ++i) {
      if (STRING_ELT(out, i) != R_BlankString) {
        continue;
      }

      auto str = *b;
      auto val = info->locale->encoder_.makeSEXP(str.begin(), str.end(), true);
      PROTECT(val);
      if (Rf_xlength(val) < str.end() - str.begin()) {
        info->errors->add_error(
            b.index(), col->get_index(), "", "embedded null", b.filename());
      }

      SET_STRING_ELT(out, i, check_na(nas, val));
      UNPROTECT(1);
    }
  });

  info->errors->warn_for_errors();
}

// Copy strings cached by vroom_chr::string_Elt() into `out`, so that
// materializing does not parse them a second time.
static void copy_cached_chr(SEXP cache, SEXP out) {
  if (cache == R_NilValue) {
    return;
  }

  R_xlen_t n_chunks = Rf_xlength(cache);
  for (R_xlen_t c = 0; c < n_chunks; ++c) {
    SEXP chunk = VECTOR_ELT(cache, c);
    if (chunk == R_NilValue) {
      continue;
    }

    R_xlen_t offset = c << vroom_chr::chunk_shift;
    R_xlen_t len = Rf_xlength(chunk);
    for (R_xlen_t j = 0; j < len; ++j) {
      SEXP val = STRING_ELT(chunk, j);
      if (val != R_BlankString) {
        SET_STRING_ELT(out, offset + j, val);
      }
    }
  }
}

R_altrep_class_t vroom_chr::class_t;

void init_vroom_chr(DllInfo* dll) { vroom_chr::Init(dll); }
