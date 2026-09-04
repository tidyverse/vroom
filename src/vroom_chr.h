#pragma once

#include <cpp11/strings.hpp>

#include "altrep.h"

#include "r_utils.h"
#include "vroom_vec.h"

// Allocate and parse a full character vector for `info`, reusing any strings
// already cached by vroom_chr::string_Elt() (see vroom_chr::EltChunk).
cpp11::strings read_chr(vroom_vec_info* info, SEXP cache = R_NilValue);

// Parse into `out` every element that is still R_BlankString.
void fill_chr(vroom_vec_info* info, SEXP out);

SEXP check_na(SEXP na, SEXP val);

struct vroom_chr : vroom_vec {

public:
  static R_altrep_class_t class_t;

  // Strings created by string_Elt() must stay reachable for as long as the
  // vector does, because R may hold a pointer from STRING_ELT() across a
  // later allocation. They are kept in fixed-size STRSXP chunks hanging off
  // a VECSXP in the external pointer's protected slot. Chunks are allocated
  // on first touch, so sparse access costs one chunk rather than a
  // full-length vector. Within a chunk, R_BlankString marks a miss: empty
  // strings share that permanent singleton and need no rooting.
  static constexpr R_xlen_t chunk_shift = 10;
  static constexpr R_xlen_t chunk_size = R_xlen_t(1) << chunk_shift;
  static constexpr R_xlen_t chunk_mask = chunk_size - 1;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(vroom_vec_info* info) {

    SEXP out = PROTECT(R_MakeExternalPtr(info, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(out, vroom_vec::Finalize, FALSE);

    // make a new altrep object of class `vroom_chr::class_t`
    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(1);

    MARK_NOT_MUTABLE(res); /* force duplicate on modify */

    return res;
  }

  // ALTREP methods -------------------

  // What gets printed when .Internal(inspect()) is used
  static Rboolean
  Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
    Rprintf(
        "vroom_chr (len=%" R_PRIdXLEN_T ", materialized=%s)\n",
        Length(x),
        R_altrep_data2(x) != R_NilValue ? "T" : "F");
    return TRUE;
  }

  // ALTSTRING methods -----------------

  static SEXP Val(SEXP vec, R_xlen_t i) {
    auto& info = Info(vec);

    auto&& col = info.column;
    auto str = col->at(i);

    auto val =
        PROTECT(info.locale->encoder_.makeSEXP(str.begin(), str.end(), true));

    if (Rf_xlength(val) < str.end() - str.begin()) {
      auto itr = col->begin() + i;
      info.errors->add_error(
          itr.index(), col->get_index(), "", "embedded null", itr.filename());
    }

    val = check_na(*info.na, val);

    info.errors->warn_for_errors();

    UNPROTECT(1);

    return val;
  }

  // The cache chunk holding element `i`, allocating it (and the chunk
  // vector) on first use.
  static SEXP EltChunk(SEXP vec, R_xlen_t i) {
    SEXP ptr = R_altrep_data1(vec);
    SEXP chunks = R_ExternalPtrProtected(ptr);
    if (chunks == R_NilValue) {
      R_xlen_t n_chunks = (Length(vec) + chunk_mask) >> chunk_shift;
      chunks = PROTECT(Rf_allocVector(VECSXP, n_chunks));
      R_SetExternalPtrProtected(ptr, chunks);
      UNPROTECT(1);
    }

    R_xlen_t c = i >> chunk_shift;
    SEXP chunk = VECTOR_ELT(chunks, c);
    if (chunk == R_NilValue) {
      R_xlen_t len = Length(vec) - (c << chunk_shift);
      if (len > chunk_size) {
        len = chunk_size;
      }
      chunk = PROTECT(Rf_allocVector(STRSXP, len));
      SET_VECTOR_ELT(chunks, c, chunk);
      UNPROTECT(1);
    }

    return chunk;
  }

  // the element at the index `i`
  //
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return STRING_ELT(data2, i);
    }
    SPDLOG_TRACE("{0:x}: vroom_chr string_Elt {1}", (size_t)vec, i);

    SEXP chunk = EltChunk(vec, i);
    R_xlen_t j = i & chunk_mask;
    SEXP val = STRING_ELT(chunk, j);
    if (val == R_BlankString) {
      val = PROTECT(Val(vec, i));
      SET_STRING_ELT(chunk, j, val);
      UNPROTECT(1);
    }

    return val;
  }

  // --- Altvec
  static SEXP Materialize(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 != R_NilValue) {
      return data2;
    }

    SPDLOG_TRACE("{0:x}: vroom_chr Materialize", (size_t)vec);
    SEXP data1 = R_altrep_data1(vec);
    auto out = read_chr(&Info(vec), R_ExternalPtrProtected(data1));
    R_set_altrep_data2(vec, out);

    // Once we have materialized we no longer need the cache or the info
    R_SetExternalPtrProtected(data1, R_NilValue);
    Finalize(data1);

    return out;
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return DATAPTR_RW(Materialize(vec));
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("vroom_chr", "vroom", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);
    R_set_altvec_Extract_subset_method(class_t, Extract_subset<vroom_chr>);

    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
};

// Called the package is loaded
[[cpp11::init]] void init_vroom_chr(DllInfo* dll);
