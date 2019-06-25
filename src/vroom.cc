#include <mio/shared_mmap.hpp>

#include "LocaleInfo.h"
#include "columns.h"
#include "connection.h"
#include "index.h"
#include "index_collection.h"
#include "vroom_rle.h"
#include <Rcpp.h>
#include <algorithm>

using namespace Rcpp;

// [[Rcpp::export]]
SEXP vroom_(
    List inputs,
    SEXP delim,
    const char quote,
    bool trim_ws,
    bool escape_double,
    bool escape_backslash,
    const char comment,
    size_t skip,
    ptrdiff_t n_max,
    bool progress,
    RObject col_names,
    RObject col_types,
    RObject col_select,
    SEXP id,
    CharacterVector na,
    List locale,
    ptrdiff_t guess_max,
    size_t num_threads,
    size_t altrep_opts) {

  Rcpp::CharacterVector tempfile;

  bool has_header =
      col_names.sexp_type() == LGLSXP && as<LogicalVector>(col_names)[0];

  std::vector<std::string> filenames;

  bool add_filename = !Rf_isNull(id);

  // We need to retrieve filenames now before the connection objects are read,
  // as they are invalid afterwards.
  if (add_filename) {
    filenames = get_filenames(inputs);
  }

  auto idx = std::make_shared<vroom::index_collection>(
      inputs,
      Rf_isNull(delim) ? nullptr : Rcpp::as<const char*>(delim),
      quote,
      trim_ws,
      escape_double,
      escape_backslash,
      has_header,
      skip,
      n_max,
      comment,
      num_threads,
      progress);

  return create_columns(
      idx,
      col_names,
      col_types,
      col_select,
      id,
      filenames,
      na,
      locale,
      altrep_opts,
      guess_max,
      num_threads);
}

// [[Rcpp::export]]
bool has_trailing_newline(std::string filename) {
  std::FILE* f = std::fopen(filename.c_str(), "rb");

  if (!f) {
    return true;
  }

  fseek(f, -1, SEEK_END);
  char c = fgetc(f);
  return c == '\n';
}

// [[Rcpp::export]]
SEXP vroom_rle(Rcpp::IntegerVector input) {
#ifdef HAS_ALTREP
  return vroom_rle::Make(input);
#else
  R_xlen_t total_size = std::accumulate(input.begin(), input.end(), 0);
  CharacterVector out(total_size);
  CharacterVector nms = input.names();
  R_xlen_t idx = 0;
  for (R_xlen_t i = 0; i < Rf_xlength(input); ++i) {
    for (R_xlen_t j = 0; j < input[i]; ++j) {
      SET_STRING_ELT(out, idx++, nms[i]);
    }
  }
  return out;
#endif
}
