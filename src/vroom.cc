#include <mio/shared_mmap.hpp>

#include "LocaleInfo.h"
#include "columns.h"
#include "connection.h"
#include "index.h"
#include "index_collection.h"
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
    size_t n_max,
    bool progress,
    RObject col_names,
    RObject col_types,
    RObject col_select,
    SEXP id,
    CharacterVector na,
    List locale,
    size_t guess_max,
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
