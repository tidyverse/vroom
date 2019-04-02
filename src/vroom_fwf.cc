#include "columns.h"

#include "fixed_width_index.h"

#include "LocaleInfo.h"

using namespace Rcpp;

// [[Rcpp::export]]
List vroom_fwf_(
    List inputs,
    std::vector<int> col_starts,
    std::vector<int> col_ends,
    bool trim_ws,
    RObject col_names,
    RObject col_types,
    RObject col_keep,
    RObject col_skip,
    size_t skip,
    const char comment,
    size_t n_max,
    SEXP id,
    CharacterVector na,
    List locale,
    size_t guess_max,
    size_t num_threads,
    size_t altrep_opts,
    bool progress) {

  std::vector<std::string> filenames;

  bool add_filename = !Rf_isNull(id);

  // We need to retrieve filenames now before the connection objects are read,
  // as they are invalid afterwards.
  if (add_filename) {
    filenames = get_filenames(inputs);
  }

  auto idx = std::make_shared<vroom::index_collection>(
      inputs, col_starts, col_ends, trim_ws, skip, comment, n_max, progress);

  return create_columns(
      idx,
      col_names,
      col_types,
      col_keep,
      col_skip,
      id,
      filenames,
      na,
      locale,
      altrep_opts,
      guess_max,
      num_threads);
}
