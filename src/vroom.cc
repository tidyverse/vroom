#include <mio/shared_mmap.hpp>

#include <cpp11/R.hpp>
#include <cpp11/external_pointer.hpp>
#include <cpp11/list.hpp>
#include <cpp11/strings.hpp>

#include "LocaleInfo.h"
#include "columns.h"
#include "connection.h"
#include "index.h"
#include "index_collection.h"
#include "vroom_rle.h"
#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>


#include "unicode_fopen.h"
#include "vroom_errors.h"

[[cpp11::register]] SEXP vroom_(
    const cpp11::list& inputs,
    SEXP delim,
    const char quote,
    bool trim_ws,
    bool escape_double,
    bool escape_backslash,
    const char* comment,
    const bool skip_empty_rows,
    size_t skip,
    ptrdiff_t n_max,
    bool progress,
    const cpp11::sexp& col_names,
    cpp11::sexp col_types,
    cpp11::sexp col_select,
    cpp11::sexp name_repair,
    SEXP id,
    const cpp11::strings& na,
    const cpp11::list& locale,
    ptrdiff_t guess_max,
    size_t num_threads,
    size_t altrep) {

  bool has_header =
      TYPEOF(col_names) == LGLSXP && cpp11::logicals(col_names)[0];

  std::vector<std::string> filenames;

  bool add_filename = !Rf_isNull(id);

  // We need to retrieve filenames now before the connection objects are read,
  // as they are invalid afterwards.
  if (add_filename) {
    filenames = get_filenames(inputs);
  }

  auto errors = new std::shared_ptr<vroom_errors>(new vroom_errors());

  (*errors)->has_header(has_header);

  auto idx = std::make_shared<vroom::index_collection>(
      inputs,
      Rf_isNull(delim) ? nullptr : cpp11::as_cpp<const char*>(delim),
      quote,
      trim_ws,
      escape_double,
      escape_backslash,
      has_header,
      skip,
      n_max,
      comment,
      skip_empty_rows,
      *errors,
      num_threads,
      progress);

  (*errors)->resolve_parse_errors(*idx);

  return create_columns(
      idx,
      col_names,
      std::move(col_types),
      std::move(col_select),
      std::move(name_repair),
      id,
      filenames,
      na,
      locale,
      altrep,
      guess_max,
      errors,
      num_threads);
}

[[cpp11::register]] bool has_trailing_newline(const cpp11::strings& filename) {
  std::FILE* f = unicode_fopen(CHAR(filename[0]), "rb");

  if (!f) {
    return true;
  }

  std::setvbuf(f, nullptr, _IONBF, 0);

  fseek(f, -1, SEEK_END);
  char c = fgetc(f);

  fclose(f);

  return c == '\n';
}

[[cpp11::register]] SEXP vroom_rle(const cpp11::integers& input) {
#ifdef HAS_ALTREP
  return vroom_rle::Make(input);
#else
  R_xlen_t total_size = std::accumulate(input.begin(), input.end(), 0);
  cpp11::writable::strings out(total_size);
  cpp11::strings nms = input.names();
  R_xlen_t idx = 0;
  for (R_xlen_t i = 0; i < Rf_xlength(input); ++i) {
    for (R_xlen_t j = 0; j < input[i]; ++j) {
      SET_STRING_ELT(out, idx++, nms[i]);
    }
  }
  return out;
#endif
}
