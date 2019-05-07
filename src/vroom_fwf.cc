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
    RObject col_select,
    size_t skip,
    const char comment,
    ptrdiff_t n_max,
    SEXP id,
    CharacterVector na,
    List locale,
    ptrdiff_t guess_max,
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
      col_select,
      id,
      filenames,
      na,
      locale,
      altrep_opts,
      guess_max,
      num_threads);
}

template <typename Iterator>
std::vector<bool> find_empty_cols(Iterator begin, Iterator end, size_t n) {

  std::vector<bool> is_white;

  size_t row = 0, col = 0;
  for (Iterator cur = begin; cur != end; ++cur) {
    if (row > n)
      break;

    switch (*cur) {
    case '\n':
      col = 0;
      row++;
      break;
    case '\r':
    case ' ':
      col++;
      break;
    default:
      // Make sure there's enough room
      if (col >= is_white.size())
        is_white.resize(col + 1, true);
      is_white[col] = false;
      col++;
    }
  }

  return is_white;
}

// [[Rcpp::export]]
List whitespace_columns_(
    std::string filename, size_t skip, int n = 100, std::string comment = "") {

  std::error_code error;
  auto mmap = mio::make_mmap_source(filename, error);
  if (error) {
    // We cannot actually portably compare error messages due to a bug in
    // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
    // the message on stderr return
    Rcpp::Rcerr << "mapping error: " << error.message();
    return List();
  }

  size_t s = find_first_line(mmap, 0, comment[0]);

  std::vector<bool> empty = find_empty_cols(mmap.begin() + s, mmap.end(), n);
  std::vector<int> begin, end;

  bool in_col = false;

  for (size_t i = 0; i < empty.size(); ++i) {
    if (in_col && empty[i]) {
      end.push_back(i);
      in_col = false;
    } else if (!in_col && !empty[i]) {
      begin.push_back(i);
      in_col = true;
    }
  }

  if (in_col)
    end.push_back(empty.size());

  return List::create(_["begin"] = begin, _["end"] = end);
}
