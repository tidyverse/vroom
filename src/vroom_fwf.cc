#include <cpp11/list.hpp>
#include <cpp11/sexp.hpp>
#include <cpp11/strings.hpp>
#include <utility>

#include "LocaleInfo.h"
#include "columns.h"
#include "fixed_width_index.h"
#include "unicode_fopen.h"

[[cpp11::register]] cpp11::list vroom_fwf_(
    const cpp11::list& inputs,
    const std::vector<int>& col_starts,
    const std::vector<int>& col_ends,
    bool trim_ws,
    cpp11::sexp col_names,
    cpp11::sexp col_types,
    cpp11::sexp col_select,
    cpp11::sexp name_repair,
    size_t skip,
    const char* comment,
    bool skip_empty_rows,
    ptrdiff_t n_max,
    SEXP id,
    const cpp11::strings& na,
    const cpp11::list& locale,
    ptrdiff_t guess_max,
    size_t num_threads,
    size_t altrep,
    bool progress) {

  std::vector<std::string> filenames;

  bool add_filename = !Rf_isNull(id);

  // We need to retrieve filenames now before the connection objects are read,
  // as they are invalid afterwards.
  if (add_filename) {
    filenames = get_filenames(inputs);
  }

  auto idx = std::make_shared<vroom::index_collection>(
      inputs,
      col_starts,
      col_ends,
      trim_ws,
      skip,
      comment,
      skip_empty_rows,
      n_max,
      progress);

  auto errors = new std::shared_ptr<vroom_errors>(new vroom_errors());

  return create_columns(
      idx,
      std::move(col_names),
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

template <typename Iterator>
std::vector<bool> find_empty_cols(Iterator begin, Iterator end, ptrdiff_t n) {

  std::vector<bool> is_white;

  size_t row = 0, col = 0;
  for (Iterator cur = begin; cur != end; ++cur) {
    if (n > 0 && row > static_cast<size_t>(n)) {
      break;
    }

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

[[cpp11::register]] cpp11::list whitespace_columns_(
    const std::string& filename,
    size_t skip,
    ptrdiff_t n,
    const std::string& comment) {

  std::error_code error;
  auto mmap = make_mmap_source(filename.c_str(), error);
  if (error) {
    // We cannot actually portably compare error messages due to a bug in
    // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
    // the message on stderr return
    REprintf("mapping error: %s", error.message().c_str());
    return cpp11::list();
  }

  size_t s = find_first_line(
      mmap,
      skip,
      comment.data(),
      /* skip_empty_rows */ true,
      /* embedded_nl */ false,
      /* quote */ '\0');

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

  using namespace cpp11::literals;
  return cpp11::writable::list({"begin"_nm = begin, "end"_nm = end});
}
