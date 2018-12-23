#include "idx.h"

// SEXP resize(SEXP in, size_t n) {
//// Rcerr << "Resizing to: " << n << std::endl;
// size_t sz = Rf_xlength(in);
// if (sz == n)
// return in;

// if (n > 0 && n < sz) {
// SETLENGTH(in, n);
// SET_TRUELENGTH(in, n);
//} else {
// in = Rf_xlengthgets(in, n);
//}
// return in;
//}

/**
 * Get the size of a file.
 * @param filename The name of the file to check size for
 * @return The filesize, or 0 if the file does not exist.
 */
size_t get_file_size(const std::string& filename) {
  struct stat st;
  if (stat(filename.c_str(), &st) != 0) {
    return 0;
  }
  return st.st_size;
}

size_t guess_size(size_t records, size_t bytes, size_t file_size) {
  double percent_complete = (double)(bytes) / file_size;
  size_t total_records = records / percent_complete * 1.1;
  return total_records;
}

std::tuple<
    std::shared_ptr<std::vector<size_t> >,
    size_t,
    mio::shared_mmap_source>
create_index(const std::string& filename) {
  auto out = std::make_shared<std::vector<size_t> >();

  // TODO: probably change this to something like 1024
  out->reserve(1024);

  size_t columns = 0;

  mio::shared_mmap_source mmap(filename);
  // From https://stackoverflow.com/a/17925143/2055486

  auto begin_p = mmap.cbegin();
  auto nl_p = begin_p;
  auto end_p = mmap.cend();
  auto sep_p = begin_p;
  auto file_size = end_p - begin_p;
  size_t prev_loc = 0;

  while (
      (nl_p =
           static_cast<const char*>(std::memchr(nl_p, '\n', (end_p - nl_p))))) {
    size_t cur_loc;

    while (sep_p < nl_p && (sep_p = static_cast<const char*>(
                                std::memchr(sep_p, '\t', end_p - sep_p)))) {
      cur_loc = sep_p - begin_p;

      out->push_back(prev_loc);
      // Rcpp::Rcout << prev_loc << " 1\n";
      prev_loc = cur_loc + 1;
      ++sep_p;
    }

    cur_loc = nl_p - begin_p;
    if (columns == 0) {
      columns = out->size();

      size_t new_size = guess_size(out->size(), cur_loc, file_size);
      out->reserve(new_size);
    }
    out->push_back(cur_loc + 1);
    // Rcpp::Rcout << cur_loc + 1 << " 2\n";

    ++nl_p;
  }

  out->push_back(file_size);

  return std::make_tuple(out, columns, mmap);
}
