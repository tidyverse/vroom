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

  auto begin = mmap.cbegin();
  auto end = mmap.cend();
  auto file_size = end - begin;
  size_t prev_loc = 0;
  size_t cur_loc = 0;

  for (auto i = begin; i != end; ++i) {
    switch (*i) {
    case '\n': {
      if (columns == 0) {
        columns = out->size() + 1;

        size_t new_size = guess_size(out->size(), cur_loc, file_size);
        out->reserve(new_size);
      }
      /* explicit fall through */
    }
    case '\t': {
      out->push_back(prev_loc);
      prev_loc = cur_loc + 1;
      break;
    }
    }
    ++cur_loc;
  }

  out->push_back(file_size);

  return std::make_tuple(out, columns, mmap);
}
