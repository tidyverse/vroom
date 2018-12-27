#include "idx.h"

#include "parallel.h"

#include <array>

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

// std::vector<T>&& src - src MUST be an rvalue reference
// std::vector<T> src - src MUST NOT, but MAY be an rvalue reference
template <typename T>
inline void append(std::vector<T> source, std::vector<T>& destination) {
  if (destination.empty())
    destination = std::move(source);
  else
    destination.insert(
        std::end(destination),
        std::make_move_iterator(std::begin(source)),
        std::make_move_iterator(std::end(source)));
}

std::tuple<
    std::shared_ptr<std::vector<size_t> >,
    size_t,
    mio::shared_mmap_source>
create_index(const std::string& filename, int num_threads) {
  size_t columns = 0;

  mio::shared_mmap_source mmap(filename);
  // From https://stackoverflow.com/a/17925143/2055486

  auto file_size = mmap.cend() - mmap.cbegin();

  // We read the values into a vector of vectors, then merge them afterwards
  std::vector<std::vector<size_t> > values(num_threads);

  parallel_for(
      file_size,
      [&](int start, int end, int id) {
        // Rcpp::Rcerr << start << '\t' << end - start << '\n';
        mio::mmap_source thread_mmap(filename, start, end - start);

        size_t cur_loc = start;
        values[id].reserve(128);

        // The actual parsing is here
        for (auto i = thread_mmap.cbegin(); i != thread_mmap.cend(); ++i) {
          switch (*i) {
          case '\n': {
            if (id == 0 && columns == 0) {
              columns = values[id].size() + 1;
            }
          }
          case '\t': {
            // Rcpp::Rcout << id << '\n';
            values[id].push_back(cur_loc + 1);
            break;
          }
          }
          ++cur_loc;
        }
      },
      num_threads,
      true);

  // Rcpp::Rcerr << "Calculating total size\n";
  auto total_size = std::accumulate(
      values.begin(),
      values.end(),
      0,
      [](size_t sum, const std::vector<size_t>& v) {
        sum += v.size();
        return sum;
      });

  auto out = std::make_shared<std::vector<size_t> >();

  out->reserve(total_size + 1);

  out->push_back(0);

  // Rcpp::Rcerr << "combining vectors\n";
  for (auto& v : values) {
    append<size_t>(std::move(v), *out);
  }

  return std::make_tuple(out, columns, mmap);
}
