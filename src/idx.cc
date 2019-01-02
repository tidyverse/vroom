#include "idx.h"

#include "parallel.h"

#include <fstream>

size_t guess_size(size_t records, size_t bytes, size_t file_size) {
  double percent_complete = (double)(bytes) / file_size;
  size_t total_records = records / percent_complete * 1.1;
  return total_records;
}

// From https://stackoverflow.com/a/53710597/2055486
// To move an object use `std::move()` when calling `append()`
// append<type>(std::move(source),destination)
//
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
create_index(const char* filename, char delim, int num_threads) {
  size_t columns = 0;

  std::error_code error;
  mio::shared_mmap_source mmap = mio::make_mmap_source(filename, error);
  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  // From https://stackoverflow.com/a/17925143/2055486

  auto file_size = mmap.cend() - mmap.cbegin();

  // This should be enough to ensure the first line fits in one thread, I
  // hope...
  if (file_size < 32768) {
    num_threads = 1;
  }

  // We read the values into a vector of vectors, then merge them afterwards
  std::vector<std::vector<size_t> > values(num_threads);

  parallel_for(
      file_size,
      [&](int start, int end, int id) {
        // Rcpp::Rcerr << start << '\t' << end - start << '\n';
        std::error_code error;
        auto thread_mmap =
            mio::make_mmap_source(filename, start, end - start, error);
        if (error) {
          throw Rcpp::exception(error.message().c_str(), false);
        }

        size_t cur_loc = start;
        values[id].reserve(128);

        // The actual parsing is here
        for (auto i = thread_mmap.cbegin(); i != thread_mmap.cend(); ++i) {
          if (*i == '\n') {
            if (id == 0 && columns == 0) {
              columns = values[id].size() + 1;
            }
            values[id].push_back(cur_loc + 1);

          } else if (*i == delim) {
            // Rcpp::Rcout << id << '\n';
            values[id].push_back(cur_loc + 1);
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

  // std::ofstream log(
  //"test2.idx",
  // std::fstream::out | std::fstream::binary | std::fstream::trunc);
  // for (auto& v : *out) {
  // log << v << '\n';
  //}
  // log.close();

  return std::make_tuple(out, columns, mmap);
}
