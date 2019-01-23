#include "index.h"

#include "parallel.h"

#include <fstream>

size_t guess_size(size_t records, size_t bytes, size_t file_size) {
  double percent_complete = (double)(bytes) / file_size;
  size_t total_records = records / percent_complete * 1.1;
  return total_records;
}

using namespace vroom;

// const char index::skip_lines() {
// auto start = mmap_.data();
// while (skip > 0) {
//--skip;
// start = strchr(start + 1, '\n');
//}

// return start;
//}

size_t find_next_newline(const mio::mmap_source& mmap_, size_t start) {
  auto begin = mmap_.data() + start;
  auto res =
      static_cast<const char*>(memchr(begin, '\n', mmap_.size() - start));
  if (!res) {
    return start;
  }
  return res - mmap_.data();
}

index::index(
    const char* filename,
    const char delim,
    const char quote,
    const bool trim_ws,
    const bool has_header,
    const size_t skip,
    size_t num_threads)
    : filename_(filename),
      has_header_(has_header),
      quote_(quote),
      trim_ws_(trim_ws),
      rows_(0),
      columns_(0) {

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename, error);

  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  // From https://stackoverflow.com/a/17925143/2055486

  auto file_size = mmap_.cend() - mmap_.cbegin();

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
        values[id].reserve(128);
        if (id == 0) {
          values[id].push_back(0);
          end = find_next_newline(mmap_, end);
        } else {
          start = find_next_newline(mmap_, start);
          end = find_next_newline(mmap_, end);
        }
        // Rcpp::Rcerr << "Indexing start: ", v.size() << '\n';
        index_region(mmap_, values[id], delim, quote, start, end, id);
      },
      num_threads,
      true);

  auto total_size = std::accumulate(
      values.begin(),
      values.end(),
      0,
      [](size_t sum, const std::vector<size_t>& v) {
        sum += v.size();
#if DEBUG
        Rcpp::Rcerr << v.size() << '\n';
#endif
        return sum;
      });

  idx_.reserve(total_size);

#if DEBUG
  Rcpp::Rcerr << "combining vectors of size: " << total_size << "\n";
#endif

  for (auto& v : values) {

    idx_.insert(
        std::end(idx_),
        std::make_move_iterator(std::begin(v)),
        std::make_move_iterator(std::end(v)));
  }

  rows_ = idx_.size() / columns_;

  if (has_header_) {
    --rows_;
  }

#if DEBUG
  std::ofstream log(
      "index.idx",
      std::fstream::out | std::fstream::binary | std::fstream::trunc);
  for (auto& v : idx_) {
    log << v << '\n';
  }
  log.close();
  Rcpp::Rcerr << columns_ << ':' << rows_ << '\n';
#endif
}

cell index::trim_quotes(cell c) const {
  if (c.begin != c.end && (*c.begin == quote_)) {
    ++c.begin;
  }

  while (c.end != c.begin && *(c.end - 1) == quote_) {
    --c.end;
  }

  return c;
}

cell index::trim_whitespace(cell c) const {
  const std::locale loc("");
  while (c.begin != c.end && std::isspace(*c.begin, loc)) {
    ++c.begin;
  }

  while (c.end != c.begin && std::isspace(*(c.end - 1), loc)) {
    --c.end;
  }

  return c;
}

const cell index::get_trimmed_val(size_t i) const {
  auto begin = mmap_.data() + idx_[i];
  auto end = mmap_.data() + idx_[i + 1] - 1;
  cell out = {begin, end};

  if (trim_ws_) {
    out = trim_whitespace(out);
  }

  if (quote_ != '\0') {
    out = trim_quotes(out);
  }

  return out;
}

const cell index::get(size_t row, size_t col) const {
  auto i = (row + has_header_) * columns_ + col;

  return get_trimmed_val(i);
}
