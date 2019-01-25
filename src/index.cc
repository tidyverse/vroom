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

index::index(
    const char* filename,
    const char delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    const char comment,
    size_t num_threads)
    : filename_(filename),
      has_header_(has_header),
      quote_(quote),
      trim_ws_(trim_ws),
      escape_double_(escape_double),
      escape_backslash_(escape_backslash),
      skip_(skip),
      comment_(comment),
      rows_(0),
      columns_(0) {

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename, error);

  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  auto file_size = mmap_.cend() - mmap_.cbegin();

  idx_ = std::vector<idx_t>(num_threads + 1);

  auto start = find_first_line(mmap_);
  idx_[0].push_back(start);

  // Index the first row
  auto first_nl = find_next_newline(mmap_, start);
  index_region(mmap_, idx_[0], delim, quote, start, first_nl + 1);
  columns_ = idx_[0].size() - 1;

  auto second_nl = find_next_newline(mmap_, first_nl + 1);
  auto one_row_size = second_nl - first_nl;
  auto guessed_rows = (file_size - first_nl) / one_row_size * 1.1;

#if DEBUG
  Rcpp::Rcerr << "columns: " << columns_ << " guessed_rows: " << guessed_rows
              << '\n';
#endif

  // This should be enough to ensure the first line fits in one thread, I
  // hope...
  if (file_size < 32768) {
    num_threads = 1;
  }

  parallel_for(
      file_size - first_nl,
      [&](int start, int end, int id) {
        idx_[id + 1].reserve((guessed_rows / num_threads) * columns_);
        start = find_next_newline(mmap_, first_nl + start);
        end = find_next_newline(mmap_, first_nl + end);
        // Rcpp::Rcerr << "Indexing start: ", v.size() << '\n';
        index_region(mmap_, idx_[id + 1], delim, quote, start, end + 1);
      },
      num_threads,
      true);

  auto total_size = std::accumulate(
      idx_.begin(), idx_.end(), 0, [](size_t sum, const idx_t& v) {
        sum += v.size();
#if DEBUG
        Rcpp::Rcerr << v.size() << '\n';
#endif
        return sum;
      });

  // idx_.reserve(total_size);

#if DEBUG
  Rcpp::Rcerr << "combining vectors of size: " << total_size << "\n";
#endif

  // for (auto& v : values) {

  // idx_.insert(
  // std::end(idx_),
  // std::make_move_iterator(std::begin(v)),
  // std::make_move_iterator(std::end(v)));
  //}

  rows_ = total_size / columns_;

  if (has_header_) {
    --rows_;
  }

  //#if DEBUG
  // std::ofstream log(
  //"index.idx",
  // std::fstream::out | std::fstream::binary | std::fstream::trunc);
  // for (auto& v : idx_) {
  // log << v << '\n';
  //}
  // log.close();
  // Rcpp::Rcerr << columns_ << ':' << rows_ << '\n';
  //#endif
}

void index::trim_quotes(const char*& begin, const char*& end) const {
  if (begin != end && (*begin == quote_)) {
    ++begin;
  }

  if (end != begin && *(end - 1) == quote_) {
    --end;
  }
}

void index::trim_whitespace(const char*& begin, const char*& end) const {
  static const std::locale loc("");
  while (begin != end && std::isspace(*begin, loc)) {
    ++begin;
  }

  while (end != begin && std::isspace(*(end - 1), loc)) {
    --end;
  }
}

const std::string
index::get_escaped_string(const char* begin, const char* end) const {
  std::string out;
  out.reserve(end - begin);

  while (begin < end) {
    if ((escape_double_ && *begin == '"') ||
        (escape_backslash_ && *begin == '\\')) {
      ++begin;
    }

    out.push_back(*begin++);
  }

  return out;
}

std::pair<const char*, const char*> index::get_cell(size_t i) const {

  for (const auto& idx : idx_) {
    auto sz = idx.size();
    if (i + 1 < sz) {
      return {mmap_.data() + idx[i], mmap_.data() + idx[i + 1] - 1};
    }

    i -= (sz - 1);
  }

  throw std::out_of_range("blah");
  /* should never get here */
  return {0, 0};
}

const std::string index::get_trimmed_val(size_t i) const {

  const char *begin, *end;
  std::tie(begin, end) = get_cell(i);

  if (trim_ws_) {
    trim_whitespace(begin, end);
  }

  if (quote_ != '\0') {
    trim_quotes(begin, end);
  }

  std::string out;

  if (escape_double_ || escape_backslash_) {
    out = get_escaped_string(begin, end);
  } else {
    out = std::string(begin, end - begin);
  }

  return out;
}

const std::string index::get(size_t row, size_t col) const {
  auto i = (row + has_header_) * columns_ + col;

  return get_trimmed_val(i);
}
