#include "index.h"

#include "parallel.h"
#include "utils.h"

#include <fstream>

size_t guess_size(size_t records, size_t bytes, size_t file_size) {
  double percent_complete = (double)(bytes) / file_size;
  size_t total_records = records / percent_complete * 1.1;
  return total_records;
}

using namespace vroom;

index::index(
    const char* filename,
    const char* delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    const char comment,
    size_t num_threads,
    bool progress)
    : filename_(filename),
      has_header_(has_header),
      quote_(quote),
      trim_ws_(trim_ws),
      escape_double_(escape_double),
      escape_backslash_(escape_backslash),
      skip_(skip),
      comment_(comment),
      rows_(0),
      columns_(0),
      progress_(progress),
      delim_len_(strlen(delim)) {

  std::error_code error;
  mmap_ = mio::make_mmap_source(filename, error);

  if (error) {
    throw Rcpp::exception(error.message().c_str(), false);
  }

  auto file_size = mmap_.cend() - mmap_.cbegin();

  auto start = find_first_line(mmap_);

  auto first_nl = find_next_newline(mmap_, start);
  auto second_nl = find_next_newline(mmap_, first_nl + 1);
  auto one_row_size = second_nl - first_nl;
  auto guessed_rows = (file_size - first_nl) / one_row_size * 1.1;

  // Check for windows newlines
  windows_newlines_ = first_nl > 0 && mmap_[first_nl - 1] == '\r';

  std::unique_ptr<multi_progress> pb = nullptr;

  if (progress_) {
    pb = std::unique_ptr<multi_progress>(
        new multi_progress(get_pb_format("file", filename), file_size));
    pb->tick(0);
  }

  //
  // We want at least 10 lines per batch, otherwise threads aren't really
  // useful
  auto batch_size = file_size / num_threads;
  auto line_size = second_nl - first_nl;
  if (batch_size < line_size * 10) {
    num_threads = 1;
  }

  idx_ = std::vector<idx_t>(num_threads + 1);

  // Index the first row
  idx_[0].push_back(start - 1);
  index_region(mmap_, idx_[0], delim, quote, start, first_nl + 1, pb, -1);
  columns_ = idx_[0].size() - 1;

  auto threads = parallel_for(
      file_size - first_nl,
      [&](int start, int end, int id) {
        idx_[id + 1].reserve((guessed_rows / num_threads) * columns_);
        start = find_next_newline(mmap_, first_nl + start);
        end = find_next_newline(mmap_, first_nl + end) + 1;
        index_region(
            mmap_, idx_[id + 1], delim, quote, start, end, pb, file_size / 200);
      },
      num_threads,
      true,
      false);

  if (progress_) {
    pb->display_progress();
  }

  for (auto& t : threads) {
    t.join();
  }

  auto total_size = std::accumulate(
      idx_.begin(), idx_.end(), 0, [](size_t sum, const idx_t& v) {
        sum += v.size() - 1;
        return sum;
      });

  // std::for_each(
  // threads.begin(), threads.end(), std::mem_fn(&std::thread::join));

  // for (auto& v : values) {

  // idx_->insert(
  // std::end(idx_),
  // std::make_move_iterator(std::begin(v)),
  // std::make_move_iterator(std::end(v)));
  //}

  rows_ = total_size / columns_;

  if (has_header_) {
    --rows_;
  }

#if DEBUG
  std::ofstream log(
      "index.idx",
      std::fstream::out | std::fstream::binary | std::fstream::trunc);
  for (auto& i : idx_) {
    for (auto& v : i) {
      log << v << '\n';
    }
    log << "---\n";
  }
  log.close();
  Rcpp::Rcerr << columns_ << ':' << rows_ << '\n';
#endif
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

#if DEBUG
  auto oi = i;
#endif

  for (const auto& idx : idx_) {
    auto sz = idx.size();
    if (i + 1 < sz) {
      size_t start;
      if (i % columns_ == 0) {
        start = idx[i] + 1;
      } else {
        start = idx[i] + delim_len_;
      }
      auto end = idx[i + 1];
      return {mmap_.data() + start, mmap_.data() + end};
    }

    i -= (sz - 1);
#if DEBUG
    Rcpp::Rcerr << "oi: " << oi << " i: " << i << " sz: " << sz << '\n';
#endif
  }

  throw std::out_of_range("blah");
  /* should never get here */
  return {0, 0};
}

const std::string index::get_trimmed_val(size_t i) const {

  const char *begin, *end;
  std::tie(begin, end) = get_cell(i);

  if (windows_newlines_) {
    bool is_last_column = i % columns_ == (columns_ - 1);
    if (is_last_column) {
      end--;
    }
  }

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

index::column::iterator::iterator(
    const index& idx, size_t column, size_t start, size_t end)
    : idx_(&idx), column_(column), start_(start + idx_->has_header_) {
  i_ = (start_ * idx_->columns_) + column_;
}

index::column::iterator index::column::iterator::operator++(int) /* postfix */ {
  index::column::iterator copy(*this);
  ++*this;
  return copy;
}
index::column::iterator& index::column::iterator::operator++() /* prefix */ {
  i_ += idx_->columns_;
  return *this;
}

bool index::column::iterator::
operator!=(const index::column::iterator& other) const {
  return i_ != other.i_;
}
bool index::column::iterator::
operator==(const index::column::iterator& other) const {
  return i_ == other.i_;
}

std::string index::column::iterator::operator*() {
  return idx_->get_trimmed_val(i_);
}

index::column::iterator& index::column::iterator::operator+=(int n) {
  i_ += idx_->columns_ * n;
  return *this;
}

index::column::iterator index::column::iterator::operator+(int n) {
  index::column::iterator out(*this);
  out += n;
  return out;
}

// Class column
index::column::column(const index& idx, size_t column)
    : idx_(idx), column_(column){};

index::column::iterator index::column::begin() {
  return index::column::iterator(idx_, column_, 0, idx_.num_rows());
}
index::column::iterator index::column::end() {
  return index::column::iterator(
      idx_, column_, idx_.num_rows(), idx_.num_rows());
}
