#ifndef READIDX_IDX_HEADER
#define READIDX_IDX_HEADER

#include <Rcpp.h>

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include <array>

namespace vroom {

struct cell {
  const char* begin;
  const char* end;
};

class index {

public:
  index(
      const char* filename,
      const char delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      const char comment,
      const size_t num_threads);

  index() : rows_(0), columns_(0){};

  const std::string get(size_t row, size_t col) const;

  size_t num_columns() const { return columns_; }

  size_t num_rows() const { return rows_; }

  std::string filename() const { return filename_; }

  class row_iterator {
    size_t i_;
    size_t row_;
    const index* idx_;

  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string;
    using difference_type = std::string;
    using pointer = std::string*;
    using reference = std::string&;

    row_iterator(int row, const index* idx) : row_(row), idx_(idx) {
      i_ = (row_ + idx_->has_header_) * idx_->columns_;
    }
    row_iterator& begin() {
      i_ = (row_ + idx_->has_header_) * idx_->columns_;
      return *this;
    }
    row_iterator& end() {
      i_ = (row_ + idx_->has_header_ + 1) * idx_->columns_;
      return *this;
    }
    row_iterator operator++(int) /* postfix */ {
      row_iterator copy(*this);
      ++*this;
      return copy;
    }
    row_iterator& operator++() /* prefix */ {
      ++i_;
      return *this;
    }
    bool operator!=(row_iterator& other) const { return i_ != other.i_; }
    const std::string operator*() { return idx_->get_trimmed_val(i_); }
  };

  class col_iterator {
    size_t i_;
    size_t column_;
    const index* idx_;
    size_t start_;
    size_t end_;

  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string;
    using difference_type = std::string;
    using pointer = std::string*;
    using reference = std::string&;

    col_iterator(size_t column, const index* idx)
        : column_(column),
          idx_(idx),
          start_(idx_->has_header_),
          end_(idx_->has_header_ + idx_->rows_) {
      i_ = (start_ * idx_->columns_) + column_;
    }

    col_iterator(size_t column, const index* idx, size_t start, size_t end)
        : column_(column),
          idx_(idx),
          start_(start + idx_->has_header_),
          end_(end + idx_->has_header_) {
      i_ = (start_ * idx_->columns_) + column_;
    }
    col_iterator& begin() {
      i_ = (start_ * idx_->columns_) + column_;
      return *this;
    }
    col_iterator& end() {
      i_ = (end_)*idx_->columns_ + column_;
      return *this;
    }
    col_iterator operator++(int) /* postfix */ {
      col_iterator copy(*this);
      ++*this;
      return copy;
    }
    col_iterator& operator++() /* prefix */ {
      i_ += idx_->columns_;
      return *this;
    }
    bool operator!=(col_iterator& other) const { return i_ != other.i_; }

    std::string operator*() { return idx_->get_trimmed_val(i_); }

    col_iterator& operator+=(int n) {
      i_ += idx_->columns_ * n;
      return *this;
    }

    col_iterator operator+(int n) {
      col_iterator out(*this);
      out += n;
      return out;
    }
  };

  col_iterator column(size_t column) const {
    return col_iterator(column, this);
  }

  col_iterator column(size_t column, size_t start, size_t end) const {
    return col_iterator(column, this, start, end);
  }

  row_iterator row(size_t row) const { return row_iterator(row, this); }

  row_iterator header() const { return row_iterator(-1, this); }

protected:
  using idx_t = std::vector<size_t>;
  std::string filename_;
  mio::mmap_source mmap_;
  std::vector<idx_t> idx_;
  bool has_header_;
  char quote_;
  bool trim_ws_;
  bool escape_double_;
  bool escape_backslash_;
  size_t skip_;
  char comment_;
  size_t rows_;
  size_t columns_;

  void skip_lines();

  bool is_blank_or_comment_line(const char* begin) const {
    if (*begin == '\n') {
      return true;
    }

    while (*begin == ' ' || *begin == '\t') {
      ++begin;
      if (*begin == '\n' || *begin == comment_) {
        return true;
      }
    }

    return false;
  }

  // This skips leading blank lines and comments (if needed)
  template <typename T> size_t find_first_line(const T& source) {
    auto begin = 0;

    while (bool should_skip =
               is_blank_or_comment_line(source.data() + begin) || skip_ > 0) {
      begin = find_next_newline(source, begin) + 1;
      if (skip_ > 0) {
        --skip_;
      }
    }

    return begin;
  }

  template <typename T>
  size_t find_next_newline(const T& source, size_t start) const {
    auto begin = source.data() + start;
    auto res =
        static_cast<const char*>(memchr(begin, '\n', source.size() - start));
    if (!res) {
      return start;
    }
    return res - source.data();
  }

  void trim_quotes(const char*& begin, const char*& end) const;
  void trim_whitespace(const char*& begin, const char*& end) const;
  const std::string
  get_escaped_string(const char* begin, const char* end) const;

  const std::string get_trimmed_val(size_t i) const;

  std::pair<const char*, const char*> get_cell(size_t i) const;

  template <typename T>
  void index_region(
      const T& source,
      idx_t& destination,
      const char delim,
      const char quote,
      const size_t start,
      const size_t end) {

    // If there are no quotes quote will be '\0', so will just work
    std::array<char, 4> query = {delim, '\n', '\\', quote};

    size_t last = start;
#if DEBUG
    Rcpp::Rcerr << "start:\t" << start << '\n' << "end:\t" << end << '\n';
#endif

    bool in_quote = false;

    auto begin = source.data();

    // The actual parsing is here
    auto i = strcspn(begin + last, query.data()) + last;
    while (i < end) {
      auto c = source[i];

      if (c == delim && !in_quote) {
        destination.push_back(i + 1);
      }

      else if (escape_backslash_ && c == '\\') {
        ++i;
      }

      else if (c == quote) {
        in_quote = !in_quote;
      }

      else if (c == '\n') { // no embedded quotes allowed
        destination.push_back(i + 1);
      }

      last = i;
      i = strcspn(begin + last + 1, query.data()) + last + 1;
    }
  }
};

} // namespace vroom

#endif /* READIDX_IDX_HEADER */
