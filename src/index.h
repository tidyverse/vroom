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

  class column {
    const index& idx_;
    size_t column_;

  public:
    column(const index& idx, size_t column);

    class iterator {
      size_t i_;
      const index* idx_;
      size_t column_;
      size_t start_;
      size_t end_;

    public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = std::string;
      using pointer = std::string*;
      using reference = std::string&;

      iterator(const index& idx, size_t column, size_t start, size_t end);
      iterator operator++(int); /* postfix */
      iterator& operator++();   /* prefix */
      bool operator!=(const iterator& other) const;
      bool operator==(const iterator& other) const;

      std::string operator*();
      iterator& operator+=(int n);
      iterator operator+(int n);
    };
    iterator begin();
    iterator end();
  };

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

  column get_column(size_t col) const {
    return vroom::index::column(*this, col);
  }

  row_iterator row(size_t row) const { return row_iterator(row, this); }

  row_iterator header() const { return row_iterator(-1, this); }

public:
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
    }

    if (*begin == '\n' || *begin == comment_) {
      return true;
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
    auto result = strcspn(begin + last, query.data());
    auto i = result + last;
#if DEBUG
    Rcpp::Rcerr << "last: " << last << " strcspn: " << result << " i: " << i
                << '\n';
#endif
    while (i < end) {
      auto c = source[i];

      if (c == delim && !in_quote) {
        destination.push_back(i);
      }

      else if (escape_backslash_ && c == '\\') {
        ++i;
      }

      else if (c == quote) {
        in_quote = !in_quote;
      }

      else if (c == '\n') { // no embedded quotes allowed
        destination.push_back(i);
      }

      last = i;
      result = strcspn(begin + last + 1, query.data());
      i = result + last + 1;
#if DEBUG
      Rcpp::Rcerr << "c: " << (int)c << " last: " << last
                  << " strcspn: " << result << " i: " << i << '\n';
#endif
    }
  }
};

} // namespace vroom

#endif /* READIDX_IDX_HEADER */
