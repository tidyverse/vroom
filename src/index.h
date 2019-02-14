#ifndef READIDX_IDX_HEADER
#define READIDX_IDX_HEADER

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include <array>

#include "multi_progress.h"

#include <Rcpp.h>

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
      const size_t num_threads,
      const bool progress);

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
  bool windows_newlines_;
  size_t skip_;
  char comment_;
  size_t rows_;
  size_t columns_;
  bool progress_;

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

  template <typename T> size_t skip_bom(const T& source) {
    /* Skip Unicode Byte Order Marks
       https://en.wikipedia.org/wiki/Byte_order_mark#Representations_of_byte_order_marks_by_encoding
       00 00 FE FF: UTF-32BE
       FF FE 00 00: UTF-32LE
       FE FF:       UTF-16BE
       FF FE:       UTF-16LE
       EF BB BF:    UTF-8
   */

    auto size = source.size();
    auto begin = source.data();

    switch (begin[0]) {
    // UTF-32BE
    case '\x00':
      if (size >= 4 && begin[1] == '\x00' && begin[2] == '\xFE' &&
          begin[3] == '\xFF') {
        return 4;
      }
      break;

    // UTF-8
    case '\xEF':
      if (size >= 3 && begin[1] == '\xBB' && begin[2] == '\xBF') {
        return 3;
      }
      break;

    // UTF-16BE
    case '\xfe':
      if (size >= 2 && begin[1] == '\xff') {
        return 2;
      }
      break;

    case '\xff':
      if (size >= 2 && begin[1] == '\xfe') {

        // UTF-32 LE
        if (size >= 4 && begin[2] == '\x00' && begin[3] == '\x00') {
          return 4;
        } else {
          // UTF-16 LE
          return 2;
        }
      }
      break;
    }

    return 0;
  }

  // This skips leading blank lines and comments (if needed)
  template <typename T> size_t find_first_line(const T& source) {

    auto begin = skip_bom(source);
    /* Skip skip parameters, comments and blank lines */

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

  template <typename T, typename P>
  void index_region(
      const T& source,
      idx_t& destination,
      const char delim,
      const char quote,
      const size_t start,
      const size_t end,
      P& pb,
      const size_t update_size = -1) {

    // If there are no quotes quote will be '\0', so will just work
    std::array<char, 4> query = {delim, '\n', '\\', quote};

    size_t last = start;
    auto last_tick = start;
    auto num_ticks = 0;

    bool in_quote = false;

    auto begin = source.data();

    // The actual parsing is here
    auto result = strcspn(begin + last, query.data());
    auto i = result + last;
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
        if (progress_) {
          auto tick_size = i - last_tick;
          if (tick_size > update_size) {
            pb->tick(i - last_tick);
            last_tick = i;
            ++num_ticks;
          }
        }
      }

      last = i;
      result = strcspn(begin + last + 1, query.data());
      i = result + last + 1;
    }

    if (progress_) {
      pb->tick(end - last_tick);
    }
    // Rcpp::Rcerr << num_ticks << '\n';
  }
};

} // namespace vroom

#endif /* READIDX_IDX_HEADER */
