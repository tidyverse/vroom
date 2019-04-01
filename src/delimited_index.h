#pragma once

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include "index.h"

#include "utils.h"
#include <Rcpp.h>
#include <array>

#include "multi_progress.h"

namespace vroom {

struct cell {
  const char* begin;
  const char* end;
};

class delimited_index : public index,
                        public std::enable_shared_from_this<delimited_index> {

public:
  delimited_index(
      const char* filename,
      const char* delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      size_t n_max,
      const char comment,
      const size_t num_threads,
      const bool progress);

  class column_iterator : public base_iterator {
    std::shared_ptr<const delimited_index> idx_;
    size_t column_;
    bool is_first_;
    bool is_last_;
    size_t i_;

  public:
    column_iterator(std::shared_ptr<const delimited_index> idx, size_t column)
        : idx_(idx),
          column_(column),
          is_first_(column == 0),
          is_last_(is_last_ = column == (idx_->columns_ - 1)),
          i_((idx_->has_header_ * idx_->columns_) + column_) {}
    void next() { i_ += idx_->columns_; }
    void prev() { i_ -= idx_->columns_; }
    void advance(ptrdiff_t n) { i_ += idx_->columns_ * n; }
    bool equal_to(const base_iterator& it) const {
      return i_ == static_cast<const column_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const {
      return (static_cast<ptrdiff_t>(
                  static_cast<const column_iterator*>(&it)->i_) -
              static_cast<ptrdiff_t>(i_)) /
             ptrdiff_t(idx_->columns_);
    }
    string value() const {
      return idx_->get_trimmed_val(i_, is_first_, is_last_);
    }
    column_iterator* clone() const { return new column_iterator(*this); }
    string at(ptrdiff_t n) const {
      size_t i = ((n + idx_->has_header_) * idx_->columns_) + column_;
      return idx_->get_trimmed_val(i, is_first_, is_last_);
    }
    virtual ~column_iterator() = default;
  };

  class row : public index::row {
    std::shared_ptr<const delimited_index> idx_;
    size_t row_;

  public:
    row(std::shared_ptr<const delimited_index> idx, size_t row)
        : idx_(idx), row_(row) {}

    class row_iterator : public base_iterator {
      std::shared_ptr<const delimited_index> idx_;
      size_t row_;
      size_t i_;

    public:
      row_iterator(std::shared_ptr<const delimited_index> idx, size_t row)
          : idx_(idx), row_(row) {
        i_ = (row_ + idx_->has_header_) * idx_->columns_;
      }
      void next() { ++i_; }
      void prev() { --i_; }
      void advance(ptrdiff_t n) { i_ += n; }
      bool equal_to(const base_iterator& it) const {
        return i_ == static_cast<const row_iterator*>(&it)->i_;
      }
      ptrdiff_t distance_to(const base_iterator& it) const {
        return static_cast<const row_iterator*>(&it)->i_ - i_;
      }
      string value() const {
        return idx_->get_trimmed_val(i_, i_ == 0, i_ == (idx_->columns_ - 1));
      }
      row_iterator* clone() const { return new row_iterator(*this); }
      string at(ptrdiff_t n) const {
        size_t i = (row_ + idx_->has_header_) * idx_->columns_ + n;
        return idx_->get_trimmed_val(i, i == 0, i == (idx_->columns_ - 1));
      }
      virtual ~row_iterator() = default;
    };
    vroom::iterator begin() const { return new row_iterator(idx_, row_); }
    vroom::iterator end() const {
      auto res = new row_iterator(idx_, row_);
      res->advance(idx_->num_columns());
      return res;
    };
    ~row() = default;
  };

  delimited_index() : rows_(0), columns_(0){};

  string get(size_t row, size_t col) const;

  size_t num_columns() const { return columns_; }

  size_t num_rows() const { return rows_; }

  std::string filename() const { return filename_; }

  std::shared_ptr<vroom::index::column> get_column(size_t column) const {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());
    return std::make_shared<vroom::delimited_index::column>(begin, end);
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const {
    return std::make_shared<vroom::delimited_index::row>(
        shared_from_this(), row);
  }

  std::shared_ptr<vroom::index::row> get_header() const {
    return std::make_shared<vroom::delimited_index::row>(
        shared_from_this(), -1);
  }

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
  size_t delim_len_;
  std::locale loc_;

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
               (begin < source.size() &&
                is_blank_or_comment_line(source.data() + begin)) ||
               skip_ > 0) {
      begin = find_next_newline(source, begin) + 1;
      if (skip_ > 0) {
        --skip_;
      }
    }

    return begin;
  }

  void trim_quotes(const char*& begin, const char*& end) const;
  void trim_whitespace(const char*& begin, const char*& end) const;
  const string
  get_escaped_string(const char* begin, const char* end, bool has_quote) const;

  const string get_trimmed_val(size_t i, bool is_first, bool is_last) const;

  std::pair<const char*, const char*> get_cell(size_t i, bool is_first) const;

  /*
   * @param source the source to index
   * @param destination the index to push to
   * @param delim the delimiter to use
   * @param quote the quoting character
   * @param start the start of the region to index
   * @param end the end of the region to index
   * @param offset an offset to add to the destination (this is needed when
   * @param pb the progress bar to use
   * @param update_size how often to update the progress bar
   * reading blocks from a connection).
   */
  template <typename T, typename P>
  size_t index_region(
      const T& source,
      idx_t& destination,
      const char* delim,
      const char quote,
      const size_t start,
      const size_t end,
      const size_t file_offset,
      const size_t n_max,
      P& pb,
      const size_t update_size = -1) {

    // If there are no quotes quote will be '\0', so will just work
    std::array<char, 5> query = {delim[0], '\n', '\\', quote, '\0'};

    auto last_tick = start;
    auto num_ticks = 0;

    bool in_quote = false;

    auto buf = source.data();

    // The actual parsing is here
    size_t pos = start;
    size_t lines_read = 0;
    while (pos < end) {
      size_t buf_offset = strcspn(buf + pos, query.data());
      pos = pos + buf_offset;
      auto c = buf[pos];

      if (!in_quote && strncmp(delim, buf + pos, delim_len_) == 0) {
        destination.push_back(pos + file_offset);
      }

      else if (c == '\n') { // no embedded quotes allowed
        if (in_quote) {
          if (progress_ && pb) {
            pb->finish();
          }
          throw std::runtime_error("Embedded newline found!");
        }
        destination.push_back(pos + file_offset);
        if (lines_read >= n_max) {
          if (progress_ && pb) {
            pb->finish();
          }
          return lines_read;
        }
        ++lines_read;
        if (progress_ && pb) {
          auto tick_size = pos - last_tick;
          if (tick_size > update_size) {
            pb->tick(pos - last_tick);
            last_tick = pos;
            ++num_ticks;
          }
        }
      }

      else if (c == quote) {
        in_quote = !in_quote;
      }

      else if (escape_backslash_ && c == '\\') {
        ++pos;
      }

      ++pos;
    }

    if (progress_ && pb) {
      pb->tick(end - last_tick);
    }
    return lines_read;
  }
};

} // namespace vroom
