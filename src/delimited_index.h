#pragma once

#include <exception>

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
#else
#include <mio/shared_mmap.hpp>
#endif
// clang-format on

#include "index.h"

#include "utils.h"
#include <array>

#include "multi_progress.h"
#include "vroom_errors.h"

namespace vroom {

struct cell {
  const char* begin;
  const char* end;
};

class delimited_index : public index,
                        public std::enable_shared_from_this<delimited_index> {
  class newline_error {};

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
      const char* comment,
      std::shared_ptr<vroom_errors> errors,
      const size_t num_threads,
      const bool progress,
      const bool use_threads = true);

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
          is_last_(column == (idx_->columns_ - 1)),
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
    std::string filename() const { return idx_->filename_; }
    size_t index() const { return i_ / idx_->columns_; }
    size_t position() const { return i_; }
    virtual ~column_iterator() = default;
  };

  class row_iterator : public base_iterator {
    std::shared_ptr<const delimited_index> idx_;
    size_t row_;
    size_t i_;

  public:
    row_iterator(std::shared_ptr<const delimited_index> idx, size_t row)
        : idx_(idx),
          row_(row),
          i_((row_ + idx_->has_header_) * idx_->columns_) {}
    void next() { ++i_; }
    void prev() { --i_; }
    void advance(ptrdiff_t n) { i_ += n; }
    bool equal_to(const base_iterator& it) const {
      return i_ == static_cast<const row_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const {
      return (
          static_cast<ptrdiff_t>(static_cast<const row_iterator*>(&it)->i_) -
          static_cast<ptrdiff_t>(i_));
    }
    string value() const {
      return idx_->get_trimmed_val(i_, i_ == 0, i_ == (idx_->columns_ - 1));
    }
    row_iterator* clone() const { return new row_iterator(*this); }
    string at(ptrdiff_t n) const {
      size_t i = (row_ + idx_->has_header_) * idx_->columns_ + n;
      return idx_->get_trimmed_val(i, i == 0, i == (idx_->columns_ - 1));
    }
    std::string filename() const { return idx_->filename_; }
    size_t index() const {
      return i_ - (row_ + idx_->has_header_) * idx_->columns_;
    }
    size_t position() const { return i_; }
    virtual ~row_iterator() = default;
  };

  delimited_index() : rows_(0), columns_(0){};

  string get(size_t row, size_t col) const;

  size_t num_columns() const { return columns_; }

  size_t num_rows() const { return rows_; }

  std::string filename() const { return filename_; }

  std::string get_delim() const { return delim_; }

  std::shared_ptr<vroom::index::column> get_column(size_t column) const {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());
    return std::make_shared<vroom::delimited_index::column>(begin, end, column);
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const {
    auto begin = new row_iterator(shared_from_this(), row);
    auto end = new row_iterator(shared_from_this(), row);
    end->advance(num_columns());
    return std::make_shared<vroom::delimited_index::row>(begin, end, row);
  }

  std::shared_ptr<vroom::index::row> get_header() const {
    auto begin = new row_iterator(shared_from_this(), -1);
    auto end = new row_iterator(shared_from_this(), -1);
    end->advance(num_columns());
    return std::make_shared<vroom::delimited_index::row>(begin, end, 0);
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
  const char* comment_;
  size_t rows_;
  size_t columns_;
  bool progress_;
  size_t delim_len_;
  std::string delim_;
  std::locale loc_;

  void skip_lines();

  void trim_quotes(const char*& begin, const char*& end) const;
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
      bool& in_quote,
      const size_t start,
      const size_t end,
      const size_t file_offset,
      const size_t n_max,
      size_t& cols,
      const size_t num_cols,
      std::shared_ptr<vroom_errors> errors,
      P& pb,
      const size_t num_threads,
      const size_t update_size) {

    // If there are no quotes quote will be '\0', so will just work
    std::array<char, 5> query = {delim[0], '\n', '\\', quote, '\0'};

    auto last_tick = start;

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
        ++cols;
      }

      else if (c == '\n') {
        if (in_quote) { // This will work as long as num_threads = 1
          if (num_threads != 1) {
            if (progress_ && pb) {
              pb->finish();
            }
            throw newline_error();
          }
          ++pos;
          continue;
        }

        // Ensure columns are consistent
        if (num_cols > 0 && pos > start) {
          // Remove extra columns if there are too many
          if (cols >= num_cols) {
            errors->add_parse_error(pos + file_offset, cols);
            while (cols >= num_cols) {
              destination.pop_back();
              --cols;
            }
          } else if (cols < num_cols - 1) {
            errors->add_parse_error(pos + file_offset, cols);
            // Add additional columns if there are too few
            while (cols < num_cols - 1) {
              destination.push_back(pos + file_offset - windows_newlines_);
              ++cols;
            }
          }
        }

        cols = 0;
        destination.push_back(pos + file_offset);
        if (lines_read >= n_max) {
          if (progress_ && pb) {
            pb->finish();
          }
          return lines_read;
        }
        ++lines_read;
        if (progress_ && pb) {
          size_t tick_size = pos - last_tick;
          if (tick_size > update_size) {
            pb->tick(pos - last_tick);
            last_tick = pos;
          }
        }
      }

      else if (escape_backslash_ && c == '\\') {
        ++pos;
      }

      else if (c == quote) {
        /* not already in a quote */
        if (!in_quote &&
            /* right after the start of file or line */
            (pos == start || buf[pos - 1] == '\n' ||
             /* or after a delimiter */
             strncmp(delim, buf + (pos - delim_len_), delim_len_) == 0)) {
          in_quote = !in_quote;
        } else if (
            /* already in a quote */
            in_quote &&
            /* right at the end of the file or line */
            (pos == end - 1 ||
             ((!windows_newlines_ && (pos + 1 < end && buf[pos + 1] == '\n')) ||
              (windows_newlines_ && (pos + 2 < end && buf[pos + 2] == '\n'))) ||
             /* or before a delimiter */
             (pos + delim_len_ + 1 < end &&
              strncmp(delim, buf + (pos + 1), delim_len_) == 0))) {
          in_quote = !in_quote;
        }
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
