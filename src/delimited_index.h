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
      const bool skip_empty_rows,
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
    void next() override { i_ += idx_->columns_; }
    void prev() override { i_ -= idx_->columns_; }
    void advance(ptrdiff_t n) override { i_ += idx_->columns_ * n; }
    bool equal_to(const base_iterator& it) const override {
      return i_ == static_cast<const column_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const override {
      ptrdiff_t i = i_;
      ptrdiff_t j = static_cast<const column_iterator*>(&it)->i_;
      ptrdiff_t columns = idx_->columns_;
      return (j - i) / columns;
    }

    string value() const override {
      return idx_->get_trimmed_val(i_, is_first_, is_last_);
    }
    column_iterator* clone() const override {
      return new column_iterator(*this);
    }
    string at(ptrdiff_t n) const override {
      size_t i = ((n + idx_->has_header_) * idx_->columns_) + column_;
      return idx_->get_trimmed_val(i, is_first_, is_last_);
    }
    std::string filename() const override { return idx_->filename_; }
    size_t index() const override { return i_ / idx_->columns_; }
    size_t position() const override {
      size_t begin, end;
      std::tie(begin, end) = idx_->get_cell(i_, is_first_);
      return begin;
    }
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
    void next() override { ++i_; }
    void prev() override { --i_; }
    void advance(ptrdiff_t n) override { i_ += n; }
    bool equal_to(const base_iterator& it) const override {
      return i_ == static_cast<const row_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const override {
      return (
          static_cast<ptrdiff_t>(static_cast<const row_iterator*>(&it)->i_) -
          static_cast<ptrdiff_t>(i_));
    }
    string value() const override {
      return idx_->get_trimmed_val(i_, i_ == 0, i_ == (idx_->columns_ - 1));
    }
    row_iterator* clone() const override { return new row_iterator(*this); }
    string at(ptrdiff_t n) const override {
      size_t i = (row_ + idx_->has_header_) * idx_->columns_ + n;
      return idx_->get_trimmed_val(i, i == 0, i == (idx_->columns_ - 1));
    }
    std::string filename() const override { return idx_->filename_; }
    size_t index() const override {
      return i_ - (row_ + idx_->has_header_) * idx_->columns_;
    }
    size_t position() const override {
      size_t begin, end;
      std::tie(begin, end) = idx_->get_cell(i_, i_ == 0);
      return begin;
    }
    virtual ~row_iterator() = default;
  };

  delimited_index() : rows_(0), columns_(0){};

  string get(size_t row, size_t col) const override;

  size_t num_columns() const override { return columns_; }

  size_t num_rows() const override { return rows_; }

  std::string filename() const { return filename_; }

  std::string get_delim() const override { return delim_; }

  std::shared_ptr<vroom::index::column>
  get_column(size_t column) const override {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());
    return std::make_shared<vroom::delimited_index::column>(begin, end, column);
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const override {
    auto begin = new row_iterator(shared_from_this(), row);
    auto end = new row_iterator(shared_from_this(), row);
    end->advance(num_columns());
    return std::make_shared<vroom::delimited_index::row>(begin, end, row);
  }

  std::shared_ptr<vroom::index::row> get_header() const override {
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

  std::pair<size_t, size_t> get_cell(size_t i, bool is_first) const;

  enum csv_state {
    RECORD_START,
    FIELD_START,
    UNQUOTED_FIELD,
    QUOTED_FIELD,
    QUOTED_END
  };

  inline static csv_state quoted_state(csv_state in) {
    switch (in) {
    case RECORD_START:
      return QUOTED_FIELD;
    case FIELD_START:
      return QUOTED_FIELD;
    case UNQUOTED_FIELD:
      return UNQUOTED_FIELD; // throw std::runtime_error("invalid 1");
    case QUOTED_FIELD:
      return QUOTED_END;
    case QUOTED_END:
      return QUOTED_FIELD;
    }
    throw std::runtime_error("should never happen");
  }

  inline static csv_state comma_state(csv_state in) {
    switch (in) {
    case RECORD_START:
      return FIELD_START;
    case FIELD_START:
      return FIELD_START;
    case UNQUOTED_FIELD:
      return FIELD_START;
    case QUOTED_FIELD:
      return QUOTED_FIELD;
    case QUOTED_END:
      return FIELD_START;
    }
    throw std::runtime_error("should never happen");
  }

  inline static csv_state newline_state(csv_state in) {
    switch (in) {
    case RECORD_START:
      return RECORD_START;
    case FIELD_START:
      return RECORD_START;
    case UNQUOTED_FIELD:
      return RECORD_START;
    case QUOTED_FIELD:
      return QUOTED_FIELD;
    case QUOTED_END:
      return RECORD_START;
    }
    throw std::runtime_error("should never happen");
  }

  inline static csv_state other_state(csv_state in) {
    switch (in) {
    case RECORD_START:
      return UNQUOTED_FIELD;
    case FIELD_START:
      return UNQUOTED_FIELD;
    case UNQUOTED_FIELD:
      return UNQUOTED_FIELD;
    case QUOTED_FIELD:
      return QUOTED_FIELD;
    case QUOTED_END:
      return QUOTED_END;
    }
    throw std::runtime_error("should never happen");
  }
  void resolve_columns(
      size_t pos,
      size_t& cols,
      size_t num_cols,
      idx_t& destination,
      std::shared_ptr<vroom_errors> errors) {
    // Remove extra columns if there are too many
    if (cols >= num_cols) {
      errors->add_parse_error(pos, cols);
      while (cols > 0 && cols >= num_cols) {
        destination.pop_back();
        --cols;
      }
    } else if (cols < num_cols - 1) {
      errors->add_parse_error(pos, cols);
      // Add additional columns if there are too few
      while (cols < num_cols - 1) {
        destination.push_back(pos);
        ++cols;
      }
    }
  }

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
      newline_type nlt,
      const char quote,
      const std::string& comment,
      const bool skip_empty_rows,
      csv_state& state,
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

    const char newline = nlt == CR ? '\r' : '\n';

    // If there are no quotes quote will be '\0', so will just work
    std::array<char, 6> query = {delim[0], newline, '\\', '\0', '\0', '\0'};
    auto query_i = 3;
    if (quote != '\0') {
      query[query_i++] = quote;
    }
    if (!comment.empty()) {
      query[query_i] = comment[0];
    }

    auto last_tick = start;

    auto buf = source.data();

    // The actual parsing is here
    size_t pos = start;
    size_t lines_read = 0;

    while (pos < end && lines_read < n_max) {
      auto c = buf[pos];

      if (escape_backslash_ && c == '\\') {
        ++pos;
        if (state == RECORD_START) {
          destination.push_back(pos + file_offset);
          state = FIELD_START;
        }
        ++pos;
        continue;
      }

      else if (
          state != QUOTED_FIELD && is_comment(buf + pos, buf + end, comment)) {

        if (state != RECORD_START) {

          if (num_cols > 0 && pos > start) {
            resolve_columns(
                pos + file_offset, cols, num_cols, destination, errors);
          }
          destination.push_back(pos + file_offset);
        }
        cols = 0;
        pos = skip_rest_of_line(source, pos);
        ++pos;
        state = newline_state(state);
        continue;
      }

      if (state == RECORD_START) {
        if (is_empty_line(buf + pos, buf + end, skip_empty_rows)) {
          pos = skip_rest_of_line(source, pos);
          ++pos;
          continue;
        }
        // REprintf("RS: %i\n", pos);
        destination.push_back(pos + file_offset);
      }

      if (state != QUOTED_FIELD && strncmp(delim, buf + pos, delim_len_) == 0) {
        state = comma_state(state);
        destination.push_back(pos + file_offset);
        ++cols;
      }

      else if (c == newline) {
        if (state ==
            QUOTED_FIELD) { // This will work as long as num_threads = 1
          if (num_threads != 1) {
            if (progress_ && pb) {
              pb->finish();
            }
            throw newline_error();
          }
          ++pos;
          continue;
        }
        if (num_cols > 0 && pos > start) {
          resolve_columns(
              pos + file_offset, cols, num_cols, destination, errors);
        }

        state = newline_state(state);
        cols = 0;
        destination.push_back(pos + file_offset);
        ++lines_read;
        if (lines_read >= n_max) {
          if (progress_ && pb) {
            pb->finish();
          }
          return lines_read;
        }
        if (progress_ && pb) {
          size_t tick_size = pos - last_tick;
          if (tick_size > update_size) {
            pb->tick(pos - last_tick);
            last_tick = pos;
          }
        }
      }

      else if (c == quote) {
        state = quoted_state(state);
      }

      else {
        state = other_state(state);
        ++pos;
        size_t buf_offset;
        if (pos < end) {
          buf_offset = strcspn(buf + pos, query.data());
          pos = pos + buf_offset;
        }
        continue;
      }

      // REprintf(
      //"%i\t'%c'\t%c\n",
      // pos,
      // c,
      // state == RECORD_START
      //? 'R'
      //: state == FIELD_START
      //? 'F'
      //: state == UNQUOTED_FIELD
      //? 'U'
      //: state == QUOTED_FIELD
      //? 'Q'
      //: state == QUOTED_END ? 'E' : 'X');

      ++pos;
    }

    if (progress_ && pb) {
      pb->tick(end - last_tick);
    }
    return lines_read;
  }
};

} // namespace vroom
