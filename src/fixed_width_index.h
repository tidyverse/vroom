#include "Rcpp.h"

#include "index.h"

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include "utils.h"
#include "RProgress.h"

namespace vroom {

class fixed_width_index
    : public index,
      public std::enable_shared_from_this<fixed_width_index> {
  std::vector<size_t> newlines_;
  std::vector<int> col_starts_;
  std::vector<int> col_ends_;
  mio::mmap_source mmap_;
  const bool trim_ws_;

public:
  fixed_width_index(
      const char* filename,
      std::vector<int> col_starts,
      std::vector<int> col_ends,
      bool trim_ws,
      const size_t skip,
      const char comment,
      const size_t n_max,
      const bool progress)
      : col_starts_(col_starts), col_ends_(col_ends), trim_ws_(trim_ws) {

    std::error_code error;
    mmap_ = mio::make_mmap_source(filename, error);

    if (error) {
      // We cannot actually portably compare error messages due to a bug in
      // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
      // the message on stderr return
      Rcpp::Rcerr << "mapping error: " << error.message();
      return;
    }

    size_t file_size = mmap_.size();

    size_t start = find_first_line(mmap_, skip, comment);

    if (n_max > 0) {
      newlines_.push_back(start - 1);
    }

    std::unique_ptr<RProgress::RProgress> pb = nullptr;
    size_t tick_size = file_size / 1000;
    if (progress) {
      auto format = get_pb_format("file", filename);
      auto width = get_pb_width(format);
      pb = std::unique_ptr<RProgress::RProgress>(
          new RProgress::RProgress(format, file_size, width));
      pb->tick(start);
    }

    size_t lines_read = 0;
    size_t last_tick = start;

    size_t newline = find_next_newline(mmap_, start);
    while (newline < file_size - 1) {
      ++lines_read;
      if (lines_read >= n_max) {
        break;
      }
      newlines_.push_back(newline);

      if (progress && newline > last_tick + tick_size) {
        pb->tick(newline - last_tick);
        last_tick = newline;
      }

      newline = find_next_newline(mmap_, newline + 1);
    }
    newlines_.push_back(newline);

    if (progress) {
      pb->update(1);
    }
  }

  size_t num_rows() const { return newlines_.size() - 1; }
  size_t num_columns() const { return col_starts_.size(); }

  string get(size_t row, size_t col) const {
    size_t nl_size = 1;
    auto begin = mmap_.data() + (newlines_[row] + nl_size + col_starts_[col]);
    const char* end;
    if (col_ends_[col] == NA_INTEGER) {
      end = mmap_.data() + newlines_[row + 1];
    } else {
      end = mmap_.data() + (newlines_[row] + nl_size + col_ends_[col]);
    }
    if (trim_ws_) {
      trim_whitespace(begin, end);
    }
    return {begin, end};
  }

  class column_iterator : public base_iterator {
    std::shared_ptr<const fixed_width_index> idx_;
    size_t column_;
    size_t i_;

  public:
    column_iterator(std::shared_ptr<const fixed_width_index> idx, size_t column)
        : idx_(idx), column_(column), i_(0) {}
    void next() { ++i_; }
    void prev() { --i_; }
    void advance(ptrdiff_t n) { i_ += n; }
    bool equal_to(const base_iterator& it) const {
      return i_ == static_cast<const column_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const {
      return static_cast<ptrdiff_t>(
                 static_cast<const column_iterator*>(&it)->i_) -
             static_cast<ptrdiff_t>(i_);
    }
    string value() const { return idx_->get(i_, column_); }
    column_iterator* clone() const { return new column_iterator(*this); }
    string at(ptrdiff_t n) const { return idx_->get(i_, column_); }
    virtual ~column_iterator() = default;
  };

  std::shared_ptr<vroom::index::column> get_column(size_t column) const {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());

    return std::make_shared<vroom::index::column>(begin, end);
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const {
    // TODO: implement
    return nullptr;
  }
  std::shared_ptr<vroom::index::row> get_header() const {
    // TODO: implement
    return nullptr;
  }
};
} // namespace vroom
