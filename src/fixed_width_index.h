#pragma once

#include "index.h"

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

#ifndef VROOM_STANDALONE
#include "r_utils.h"
#include "RProgress.h"
#else
#include "utils.h"
#define NA_INTEGER INT_MIN
#endif

#ifdef VROOM_LOG
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "spdlog/spdlog.h"
#endif

#include "unicode_fopen.h"

namespace vroom {

class fixed_width_index
    : public index,
      public std::enable_shared_from_this<fixed_width_index> {

protected:
  fixed_width_index() : trim_ws_(0) {}
  std::vector<size_t> newlines_;
  std::vector<int> col_starts_;
  std::vector<int> col_ends_;
  mio::mmap_source mmap_;
  bool trim_ws_;
  bool windows_newlines_;

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
    mmap_ = make_mmap_source(filename, error);

    if (error) {
      // We cannot actually portably compare error messages due to a bug in
      // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
      // the message on stderr return
#ifndef VROOM_STANDALONE
      Rcpp::Rcerr << "mapping error: " << error.message() << '\n';
#else
      std::cerr << "mapping error: " << error.message() << '\n';
#endif

      return;
    }

    size_t file_size = mmap_.size();

    size_t start = find_first_line(mmap_, skip, comment);

    // Check for windows newlines
    size_t first_nl = find_next_newline(mmap_, start, false);
    windows_newlines_ = first_nl > 0 && mmap_[first_nl - 1] == '\r';

    std::unique_ptr<RProgress::RProgress> pb = nullptr;
    if (progress) {
#ifndef VROOM_STANDALONE
      auto format = get_pb_format("file", filename);
      auto width = get_pb_width(format);
      pb = std::unique_ptr<RProgress::RProgress>(
          new RProgress::RProgress(format, file_size, width));
      pb->tick(start);
#endif
    }

    if (n_max > 0) {
      newlines_.push_back(start - 1);
    }

    index_region(
        mmap_, newlines_, start, file_size - 1, 0, n_max, pb, file_size / 1000);

    newlines_.push_back(file_size - 1);

    if (progress) {
#ifndef VROOM_STANDALONE
      pb->update(1);
#endif
    }

#ifdef VROOM_LOG
#if SPDLOG_ACTIVE_LEVEL <= SPD_LOG_LEVEL_DEBUG
    auto log = spdlog::basic_logger_mt(
        "basic_logger", "logs/fixed_width_index.idx", true);
    for (auto& v : newlines_) {
      SPDLOG_LOGGER_DEBUG(log, "{}", v);
    }
    SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&newlines_);
    spdlog::drop("basic_logger");
#endif
#endif
  }

  size_t num_rows() const { return newlines_.size() - 1; }
  size_t num_columns() const { return col_starts_.size(); }

  std::string get_delim() const {
    /* TODO: FIXME */
    return "";
  }

  string get(size_t row, size_t col) const {
    auto begin = mmap_.data() + (newlines_[row] + 1 + col_starts_[col]);
    auto line_end = mmap_.data() + (newlines_[row + 1]) - windows_newlines_;
    const char* end;
    if (col_ends_[col] == NA_INTEGER) {
      end = mmap_.data() + newlines_[row + 1] - windows_newlines_;
    } else {
      end = mmap_.data() + (newlines_[row] + 1 + col_ends_[col]);
    }
    if (begin > line_end) {
      begin = line_end;
    }
    if (end > line_end) {
      end = line_end;
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
    string at(ptrdiff_t n) const { return idx_->get(n, column_); }
    virtual ~column_iterator() = default;
  };

  std::shared_ptr<vroom::index::column> get_column(size_t column) const {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());

    return std::make_shared<vroom::index::column>(begin, end);
  }

  template <typename T>
  size_t index_region(
      const T& source,
      std::vector<size_t>& destination,
      size_t start,
      size_t end,
      size_t offset,
      size_t n_max,
      std::unique_ptr<RProgress::RProgress>& pb,
      size_t update_size = -1) {

    size_t pos = find_next_newline(source, start, false);

    size_t lines_read = 0;
    auto last_tick = start;

    while (pos < end) {
      ++lines_read;
      if (lines_read >= n_max) {
        return lines_read;
      }

      destination.push_back(offset + pos);

      if (pb) {
        auto tick_size = pos - last_tick;
        if (tick_size > update_size) {
          pb->tick(pos - last_tick);
          last_tick = pos;
        }
      }

      pos = find_next_newline(source, pos + 1, false);
    }

    if (pb) {
      pb->tick(end - last_tick);
    }

    return lines_read;
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const {
    // TODO: UNUSED
    return nullptr;
  }
  std::shared_ptr<vroom::index::row> get_header() const {
    // TODO: UNUSED
    return nullptr;
  }
};
} // namespace vroom
