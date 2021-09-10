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
  std::string filename_;

public:
  fixed_width_index(
      const char* filename,
      std::vector<int> col_starts,
      std::vector<int> col_ends,
      bool trim_ws,
      const size_t skip,
      const char* comment,
      const bool skip_empty_rows,
      const size_t n_max,
      const bool progress)
      : col_starts_(col_starts),
        col_ends_(col_ends),
        trim_ws_(trim_ws),
        filename_(filename) {

    std::error_code error;
    mmap_ = make_mmap_source(filename, error);

    if (error) {
      // We cannot actually portably compare error messages due to a bug in
      // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
      // the message on stderr return
#ifndef VROOM_STANDALONE
      REprintf("mapping error: %s\n", error.message().c_str());
#else
      std::cerr << "mapping error: " << error.message() << '\n';
#endif

      return;
    }

    size_t file_size = mmap_.size();

    size_t start = find_first_line(
        mmap_,
        skip,
        comment,
        skip_empty_rows,
        /* embedded_nl */ false,
        /* quote */ '\0');

    // Check for windows newlines
    size_t first_nl;
    newline_type nl;
    std::tie(first_nl, nl) = find_next_newline(
        mmap_,
        start,
        comment,
        skip_empty_rows,
        /* embedded_nl */
        false,
        /* quote */ '\0');

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

    bool n_max_set = n_max != static_cast<size_t>(-1);

    if (n_max > 0) {
      newlines_.push_back(start - 1);
    }

    index_region(
        mmap_,
        newlines_,
        start,
        file_size - 1,
        0,
        comment,
        skip_empty_rows,
        n_max,
        pb,
        file_size / 1000);

    if (!n_max_set) {
      newlines_.push_back(file_size - 1);
    }

    if (progress) {
#ifndef VROOM_STANDALONE
      pb->update(1);
#endif
    }

#ifdef VROOM_LOG
    auto log = spdlog::basic_logger_mt(
        "basic_logger", "logs/fixed_width_index.idx", true);
    log->set_level(spdlog::level::debug);
    for (auto& v : newlines_) {
      SPDLOG_LOGGER_DEBUG(log, "{}", v);
    }
    SPDLOG_LOGGER_DEBUG(log, "end of idx {0:x}", (size_t)&newlines_);
    spdlog::drop("basic_logger");
#endif
  }

  size_t num_rows() const override { return newlines_.size() - 1; }
  size_t num_columns() const override { return col_starts_.size(); }

  std::string get_delim() const override {
    /* TODO: FIXME */
    return "";
  }

  string get(size_t row, size_t col) const override {
    auto begin = mmap_.data() + (newlines_[row] + 1 + col_starts_[col]);
    auto line_end = mmap_.data() + (newlines_[row + 1]);
    if (line_end > begin && *(line_end - 1) == '\r') {
      --line_end;
    }
    const char* end;
    if (col_ends_[col] == NA_INTEGER) {
      end = mmap_.data() + newlines_[row + 1];
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
    void next() override { ++i_; }
    void prev() override { --i_; }
    void advance(ptrdiff_t n) override { i_ += n; }
    bool equal_to(const base_iterator& it) const override {
      return i_ == static_cast<const column_iterator*>(&it)->i_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const override {
      return static_cast<ptrdiff_t>(
                 static_cast<const column_iterator*>(&it)->i_) -
             static_cast<ptrdiff_t>(i_);
    }
    string value() const override { return idx_->get(i_, column_); }
    column_iterator* clone() const override {
      return new column_iterator(*this);
    }
    string at(ptrdiff_t n) const override { return idx_->get(n, column_); }
    std::string filename() const override { return idx_->filename_; }
    size_t index() const override { return i_ / idx_->num_columns(); }
    size_t position() const override { return i_; }
    virtual ~column_iterator() = default;
  };

  std::shared_ptr<vroom::index::column>
  get_column(size_t column) const override {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());

    return std::make_shared<vroom::index::column>(begin, end, column);
  }

  template <typename T>
  size_t index_region(
      const T& source,
      std::vector<size_t>& destination,
      size_t start,
      size_t end,
      size_t offset,
      const char* comment,
      const bool skip_empty_rows,
      size_t n_max,
      std::unique_ptr<RProgress::RProgress>& pb,
      size_t update_size = -1) {

    size_t pos;
    newline_type nl;
    std::tie(pos, nl) = find_next_newline(
        source,
        start,
        comment,
        skip_empty_rows,
        /* embededd_nl */
        false,
        /* quote */ '\0');

    size_t lines_read = 0;
    auto last_tick = start;

    while (pos < end) {
      ++lines_read;
      destination.push_back(offset + pos);

      if (lines_read >= n_max) {
        return lines_read;
      }

      if (pb) {
        auto tick_size = pos - last_tick;
        if (tick_size > update_size) {
          pb->tick(pos - last_tick);
          last_tick = pos;
        }
      }

      std::tie(pos, nl) = find_next_newline(
          source,
          pos + 1,
          comment,
          skip_empty_rows,
          /* embedded_nl */ false,
          /* quote */ '\0');
    }

    if (pb) {
      pb->tick(end - last_tick);
    }

    return lines_read;
  }

  std::shared_ptr<vroom::index::row> get_row(size_t) const override {
    // TODO: UNUSED
    return nullptr;
  }
  std::shared_ptr<vroom::index::row> get_header() const override {
    // TODO: UNUSED
    return nullptr;
  }

  std::string filename() const { return filename_; }
};
} // namespace vroom
