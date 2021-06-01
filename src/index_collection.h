#pragma once

#include "index.h"
#include "iterator.h"
#include "vroom_errors.h"

#include <cpp11/list.hpp>
#include <memory>

#ifdef VROOM_LOG
#include "spdlog/spdlog.h"
#endif

namespace vroom {

class index_collection : public index,
                         public std::enable_shared_from_this<index_collection> {

public:
  // For delimited files
  index_collection(
      const cpp11::list& in,
      const char* delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      const size_t n_max,
      const char* comment,
      const bool skip_empty_rows,
      const std::shared_ptr<vroom_errors>& errors,
      const size_t num_threads,
      const bool progress);

  // For fixed width files
  index_collection(
      const cpp11::list& in,
      const std::vector<int>& col_starts,
      const std::vector<int>& col_ends,
      const bool trim_ws,
      const size_t skip,
      const char* comment,
      const bool skip_empty_rows,
      const size_t n_max,
      const bool progress);

  string get(size_t row, size_t col) const override;

  size_t num_columns() const override { return columns_; }

  size_t num_rows() const override { return rows_; }

  std::vector<size_t> row_sizes() const {
    std::vector<size_t> out;
    for (const auto& index : indexes_) {
      out.push_back(index->num_rows());
    }
    return out;
  }

  std::string get_delim() const override { return indexes_[0]->get_delim(); }

public:
  class full_iterator : public base_iterator {
    size_t i_;
    std::shared_ptr<const index_collection> idx_;
    size_t column_;
    size_t start_;
    size_t end_;
    iterator it_;
    iterator it_end_;
    iterator it_start_;

  public:
    full_iterator(std::shared_ptr<const index_collection> idx, size_t column);
    void next() override;
    void prev() override;
    void advance(ptrdiff_t n) override;
    inline bool equal_to(const base_iterator& other) const override {
      auto other_ = static_cast<const full_iterator*>(&other);
      return i_ == other_->i_ && it_ == other_->it_;
    }
    ptrdiff_t distance_to(const base_iterator& it) const override;
    string value() const override;
    full_iterator* clone() const override;
    string at(ptrdiff_t n) const override;
    std::string filename() const override { return it_.filename(); }
    size_t index() const override { return it_.index(); }
    size_t position() const override { return it_.position(); }
    virtual ~full_iterator() {}
  };

  std::shared_ptr<vroom::index::column>
  get_column(size_t column) const override {
    auto begin = new full_iterator(shared_from_this(), column);
    auto end = new full_iterator(shared_from_this(), column);
    end->advance(rows_);

    return std::make_shared<vroom::index::column>(begin, end, column);
  }

  std::shared_ptr<index::row> get_row(size_t row) const override {

    for (const auto& idx : indexes_) {

      auto sz = idx->num_rows();
      if (row < sz) {
        return idx->get_row(row);
      }
      row -= sz;
    }
    /* should never get here */
    return indexes_[0]->get_header();
  }

  std::shared_ptr<index::row> get_header() const override {
    return indexes_[0]->get_header();
  }

private:
  std::vector<std::shared_ptr<index>> indexes_;

  size_t rows_;
  size_t columns_;
};
} // namespace vroom
