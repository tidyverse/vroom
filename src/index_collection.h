#pragma once

#include "index.h"

#include "Rcpp.h"

namespace vroom {

class index_collection {

public:
  index_collection(
      Rcpp::List in,
      const char* delim,
      const char quote,
      const bool trim_ws,
      const bool escape_double,
      const bool escape_backslash,
      const bool has_header,
      const size_t skip,
      const char comment,
      const size_t num_threads,
      const bool progress);

  const std::string get(size_t row, size_t col) const;

  size_t num_columns() const { return columns_; }

  size_t num_rows() const { return rows_; }

  class column {

    const index_collection& idx_;
    size_t column_;
    size_t start_;
    size_t end_;

  public:
    column(const index_collection& idx, size_t column);

    column(
        const index_collection& idx, size_t column, size_t start, size_t end);

    class iterator {
      size_t i_;
      const index_collection& idx_;
      size_t column_;
      size_t end_;
      index::column::iterator it_;
      index::column::iterator it_end_;

    public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = std::string;
      using pointer = std::string*;
      using reference = std::string&;

      iterator(const index_collection& idx, size_t column, size_t start);
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

  column get_column(size_t num) const { return column(*this, num); }

  column get_column(size_t num, size_t start, size_t end) const {
    return column(*this, num, start, end);
  }

  index::row_iterator row(size_t row) const {

    for (const auto& idx : indexes_) {

      auto sz = idx->num_rows();
      if (row < sz) {
        return idx->row(row);
      }
      row -= sz;
    }
    /* should never get here */
    return indexes_[0]->header();
  }

  index::row_iterator header() const { return indexes_[0]->header(); }

private:
  std::vector<std::shared_ptr<index> > indexes_;

  size_t rows_;
  size_t columns_;
};
} // namespace vroom
