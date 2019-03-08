#pragma once

#include "index.h"

#include "Rcpp.h"

#include <memory>

namespace vroom {

class index_collection : public std::enable_shared_from_this<index_collection> {

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

  const string get(size_t row, size_t col) const;

  size_t num_columns() const { return columns_; }

  size_t num_rows() const { return rows_; }

  std::vector<std::string> filenames() const {
    std::vector<std::string> out;
    for (const auto& index : indexes_) {
      out.push_back(index->filename());
    }
    return out;
  }

  std::vector<size_t> row_sizes() const {
    std::vector<size_t> out;
    for (const auto& index : indexes_) {
      out.push_back(index->num_rows());
    }
    return out;
  }

  class column {

    std::shared_ptr<const index_collection> idx_;
    size_t column_;
    size_t start_;
    size_t end_;

  public:
    column() = default;
    column(std::shared_ptr<const index_collection> idx, size_t column);

    class iterator {
      size_t i_;
      std::shared_ptr<const index_collection> idx_;
      size_t column_;
      size_t end_;
      index::column::iterator it_;
      index::column::iterator it_end_;

    public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = string;
      using pointer = string*;
      using reference = string&;

      iterator(std::shared_ptr<const index_collection> idx, size_t column);
      virtual iterator operator++(int); /* postfix */
      virtual iterator& operator++();   /* prefix */
      virtual bool operator!=(const iterator& other) const;
      virtual bool operator==(const iterator& other) const;

      virtual string operator*();
      virtual iterator& operator+=(int n);
      virtual iterator operator+(int n);
    };
    virtual iterator begin();
    virtual iterator end();

    virtual std::shared_ptr<column> slice(size_t start, size_t end);
    virtual size_t size() const;
    virtual string operator[](size_t i) const;
  };

  class column_subset : public column {
    std::shared_ptr<column> col_;
    std::shared_ptr<std::vector<size_t> > idx_;
    size_t start_;
    size_t end_;

  public:
    column_subset(
        std::shared_ptr<column> col, std::shared_ptr<std::vector<size_t> > idx);

    class iterator {
      using iterator_category = std::forward_iterator_tag;
      using value_type = string;
      using pointer = string*;
      using reference = string&;

      std::shared_ptr<column> col_;
      std::shared_ptr<std::vector<size_t> > idx_;
      size_t i_;

    public:
      iterator(
          std::shared_ptr<column> col,
          std::shared_ptr<std::vector<size_t> > idx);
      iterator operator++(int); /* postfix */
      iterator& operator++();   /* prefix */
      bool operator!=(const iterator& other) const;
      bool operator==(const iterator& other) const;

      string operator*();
      iterator& operator+=(int n);
      iterator operator+(int n);
    };
    iterator begin();
    iterator end();

    std::shared_ptr<column_subset> slice(size_t start, size_t end);
    size_t size() const;
    string operator[](size_t i) const;
  };

  std::shared_ptr<column> get_column(size_t num) const {
    return std::make_shared<column>(shared_from_this(), num);
  }

  index::row row(size_t row) const {

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

  index::row get_header() const { return indexes_[0]->get_header(); }

private:
  std::vector<std::shared_ptr<index> > indexes_;

  size_t rows_;
  size_t columns_;
};
} // namespace vroom
