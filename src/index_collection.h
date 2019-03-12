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

  public:
    class base_iterator {
    public:
      virtual void next() = 0;
      virtual void prev() = 0;
      virtual void advance(int n) = 0;
      virtual bool equal_to(const base_iterator& it) = 0;
      virtual ptrdiff_t distance_to(const base_iterator& it) = 0;
      virtual string value() = 0;
      virtual base_iterator* clone() const = 0;
      virtual ~base_iterator() = default;
    };

    class iterator {
      base_iterator* it_;

    public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = string;
      using pointer = string*;
      using reference = string&;

      iterator(base_iterator* it) : it_(it) {}
      iterator(const iterator& other) : it_(other.it_->clone()) {}

      iterator operator++(int) { /* postfix */
        iterator copy(*this);
        it_->next();
        return copy;
      }

      iterator& operator++() /* prefix */ {
        it_->next();
        return *this;
      }

      iterator operator--(int) { /* postfix */
        iterator copy(*this);
        it_->prev();
        return copy;
      }

      iterator& operator--() /* prefix */ {
        it_->prev();
        return *this;
      }

      bool operator!=(const iterator& other) const {
        return !it_->equal_to(*other.it_);
      }

      bool operator==(const iterator& other) const {
        return it_->equal_to(*other.it_);
      }

      string operator*() const { return it_->value(); }

      iterator& operator+=(int n) {
        it_->advance(n);
        return *this;
      }
      iterator operator+(int n) {
        iterator copy(*this);
        copy.it_->advance(n);
        return copy;
      }

      iterator operator-(int n) {
        iterator copy(*this);
        copy.it_->advance(-n);
        return copy;
      }

      ptrdiff_t operator-(const iterator& other) const {
        return -it_->distance_to(*other.it_);
      }

      ~iterator() { delete it_; }
    };

    class full_iterator : public base_iterator {
      size_t i_;
      std::shared_ptr<const index_collection> idx_;
      size_t column_;
      size_t start_;
      size_t end_;
      index::column::iterator it_;
      index::column::iterator it_end_;
      index::column::iterator it_start_;

    public:
      full_iterator(std::shared_ptr<const index_collection> idx, size_t column);
      void next();
      void prev();
      void advance(int n);
      bool equal_to(const base_iterator& it);
      ptrdiff_t distance_to(const base_iterator& it);
      string value();
      full_iterator* clone() const;
      virtual ~full_iterator() = default;
    };

    iterator begin() { return begin_; }
    iterator end() { return end_; }

    std::shared_ptr<column> slice(size_t start, size_t end) {
      return std::make_shared<column>(begin_ + start, begin_ + end);
    }

    size_t size() { return end_ - begin_; }
    string operator[](int i) { return *(begin_ + i); }

    column() = delete;
    column(const iterator& begin, const iterator& end)
        : begin_(begin), end_(end){};

  private:
    iterator begin_;
    iterator end_;
  };

  // class column_subset : public column {
  // std::shared_ptr<column> col_;
  // std::shared_ptr<std::vector<size_t> > idx_;
  // size_t start_;
  // size_t end_;

  // public:
  // column_subset(
  // std::shared_ptr<column> col, std::shared_ptr<std::vector<size_t> > idx);

  // class iterator {
  // using iterator_category = std::forward_iterator_tag;
  // using value_type = string;
  // using pointer = string*;
  // using reference = string&;

  // std::shared_ptr<column> col_;
  // std::shared_ptr<std::vector<size_t> > idx_;
  // size_t i_;

  // public:
  // iterator(
  // std::shared_ptr<column> col,
  // std::shared_ptr<std::vector<size_t> > idx);
  // iterator operator++(int); [> postfix <]
  // iterator& operator++();   [> prefix <]
  // bool operator!=(const iterator& other) const;
  // bool operator==(const iterator& other) const;

  // string operator*();
  // iterator& operator+=(int n);
  // iterator operator+(int n);
  //};
  // iterator begin();
  // iterator end();

  // std::shared_ptr<column_subset> slice(size_t start, size_t end);
  // size_t size() const;
  // string operator[](size_t i) const;
  //};

  std::shared_ptr<column> get_column(size_t num) const {
    auto begin =
        column::iterator(new column::full_iterator(shared_from_this(), num));
    auto end =
        column::iterator(new column::full_iterator(shared_from_this(), num)) +
        rows_;
    return std::make_shared<column>(begin, end);
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
}; // namespace vroom
} // namespace vroom
