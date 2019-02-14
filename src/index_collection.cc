#include "index_collection.h"
#include "index.h"
#include "index_connection.h"
#include <memory>

using namespace vroom;
using namespace Rcpp;

// Class index_collection::column::iterator

index_collection::column::iterator::iterator(
    const index_collection& idx, size_t column, size_t start)
    : i_(0),
      idx_(idx),
      column_(column),
      // start_(0),
      end_(idx_.indexes_.size() - 1),
      it_(idx_.indexes_[i_]->get_column(column_).begin()),
      it_end_(idx_.indexes_[i_]->get_column(column_).end()) {
  *this += start;
}

index_collection::column::iterator index_collection::column::iterator::
operator++(int) /* postfix */ {
  index_collection::column::iterator copy(*this);
  ++*this;
  return copy;
}
index_collection::column::iterator& index_collection::column::iterator::
operator++() /* prefix */ {
  ++it_;
  if (it_ == it_end_ && i_ < end_) {
    ++i_;
    it_ = idx_.indexes_[i_]->get_column(column_).begin();
    it_end_ = idx_.indexes_[i_]->get_column(column_).end();
  }
  return *this;
}

index_collection::column::iterator& index_collection::column::iterator::
operator+=(int n) {
  while (n > 0) {
    ++(*this);
    --n;
  }
  return *this;
}

bool index_collection::column::iterator::
operator!=(const index_collection::column::iterator& other) const {
  return i_ != other.i_ || (i_ == other.i_ && it_ != other.it_);
}
bool index_collection::column::iterator::
operator==(const index_collection::column::iterator& other) const {
  return !(i_ != other.i_);
}

std::string index_collection::column::iterator::operator*() { return *it_; }

index_collection::column::iterator index_collection::column::iterator::
operator+(int n) {
  index_collection::column::iterator out(*this);
  out += n;
  return out;
}

// Class index_collection::column
index_collection::column::column(const index_collection& idx, size_t column)
    : idx_(idx), column_(column), start_(0), end_(idx.rows_){};

index_collection::column::column(
    const index_collection& idx, size_t column, size_t start, size_t end)
    : idx_(idx), column_(column), start_(start), end_(end){};

index_collection::column::iterator index_collection::column::begin() {
  return index_collection::column::iterator(idx_, column_, start_);
}

index_collection::column::iterator index_collection::column::end() {
  return index_collection::column::iterator(idx_, column_, end_);
}

// Index_collection
index_collection::index_collection(
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
    const bool progress)
    : rows_(0), columns_(0) {

  for (int i = 0; i < in.size(); ++i) {
    auto x = in[i];

    bool is_connection = TYPEOF(x) != STRSXP;

    std::unique_ptr<vroom::index> p;
    if (is_connection) {
      p = std::unique_ptr<vroom::index>(new vroom::index_connection(
          x,
          delim,
          quote,
          trim_ws,
          escape_double,
          escape_backslash,
          has_header,
          skip,
          comment,
          1 << 20,
          progress));
    } else {
      auto filename = as<std::string>(x);
      p = std::unique_ptr<vroom::index>(new vroom::index(
          filename.c_str(),
          delim,
          quote,
          trim_ws,
          escape_double,
          escape_backslash,
          has_header,
          skip,
          comment,
          num_threads,
          progress));
    }
    rows_ += p->num_rows();
    columns_ = p->num_columns();

    indexes_.push_back(std::move(p));
  }
}

const std::string index_collection::get(size_t row, size_t column) const {
  for (const auto& idx : indexes_) {
    if (row < idx->num_rows()) {
      return idx->get(row, column);
    }
    row -= idx->num_rows();
  }
  /* should never get here */
  return "";
}
