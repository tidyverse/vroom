#include "index_collection.h"
#include "index.h"
#include "index_connection.h"
#include <memory>

using namespace vroom;
using namespace Rcpp;

// Class index_collection::column::iterator

index_collection::column::full_iterator::full_iterator(
    std::shared_ptr<const index_collection> idx, size_t column)
    : i_(0),
      idx_(idx),
      column_(column),
      start_(0),
      end_(idx_->indexes_.size() - 1),
      it_(idx_->indexes_[i_]->get_column(column_).begin()),
      it_end_(idx_->indexes_[i_]->get_column(column_).end()),
      it_start_(idx_->indexes_[i_]->get_column(column_).begin()) {}

void index_collection::column::full_iterator::next() {
  ++it_;
  if (it_ == it_end_ && i_ < end_) {
    ++i_;
    it_ = idx_->indexes_[i_]->get_column(column_).begin();
    it_end_ = idx_->indexes_[i_]->get_column(column_).end();
  }
}

void index_collection::column::full_iterator::prev() {
  --it_;
  if (it_ == it_start_ && i_ > start_) {
    --i_;
    it_ = idx_->indexes_[i_]->get_column(column_).end();
    it_start_ = idx_->indexes_[i_]->get_column(column_).begin();
  }
}

void index_collection::column::full_iterator::advance(int n) {
  while (n > 0) {
    next();
    --n;
  }
  while (n < 0) {
    prev();
    ++n;
  }
}

bool index_collection::column::full_iterator::equal_to(
    const base_iterator& other) {
  auto other_ = dynamic_cast<const full_iterator&>(other);
  return i_ != other_.i_ || (i_ == other_.i_ && it_ != other_.it_);
}

ptrdiff_t index_collection::column::full_iterator::distance_to(
    const base_iterator& that) {

  auto that_ = dynamic_cast<const full_iterator&>(that);

  if (i_ == that_.i_) {
    ptrdiff_t res = that_.it_ - it_;
    return res;
  }
  ptrdiff_t count = 0;
  size_t i = i_;

  if (i_ < that_.i_) {
    count = it_end_ - it_;
    ++i;
    while (i < that_.i_) {
      count += idx_->indexes_[i]->num_rows();
      ++i;
    }
    auto begin = idx_->indexes_[i]->get_column(column_).begin();
    count += that_.it_ - begin;
    return count;
  }

  count = it_start_ - it_;
  --i;
  while (i > that_.i_) {
    count -= idx_->indexes_[i]->num_rows();
    --i;
  }
  auto end = idx_->indexes_[i]->get_column(column_).end();
  count += that_.it_ - end;
  return count;
}

string index_collection::column::full_iterator::value() { return *it_; }

index_collection::column::full_iterator*
index_collection::column::full_iterator::clone() const {
  return new index_collection::column::full_iterator(*this);
}

// Class index_collection::column_subset::iterator

// index_collection::column_subset::iterator::iterator(
// std::shared_ptr<column> col, std::shared_ptr<std::vector<size_t> > idx)
//: col_(col), idx_(idx), i_(0) {}

// index_collection::column_subset::iterator
// index_collection::column_subset::iterator::operator++(int) [> postfix <] {
// index_collection::column_subset::iterator copy(*this);
//++*this;
// return copy;
//}
// index_collection::column_subset::iterator&
// index_collection::column_subset::iterator::operator++() [> prefix <] {
//++i_;
// return *this;
//}

// index_collection::column_subset::iterator&
// index_collection::column_subset::iterator::operator+=(int n) {
// i_ += n;
// return *this;
//}

// bool index_collection::column_subset::iterator::
// operator!=(const index_collection::column_subset::iterator& other) const {
// return i_ != other.i_;
//}
// bool index_collection::column_subset::iterator::
// operator==(const index_collection::column_subset::iterator& other) const {
// return !(i_ != other.i_);
//}

// string index_collection::column_subset::iterator::operator*() {
// return (*col_)[(*idx_)[i_]];
//}

// index_collection::column_subset::iterator
// index_collection::column_subset::iterator::operator+(int n) {
// index_collection::column_subset::iterator out(*this);
// out += n;
// return out;
//}

//// Class index_collection::column_subset
// index_collection::column_subset::column_subset(
// std::shared_ptr<column> col, std::shared_ptr<std::vector<size_t> > idx)
//: col_(col), idx_(idx), start_(0), end_(idx->size()) {}

// index_collection::column_subset::iterator
// index_collection::column_subset::begin() {
// return index_collection::column_subset::iterator(col_, idx_) + start_;
//}

// index_collection::column_subset::iterator
// index_collection::column_subset::end() {
// return index_collection::column_subset::iterator(col_, idx_) += end_;
//}

// std::shared_ptr<vroom::index_collection::column_subset>
// index_collection::column_subset::slice(size_t start, size_t end) {
// auto copy = std::make_shared<index_collection::column_subset>(*this);
// copy->start_ = start;
// copy->end_ = end;
// return copy;
//}

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
          get_option("vroom.connection_size", 1 << 17),
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

const string index_collection::get(size_t row, size_t column) const {
  for (const auto& idx : indexes_) {
    if (row < idx->num_rows()) {
      return idx->get(row, column);
    }
    row -= idx->num_rows();
  }
  /* should never get here */
  return std::string("");
}
