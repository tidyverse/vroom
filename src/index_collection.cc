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
      it_start_(idx_->indexes_[i_]->get_column(column_).begin()) {
  SPDLOG_TRACE("{0:x}: full_iterator ctor", (size_t)this);
}

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

void index_collection::column::full_iterator::advance(ptrdiff_t n) {
  if (n == 0) {
    return;
  }
  if (n > 0) {
    while (n > 0) {
      auto diff = it_end_ - it_;
      if (n <= diff) {
        it_ += n;
        return;
      }
      it_ += (diff - 1);
      n -= diff;
      next();
    }
    return;
  }
  if (n < 0) {
    while (n < 0) {
      auto diff = it_start_ - it_;
      if (n >= diff) {
        it_ -= n;
        return;
      }
      it_ -= (diff + 1);
      n += diff;
      prev();
    }
    return;
  }
}

ptrdiff_t index_collection::column::full_iterator::distance_to(
    const base_iterator& that) const {

  auto that_ = static_cast<const full_iterator&>(that);

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

string index_collection::column::full_iterator::value() const { return *it_; }

index_collection::column::full_iterator*
index_collection::column::full_iterator::clone() const {

  SPDLOG_TRACE("{0:x}: full_iterator clone", (size_t)this);
  auto copy = new index_collection::column::full_iterator(*this);
  return copy;
}

string index_collection::column::full_iterator::at(ptrdiff_t n) const {
  return idx_->get(n, column_);
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
          get_env("vroom.connection_size", 1 << 17),
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
