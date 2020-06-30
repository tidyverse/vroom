#include <cpp11/function.hpp>

#include "delimited_index_connection.h"
#include "fixed_width_index.h"
#include "fixed_width_index_connection.h"
#include "index.h"
#include "index_collection.h"
#include <memory>

#include "r_utils.h"

using namespace vroom;

// Class index_collection::column::iterator

index_collection::full_iterator::full_iterator(
    std::shared_ptr<const index_collection> idx, size_t column)
    : i_(0),
      idx_(idx),
      column_(column),
      start_(0),
      end_(idx_->indexes_.size() - 1) {
  auto col = idx_->indexes_[i_]->get_column(column_);
  it_ = col->begin();
  it_end_ = col->end();
  it_start_ = col->begin();
}

void index_collection::full_iterator::next() {
  ++it_;
  if (it_ == it_end_ && i_ < end_) {
    ++i_;
    it_ = idx_->indexes_[i_]->get_column(column_)->begin();
    it_end_ = idx_->indexes_[i_]->get_column(column_)->end();
  }
}

void index_collection::full_iterator::prev() {
  --it_;
  if (it_ == it_start_ && i_ > start_) {
    --i_;
    it_ = idx_->indexes_[i_]->get_column(column_)->end();
    it_start_ = idx_->indexes_[i_]->get_column(column_)->begin();
  }
}

void index_collection::full_iterator::advance(ptrdiff_t n) {
  if (n == 0) {
    return;
  }
  if (n > 0) {
    while (n > 0) {
      auto diff = it_end_ - it_;
      if (n < diff) {
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
      if (n > diff) {
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

ptrdiff_t
index_collection::full_iterator::distance_to(const base_iterator& that) const {

  auto that_ = static_cast<const full_iterator*>(&that);

  if (i_ == that_->i_) {
    ptrdiff_t res = that_->it_ - it_;
    return res;
  }
  ptrdiff_t count = 0;
  size_t i = i_;

  if (i_ < that_->i_) {
    count = it_end_ - it_;
    ++i;
    while (i < that_->i_) {
      count += idx_->indexes_[i]->num_rows();
      ++i;
    }
    auto begin = idx_->indexes_[i]->get_column(column_)->begin();
    count += that_->it_ - begin;
    return count;
  }

  count = it_start_ - it_;
  --i;
  while (i > that_->i_) {
    count -= idx_->indexes_[i]->num_rows();
    --i;
  }
  auto end = idx_->indexes_[i]->get_column(column_)->end();
  count += that_->it_ - end;
  return count;
}

string index_collection::full_iterator::value() const { return *it_; }

index_collection::full_iterator*
index_collection::full_iterator::clone() const {
  auto copy = new index_collection::full_iterator(*this);
  return copy;
}

string index_collection::full_iterator::at(ptrdiff_t n) const {
  return idx_->get(n, column_);
}

std::shared_ptr<vroom::index> make_delimited_index(
    cpp11::sexp in,
    const char* delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    const size_t n_max,
    const char comment,
    const size_t num_threads,
    const bool progress) {

  auto standardise_one_path = cpp11::package("vroom")["standardise_one_path"];

  auto x = standardise_one_path(in);

  bool is_connection = TYPEOF(x) != STRSXP;

  if (is_connection) {
    return std::make_shared<vroom::delimited_index_connection>(
        x,
        delim,
        quote,
        trim_ws,
        escape_double,
        escape_backslash,
        has_header,
        skip,
        n_max,
        comment,
        get_env("VROOM_CONNECTION_SIZE", 1 << 17),
        progress);
  }

  auto filename = cpp11::as_cpp<std::string>(x);
  return std::make_shared<vroom::delimited_index>(
      filename.c_str(),
      delim,
      quote,
      trim_ws,
      escape_double,
      escape_backslash,
      has_header,
      skip,
      n_max,
      comment,
      num_threads,
      progress);
}

void check_column_consistency(
    std::shared_ptr<vroom::index> first,
    std::shared_ptr<vroom::index> check,
    bool has_header,
    size_t i) {

  if (check->num_columns() != first->num_columns()) {

    std::stringstream ss;
    ss << "Files must all have " << first->num_columns()
       << " columns:\n"
          "* File "
       << i + 1 << " has " << check->num_columns() << " columns";

    cpp11::stop("%s", ss.str().c_str());
  }

  // If the files have a header ensure they are consistent with each other.
  if (has_header) {
    auto first_header = first->get_header()->begin();

    auto check_header = check->get_header();
    auto col = 0;
    for (auto header : *check_header) {
      if (!(header == *first_header)) {

        std::stringstream ss;
        ss << "Files must have consistent column names:\n"
              "* File 1 column "
           << col + 1 << " is: " << (*first_header).str()
           << "\n"
              "* File "
           << i + 1 << " column " << col + 1 << " is: " << header.str();

        cpp11::stop("%s", ss.str().c_str());
      }

      ++first_header;
      ++col;
    }
  }
}

// Index_collection
index_collection::index_collection(
    cpp11::list in,
    const char* delim,
    const char quote,
    const bool trim_ws,
    const bool escape_double,
    const bool escape_backslash,
    const bool has_header,
    const size_t skip,
    const size_t n_max,
    const char comment,
    const size_t num_threads,
    const bool progress)
    : rows_(0), columns_(0) {

  std::shared_ptr<vroom::index> first = make_delimited_index(
      in[0],
      delim,
      quote,
      trim_ws,
      escape_double,
      escape_backslash,
      has_header,
      skip,
      n_max,
      comment,
      num_threads,
      progress);

  indexes_.push_back(first);
  columns_ = first->num_columns();
  rows_ = first->num_rows();

  for (int i = 1; i < in.size(); ++i) {

    std::shared_ptr<vroom::index> idx = make_delimited_index(
        in[i],
        delim,
        quote,
        trim_ws,
        escape_double,
        escape_backslash,
        has_header,
        skip,
        n_max,
        comment,
        num_threads,
        progress);

    check_column_consistency(first, idx, has_header, i);

    rows_ += idx->num_rows();

    indexes_.emplace_back(std::move(idx));
  }
}

std::shared_ptr<vroom::index> make_fixed_width_index(
    cpp11::sexp in,
    const std::vector<int>& col_starts,
    const std::vector<int>& col_ends,
    const bool trim_ws,
    const size_t skip,
    const char comment,
    const size_t n_max,
    const bool progress) {

  auto standardise_one_path = cpp11::package("vroom")["standardise_one_path"];

  auto x = standardise_one_path(in);

  bool is_connection = TYPEOF(x) != STRSXP;

  if (is_connection) {
    return std::make_shared<vroom::fixed_width_index_connection>(
        x,
        col_starts,
        col_ends,
        trim_ws,
        skip,
        comment,
        n_max,
        progress,
        get_env("VROOM_CONNECTION_SIZE", 1 << 17));
  } else {
    auto filename = cpp11::as_cpp<std::string>(x);
    return std::make_shared<vroom::fixed_width_index>(
        filename.c_str(),
        col_starts,
        col_ends,
        trim_ws,
        skip,
        comment,
        n_max,
        progress);
  }
}

index_collection::index_collection(
    cpp11::list in,
    const std::vector<int>& col_starts,
    const std::vector<int>& col_ends,
    const bool trim_ws,
    const size_t skip,
    const char comment,
    const size_t n_max,
    const bool progress)
    : rows_(0), columns_(0) {

  auto first = make_fixed_width_index(
      in[0], col_starts, col_ends, trim_ws, skip, comment, n_max, progress);

  columns_ = first->num_columns();
  rows_ = first->num_rows();

  indexes_.push_back(first);

  for (int i = 1; i < in.size(); ++i) {
    auto idx = make_fixed_width_index(
        in[i], col_starts, col_ends, trim_ws, skip, comment, n_max, progress);

    check_column_consistency(first, idx, false, i);

    rows_ += idx->num_rows();

    indexes_.emplace_back(std::move(idx));
  }
}

string index_collection::get(size_t row, size_t column) const {
  for (const auto& idx : indexes_) {
    if (row < idx->num_rows()) {
      return idx->get(row, column);
    }
    row -= idx->num_rows();
  }
  /* should never get here */
  return std::string("");
}
