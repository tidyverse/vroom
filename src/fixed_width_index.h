#include "Rcpp.h"

#include "index.h"

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

#include "utils.h"

namespace vroom {

class fixed_width_index
    : public std::enable_shared_from_this<fixed_width_index> {
  //: public index,
  // public std::enable_shared_from_this<fixed_width_index> {
  std::vector<size_t> newlines_;
  std::vector<int> col_starts_;
  std::vector<int> col_ends_;
  mio::mmap_source mmap_;

public:
  fixed_width_index(
      const char* filename,
      std::vector<int> col_starts,
      std::vector<int> col_ends)
      : col_starts_(col_starts), col_ends_(col_ends) {

    std::error_code error;
    mmap_ = mio::make_mmap_source(filename, error);

    if (error) {
      // We cannot actually portably compare error messages due to a bug in
      // libstdc++ (https://stackoverflow.com/a/54316671/2055486), so just print
      // the message on stderr return
      Rcpp::stop("mapping error: %s", error.message());
    }

    size_t file_size = mmap_.size();

    newlines_.push_back(-1);

    size_t newline = find_next_newline(mmap_, 0);
    while (newline < file_size) {
      newlines_.push_back(newline);
      newline = find_next_newline(mmap_, newline + 1);
    }
    // Rcpp::Rcerr << "rows: " << newlines_.size() << '\n';
  }

  size_t num_rows() const { return newlines_.size() - 1; }
  size_t num_columns() const { return col_starts_.size(); }

  string get(size_t row, size_t col) const {
    size_t nl_size = 1;
    auto begin = mmap_.data() + (newlines_[row] + nl_size + col_starts_[col]);
    const char* end;
    if (col_ends_[col] == NA_INTEGER) {
      end = mmap_.data() + newlines_[row + 1];
    } else {
      end = mmap_.data() + (newlines_[row] + nl_size + col_ends_[col]);
    }
    return {begin, end};
  }

  class column : public index::column {
    std::shared_ptr<const fixed_width_index> idx_;
    size_t column_;

  public:
    column(std::shared_ptr<const fixed_width_index> idx, size_t column)
        : idx_(idx), column_(column) {}

    class column_iterator : public base_iterator {
      std::shared_ptr<const fixed_width_index> idx_;
      size_t column_;
      size_t i_;

    public:
      column_iterator(
          std::shared_ptr<const fixed_width_index> idx, size_t column)
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
      string at(ptrdiff_t n) const { return idx_->get(i_, column_); }
      virtual ~column_iterator() = default;
    };
    vroom::iterator begin() const { return new column_iterator(idx_, column_); }
    vroom::iterator end() const {
      auto res = new column_iterator(idx_, column_);
      res->advance(idx_->num_rows());
      return res;
    };
    string at(size_t i) const { return idx_->get(i, column_); }
    size_t size() const { return idx_->num_rows(); }
    std::shared_ptr<vroom::index::column> slice() const { return nullptr; }
    std::shared_ptr<vroom::index::column> subset() const { return nullptr; }
    ~column() = default;
  };

  std::shared_ptr<vroom::index::column> get_column(size_t column) const {
    return std::make_shared<vroom::fixed_width_index::column>(
        shared_from_this(), column);
  }
};
} // namespace vroom
