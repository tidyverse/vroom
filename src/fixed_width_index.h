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

class fixed_width_index {
  //: public index,
  // public std::enable_shared_from_this<fixed_width_index> {
  std::vector<size_t> newlines_;
  std::vector<size_t> col_starts_;
  std::vector<size_t> col_ends_;
  mio::mmap_source mmap_;

public:
  fixed_width_index(
      const char* filename,
      std::vector<size_t> col_starts,
      std::vector<size_t> col_ends)
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
    size_t file_size2 = mmap_.cend() - mmap_.cbegin();

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
    auto end = mmap_.data() + (newlines_[row] + nl_size + col_ends_[col]);
    return {begin, end};
  }
};
} // namespace vroom
