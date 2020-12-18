#pragma once

#include "index.h"
#include <condition_variable>
#include <cpp11/data_frame.hpp>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

using namespace cpp11::literals;

class vroom_errors {
public:
  vroom_errors() {}

  void add_error(
      size_t row,
      size_t column,
      std::string expected = "",
      std::string actual = "",
      std::string filename = "") {
    std::lock_guard<std::mutex> guard(mutex_);
    rows_.push_back(row + 1);
    columns_.push_back(column + 1);
    expected_.emplace_back(expected);
    actual_.emplace_back(actual);
    filenames_.emplace_back(filename);
  }

  void add_parse_error(size_t position, size_t columns) {
    std::lock_guard<std::mutex> guard(mutex_);
    positions_.push_back(position);
    actual_columns_.push_back(columns);
  }

  void resolve_parse_errors(const vroom::index& idx) {
    if (positions_.size() == 0) {
      return;
    }
    auto row = idx.get_column(0)->begin();
    size_t row_num = 0;

    bool has_header = idx.get_header()->size() > 0;
    std::sort(positions_.begin(), positions_.end());
    for (size_t i = 0; i < positions_.size(); ++i) {
      size_t p = positions_[i];
      while (p < row.position()) {
        ++row;
        ++row_num;
      }
      std::stringstream ss_expected, ss_actual;
      ss_expected << idx.num_columns() << " columns";
      ss_actual << actual_columns_[i] + 1 << " columns";
      add_error(
          row_num + has_header,
          actual_columns_[i],
          ss_expected.str(),
          ss_actual.str(),
          row.filename());
    }
  }

  cpp11::data_frame error_table() const {
    return cpp11::writable::data_frame(
        {"row"_nm = rows_,
         "col"_nm = columns_,
         "expected"_nm = expected_,
         "actual"_nm = actual_,
         "file"_nm = filenames_});
  }

  bool has_errors() const { return rows_.size() > 0; }

  void warn_for_errors() const {
    if (!have_warned_ && rows_.size() > 0) {
      have_warned_ = true;
      Rf_warningcall(
          R_NilValue,
          "One or more parsing issues, see `problems()` for details");
    }
  }

private:
  mutable bool have_warned_ = false;
  std::mutex mutex_;
  std::vector<std::string> filenames_;
  std::vector<size_t> positions_;
  std::vector<size_t> actual_columns_;
  std::vector<size_t> rows_;
  std::vector<size_t> columns_;
  std::vector<std::string> expected_;
  std::vector<std::string> actual_;
};
