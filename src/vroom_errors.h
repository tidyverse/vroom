#pragma once

#include <condition_variable>
#include <cpp11/data_frame.hpp>
#include <mutex>
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
      Rf_warning("One or more parsing issues, see `problems()` for details");
    }
  }

private:
  mutable bool have_warned_ = false;
  std::mutex mutex_;
  std::vector<std::string> filenames_;
  std::vector<size_t> rows_;
  std::vector<size_t> columns_;
  std::vector<std::string> expected_;
  std::vector<std::string> actual_;
};
