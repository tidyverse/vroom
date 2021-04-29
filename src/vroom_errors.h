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
  struct parse_error {
    size_t position;
    size_t column;
    parse_error(size_t pos, size_t col) : position(pos), column(col) {}
  };

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

  void add_parse_error(size_t position, size_t column) {
    std::lock_guard<std::mutex> guard(mutex_);
    parse_errors_.emplace_back(position, column);
  }

  void resolve_parse_errors(const vroom::index& idx) {
    if (parse_errors_.size() == 0) {
      return;
    }
    // Sort the parse errors by their position
    std::sort(
        parse_errors_.begin(),
        parse_errors_.end(),
        [](const parse_error& lhs, const parse_error& rhs) {
          return lhs.position < rhs.position;
        });
    auto row = idx.get_column(0)->begin();
    auto row_end = idx.get_column(0)->end();

    for (const auto& e : parse_errors_) {
      while (row != row_end && e.position > row.position()) {
        ++row;
      }
      std::stringstream ss_expected, ss_actual;
      ss_expected << idx.num_columns() << " columns";
      ss_actual << e.column + 1 << " columns";
      add_error(
          row.index() - 1,
          e.column,
          ss_expected.str(),
          ss_actual.str(),
          row.filename());
    }
  }

  cpp11::data_frame error_table() const {
    return cpp11::writable::data_frame({"row"_nm = rows_,
                                        "col"_nm = columns_,
                                        "expected"_nm = expected_,
                                        "actual"_nm = actual_,
                                        "file"_nm = filenames_});
  }

  bool has_errors() const { return rows_.size() > 0; }

  void warn_for_errors() const {
    if (!have_warned_ && rows_.size() > 0) {
      have_warned_ = true;
      static auto warn = Rf_findFun(
          Rf_install("warn"),
          Rf_findVarInFrame(R_NamespaceRegistry, Rf_install("rlang")));
      cpp11::sexp warn_call = Rf_lang3(
          warn,
          Rf_mkString(
              "One or more parsing issues, see `problems()` for details"),
          Rf_mkString("vroom_parse_issue"));
      Rf_eval(warn_call, R_EmptyEnv);
    }
  }

private:
  mutable bool have_warned_ = false;
  std::mutex mutex_;
  std::vector<std::string> filenames_;
  std::vector<parse_error> parse_errors_;
  std::vector<size_t> rows_;
  std::vector<size_t> columns_;
  std::vector<std::string> expected_;
  std::vector<std::string> actual_;
};
