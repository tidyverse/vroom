#pragma once

#include "index.h"
#include <condition_variable>
#include <cpp11/data_frame.hpp>
#include <cpp11/function.hpp>
#include <cpp11/strings.hpp>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace cpp11::literals;

class vroom_errors {
  struct parse_error {
    size_t position;
    size_t column;
    std::string expected;
    std::string actual;
    std::string filename;
    parse_error(
        size_t pos,
        size_t col,
        std::string exp,
        std::string act,
        std::string file)
        : position(pos),
          column(col),
          expected(exp),
          actual(act),
          filename(file) {}
  };

public:
  vroom_errors() {}

  void add_error(
      size_t row,
      size_t column,
      std::string expected = "",
      std::string actual = "",
      std::string filename = "",
      size_t line = 0) {
    std::lock_guard<std::mutex> guard(mutex_);
    lines_.push_back(line == 0 ? row + 1 : line);
    rows_.push_back(row + 1);
    columns_.push_back(column + 1);
    expected_.emplace_back(expected);
    actual_.emplace_back(actual);
    filenames_.emplace_back(filename);
  }

  void add_parse_error(
      size_t position,
      size_t column,
      std::string expected,
      std::string actual,
      std::string filename) {
    std::lock_guard<std::mutex> guard(mutex_);
    parse_errors_.emplace_back(
        position, column, expected, actual, filename);
  }

  size_t parse_error_count() {
    std::lock_guard<std::mutex> guard(mutex_);
    return parse_errors_.size();
  }

  void rollback_parse_errors(size_t size) {
    std::lock_guard<std::mutex> guard(mutex_);
    parse_errors_.erase(parse_errors_.begin() + size, parse_errors_.end());
  }

  void resolve_parse_errors(const vroom::index& idx) {
    if (parse_errors_.size() == 0) {
      return;
    }
    std::unordered_map<
        std::string,
        std::vector<std::pair<size_t, size_t>>>
        rows;
    auto row = idx.get_column(0)->begin();
    auto row_end = idx.get_column(0)->end();
    while (row != row_end) {
      rows[row.filename()].emplace_back(row.position(), row.index());
      ++row;
    }

    for (const auto& e : parse_errors_) {
      const auto& starts = rows[e.filename];
      auto match = std::upper_bound(
          starts.begin(),
          starts.end(),
          e.position,
          [](size_t position, const std::pair<size_t, size_t>& row) {
            return position < row.first;
          });
      size_t data_row = match == starts.begin() ? 0 : std::prev(match)->second;

      add_error(
          data_row,
          e.column,
          e.expected,
          e.actual,
          e.filename,
          idx.source_line(e.position, e.filename));
    }
  }

  cpp11::data_frame error_table() const {
    return cpp11::writable::data_frame(
        {"line"_nm = lines_,
         "row"_nm = rows_,
         "col"_nm = columns_,
         "expected"_nm = expected_,
         "actual"_nm = actual_,
         "file"_nm = filenames_});
  }

  bool has_errors() const { return rows_.size() > 0; }

  void warn_for_errors() const {
    if (!have_warned_ && rows_.size() > 0) {
      have_warned_ = true;
      // it is intentional that we aren't using cpp11::package
      // https://github.com/tidyverse/vroom/commit/984a3e5e37e124feacfec3d184dbeb02eb1145c4
      SEXP cli_str = Rf_mkString("cli");
      PROTECT(cli_str);
      SEXP get_ns_call = Rf_lang2(Rf_install("getNamespace"), cli_str);
      PROTECT(get_ns_call);
      SEXP cli_ns = Rf_eval(get_ns_call, R_BaseEnv);
      PROTECT(cli_ns);
      SEXP cli_warn = Rf_findFun(Rf_install("cli_warn"), cli_ns);
      PROTECT(cli_warn);
      cpp11::strings bullets({
        "w"_nm = "One or more parsing issues, call {.fun problems} on your data for details, e.g.:",
        " "_nm = "problems(x)"});
      cpp11::sexp cli_warn_call = Rf_lang3(
        cli_warn,
        bullets,
        Rf_mkString("vroom_parse_issue"));
      Rf_eval(cli_warn_call, R_EmptyEnv);
      UNPROTECT(4);
    }
  }

  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    lines_.clear();
    rows_.clear();
    columns_.clear();
    expected_.clear();
    actual_.clear();
    filenames_.clear();
    parse_errors_.clear();
  }

private:
  mutable bool have_warned_ = false;
  std::mutex mutex_;
  std::vector<std::string> filenames_;
  std::vector<parse_error> parse_errors_;
  std::vector<size_t> lines_;
  std::vector<size_t> rows_;
  std::vector<size_t> columns_;
  std::vector<std::string> expected_;
  std::vector<std::string> actual_;
};
