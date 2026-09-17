#pragma once

#include <cstddef>
#include <string>

#ifndef VROOM_STANDALONE
#include <cpp11/R.hpp>
#include <cpp11/sexp.hpp>
#endif

namespace vroom {

enum class progress_type { file, connection, write };

#ifdef VROOM_STANDALONE

class progress_bar {
public:
  progress_bar(
      bool,
      progress_type,
      double = 0,
      const std::string& = std::string()) {}

  void tick(double) {}
  void done() {}
};

#else

class progress_bar {
public:
  progress_bar(
      bool enabled,
      progress_type type,
      double total = NA_REAL,
      const std::string& filename = std::string());

  progress_bar(const progress_bar&) = delete;
  progress_bar& operator=(const progress_bar&) = delete;

  ~progress_bar();

  void tick(double increment);
  void done();

private:
  static std::string basename(const std::string& path);

  bool enabled_;
  cpp11::sexp bar_;
};

#endif

} // namespace vroom
