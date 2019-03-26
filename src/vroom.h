#pragma once

#ifndef VROOM_LOG
#define SPDLOG_TRACE(...) (void)0
#define SPDLOG_DEBUG(...) (void)0
#define SPDLOG_INFO(...) (void)0
#else
#include "spdlog/spdlog.h"
#endif

#include <string>

namespace vroom {

// A custom string wrapper that avoids constructing a string object unless
// needed because of escapes.
class string {
public:
  string(std::string str) : str_(str) {
    begin_ = str_.c_str();
    end_ = begin_ + str_.length();
  }
  string(const char* begin, const char* end) : begin_(begin), end_(end) {}

  const char* begin() const { return begin_; }

  const char* end() const { return end_; }

  size_t length() const { return end_ - begin_; }

  size_t size() const { return end_ - begin_; }

  bool operator==(const string& other) const {
    return length() == other.length() &&
           strncmp(begin_, other.begin_, length()) == 0;
  }

  bool operator==(const std::string& other) const {
    return length() == other.length() &&
           strncmp(begin_, other.data(), length()) == 0;
  }

private:
  const char* begin_;
  const char* end_;
  std::string str_;
};

} // namespace vroom
