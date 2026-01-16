#pragma once

#ifndef VROOM_LOG
#define SPDLOG_TRACE(...) (void)0
#define SPDLOG_DEBUG(...) (void)0
#define SPDLOG_INFO(...) (void)0
#else
#include "spdlog/spdlog.h"
#endif

#include <cstring>
#include <limits>
#include <string>

namespace vroom {

/// Sentinel value indicating an invalid or unset position.
constexpr static size_t null_pos = std::numeric_limits<size_t>::max();

/**
 * @brief Represents a field's byte boundaries in the source buffer.
 *
 * field_span provides the byte range for a single CSV field, enabling
 * efficient direct type parsing without intermediate string allocation.
 * This is particularly useful for numeric parsing where we can parse
 * directly from the memory-mapped buffer.
 */
struct field_span {
  size_t start; ///< Byte offset of field start (inclusive)
  size_t end;   ///< Byte offset of field end (exclusive)

  /// Default constructor, creates an invalid span.
  field_span() : start(null_pos), end(null_pos) {}

  /// Construct with explicit start and end positions.
  field_span(size_t start, size_t end) : start(start), end(end) {}

  /// Check if this span is valid.
  bool is_valid() const { return start != null_pos && end != null_pos; }

  /// Get the length of the field in bytes.
  size_t length() const { return is_valid() ? end - start : 0; }
};

enum column_type {
  Chr = 1,
  Fct = 2,
  Int = 4,
  Dbl = 8,
  Num = 16,
  Lgl = 32,
  Dttm = 64,
  Date = 128,
  Time = 256,
  BigInt = 512,
  Skip = 1024
};

// A custom string wrapper that avoids constructing a string object unless
// needed because of escapes.
class string {
public:
  string(const std::string& str) : str_(str) {
    begin_ = str_.c_str();
    end_ = begin_ + str_.length();
  }
  string(std::string&& str) : str_(std::move(str)) {
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

  std::string str() const {
    if (size() > 0 && str_.size() == 0) {
      return std::string(begin_, end_);
    } else {
      return str_;
    }
  }

private:
  const char* begin_;
  const char* end_;
  const std::string str_;
};

template <typename T> inline T na();

} // namespace vroom

// Specialization for our custom strings, needed so we can use them in
// unordered_maps
// [1]: https://stackoverflow.com/a/17017281/2055486
// [2]: https://stackoverflow.com/a/34597485/2055486
namespace std {

template <> struct hash<vroom::string> {
  std::size_t operator()(const vroom::string& k) const {
    const char* begin = k.begin();
    const char* end = k.end();

    size_t result = 0;
    const size_t prime = 31;
    while (begin != end) {
      result = *begin++ + (result * prime);
    }
    return result;
  }
};

} // namespace std
