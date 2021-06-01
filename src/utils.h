#pragma once

#include <array>
#include <cstring>
#include <sstream>
#include <string>

namespace vroom {

inline bool
is_comment(const char* begin, const char* end, const std::string& comment) {
  if (comment.empty() || comment.size() >= static_cast<size_t>(end - begin)) {
    return false;
  }

  return strncmp(comment.data(), begin, comment.size()) == 0;
}

template <typename T>
inline size_t skip_rest_of_line(const T& source, size_t start) {
  auto out = memchr(source.data() + start, '\n', source.size() - start);
  if (!out) {
    return (source.size());
  }
  return static_cast<const char*>(out) - source.data();
}

inline bool
is_empty_line(const char* begin, const char* end, const bool skip_empty_rows) {
  if (!skip_empty_rows) {
    return false;
  }

  if (skip_empty_rows && *begin == '\n') {
    return true;
  }

  while (begin < end && (*begin == ' ' || *begin == '\t' || *begin == '\r')) {
    ++begin;
  }

  return *begin == '\n';
}

inline bool is_blank_or_comment_line(
    const char* begin,
    const char* end,
    const std::string& comment,
    const bool skip_empty_rows) {
  if (!skip_empty_rows && comment.empty()) {
    return false;
  }

  if (skip_empty_rows && *begin == '\n') {
    return true;
  }

  while (begin < end && (*begin == ' ' || *begin == '\t' || *begin == '\r')) {
    ++begin;
  }

  if ((skip_empty_rows && *begin == '\n') ||
      (!comment.empty() &&
       strncmp(begin, comment.data(), comment.size()) == 0)) {
    return true;
  }

  return false;
}

template <typename T>
static size_t find_next_non_quoted_newline(
    const T& source, size_t start, const char quote = '"') {
  if (start > source.size() - 1) {
    return source.size() - 1;
  }

  std::array<char, 3> query = {'\n', quote, '\0'};

  auto buf = source.data();
  size_t pos = start;

  size_t end = source.size() - 1;
  bool in_quote = false;

  while (pos < end) {
    size_t buf_offset = strcspn(buf + pos, query.data());
    pos = pos + buf_offset;
    auto c = buf[pos];
    if (c == '\n') {
      if (in_quote) {
        ++pos;
        continue;
      }
      return pos;
    }

    else if (c == quote) {
      in_quote = !in_quote;
    }
    ++pos;
  }

  if (pos > end) {
    return end;
  }

  return pos;
}

template <typename T>
static size_t find_next_newline(
    const T& source,
    size_t start,
    const std::string& comment,
    const bool skip_empty_rows,
    bool embedded_nl) {

  if (start >= source.size()) {
    return source.size() - 1;
  }

  if (embedded_nl) {
    return find_next_non_quoted_newline(source, start);
  }

  auto begin = source.data() + start;

  auto end = source.data() + source.size();

  while (begin && begin < end) {
    begin = static_cast<const char*>(memchr(begin, '\n', end - begin));
    break;
    if (!(begin && begin + 1 < end &&
          is_blank_or_comment_line(begin + 1, end, comment, skip_empty_rows))) {
      break;
    }
  }

  if (!begin) {
    return source.size() - 1;
  }

  return begin - source.data();
}

template <typename T> T get_env(const char* name, T default_value) {

  char* p;

  p = getenv(name);
  if (!p || strlen(p) == 0) {
    return default_value;
  }

  std::stringstream ss(p);
  T out;
  ss >> out;
  return out;
}

inline bool is_space(const char* c) {
  return *c == ' ' || *c == '\t' || *c == '\0' || *c == '\r';
}

inline void trim_whitespace(const char*& begin, const char*& end) {
  while (begin != end && is_space(begin)) {
    ++begin;
  }

  while (end != begin && is_space(end - 1)) {
    --end;
  }
}

template <typename T> size_t skip_bom(const T& source) {
  /* Skip Unicode Byte Order Marks
https://en.wikipedia.org/wiki/Byte_order_mark#Representations_of_byte_order_marks_by_encoding
00 00 FE FF: UTF-32BE
FF FE 00 00: UTF-32LE
FE FF:       UTF-16BE
FF FE:       UTF-16LE
EF BB BF:    UTF-8
*/

  auto size = source.size();
  auto begin = source.data();

  switch (begin[0]) {
  // UTF-32BE
  case '\x00':
    if (size >= 4 && begin[1] == '\x00' && begin[2] == '\xFE' &&
        begin[3] == '\xFF') {
      return 4;
    }
    break;

    // UTF-8
  case '\xEF':
    if (size >= 3 && begin[1] == '\xBB' && begin[2] == '\xBF') {
      return 3;
    }
    break;

    // UTF-16BE
  case '\xfe':
    if (size >= 2 && begin[1] == '\xff') {
      return 2;
    }
    break;

  case '\xff':
    if (size >= 2 && begin[1] == '\xfe') {

      // UTF-32 LE
      if (size >= 4 && begin[2] == '\x00' && begin[3] == '\x00') {
        return 4;
      } else {
        // UTF-16 LE
        return 2;
      }
    }
    break;
  }

  return 0;
}

// This skips leading blank lines and comments (if needed)
template <typename T>
size_t find_first_line(
    const T& source,
    size_t skip,
    const char* comment,
    const bool skip_empty_rows,
    const bool embedded_nl) {

  auto begin = skip_bom(source);
  /* Skip skip parameters, comments and blank lines */

  while (bool should_skip =
             (begin < source.size() - 1 && is_blank_or_comment_line(
                                               source.data() + begin,
                                               source.data() + source.size(),
                                               comment,
                                               skip_empty_rows)) ||
             skip > 0) {
    begin = find_next_newline(
                source,
                begin,
                "",
                /* skip_empty_rows */ false,
                embedded_nl) +
            1;
    skip = skip > 0 ? skip - 1 : skip;
  }

  return begin;
}

inline bool
matches(const char* start, const char* end, const std::string& needle) {
  if (end <= start || static_cast<unsigned long>(end - start) < needle.size()) {
    return false;
  }
  bool res = strncmp(start, needle.data(), needle.size()) == 0;
  return res;
}

} // namespace vroom
