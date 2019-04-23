#pragma once

#include <array>
#include <sstream>

namespace vroom {

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
static size_t
find_next_newline(const T& source, size_t start, bool embedded_nl = true) {
  if (embedded_nl) {
    return find_next_non_quoted_newline(source, start);
  }

  auto begin = source.data() + start;

  if (start >= source.size()) {
    return source.size() - 1;
  }

  auto res =
      static_cast<const char*>(memchr(begin, '\n', source.size() - start));
  if (!res) {
    return source.size() - 1;
  }
  return res - source.data();
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

inline bool is_space(const char* c) { return *c == ' ' || *c == '\t'; }

inline void trim_whitespace(const char*& begin, const char*& end) {
  while (begin != end && is_space(begin)) {
    ++begin;
  }

  while (end != begin && is_space((end - 1))) {
    --end;
  }
}

inline bool is_blank_or_comment_line(const char* begin, const char comment) {
  if (*begin == '\n') {
    return true;
  }

  while (*begin == ' ' || *begin == '\t') {
    ++begin;
  }

  if (*begin == '\n' || *begin == comment) {
    return true;
  }

  return false;
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
size_t find_first_line(const T& source, size_t skip, const char comment) {

  auto begin = skip_bom(source);
  /* Skip skip parameters, comments and blank lines */

  while (bool should_skip =
             (begin < source.size() - 1 &&
              is_blank_or_comment_line(source.data() + begin, comment)) ||
             skip > 0) {
    begin = find_next_newline(source, begin) + 1;
    skip = skip > 0 ? skip - 1 : skip;
  }

  return begin;
}

} // namespace vroom
