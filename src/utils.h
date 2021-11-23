#pragma once

#include <array>
#include <cpp11/R.hpp>
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

inline std::pair<bool, bool> is_blank_or_comment_line(
    const char* begin,
    const char* end,
    const std::string& comment,
    const bool skip_empty_rows) {
  bool should_skip = false;
  bool is_comment = false;

  if (!skip_empty_rows && comment.empty()) {
    return std::pair<bool, bool>(should_skip, is_comment);
  }

  if (skip_empty_rows && (*begin == '\n' || *begin == '\r')) {
    should_skip = true;
    return std::pair<bool, bool>(should_skip, is_comment);
  }

  while (begin < end && (*begin == ' ' || *begin == '\t')) {
    ++begin;
  }

  if (skip_empty_rows && (*begin == '\n' || *begin == '\r')) {
    should_skip = true;
    return std::pair<bool, bool>(should_skip, is_comment);
  }

  if (!comment.empty() && strncmp(begin, comment.data(), comment.size()) == 0) {
    should_skip = true;
    is_comment = true;
    return std::pair<bool, bool>(should_skip, is_comment);
  }

  return std::pair<bool, bool>(should_skip, is_comment);
}

inline bool is_crlf(const char* buf, size_t pos, size_t end) {
  return buf[pos] == '\r' && pos + 1 < end && buf[pos + 1] == '\n';
}

enum newline_type {
  CR /* linux */,
  CRLF /* windows */,
  LF /* old macOS */,
  NA /* unknown */
};

template <typename T>
static std::pair<size_t, newline_type>
find_next_non_quoted_newline(const T& source, size_t start, const char quote) {
  if (start > source.size() - 1) {
    return {source.size() - 1, NA};
  }

  std::array<char, 4> query = {'\r', '\n', quote, '\0'};

  auto buf = source.data();
  size_t pos = start;

  size_t end = source.size() - 1;
  bool in_quote = false;

  while (pos < end) {
    size_t buf_offset = strcspn(buf + pos, query.data());
    pos = pos + buf_offset;
    auto c = buf[pos];
    if (c == '\n' || c == '\r') {
      if (in_quote) {
        ++pos;
        continue;
      }
      if (c == '\n') {
        return {pos, LF};
      }

      if (is_crlf(buf, pos, end)) {
        return {pos + 1, CRLF};
      }
      return {pos, CR};
    }

    else if (c == quote) {
      in_quote = !in_quote;
    }
    ++pos;
  }

  if (pos > end) {
    return {end, NA};
  }

  return {pos, NA};
}

template <typename T>
static std::pair<size_t, newline_type> find_next_newline(
    const T& source,
    size_t start,
    const std::string& comment,
    const bool skip_empty_rows,
    bool embedded_nl,
    const char quote,
    newline_type type = NA) {

  if (start >= source.size()) {
    return {source.size() - 1, NA};
  }

  if (embedded_nl) {
    size_t value;
    newline_type nl;
    std::tie(value, nl) = find_next_non_quoted_newline(source, start, quote);
    // REprintf("%i\n", value);
    return {value, nl};
  }

  const char* begin = source.data() + start;

  const char* end = source.data() + source.size();

  std::array<char, 3> query;
  switch (type) {
  case NA:
    query = {'\n', '\r', '\0'};
    break;
  case CR:
    query = {'\r', '\0'};
    break;
  case CRLF:
  case LF:
    query = {'\n', '\0'};
    break;
  }
  bool should_skip;
  while (begin && begin < end) {
    size_t offset = strcspn(begin, query.data());
    // REprintf("%i\n", offset);
    begin += offset;
    break;
    if (!(begin && begin + 1 < end)) {
      std::tie(should_skip, std::ignore) =
          is_blank_or_comment_line(begin + 1, end, comment, skip_empty_rows);
      if (should_skip) {
        break;
      }
    }
  }

  if (!begin) {
    return {source.size() - 1, NA};
  }

  size_t pos = begin - source.data();
  if (begin[0] == '\n') {
    return {pos, LF};
  }

  if (begin[0] == '\r') {
    if (is_crlf(source.data(), pos, end - source.data())) {
      return {pos + 1, CRLF};
    }
    return {pos, CR};
  }

  return {pos, NA};
}

template <typename T> T get_env(const char* name, T default_value) {

  char* p;

  p = getenv(name);
  if (!p || strlen(p) == 0) {
    return default_value;
  }

  std::stringstream ss(p);
  double out;
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
    const bool embedded_nl,
    const char quote) {

  auto begin = skip_bom(source);
  /* Skip skip parameters, comments and blank lines */

  bool should_skip, is_comment;
  std::tie(should_skip, is_comment) = is_blank_or_comment_line(
      source.data() + begin,
      source.data() + source.size(),
      comment,
      skip_empty_rows);
  while (begin < source.size() - 1 && (should_skip || skip > 0)) {
    std::tie(begin, std::ignore) = find_next_newline(
        source,
        begin,
        "",
        /* skip_empty_rows */ false,
        embedded_nl,
        is_comment ? '\0' : quote); /* don't deal with quotes in comment lines*/
    ++begin;
    skip = skip > 0 ? skip - 1 : skip;

    std::tie(should_skip, is_comment) = is_blank_or_comment_line(
        source.data() + begin,
        source.data() + source.size(),
        comment,
        skip_empty_rows);
  }

  return begin;
}

inline bool
matches(const char* start, const char* end, const std::string& needle) {
  if (end <= start || needle.empty() ||
      static_cast<unsigned long>(end - start) < needle.size()) {
    return false;
  }
  bool res = strncmp(start, needle.data(), needle.size()) == 0;
  return res;
}

inline bool has_expected_line_ending(newline_type nl, const char value) {
  if (nl == CR && value == '\r') {
    return true;
  }
  return value == '\n';
}

} // namespace vroom
