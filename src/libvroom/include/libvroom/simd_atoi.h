#pragma once

// SIMD-accelerated integer parsing using Google Highway
// Provides runtime dispatch to optimal SIMD implementation

#include <cstddef>
#include <cstdint>

namespace libvroom {
namespace simd {

// Highway-based implementations (defined in src/parser/simd_atoi.cpp)
bool parse_uint64_highway(const char* p, size_t len, uint64_t& out);
bool parse_int32_highway(const char* p, size_t len, int32_t& out);
bool parse_int64_highway(const char* p, size_t len, int64_t& out);

// Scalar implementations for reference/testing
inline bool parse_uint64_scalar(const char* p, size_t len, uint64_t& out) {
  if (len == 0 || len > 20)
    return false;
  uint64_t result = 0;
  for (size_t i = 0; i < len; ++i) {
    unsigned char c = static_cast<unsigned char>(p[i]) - '0';
    if (c > 9)
      return false;
    result = result * 10 + c;
  }
  out = result;
  return true;
}

inline bool parse_int32_scalar(const char* p, size_t len, int32_t& out) {
  if (len == 0 || len > 11)
    return false;

  bool negative = false;
  if (*p == '-') {
    negative = true;
    p++;
    len--;
    if (len == 0)
      return false;
  } else if (*p == '+') {
    p++;
    len--;
    if (len == 0)
      return false;
  }

  uint64_t result;
  if (!parse_uint64_scalar(p, len, result))
    return false;

  if (result > 2147483647ULL && !negative)
    return false;
  if (result > 2147483648ULL && negative)
    return false;

  out = negative ? -static_cast<int32_t>(result) : static_cast<int32_t>(result);
  return true;
}

inline bool parse_int64_scalar(const char* p, size_t len, int64_t& out) {
  if (len == 0 || len > 20)
    return false;

  bool negative = false;
  if (*p == '-') {
    negative = true;
    p++;
    len--;
    if (len == 0)
      return false;
  } else if (*p == '+') {
    p++;
    len--;
    if (len == 0)
      return false;
  }

  uint64_t result;
  if (!parse_uint64_scalar(p, len, result))
    return false;

  if (!negative && result > 9223372036854775807ULL)
    return false;
  if (negative && result > 9223372036854775808ULL)
    return false;

  out = negative ? -static_cast<int64_t>(result) : static_cast<int64_t>(result);
  return true;
}

// Main API - uses Highway with runtime dispatch
inline bool parse_int32_simd(const char* p, size_t len, int32_t& out) {
  return parse_int32_highway(p, len, out);
}

inline bool parse_int64_simd(const char* p, size_t len, int64_t& out) {
  return parse_int64_highway(p, len, out);
}

} // namespace simd
} // namespace libvroom
