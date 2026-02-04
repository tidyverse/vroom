// SIMD integer parsing using Google Highway.
//
// This file is included multiple times by simd_atoi.cpp with different
// SIMD targets defined by Highway's foreach_target.h mechanism.

#include "hwy/highway.h"

#include <cstddef>
#include <cstdint>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Parse up to 8 digits using SIMD
// Returns true on success, stores result in 'out'
HWY_NOINLINE bool ParseUint64SimdImpl(const char* p, size_t len, uint64_t& out) {
  if (len == 0 || len > 16) {
    return false;
  }

  // For short strings, scalar is faster due to SIMD setup overhead
  if (len <= 8) {
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

  // For 9-16 digits, use SIMD validation then scalar combination
  // Highway's vector operations for digit validation
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // We need at least 16 lanes for this to be worthwhile
  if (N < 16) {
    // Fall back to scalar for small vectors
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

  // Load bytes (with padding handled by checking only 'len' bytes)
  const auto zero_char = hn::Set(d, static_cast<uint8_t>('0'));
  const auto nine_char = hn::Set(d, static_cast<uint8_t>('9'));

  // Load up to N bytes
  alignas(64) uint8_t buf[64] = {0};
  for (size_t i = 0; i < len && i < 64; ++i) {
    buf[i] = static_cast<uint8_t>(p[i]);
  }

  auto chunk = hn::Load(d, buf);

  // Validate: check each byte is '0'-'9'
  auto lt_zero = hn::Lt(chunk, zero_char);
  auto gt_nine = hn::Gt(chunk, nine_char);
  auto invalid = hn::Or(lt_zero, gt_nine);

  // Check only the first 'len' bytes for validity
  // Use mask bits to check
  alignas(64) uint8_t invalid_bytes[64];
  hn::Store(hn::VecFromMask(d, invalid), d, invalid_bytes);

  for (size_t i = 0; i < len; ++i) {
    if (invalid_bytes[i] != 0) {
      return false;
    }
  }

  // All digits valid - compute result with scalar (fast for 9-16 digits)
  uint64_t result = 0;
  for (size_t i = 0; i < len; ++i) {
    result = result * 10 + (static_cast<unsigned char>(p[i]) - '0');
  }
  out = result;
  return true;
}

// Parse signed int32
HWY_NOINLINE bool ParseInt32SimdImpl(const char* p, size_t len, int32_t& out) {
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
  if (!ParseUint64SimdImpl(p, len, result)) {
    return false;
  }

  if (result > 2147483647ULL && !negative)
    return false;
  if (result > 2147483648ULL && negative)
    return false;

  out = negative ? -static_cast<int32_t>(result) : static_cast<int32_t>(result);
  return true;
}

// Parse signed int64
HWY_NOINLINE bool ParseInt64SimdImpl(const char* p, size_t len, int64_t& out) {
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
  // For lengths > 16, use pure scalar
  if (len > 16) {
    result = 0;
    for (size_t i = 0; i < len; ++i) {
      unsigned char c = static_cast<unsigned char>(p[i]) - '0';
      if (c > 9)
        return false;
      if (result > (18446744073709551615ULL - c) / 10) {
        return false; // Overflow
      }
      result = result * 10 + c;
    }
  } else {
    if (!ParseUint64SimdImpl(p, len, result)) {
      return false;
    }
  }

  if (!negative && result > 9223372036854775807ULL)
    return false;
  if (negative && result > 9223372036854775808ULL)
    return false;

  out = negative ? -static_cast<int64_t>(result) : static_cast<int64_t>(result);
  return true;
}

} // namespace HWY_NAMESPACE
} // namespace simd
} // namespace libvroom
HWY_AFTER_NAMESPACE();
