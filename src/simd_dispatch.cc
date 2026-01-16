// vroom SIMD dispatch implementation using Highway's dynamic dispatch mechanism.
// This file compiles the SIMD functions for each target and provides
// runtime dispatch wrappers that select the optimal implementation.
//
// Provides SIMD primitives for:
// 1. Whitespace trimming (TrimWhitespaceSIMD)
// 2. CSV indexing (find_quote_mask, compute_line_ending_mask)

// For dynamic dispatch, the file re-includes itself via foreach_target.h
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "simd_dispatch.cc"
#include "hwy/foreach_target.h" // IWYU pragma: keep

// Must come after foreach_target.h
#include "hwy/highway.h"
#include "simd_dispatch.h"

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>

HWY_BEFORE_NAMESPACE();
namespace vroom {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// =============================================================================
// SIMD Primitives - compiled once per target for optimal performance
// =============================================================================

/**
 * @brief Compare each byte of a 64-byte block against a match value.
 */
uint64_t CmpMaskAgainstInputImpl(const uint8_t* data, uint8_t m) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto match_value = hn::Set(d, m);

  uint64_t result = 0;

  size_t i = 0;
  for (; i + N <= 64; i += N) {
    auto vec = hn::LoadU(d, data + i);
    auto mask = hn::Eq(vec, match_value);
    uint64_t bits = hn::BitsFromMask(d, mask);
    result |= (bits << i);
  }

  // Scalar fallback for non-64-byte aligned SIMD widths
  for (; i < 64; ++i) {
    if (data[i] == m) {
      result |= (1ULL << i);
    }
  }

  return result;
}

/**
 * @brief Create a mask for whitespace characters using SIMD.
 * Whitespace is: space ' ', tab '\t', carriage return '\r', null '\0'
 */
uint64_t WhitespaceMaskImpl(const uint8_t* data) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto space = hn::Set(d, static_cast<uint8_t>(' '));
  const auto tab = hn::Set(d, static_cast<uint8_t>('\t'));
  const auto cr = hn::Set(d, static_cast<uint8_t>('\r'));
  const auto null = hn::Set(d, static_cast<uint8_t>('\0'));

  uint64_t result = 0;

  size_t i = 0;
  for (; i + N <= 64; i += N) {
    auto vec = hn::LoadU(d, data + i);

    // Check for each whitespace character and OR the results
    auto is_space = hn::Eq(vec, space);
    auto is_tab = hn::Eq(vec, tab);
    auto is_cr = hn::Eq(vec, cr);
    auto is_null = hn::Eq(vec, null);

    auto is_ws = hn::Or(hn::Or(is_space, is_tab), hn::Or(is_cr, is_null));

    uint64_t bits = hn::BitsFromMask(d, is_ws);
    result |= (bits << i);
  }

  // Scalar fallback for remaining bytes
  for (; i < 64; ++i) {
    uint8_t c = data[i];
    if (c == ' ' || c == '\t' || c == '\r' || c == '\0') {
      result |= (1ULL << i);
    }
  }

  return result;
}

/**
 * @brief Find leading whitespace using SIMD.
 * Returns the number of leading whitespace bytes.
 */
size_t FindLeadingWhitespaceImpl(const uint8_t* data, size_t len) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto space = hn::Set(d, static_cast<uint8_t>(' '));
  const auto tab = hn::Set(d, static_cast<uint8_t>('\t'));
  const auto cr = hn::Set(d, static_cast<uint8_t>('\r'));
  const auto null = hn::Set(d, static_cast<uint8_t>('\0'));

  size_t pos = 0;

  // Process full vectors
  while (pos + N <= len) {
    auto vec = hn::LoadU(d, data + pos);

    // Check for whitespace characters
    auto is_space = hn::Eq(vec, space);
    auto is_tab = hn::Eq(vec, tab);
    auto is_cr = hn::Eq(vec, cr);
    auto is_null = hn::Eq(vec, null);

    auto is_ws = hn::Or(hn::Or(is_space, is_tab), hn::Or(is_cr, is_null));

    // If not all whitespace, find the first non-whitespace
    if (!hn::AllTrue(d, is_ws)) {
      // Get mask of non-whitespace
      auto not_ws = hn::Not(is_ws);
      uint64_t bits = hn::BitsFromMask(d, not_ws);
      // Find first set bit (first non-whitespace)
      if (bits != 0) {
        return pos + __builtin_ctzll(bits);
      }
    }

    pos += N;
  }

  // Scalar fallback for remaining bytes
  while (pos < len) {
    uint8_t c = data[pos];
    if (c != ' ' && c != '\t' && c != '\r' && c != '\0') {
      return pos;
    }
    ++pos;
  }

  return len; // All whitespace
}

/**
 * @brief Find trailing whitespace using SIMD.
 * Returns the position after the last non-whitespace character.
 */
size_t FindTrailingNonWhitespaceImpl(const uint8_t* data, size_t len) {
  if (len == 0) return 0;

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto space = hn::Set(d, static_cast<uint8_t>(' '));
  const auto tab = hn::Set(d, static_cast<uint8_t>('\t'));
  const auto cr = hn::Set(d, static_cast<uint8_t>('\r'));
  const auto null = hn::Set(d, static_cast<uint8_t>('\0'));

  // Start from the end
  size_t pos = len;

  // Process full vectors from the end
  while (pos >= N) {
    auto vec = hn::LoadU(d, data + pos - N);

    // Check for whitespace characters
    auto is_space = hn::Eq(vec, space);
    auto is_tab = hn::Eq(vec, tab);
    auto is_cr = hn::Eq(vec, cr);
    auto is_null = hn::Eq(vec, null);

    auto is_ws = hn::Or(hn::Or(is_space, is_tab), hn::Or(is_cr, is_null));

    // If not all whitespace, find the last non-whitespace
    if (!hn::AllTrue(d, is_ws)) {
      // Get mask of non-whitespace
      auto not_ws = hn::Not(is_ws);
      uint64_t bits = hn::BitsFromMask(d, not_ws);
      // Find last set bit (last non-whitespace in this block)
      if (bits != 0) {
        int last_non_ws = 63 - __builtin_clzll(bits);
        return pos - N + last_non_ws + 1;
      }
    }

    pos -= N;
  }

  // Scalar fallback for remaining bytes at the beginning
  while (pos > 0) {
    uint8_t c = data[pos - 1];
    if (c != ' ' && c != '\t' && c != '\r' && c != '\0') {
      return pos;
    }
    --pos;
  }

  return 0; // All whitespace
}

// =============================================================================
// CSV Indexing SIMD Primitives
// =============================================================================

/**
 * @brief Find quote mask using carryless multiplication.
 *
 * Uses parallel prefix XOR to compute which positions are inside quoted fields.
 * The CLMul instruction (available on x86 with PCLMULQDQ, ARM with PMULL)
 * computes prefix XOR in constant time.
 */
uint64_t FindQuoteMaskImpl(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
  const hn::FixedTag<uint64_t, 2> d;

  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  // CLMulLower computes the prefix XOR (carryless multiplication by all 1s)
  auto result = hn::CLMulLower(quote_vec, all_ones);

  uint64_t quote_mask = hn::GetLane(result);
  quote_mask ^= prev_iter_inside_quote;

  return quote_mask;
}

/**
 * @brief Find quote mask with state update for next iteration.
 */
uint64_t FindQuoteMask2Impl(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  const hn::FixedTag<uint64_t, 2> d;

  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  auto result = hn::CLMulLower(quote_vec, all_ones);

  uint64_t quote_mask = hn::GetLane(result);
  quote_mask ^= prev_iter_inside_quote;

  // Update state: if MSB is set, we're inside a quote at the end of this block
  prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);

  return quote_mask;
}

/**
 * @brief Compute line ending mask supporting LF, CRLF, and CR-only.
 */
uint64_t ComputeLineEndingMaskImpl(const uint8_t* data, uint64_t mask) {
  uint64_t lf_mask = CmpMaskAgainstInputImpl(data, '\n') & mask;
  uint64_t cr_mask = CmpMaskAgainstInputImpl(data, '\r') & mask;

  // CR followed by LF within this block - don't count CR as line ending
  uint64_t crlf_cr_mask = cr_mask & (lf_mask >> 1);
  uint64_t standalone_cr = cr_mask & ~crlf_cr_mask;

  return lf_mask | standalone_cr;
}

} // namespace HWY_NAMESPACE
} // namespace simd
} // namespace vroom
HWY_AFTER_NAMESPACE();

// =============================================================================
// The dispatch table and wrapper functions - compiled only once
// =============================================================================
#if HWY_ONCE

namespace vroom {
namespace simd {

// Export creates the dispatch tables
HWY_EXPORT(CmpMaskAgainstInputImpl);
HWY_EXPORT(WhitespaceMaskImpl);
HWY_EXPORT(FindLeadingWhitespaceImpl);
HWY_EXPORT(FindTrailingNonWhitespaceImpl);
HWY_EXPORT(FindQuoteMaskImpl);
HWY_EXPORT(FindQuoteMask2Impl);
HWY_EXPORT(ComputeLineEndingMaskImpl);

// Public dispatch wrappers
uint64_t CmpMaskAgainstInput(const uint8_t* data, uint8_t m) {
  return HWY_DYNAMIC_DISPATCH(CmpMaskAgainstInputImpl)(data, m);
}

uint64_t WhitespaceMask(const uint8_t* data) {
  return HWY_DYNAMIC_DISPATCH(WhitespaceMaskImpl)(data);
}

uint64_t FindQuoteMask(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
  return HWY_DYNAMIC_DISPATCH(FindQuoteMaskImpl)(quote_bits, prev_iter_inside_quote);
}

uint64_t FindQuoteMask2(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  return HWY_DYNAMIC_DISPATCH(FindQuoteMask2Impl)(quote_bits, prev_iter_inside_quote);
}

uint64_t ComputeLineEndingMask(const uint8_t* data, uint64_t mask) {
  return HWY_DYNAMIC_DISPATCH(ComputeLineEndingMaskImpl)(data, mask);
}

const char* TrimWhitespaceBeginSIMD(const char* begin, const char* end) {
  if (begin >= end) return end;

  size_t len = static_cast<size_t>(end - begin);
  size_t skip = HWY_DYNAMIC_DISPATCH(FindLeadingWhitespaceImpl)(
      reinterpret_cast<const uint8_t*>(begin), len);
  return begin + skip;
}

const char* TrimWhitespaceEndSIMD(const char* begin, const char* end) {
  if (begin >= end) return begin;

  size_t len = static_cast<size_t>(end - begin);
  size_t new_end = HWY_DYNAMIC_DISPATCH(FindTrailingNonWhitespaceImpl)(
      reinterpret_cast<const uint8_t*>(begin), len);
  return begin + new_end;
}

void TrimWhitespaceSIMD(const char*& begin, const char*& end) {
  begin = TrimWhitespaceBeginSIMD(begin, end);
  end = TrimWhitespaceEndSIMD(begin, end);
}

int64_t GetSupportedTargets() {
  return hwy::SupportedTargets();
}

int64_t GetChosenTarget() {
  int64_t supported = hwy::SupportedTargets();
  if (supported == 0)
    return 0;
  // Find highest bit set (best target)
  int64_t highest = 0;
  for (int64_t bit = 1LL << 62; bit > 0; bit >>= 1) {
    if (supported & bit) {
      highest = bit;
      break;
    }
  }
  return highest;
}

const char* GetTargetName(int64_t target) {
  return hwy::TargetName(target);
}

} // namespace simd
} // namespace vroom

#endif // HWY_ONCE
