// Copyright 2024 libvroom Authors
// SPDX-License-Identifier: MIT
//
// SIMD dispatch implementation using Highway's dynamic dispatch mechanism.
// This file compiles the SIMD functions for each target and provides
// runtime dispatch wrappers that select the optimal implementation.

// For dynamic dispatch, the file re-includes itself via foreach_target.h
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "src/simd_dispatch.cpp"
#include "hwy/foreach_target.h" // IWYU pragma: keep

// Must come after foreach_target.h
#include "common_defs.h"
#include "hwy/highway.h"

#include <cstdint>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
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
 * @brief Find quote mask using carryless multiplication.
 */
uint64_t FindQuoteMaskImpl(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
  const hn::FixedTag<uint64_t, 2> d;

  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  auto result = hn::CLMulLower(quote_vec, all_ones);

  uint64_t quote_mask = hn::GetLane(result);
  quote_mask ^= prev_iter_inside_quote;

  return quote_mask;
}

/**
 * @brief Find quote mask with state tracking.
 */
uint64_t FindQuoteMask2Impl(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  const hn::FixedTag<uint64_t, 2> d;

  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  auto result = hn::CLMulLower(quote_vec, all_ones);

  uint64_t quote_mask = hn::GetLane(result);
  quote_mask ^= prev_iter_inside_quote;

  prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);

  return quote_mask;
}

/**
 * @brief Compute line ending mask (simple version).
 */
uint64_t ComputeLineEndingMaskSimpleImpl(const uint8_t* data, uint64_t mask) {
  uint64_t lf_mask = CmpMaskAgainstInputImpl(data, '\n') & mask;
  uint64_t cr_mask = CmpMaskAgainstInputImpl(data, '\r') & mask;

  uint64_t crlf_cr_mask = cr_mask & (lf_mask >> 1);
  uint64_t standalone_cr = cr_mask & ~crlf_cr_mask;

  return lf_mask | standalone_cr;
}

/**
 * @brief Compute line ending mask with cross-block CRLF tracking.
 */
uint64_t ComputeLineEndingMaskImpl(const uint8_t* data, uint64_t mask, bool& prev_ended_with_cr,
                                   bool prev_block_ended_cr) {
  uint64_t lf_mask = CmpMaskAgainstInputImpl(data, '\n') & mask;
  uint64_t cr_mask = CmpMaskAgainstInputImpl(data, '\r') & mask;

  uint64_t crlf_cr_mask = cr_mask & (lf_mask >> 1);
  uint64_t standalone_cr = cr_mask & ~crlf_cr_mask;

  prev_ended_with_cr = (cr_mask & (1ULL << 63)) != 0;

  // Suppress unused parameter warning
  (void)prev_block_ended_cr;

  return lf_mask | standalone_cr;
}

/**
 * @brief Compute escaped character mask.
 */
uint64_t ComputeEscapedMaskImpl(uint64_t escape_mask, uint64_t& prev_escape_carry) {
  if (escape_mask == 0 && prev_escape_carry == 0) {
    return 0;
  }

  uint64_t escaped = 0;
  bool in_escape = prev_escape_carry != 0;

  for (int i = 0; i < 64; ++i) {
    uint64_t bit = 1ULL << i;
    if (in_escape) {
      escaped |= bit;
      in_escape = false;
    } else if (escape_mask & bit) {
      in_escape = true;
    }
  }

  prev_escape_carry = in_escape ? 1 : 0;

  return escaped;
}

} // namespace HWY_NAMESPACE
} // namespace libvroom
HWY_AFTER_NAMESPACE();

// =============================================================================
// The dispatch table and wrapper functions - compiled only once
// =============================================================================
#if HWY_ONCE

namespace libvroom {

// Export creates the dispatch tables
HWY_EXPORT(CmpMaskAgainstInputImpl);
HWY_EXPORT(FindQuoteMaskImpl);
HWY_EXPORT(FindQuoteMask2Impl);
HWY_EXPORT(ComputeLineEndingMaskSimpleImpl);
HWY_EXPORT(ComputeLineEndingMaskImpl);
HWY_EXPORT(ComputeEscapedMaskImpl);

// Public dispatch wrappers
uint64_t DispatchCmpMaskAgainstInput(const uint8_t* data, uint8_t m) {
  return HWY_DYNAMIC_DISPATCH(CmpMaskAgainstInputImpl)(data, m);
}

uint64_t DispatchFindQuoteMask(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
  return HWY_DYNAMIC_DISPATCH(FindQuoteMaskImpl)(quote_bits, prev_iter_inside_quote);
}

uint64_t DispatchFindQuoteMask2(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  return HWY_DYNAMIC_DISPATCH(FindQuoteMask2Impl)(quote_bits, prev_iter_inside_quote);
}

uint64_t DispatchComputeLineEndingMaskSimple(const uint8_t* data, uint64_t mask) {
  return HWY_DYNAMIC_DISPATCH(ComputeLineEndingMaskSimpleImpl)(data, mask);
}

uint64_t DispatchComputeLineEndingMask(const uint8_t* data, uint64_t mask, bool& prev_ended_with_cr,
                                       bool prev_block_ended_cr) {
  return HWY_DYNAMIC_DISPATCH(ComputeLineEndingMaskImpl)(data, mask, prev_ended_with_cr,
                                                         prev_block_ended_cr);
}

uint64_t DispatchComputeEscapedMask(uint64_t escape_mask, uint64_t& prev_escape_carry) {
  return HWY_DYNAMIC_DISPATCH(ComputeEscapedMaskImpl)(escape_mask, prev_escape_carry);
}

// Note: We don't use static initialization for CPU detection because it can
// cause crashes when the library is loaded (e.g., in Python bindings).
// Highway's HWY_DYNAMIC_DISPATCH handles lazy initialization automatically.

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

} // namespace libvroom

#endif // HWY_ONCE
