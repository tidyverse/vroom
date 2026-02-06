// CLMUL-based quote parity implementation using Google Highway.
//
// This file is included multiple times by quote_parity.cpp with different
// SIMD targets defined by Highway's foreach_target.h mechanism.

#include "hwy/highway.h"

#include <cstdint>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Portable prefix XOR sum using the doubling trick from Polars.
// Used as fallback when CLMUL is not available.
HWY_INLINE uint64_t PortablePrefixXorsum(uint64_t x) {
  for (int i = 0; i < 6; i++) {
    x ^= x << (1 << i); // shifts: 1, 2, 4, 8, 16, 32
  }
  return x;
}

// Compute prefix XOR sum.
// Uses CLMUL when available, falls back to portable implementation otherwise.
HWY_NOINLINE uint64_t PrefixXorsumInclusiveImpl(uint64_t quote_bits) {
#if HWY_TARGET == HWY_EMU128 || HWY_TARGET == HWY_SCALAR
  // EMU128 and SCALAR targets don't have CLMUL, use portable fallback
  return PortablePrefixXorsum(quote_bits);
#else
  // Use CLMUL for fast prefix XOR computation
  // clmul(x, 0xFFFFFFFFFFFFFFFF) computes the inclusive prefix XOR
  const hn::FixedTag<uint64_t, 2> d;
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~uint64_t{0});
  auto result = hn::CLMulLower(quote_vec, all_ones);
  return hn::GetLane(result);
#endif
}

// Find quote mask with state tracking for multi-block processing.
HWY_NOINLINE uint64_t FindQuoteMaskImpl(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  uint64_t mask = PrefixXorsumInclusiveImpl(quote_bits);

  // XOR with previous state to continue quote tracking across blocks
  mask ^= prev_iter_inside_quote;

  // Update state: sign-extend bit 63 to get 0 or ~0ULL
  // If MSB is 1, we ended inside a quote (propagate all 1s)
  // If MSB is 0, we ended outside a quote (propagate all 0s)
  prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(mask) >> 63);

  return mask;
}

} // namespace HWY_NAMESPACE
} // namespace libvroom
HWY_AFTER_NAMESPACE();
