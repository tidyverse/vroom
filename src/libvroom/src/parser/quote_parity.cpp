// CLMUL-based quote parity implementation using Google Highway.
//
// This file uses Highway's dynamic dispatch to select the optimal
// implementation at runtime based on CPU capabilities:
// - x86 with PCLMULQDQ: Uses _mm_clmulepi64_si128
// - ARM with NEON+AES: Uses vmull_p64
// - Fallback: Software CLMUL via Highway's generic_ops

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "parser/quote_parity-inl.h"
#include "hwy/foreach_target.h"
#include "parser/quote_parity-inl.h"

// Generate dispatch tables and public API (only once)
#if HWY_ONCE

#include "libvroom/quote_parity.h"

namespace libvroom {

// Export implementations for dynamic dispatch
HWY_EXPORT(PrefixXorsumInclusiveImpl);
HWY_EXPORT(FindQuoteMaskImpl);

// Public API: prefix XOR sum using best available SIMD
uint64_t prefix_xorsum_inclusive(uint64_t quote_bits) {
  return HWY_DYNAMIC_DISPATCH(PrefixXorsumInclusiveImpl)(quote_bits);
}

// Public API: find quote mask with state tracking
uint64_t find_quote_mask(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
  return HWY_DYNAMIC_DISPATCH(FindQuoteMaskImpl)(quote_bits, prev_iter_inside_quote);
}

// Portable fallback using the doubling trick from Polars.
// Computes prefix XOR in O(log n) operations instead of O(n).
uint64_t portable_prefix_xorsum_inclusive(uint64_t x) {
  // After iteration i, each bit knows XOR of itself and 2^i predecessors
  // Iteration 0: shift 1, each bit XORs with 1 predecessor
  // Iteration 1: shift 2, each bit XORs with 3 predecessors
  // Iteration 2: shift 4, each bit XORs with 7 predecessors
  // ... until all 64 bits covered
  for (int i = 0; i < 6; i++) {
    x ^= x << (1 << i); // shifts: 1, 2, 4, 8, 16, 32
  }
  return x;
}

// Scalar reference implementation for testing.
// Simple O(n) loop that tracks quote state bit-by-bit.
uint64_t scalar_find_quote_mask(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
  uint64_t quote_mask = 0;
  uint64_t state = prev_iter_inside_quote & 1;

  for (size_t i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1; // Toggle state on quote character
    }
    quote_mask |= (state << i); // Record state at each position
  }

  return quote_mask;
}

} // namespace libvroom

#endif // HWY_ONCE
