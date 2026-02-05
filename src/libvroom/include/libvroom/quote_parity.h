#pragma once

#include <cstdint>

namespace libvroom {

// Compute prefix XOR sum using CLMUL instruction if available.
// Each bit i in result = XOR of quote_bits[0..=i]
// Returns mask where bit is 1 if position is inside a quoted field.
//
// Example:
//   quote_bits = 0b00100100  // quotes at positions 2 and 5
//   result     = 0b00111100  // positions 2-5 are inside quotes
uint64_t prefix_xorsum_inclusive(uint64_t quote_bits);

// Portable fallback implementation using the doubling trick.
// Uses 6 shift-XOR operations instead of a single CLMUL instruction.
// Should produce identical results to prefix_xorsum_inclusive().
uint64_t portable_prefix_xorsum_inclusive(uint64_t quote_bits);

// Find quote mask with iteration state tracking for multi-block processing.
// Returns mask of positions inside quotes (1 = inside, 0 = outside).
// Updates prev_iter_inside_quote for continuation across 64-byte chunks:
//   - Pass 0 initially (starting outside quotes)
//   - Pass ~0ULL if starting inside a quote
//   - After call, prev_iter_inside_quote is 0 or ~0ULL based on end state
uint64_t find_quote_mask(uint64_t quote_bits, uint64_t& prev_iter_inside_quote);

// Scalar version for comparison and correctness testing.
// Uses a simple loop to compute quote parity bit-by-bit.
// Note: Only uses LSB of prev_iter_inside_quote (0 = outside, 1 = inside).
// For consistency, pass 0 or ~0ULL as with find_quote_mask().
uint64_t scalar_find_quote_mask(uint64_t quote_bits, uint64_t prev_iter_inside_quote);

} // namespace libvroom
