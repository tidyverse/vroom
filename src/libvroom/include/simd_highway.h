#ifndef SIMD_HIGHWAY_H
#define SIMD_HIGHWAY_H

// Portable SIMD implementation using Google Highway
// Replaces x86-specific AVX2 intrinsics with cross-platform Highway API
//
// Dynamic Dispatch (default):
//   The SIMD functions use runtime CPU detection to select the optimal
//   implementation (AVX2, AVX-512, SSE4, NEON, etc.). This allows a single
//   binary to work across different CPUs.
//
// Static Dispatch (HWY_COMPILE_ONLY_STATIC):
//   Define HWY_COMPILE_ONLY_STATIC before including this header or pass
//   -DHWY_COMPILE_ONLY_STATIC=ON to CMake to use compile-time SIMD selection.
//   This provides slightly better inlining but requires compiling for the
//   target CPU.

#include "common_defs.h"

#include <cstdint>
#include <cstring>

// MSVC intrinsics for bit manipulation
#ifdef _MSC_VER
#include <intrin.h>
#pragma intrinsic(_BitScanForward64)
#pragma intrinsic(__popcnt64)
#endif

// Include Highway for portable SIMD
#undef HWY_TARGET_INCLUDE
#include "hwy/highway.h"

// Include dispatch declarations for runtime SIMD selection
#ifndef HWY_COMPILE_ONLY_STATIC
#include "simd_dispatch.h"
#endif

namespace libvroom {

// Namespace alias for Highway operations
namespace hn = hwy::HWY_NAMESPACE;

// SIMD input structure - holds 64 bytes of data
struct simd_input {
  alignas(64) uint8_t data[64];

  static simd_input load(const uint8_t* ptr) {
    simd_input in;
    std::memcpy(in.data, ptr, 64);
    return in;
  }
};

// Bit manipulation functions (portable, no SIMD needed)
really_inline uint64_t clear_lowest_bit(uint64_t input_num) {
  return input_num & (input_num - 1);
}

really_inline uint64_t blsmsk_u64(uint64_t input_num) {
  return input_num ^ (input_num - 1);
}

really_inline int trailing_zeroes(uint64_t input_num) {
  if (input_num == 0)
    return 64;
#ifdef _MSC_VER
  unsigned long index;
  _BitScanForward64(&index, input_num);
  return static_cast<int>(index);
#else
  return __builtin_ctzll(input_num);
#endif
}

really_inline long long int count_ones(uint64_t input_num) {
#ifdef _MSC_VER
  return static_cast<long long int>(__popcnt64(input_num));
#else
  return __builtin_popcountll(input_num);
#endif
}

// Fill SIMD input from memory (caller must ensure at least 64 bytes are readable)
really_inline simd_input fill_input(const uint8_t* ptr) {
  return simd_input::load(ptr);
}

// Fill SIMD input from memory with bounds checking for partial final blocks.
// Only reads 'remaining' bytes from ptr, padding the rest with zeros.
// This avoids out-of-bounds reads when processing the final block of a buffer.
really_inline simd_input fill_input_safe(const uint8_t* ptr, size_t remaining) {
  simd_input in;
  if (remaining >= 64) {
    std::memcpy(in.data, ptr, 64);
  } else {
    // Zero-initialize first, then copy only the valid bytes
    std::memset(in.data, 0, 64);
    std::memcpy(in.data, ptr, remaining);
  }
  return in;
}

// Compare mask against input using Highway SIMD
// Pass by const reference to avoid ABI issues with 64-byte alignment on ARM
really_inline uint64_t cmp_mask_against_input(const simd_input& in, uint8_t m) {
#ifdef HWY_COMPILE_ONLY_STATIC
  // Static dispatch: use inline SIMD code compiled for the build machine
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // Set comparison value in all lanes
  const auto match_value = hn::Set(d, m);

  uint64_t result = 0;

  // Process data in Highway vector-sized chunks
  size_t i = 0;
  for (; i + N <= 64; i += N) {
    auto vec = hn::LoadU(d, in.data + i);
    auto mask = hn::Eq(vec, match_value);
    uint64_t bits = hn::BitsFromMask(d, mask);
    result |= (bits << i);
  }

  // LCOV_EXCL_START - scalar fallback for non-64-byte aligned SIMD widths
  // Handle remaining bytes with scalar code
  for (; i < 64; ++i) {
    if (in.data[i] == m) {
      result |= (1ULL << i);
    }
  }
  // LCOV_EXCL_STOP

  return result;
#else
  // Dynamic dispatch: select optimal SIMD at runtime
  return DispatchCmpMaskAgainstInput(in.data, m);
#endif
}

// Find quote mask using carryless multiplication (PCLMULQDQ on x86, PMULL on ARM).
// This computes a parallel prefix XOR over quote bit positions in constant time,
// replacing a O(64) scalar loop with a single SIMD instruction (~28x speedup).
really_inline uint64_t find_quote_mask(uint64_t quote_bits, uint64_t prev_iter_inside_quote) {
#ifdef HWY_COMPILE_ONLY_STATIC
  // Use Highway's portable CLMul which maps to:
  // - x86: PCLMULQDQ instruction
  // - ARM: PMULL instruction
  // - Other: Software fallback
  const hn::FixedTag<uint64_t, 2> d; // 128-bit vector of uint64_t

  // Load quote_bits into the lower 64 bits of a 128-bit vector
  // Multiplying by 0xFF...FF computes the prefix XOR (each bit becomes the XOR
  // of all bits at lower positions)
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  // CLMulLower multiplies the lower 64 bits of each 128-bit block
  // The result's lower 64 bits contain the prefix XOR we need
  auto result = hn::CLMulLower(quote_vec, all_ones);

  // Extract the lower 64-bit result
  uint64_t quote_mask = hn::GetLane(result);

  // XOR with previous iteration state to handle quotes spanning chunks
  quote_mask ^= prev_iter_inside_quote;

  return quote_mask;
#else
  return DispatchFindQuoteMask(quote_bits, prev_iter_inside_quote);
#endif
}

// Find quote mask with state tracking using carryless multiplication
// This version updates prev_iter_inside_quote for the next iteration
really_inline uint64_t find_quote_mask2(uint64_t quote_bits, uint64_t& prev_iter_inside_quote) {
#ifdef HWY_COMPILE_ONLY_STATIC
  // Use Highway's portable CLMul which maps to:
  // - x86: PCLMULQDQ instruction
  // - ARM: PMULL instruction
  // - Other: Software fallback
  const hn::FixedTag<uint64_t, 2> d; // 128-bit vector of uint64_t

  // Load quote_bits into the lower 64 bits of a 128-bit vector
  // Multiplying by 0xFF...FF computes the prefix XOR
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  // CLMulLower multiplies the lower 64 bits of each 128-bit block
  auto result = hn::CLMulLower(quote_vec, all_ones);

  // Extract the lower 64-bit result
  uint64_t quote_mask = hn::GetLane(result);

  // XOR with previous iteration state to handle quotes spanning chunks
  quote_mask ^= prev_iter_inside_quote;

  // Update state for next iteration: if MSB is set, we're inside a quote
  // Arithmetic right shift propagates the sign bit to all positions
  prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);

  return quote_mask;
#else
  return DispatchFindQuoteMask2(quote_bits, prev_iter_inside_quote);
#endif
}

/**
 * @brief Compute line ending mask supporting LF, CRLF, and CR-only line endings.
 *
 * Returns a bitmask where bits are set at positions that are line endings:
 * - LF (\n) positions are always included
 * - CR (\r) positions are included ONLY if not immediately followed by LF
 *
 * For CRLF sequences, only the LF is marked as the line ending, which ensures
 * the CR becomes part of the previous field's content (stripped later during
 * value extraction).
 *
 * @param in SIMD input block (64 bytes)
 * @param mask Valid byte mask (for partial final blocks)
 * @param prev_ended_with_cr Output: set to true if this block ends with CR
 *                          (caller should check if next block starts with LF)
 * @param prev_block_ended_cr If true, previous block ended with CR. If current
 *                           block starts with LF, that LF is NOT part of a new
 *                           CRLF but completes the previous block's CRLF.
 * @return Bitmask with line ending positions
 */
really_inline uint64_t compute_line_ending_mask(const simd_input& in, uint64_t mask,
                                                bool& prev_ended_with_cr,
                                                bool prev_block_ended_cr = false) {
#ifdef HWY_COMPILE_ONLY_STATIC
  uint64_t lf_mask = cmp_mask_against_input(in, '\n') & mask;
  uint64_t cr_mask = cmp_mask_against_input(in, '\r') & mask;

  // Find CR positions that are immediately followed by LF (CRLF sequences)
  // These CRs should NOT be treated as line endings (the LF will be)
  // (cr_mask << 1) shifts CR positions right; if LF is at that position, it's CRLF
  uint64_t crlf_cr_mask = cr_mask & (lf_mask >> 1);

  // Standalone CR: CR not followed by LF within this block
  uint64_t standalone_cr = cr_mask & ~crlf_cr_mask;

  // Check for cross-block CRLF: CR at position 63
  // If this block ends with CR, we need to check the next block
  prev_ended_with_cr = (cr_mask & (1ULL << 63)) != 0;

  // Handle cross-block CRLF from previous block:
  // If previous block ended with CR and this block starts with LF,
  // that CR was part of CRLF, so we should NOT have counted it.
  // However, since we process blocks sequentially in second_pass_simd,
  // we handle this by NOT double-counting: the LF at position 0 is already
  // the line ending. The CR from the previous block would have been marked
  // as standalone (since we couldn't see the LF), but value extraction
  // strips trailing CR anyway.

  // Line endings: LF positions OR standalone CR positions
  return lf_mask | standalone_cr;
#else
  return DispatchComputeLineEndingMask(in.data, mask, prev_ended_with_cr, prev_block_ended_cr);
#endif
}

/**
 * @brief Simple line ending mask without cross-block tracking.
 *
 * For use in first-pass where we just need to find any line ending.
 */
really_inline uint64_t compute_line_ending_mask_simple(const simd_input& in, uint64_t mask) {
#ifdef HWY_COMPILE_ONLY_STATIC
  uint64_t lf_mask = cmp_mask_against_input(in, '\n') & mask;
  uint64_t cr_mask = cmp_mask_against_input(in, '\r') & mask;

  // CR followed by LF within this block - don't count CR as line ending
  uint64_t crlf_cr_mask = cr_mask & (lf_mask >> 1);
  uint64_t standalone_cr = cr_mask & ~crlf_cr_mask;

  return lf_mask | standalone_cr;
#else
  return DispatchComputeLineEndingMaskSimple(in.data, mask);
#endif
}

/**
 * @brief Compute mask of escaped characters for backslash escaping.
 *
 * For escape character mode (e.g., \" instead of ""), we need to identify
 * which characters are escaped by a preceding escape character.
 *
 * The algorithm:
 * 1. Find all escape character positions
 * 2. For consecutive escapes (\\\\), alternating escapes cancel out
 * 3. Characters immediately following an unescaped escape are escaped
 *
 * Examples:
 * - "\\\"" (backslash-backslash-quote): positions 0,1 are escapes, position 1
 *   is escaped (by pos 0), position 2 (quote) is NOT escaped
 * - "\\\\\"" (4 backslashes + quote): positions 0,1,2,3 are escapes,
 *   positions 1,3 are escaped, position 4 (quote) is NOT escaped
 *
 * @param escape_mask Bitmask of escape character positions
 * @param prev_escape_carry Whether previous block ended with an unmatched escape
 * @return Bitmask where bit i is set if character at position i is escaped
 */
really_inline uint64_t compute_escaped_mask(uint64_t escape_mask, uint64_t& prev_escape_carry) {
#ifdef HWY_COMPILE_ONLY_STATIC
  if (escape_mask == 0 && prev_escape_carry == 0) {
    return 0;
  }

  // Simple algorithm: scan through positions, tracking whether we're in
  // "escape pending" state. Each escape either:
  // - Escapes the next character (if not currently escaped)
  // - Gets escaped itself (if previous char was an unescaped escape)
  uint64_t escaped = 0;
  bool in_escape = prev_escape_carry != 0;

  for (int i = 0; i < 64; ++i) {
    uint64_t bit = 1ULL << i;
    if (in_escape) {
      // This position is escaped by previous escape
      escaped |= bit;
      in_escape = false;
    } else if (escape_mask & bit) {
      // This is a real escape (not escaped itself)
      in_escape = true;
    }
  }

  // Handle carry: if we ended in escape state, next block's first char is escaped
  prev_escape_carry = in_escape ? 1 : 0;

  return escaped;
#else
  return DispatchComputeEscapedMask(escape_mask, prev_escape_carry);
#endif
}

// Write indexes to output array using contiguous access.
// Each thread writes to its own contiguous region to avoid false sharing.
// The stride parameter is kept for API compatibility but is ignored.
really_inline int write(uint64_t* base_ptr, uint64_t& base, uint64_t idx, int /*stride*/,
                        uint64_t bits) {
  if (bits == 0)
    return 0;
  int cnt = static_cast<int>(count_ones(bits));

  for (int i = 0; i < 8; i++) {
    base_ptr[base + i] = idx + trailing_zeroes(bits);
    bits = clear_lowest_bit(bits);
  }

  // LCOV_EXCL_BR_START - unlikely branches for high separator density
  if (unlikely(cnt > 8)) {
    for (int i = 8; i < 16; i++) {
      base_ptr[base + i] = idx + trailing_zeroes(bits);
      bits = clear_lowest_bit(bits);
    }

    if (unlikely(cnt > 16)) {
      int i = 16;
      do {
        base_ptr[base + i] = idx + trailing_zeroes(bits);
        bits = clear_lowest_bit(bits);
        i++;
      } while (i < cnt);
    }
  }
  // LCOV_EXCL_BR_STOP

  base += cnt;
  return cnt;
}

} // namespace libvroom

#endif // SIMD_HIGHWAY_H
