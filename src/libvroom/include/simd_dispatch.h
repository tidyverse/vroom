// Copyright 2024 libvroom Authors
// SPDX-License-Identifier: MIT
//
// SIMD dispatch public interface.
// These functions select the optimal SIMD implementation at runtime based on
// CPU capabilities (AVX2, AVX-512, SSE4, NEON, etc.).

#ifndef LIBVROOM_SIMD_DISPATCH_H_
#define LIBVROOM_SIMD_DISPATCH_H_

#include <cstdint>

namespace libvroom {

// =============================================================================
// Dispatched SIMD Functions
// =============================================================================

/**
 * @brief Compare each byte of a 64-byte block against a match value.
 *
 * @param data Pointer to 64 bytes of input data.
 * @param m The byte value to match against.
 * @return 64-bit mask where bit i is set if data[i] == m.
 */
uint64_t DispatchCmpMaskAgainstInput(const uint8_t* data, uint8_t m);

/**
 * @brief Compute quote mask using carryless multiplication.
 *
 * Uses PCLMULQDQ (x86) or PMULL (ARM) to compute parallel prefix XOR,
 * determining which byte positions are inside quoted regions.
 *
 * @param quote_bits Bitmask of quote character positions.
 * @param prev_iter_inside_quote Previous iteration's inside-quote state.
 * @return 64-bit mask where set bits indicate positions inside quotes.
 */
uint64_t DispatchFindQuoteMask(uint64_t quote_bits, uint64_t prev_iter_inside_quote);

/**
 * @brief Compute quote mask with state tracking.
 *
 * Like DispatchFindQuoteMask but updates prev_iter_inside_quote for chaining
 * across 64-byte blocks.
 */
uint64_t DispatchFindQuoteMask2(uint64_t quote_bits, uint64_t& prev_iter_inside_quote);

/**
 * @brief Compute line ending mask (simple version).
 *
 * Identifies LF and standalone CR positions within a 64-byte block.
 * CRLF sequences are handled by marking only the LF.
 *
 * @param data Pointer to 64 bytes of input data.
 * @param mask Valid byte mask (for partial final blocks).
 * @return 64-bit mask with line ending positions set.
 */
uint64_t DispatchComputeLineEndingMaskSimple(const uint8_t* data, uint64_t mask);

/**
 * @brief Compute line ending mask with cross-block CRLF tracking.
 *
 * Like DispatchComputeLineEndingMaskSimple but tracks CR at block boundaries.
 *
 * @param data Pointer to 64 bytes of input data.
 * @param mask Valid byte mask (for partial final blocks).
 * @param[out] prev_ended_with_cr Set to true if block ends with CR.
 * @param prev_block_ended_cr True if previous block ended with CR.
 * @return 64-bit mask with line ending positions set.
 */
uint64_t DispatchComputeLineEndingMask(const uint8_t* data, uint64_t mask, bool& prev_ended_with_cr,
                                       bool prev_block_ended_cr);

/**
 * @brief Compute escaped character mask for backslash escaping.
 *
 * Identifies positions that are escaped by a preceding escape character.
 *
 * @param escape_mask Bitmask of escape character positions.
 * @param[in,out] prev_escape_carry Updated with carry for next block.
 * @return 64-bit mask where set bits indicate escaped positions.
 */
uint64_t DispatchComputeEscapedMask(uint64_t escape_mask, uint64_t& prev_escape_carry);

// =============================================================================
// SIMD Target Information
// =============================================================================

/**
 * @brief Get bitmask of all SIMD targets supported by the current CPU.
 *
 * Useful for diagnostics and debugging. Each bit represents a Highway target.
 */
int64_t GetSupportedTargets();

/**
 * @brief Get the SIMD target currently being used for dispatch.
 *
 * Returns the Highway target constant for the chosen SIMD level.
 */
int64_t GetChosenTarget();

/**
 * @brief Get human-readable name for a Highway target constant.
 *
 * @param target A Highway target constant from GetChosenTarget() or
 *               GetSupportedTargets().
 * @return String like "AVX2", "AVX3", "NEON", "SSE4", etc.
 */
const char* GetTargetName(int64_t target);

} // namespace libvroom

#endif // LIBVROOM_SIMD_DISPATCH_H_
