// vroom SIMD dispatch public interface.
// These functions select the optimal SIMD implementation at runtime based on
// CPU capabilities (AVX2, AVX-512, SSE4, NEON, etc.).
//
// This module provides direct Highway integration for vroom, using the same
// SIMD techniques as libvroom but without the full library dependency.

#ifndef VROOM_SIMD_DISPATCH_H_
#define VROOM_SIMD_DISPATCH_H_

#include <cstddef>
#include <cstdint>

namespace vroom {
namespace simd {

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
uint64_t CmpMaskAgainstInput(const uint8_t* data, uint8_t m);

/**
 * @brief Create a mask for whitespace characters (space, tab, CR, null).
 *
 * @param data Pointer to 64 bytes of input data.
 * @return 64-bit mask where bit i is set if data[i] is whitespace.
 */
uint64_t WhitespaceMask(const uint8_t* data);

/**
 * @brief Find the first non-whitespace position using SIMD.
 *
 * Efficiently scans from the beginning of a buffer to find the first
 * character that is not whitespace (space, tab, CR, or null).
 *
 * @param begin Pointer to the start of the data.
 * @param end Pointer to one past the end of the data.
 * @return Pointer to the first non-whitespace character, or end if all whitespace.
 */
const char* TrimWhitespaceBeginSIMD(const char* begin, const char* end);

/**
 * @brief Find the last non-whitespace position using SIMD.
 *
 * Efficiently scans from the end of a buffer backwards to find the last
 * character that is not whitespace (space, tab, CR, or null).
 *
 * @param begin Pointer to the start of the data.
 * @param end Pointer to one past the end of the data.
 * @return Pointer to one past the last non-whitespace character, or begin if all whitespace.
 */
const char* TrimWhitespaceEndSIMD(const char* begin, const char* end);

/**
 * @brief SIMD-optimized whitespace trimming (both ends).
 *
 * Trims whitespace from both the beginning and end of a buffer.
 * Modifies the begin and end pointers in place.
 *
 * @param begin Reference to pointer to start of data (modified to first non-whitespace).
 * @param end Reference to pointer past end of data (modified to past last non-whitespace).
 */
void TrimWhitespaceSIMD(const char*& begin, const char*& end);

// =============================================================================
// CSV Indexing SIMD Functions
// =============================================================================

/**
 * @brief Find quote mask using carryless multiplication.
 *
 * Uses parallel prefix XOR to compute which positions are inside quoted fields.
 * This enables determining which delimiters should be counted vs ignored.
 *
 * @param quote_bits Bitmask of quote character positions.
 * @param prev_iter_inside_quote State from previous 64-byte block (0 or ~0ULL).
 * @return Bitmask where bit i is set if position i is inside a quoted field.
 */
uint64_t FindQuoteMask(uint64_t quote_bits, uint64_t prev_iter_inside_quote);

/**
 * @brief Find quote mask with state update for next iteration.
 *
 * Same as FindQuoteMask, but also updates prev_iter_inside_quote for the
 * next iteration based on whether we end inside a quote.
 *
 * @param quote_bits Bitmask of quote character positions.
 * @param prev_iter_inside_quote State from previous block (updated on return).
 * @return Bitmask where bit i is set if position i is inside a quoted field.
 */
uint64_t FindQuoteMask2(uint64_t quote_bits, uint64_t& prev_iter_inside_quote);

/**
 * @brief Compute line ending mask supporting LF, CRLF, and CR-only.
 *
 * @param data Pointer to 64 bytes of input data.
 * @param mask Valid byte mask (for partial final blocks).
 * @return Bitmask with line ending positions.
 */
uint64_t ComputeLineEndingMask(const uint8_t* data, uint64_t mask);

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

} // namespace simd
} // namespace vroom

#endif // VROOM_SIMD_DISPATCH_H_
