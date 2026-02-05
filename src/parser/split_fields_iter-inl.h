// SIMD SplitFields iterator using Google Highway.
//
// This file is included multiple times by split_fields_iter.cpp with different
// SIMD targets defined by Highway's foreach_target.h mechanism.
//
// Direct port of Polars' SplitFields with Highway for portability.

#include "libvroom/quote_parity.h"

#include "hwy/highway.h"

#include <cstddef>
#include <cstdint>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Scan 64 bytes for a single character, return bitmask
HWY_NOINLINE uint64_t ScanForCharImpl(const char* data, size_t len, char c) {
  if (len < 64) {
    // Scalar fallback for short data
    uint64_t mask = 0;
    for (size_t i = 0; i < len && i < 64; ++i) {
      if (data[i] == c) {
        mask |= (1ULL << i);
      }
    }
    return mask;
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);
  const auto target = hn::Set(d, static_cast<uint8_t>(c));

  uint64_t result = 0;

  // Process 64 bytes in N-byte chunks
  for (size_t chunk = 0; chunk < 64; chunk += N) {
    auto block = hn::LoadU(d, reinterpret_cast<const uint8_t*>(data + chunk));
    auto match = hn::Eq(block, target);

    alignas(64) uint8_t mask_bytes[HWY_MAX_BYTES / 8] = {0};
    hn::StoreMaskBits(d, match, mask_bytes);

    const size_t num_mask_bytes = (N + 7) / 8;
    for (size_t b = 0; b < num_mask_bytes && chunk + b * 8 < 64; ++b) {
      size_t bit_offset = chunk + b * 8;
      if (bit_offset < 64) {
        result |= static_cast<uint64_t>(mask_bytes[b]) << bit_offset;
      }
    }
  }

  return result;
}

// Scan 64 bytes for two characters, return combined bitmask
HWY_NOINLINE uint64_t ScanForTwoCharsImpl(const char* data, size_t len, char c1, char c2) {
  if (len < 64) {
    // Scalar fallback
    uint64_t mask = 0;
    for (size_t i = 0; i < len && i < 64; ++i) {
      if (data[i] == c1 || data[i] == c2) {
        mask |= (1ULL << i);
      }
    }
    return mask;
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);
  const auto target1 = hn::Set(d, static_cast<uint8_t>(c1));
  const auto target2 = hn::Set(d, static_cast<uint8_t>(c2));

  uint64_t result = 0;

  for (size_t chunk = 0; chunk < 64; chunk += N) {
    auto block = hn::LoadU(d, reinterpret_cast<const uint8_t*>(data + chunk));
    auto match1 = hn::Eq(block, target1);
    auto match2 = hn::Eq(block, target2);
    auto match = hn::Or(match1, match2);

    alignas(64) uint8_t mask_bytes[HWY_MAX_BYTES / 8] = {0};
    hn::StoreMaskBits(d, match, mask_bytes);

    const size_t num_mask_bytes = (N + 7) / 8;
    for (size_t b = 0; b < num_mask_bytes && chunk + b * 8 < 64; ++b) {
      size_t bit_offset = chunk + b * 8;
      if (bit_offset < 64) {
        result |= static_cast<uint64_t>(mask_bytes[b]) << bit_offset;
      }
    }
  }

  return result;
}

} // namespace HWY_NAMESPACE
} // namespace libvroom
HWY_AFTER_NAMESPACE();
