// Tests for quote mask computation (PCLMULQDQ/PMULL implementation)
// Ported from quote_mask_test.cpp to use libvroom2 quote_parity.h API
#include "libvroom/quote_parity.h"

#include <cstdint>
#include <gtest/gtest.h>

namespace libvroom {
namespace {

// Reference scalar implementation for verification
uint64_t reference_quote_mask(uint64_t quote_bits, uint64_t prev_inside_quote) {
  uint64_t mask = 0;
  uint64_t inside = prev_inside_quote & 1; // 0 or 1

  for (int i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      inside ^= 1;
    }
    mask |= (inside << i);
  }
  return mask;
}

// Helper: call find_quote_mask without mutating prev_state
uint64_t find_quote_mask_readonly(uint64_t quote_bits, uint64_t prev_state) {
  uint64_t state = prev_state;
  return find_quote_mask(quote_bits, state);
}

class QuoteMaskTest : public ::testing::Test {};

// ============================================================================
// PORTED TESTS FROM quote_mask_test.cpp
// ============================================================================

// Test: No quotes at all
TEST_F(QuoteMaskTest, NoQuotes) {
  uint64_t quote_bits = 0;

  // Starting outside quotes
  EXPECT_EQ(find_quote_mask_readonly(quote_bits, 0), 0ULL);

  // Starting inside quotes (all bits should be set)
  EXPECT_EQ(find_quote_mask_readonly(quote_bits, ~0ULL), ~0ULL);
}

// Test: Single quote at position 0
TEST_F(QuoteMaskTest, SingleQuoteAtStart) {
  uint64_t quote_bits = 1ULL; // Quote at position 0

  // Starting outside: bits 0-63 should all be 1 (inside quote after pos 0)
  uint64_t result = find_quote_mask_readonly(quote_bits, 0);
  EXPECT_EQ(result, ~0ULL); // All bits set after the quote

  // Starting inside: quote closes, all bits should be 0
  result = find_quote_mask_readonly(quote_bits, ~0ULL);
  EXPECT_EQ(result, 0ULL);
}

// Test: Single quote at position 63 (last position)
TEST_F(QuoteMaskTest, SingleQuoteAtEnd) {
  uint64_t quote_bits = 1ULL << 63; // Quote at position 63

  // Starting outside: only bit 63 should be set
  uint64_t result = find_quote_mask_readonly(quote_bits, 0);
  EXPECT_EQ(result, 1ULL << 63);

  // Starting inside: all bits except 63 should be set
  result = find_quote_mask_readonly(quote_bits, ~0ULL);
  EXPECT_EQ(result, ~(1ULL << 63));
}

// Test: Two quotes (open and close)
TEST_F(QuoteMaskTest, QuotePair) {
  // Quote at positions 10 and 20
  uint64_t quote_bits = (1ULL << 10) | (1ULL << 20);

  uint64_t result = find_quote_mask_readonly(quote_bits, 0);

  // Bits 10-19 should be inside quotes (1), others outside (0)
  for (int i = 0; i < 64; i++) {
    bool expected_inside = (i >= 10 && i < 20);
    bool actual_inside = (result >> i) & 1;
    EXPECT_EQ(actual_inside, expected_inside) << "Mismatch at position " << i;
  }
}

// Test: State transition across chunk boundaries
TEST_F(QuoteMaskTest, StateTransitionAcrossBoundaries) {
  // Simulate processing two chunks where a quote opens in chunk 1
  // and closes in chunk 2

  // Chunk 1: quote opens at position 32
  uint64_t chunk1_quotes = 1ULL << 32;
  uint64_t prev_state = 0;

  uint64_t mask1 = find_quote_mask(chunk1_quotes, prev_state);

  // After chunk 1, we should be inside a quote
  EXPECT_EQ(prev_state, ~0ULL) << "Should be inside quote after chunk 1";

  // Bits 32-63 should be inside quote
  for (int i = 0; i < 64; i++) {
    bool expected = (i >= 32);
    bool actual = (mask1 >> i) & 1;
    EXPECT_EQ(actual, expected) << "Chunk 1 mismatch at position " << i;
  }

  // Chunk 2: quote closes at position 16
  uint64_t chunk2_quotes = 1ULL << 16;

  uint64_t mask2 = find_quote_mask(chunk2_quotes, prev_state);

  // After chunk 2, we should be outside a quote
  EXPECT_EQ(prev_state, 0ULL) << "Should be outside quote after chunk 2";

  // Bits 0-15 should be inside quote (carry from chunk 1), 16-63 outside
  for (int i = 0; i < 64; i++) {
    bool expected = (i < 16);
    bool actual = (mask2 >> i) & 1;
    EXPECT_EQ(actual, expected) << "Chunk 2 mismatch at position " << i;
  }
}

// Test: Alternating quotes (maximum state transitions)
TEST_F(QuoteMaskTest, AlternatingQuotes) {
  // Every other bit is a quote
  uint64_t quote_bits = 0x5555555555555555ULL; // 0101...

  uint64_t result = find_quote_mask_readonly(quote_bits, 0);

  // Trace through:
  // pos 0: quote -> inside, bit 0 = 1
  // pos 1: not quote, still inside, bit 1 = 1
  // pos 2: quote -> outside, bit 2 = 0
  // pos 3: not quote, still outside, bit 3 = 0
  // Pattern: 0b0011 repeated = 0x3333...

  EXPECT_EQ(result, 0x3333333333333333ULL);
}

// Test: All quotes (every position)
TEST_F(QuoteMaskTest, AllQuotes) {
  uint64_t quote_bits = ~0ULL; // All positions are quotes

  uint64_t result = find_quote_mask_readonly(quote_bits, 0);

  // Each bit toggles: inside at 0, outside at 1, inside at 2, ...
  // Pattern: 1,0,1,0,... = 0x5555...
  EXPECT_EQ(result, 0x5555555555555555ULL);
}

// Test: Verify SIMD matches reference implementation
TEST_F(QuoteMaskTest, MatchesReferenceImplementation) {
  uint64_t patterns[] = {
      0ULL,
      ~0ULL,
      0x0000000000000001ULL,
      0x8000000000000000ULL,
      0x00000000FFFFFFFFULL,
      0xFFFFFFFF00000000ULL,
      0x5555555555555555ULL,
      0xAAAAAAAAAAAAAAAAULL,
      0x0123456789ABCDEFULL,
      0xFEDCBA9876543210ULL,
  };

  for (uint64_t pattern : patterns) {
    for (uint64_t prev : {0ULL, ~0ULL}) {
      uint64_t expected = reference_quote_mask(pattern, prev);
      uint64_t actual = find_quote_mask_readonly(pattern, prev);
      EXPECT_EQ(actual, expected) << "Mismatch for pattern=0x" << std::hex << pattern << " prev=0x"
                                  << prev;
    }
  }
}

// Test: find_quote_mask state tracking
TEST_F(QuoteMaskTest, FindQuoteMaskStateTracking) {
  uint64_t prev_state = 0;

  // Process pattern that ends inside a quote
  uint64_t pattern1 = 1ULL << 32; // Single quote in middle
  find_quote_mask(pattern1, prev_state);
  EXPECT_EQ(prev_state, ~0ULL) << "Should be inside quote (MSB was set)";

  // Process pattern that ends outside a quote
  uint64_t pattern2 = 1ULL; // Quote at start closes it
  find_quote_mask(pattern2, prev_state);
  EXPECT_EQ(prev_state, 0ULL) << "Should be outside quote";
}

// Test: Random patterns for fuzzing
TEST_F(QuoteMaskTest, RandomPatternsFuzz) {
  // Simple PRNG for reproducibility
  uint64_t seed = 0xDEADBEEF12345678ULL;
  auto next_random = [&seed]() {
    seed ^= seed << 13;
    seed ^= seed >> 7;
    seed ^= seed << 17;
    return seed;
  };

  for (int i = 0; i < 1000; i++) {
    uint64_t pattern = next_random();
    uint64_t prev = (next_random() & 1) ? ~0ULL : 0ULL;

    uint64_t expected = reference_quote_mask(pattern, prev);
    uint64_t actual = find_quote_mask_readonly(pattern, prev);

    EXPECT_EQ(actual, expected) << "Fuzz test failed at iteration " << i << " pattern=0x"
                                << std::hex << pattern;
  }
}

// ============================================================================
// NEW TESTS: prefix_xorsum_inclusive
// ============================================================================

TEST_F(QuoteMaskTest, PrefixXorsumZero) {
  EXPECT_EQ(prefix_xorsum_inclusive(0), 0ULL);
}

TEST_F(QuoteMaskTest, PrefixXorsumSingleBitAtStart) {
  // Bit at position 0: all subsequent bits are XOR'd = all 1s
  EXPECT_EQ(prefix_xorsum_inclusive(1ULL), ~0ULL);
}

TEST_F(QuoteMaskTest, PrefixXorsumSingleBitAtEnd) {
  // Bit at position 63: only bit 63 is set in result
  EXPECT_EQ(prefix_xorsum_inclusive(1ULL << 63), 1ULL << 63);
}

TEST_F(QuoteMaskTest, PrefixXorsumTwoBits) {
  // Bits at positions 10 and 20: XOR prefix gives bits 10-19 set
  uint64_t input = (1ULL << 10) | (1ULL << 20);
  uint64_t result = prefix_xorsum_inclusive(input);

  for (int i = 0; i < 64; i++) {
    bool expected = (i >= 10 && i < 20);
    bool actual = (result >> i) & 1;
    EXPECT_EQ(actual, expected) << "Mismatch at position " << i;
  }
}

TEST_F(QuoteMaskTest, PrefixXorsumAllBits) {
  // All bits set: alternating pattern in result
  EXPECT_EQ(prefix_xorsum_inclusive(~0ULL), 0x5555555555555555ULL);
}

// ============================================================================
// NEW TESTS: portable_prefix_xorsum_inclusive consistency
// ============================================================================

TEST_F(QuoteMaskTest, PortablePrefixXorsumMatchesSIMD) {
  uint64_t patterns[] = {
      0ULL,
      ~0ULL,
      1ULL,
      1ULL << 63,
      0x5555555555555555ULL,
      0xAAAAAAAAAAAAAAAAULL,
      0x00000000FFFFFFFFULL,
      0xFFFFFFFF00000000ULL,
      0x0123456789ABCDEFULL,
      0xFEDCBA9876543210ULL,
  };

  for (uint64_t pattern : patterns) {
    uint64_t simd_result = prefix_xorsum_inclusive(pattern);
    uint64_t portable_result = portable_prefix_xorsum_inclusive(pattern);
    EXPECT_EQ(simd_result, portable_result)
        << "SIMD and portable disagree for pattern=0x" << std::hex << pattern;
  }
}

TEST_F(QuoteMaskTest, PortablePrefixXorsumFuzz) {
  uint64_t seed = 0xCAFEBABE42424242ULL;
  auto next_random = [&seed]() {
    seed ^= seed << 13;
    seed ^= seed >> 7;
    seed ^= seed << 17;
    return seed;
  };

  for (int i = 0; i < 1000; i++) {
    uint64_t pattern = next_random();
    uint64_t simd_result = prefix_xorsum_inclusive(pattern);
    uint64_t portable_result = portable_prefix_xorsum_inclusive(pattern);
    EXPECT_EQ(simd_result, portable_result)
        << "Fuzz iteration " << i << " pattern=0x" << std::hex << pattern;
  }
}

// ============================================================================
// NEW TESTS: scalar_find_quote_mask consistency
// ============================================================================

TEST_F(QuoteMaskTest, ScalarMatchesSIMDQuoteMask) {
  uint64_t patterns[] = {
      0ULL,
      ~0ULL,
      1ULL,
      1ULL << 63,
      0x5555555555555555ULL,
      0xAAAAAAAAAAAAAAAAULL,
      0x00000000FFFFFFFFULL,
      0xFFFFFFFF00000000ULL,
      0x0123456789ABCDEFULL,
      0xFEDCBA9876543210ULL,
  };

  for (uint64_t pattern : patterns) {
    for (uint64_t prev : {0ULL, ~0ULL}) {
      uint64_t simd_result = find_quote_mask_readonly(pattern, prev);
      uint64_t scalar_result = scalar_find_quote_mask(pattern, prev);
      EXPECT_EQ(simd_result, scalar_result)
          << "SIMD and scalar disagree for pattern=0x" << std::hex << pattern << " prev=0x" << prev;
    }
  }
}

TEST_F(QuoteMaskTest, ScalarVsSIMDFuzz) {
  uint64_t seed = 0x1234567890ABCDEFULL;
  auto next_random = [&seed]() {
    seed ^= seed << 13;
    seed ^= seed >> 7;
    seed ^= seed << 17;
    return seed;
  };

  for (int i = 0; i < 1000; i++) {
    uint64_t pattern = next_random();
    uint64_t prev = (next_random() & 1) ? ~0ULL : 0ULL;

    uint64_t simd_result = find_quote_mask_readonly(pattern, prev);
    uint64_t scalar_result = scalar_find_quote_mask(pattern, prev);

    EXPECT_EQ(simd_result, scalar_result)
        << "Fuzz iteration " << i << " pattern=0x" << std::hex << pattern << " prev=0x" << prev;
  }
}

// ============================================================================
// NEW TESTS: Multi-chunk state tracking consistency
// ============================================================================

TEST_F(QuoteMaskTest, MultiChunkStateConsistency) {
  // Process a sequence of chunks and verify SIMD find_quote_mask tracks state
  // identically to the reference implementation across chunk boundaries
  uint64_t seed = 0xABCDEF0123456789ULL;
  auto next_random = [&seed]() {
    seed ^= seed << 13;
    seed ^= seed >> 7;
    seed ^= seed << 17;
    return seed;
  };

  uint64_t simd_state = 0;
  uint64_t ref_inside = 0; // 0 or 1

  for (int chunk = 0; chunk < 100; chunk++) {
    uint64_t pattern = next_random();

    // SIMD path: find_quote_mask updates simd_state
    uint64_t simd_mask = find_quote_mask(pattern, simd_state);

    // Reference path: compute mask and track state manually
    uint64_t ref_mask = reference_quote_mask(pattern, ref_inside);

    EXPECT_EQ(simd_mask, ref_mask) << "Mask mismatch at chunk " << chunk;

    // Update reference state: count quote bits to determine final parity
    // Note: __builtin_popcountll is GCC/Clang-specific. The production code in
    // simd_chunk_finder.cpp also uses it, so this matches the project's compiler requirements.
    uint64_t total_quotes = __builtin_popcountll(pattern);
    ref_inside ^= (total_quotes & 1); // Toggle if odd number of quotes

    // Verify SIMD state matches reference
    uint64_t expected_simd_state = ref_inside ? ~0ULL : 0ULL;
    EXPECT_EQ(simd_state, expected_simd_state) << "State mismatch at chunk " << chunk;
  }
}

} // namespace
} // namespace libvroom
