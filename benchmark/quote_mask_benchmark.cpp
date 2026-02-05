// Micro-benchmark comparing scalar vs CLMul quote mask implementations
#include <benchmark/benchmark.h>
#include <cstdint>
#include <random>

#include "hwy/highway.h"

namespace hn = hwy::HWY_NAMESPACE;

// Chunk size for quote mask processing (64 bytes = 512 bits, but we process
// quote positions as a 64-bit mask)
constexpr size_t kChunkBits = 64;

// Old scalar implementation (for comparison)
// Note: This version correctly handles prev_iter_inside_quote as 0 or ~0ULL
static inline uint64_t find_quote_mask_scalar(uint64_t quote_bits,
                                              uint64_t prev_iter_inside_quote) {
  uint64_t quote_mask = 0;
  // Convert ~0ULL to 1 or 0ULL to 0 for the bit-by-bit computation
  uint64_t state = prev_iter_inside_quote & 1;

  for (size_t i = 0; i < kChunkBits; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1;
    }
    quote_mask |= (state << i);
  }

  return quote_mask;
}

// New CLMul implementation using Highway's portable carryless multiply
HWY_ATTR static inline uint64_t find_quote_mask_clmul(uint64_t quote_bits,
                                                      uint64_t prev_iter_inside_quote) {
  const hn::FixedTag<uint64_t, 2> d;
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);
  auto result = hn::CLMulLower(quote_vec, all_ones);
  uint64_t quote_mask = hn::GetLane(result);
  quote_mask ^= prev_iter_inside_quote;
  return quote_mask;
}

// Generate test data with varying quote densities
std::vector<uint64_t> generate_quote_bits(size_t count, int density_percent) {
  std::vector<uint64_t> data(count);
  std::mt19937_64 rng(42);  // Fixed seed for reproducibility

  for (size_t i = 0; i < count; i++) {
    uint64_t bits = 0;
    for (int j = 0; j < 64; j++) {
      if ((rng() % 100) < static_cast<uint64_t>(density_percent)) {
        bits |= (1ULL << j);
      }
    }
    data[i] = bits;
  }
  return data;
}

static void BM_QuoteMask_Scalar(benchmark::State& state) {
  const int density = state.range(0);
  const size_t count = 10000;
  auto quote_bit_patterns = generate_quote_bits(count, density);

  for (auto _ : state) {
    uint64_t prev_state = 0;
    uint64_t checksum = 0;

    for (size_t i = 0; i < count; i++) {
      uint64_t quote_mask = find_quote_mask_scalar(quote_bit_patterns[i], prev_state);
      checksum ^= quote_mask;
      // Update state using arithmetic right shift (matches actual implementation)
      prev_state = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);
    }
    benchmark::DoNotOptimize(checksum);
  }

  state.SetItemsProcessed(state.iterations() * count);
  state.SetBytesProcessed(state.iterations() * count * kChunkBits);
  state.counters["QuoteDensity%"] = density;
}

static void BM_QuoteMask_CLMul(benchmark::State& state) {
  const int density = state.range(0);
  const size_t count = 10000;
  auto quote_bit_patterns = generate_quote_bits(count, density);

  for (auto _ : state) {
    uint64_t prev_state = 0;
    uint64_t checksum = 0;

    for (size_t i = 0; i < count; i++) {
      uint64_t quote_mask = find_quote_mask_clmul(quote_bit_patterns[i], prev_state);
      checksum ^= quote_mask;
      // Update state using arithmetic right shift (matches actual implementation)
      prev_state = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);
    }
    benchmark::DoNotOptimize(checksum);
  }

  state.SetItemsProcessed(state.iterations() * count);
  state.SetBytesProcessed(state.iterations() * count * kChunkBits);
  state.counters["QuoteDensity%"] = density;
}

// Benchmark with different quote densities (0%, 1%, 5%, 10%, 50%)
BENCHMARK(BM_QuoteMask_Scalar)->Arg(0)->Arg(1)->Arg(5)->Arg(10)->Arg(50);
BENCHMARK(BM_QuoteMask_CLMul)->Arg(0)->Arg(1)->Arg(5)->Arg(10)->Arg(50);

BENCHMARK_MAIN();
