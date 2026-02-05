#include "libvroom/elias_fano.h"

#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <numeric>
#include <random>
#include <vector>

using namespace libvroom;

// =============================================================================
// Basic encode/decode roundtrip tests
// =============================================================================

TEST(EliasFano, EmptySequence) {
  std::vector<uint64_t> values;
  auto ef = EliasFano::encode(values, 0);
  EXPECT_EQ(ef.size(), 0u);
}

TEST(EliasFano, SingleElement) {
  std::vector<uint64_t> values = {42};
  auto ef = EliasFano::encode(values, 100);
  ASSERT_EQ(ef.size(), 1u);
  EXPECT_EQ(ef.select(0), 42u);
}

TEST(EliasFano, SmallSequence) {
  std::vector<uint64_t> values = {3, 7, 15, 20, 42, 100};
  auto ef = EliasFano::encode(values, 101);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), values[i]) << "at index " << i;
  }
}

TEST(EliasFano, ConsecutiveValues) {
  std::vector<uint64_t> values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto ef = EliasFano::encode(values, 10);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), values[i]) << "at index " << i;
  }
}

TEST(EliasFano, AllSameValue) {
  std::vector<uint64_t> values = {42, 42, 42, 42, 42};
  auto ef = EliasFano::encode(values, 43);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), 42u) << "at index " << i;
  }
}

TEST(EliasFano, LargeValues) {
  std::vector<uint64_t> values = {1000000, 2000000, 3000000, 10000000};
  auto ef = EliasFano::encode(values, 10000001);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), values[i]) << "at index " << i;
  }
}

TEST(EliasFano, AllZeros) {
  std::vector<uint64_t> values = {0, 0, 0};
  auto ef = EliasFano::encode(values, 1);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), 0u) << "at index " << i;
  }
}

TEST(EliasFano, PowersOfTwo) {
  std::vector<uint64_t> values = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
  auto ef = EliasFano::encode(values, 1025);
  ASSERT_EQ(ef.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef.select(i), values[i]) << "at index " << i;
  }
}

// =============================================================================
// Serialization roundtrip
// =============================================================================

TEST(EliasFano, SerializeDeserializeEmpty) {
  auto ef = EliasFano::encode({}, 0);
  std::vector<uint8_t> buf(ef.serialized_size());
  ef.serialize(buf.data());

  size_t consumed = 0;
  auto ef2 = EliasFano::deserialize(buf.data(), buf.size(), consumed);
  EXPECT_EQ(ef2.size(), 0u);
  EXPECT_GT(consumed, 0u);
}

TEST(EliasFano, SerializeDeserializeSmall) {
  std::vector<uint64_t> values = {5, 10, 20, 50, 100};
  auto ef = EliasFano::encode(values, 101);

  std::vector<uint8_t> buf(ef.serialized_size());
  ef.serialize(buf.data());

  size_t consumed = 0;
  auto ef2 = EliasFano::deserialize(buf.data(), buf.size(), consumed);
  ASSERT_EQ(ef2.size(), values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(ef2.select(i), values[i]) << "at index " << i;
  }
}

TEST(EliasFano, SerializeDeserializeLarge) {
  // Simulate row offsets for a 1GB file with ~10M rows, sampled every 32 rows
  std::vector<uint64_t> values;
  uint64_t universe = 1000000000ULL;  // 1GB
  size_t num_samples = 10000000 / 32; // ~312K samples
  uint64_t stride = universe / num_samples;

  for (size_t i = 0; i < num_samples; ++i) {
    values.push_back(i * stride);
  }

  auto ef = EliasFano::encode(values, universe);
  ASSERT_EQ(ef.size(), num_samples);

  // Verify a few select values
  EXPECT_EQ(ef.select(0), 0u);
  EXPECT_EQ(ef.select(1), stride);
  EXPECT_EQ(ef.select(num_samples - 1), (num_samples - 1) * stride);

  // Serialize and deserialize
  std::vector<uint8_t> buf(ef.serialized_size());
  ef.serialize(buf.data());

  size_t consumed = 0;
  auto ef2 = EliasFano::deserialize(buf.data(), buf.size(), consumed);
  ASSERT_EQ(ef2.size(), num_samples);
  EXPECT_EQ(ef2.select(0), 0u);
  EXPECT_EQ(ef2.select(num_samples / 2), (num_samples / 2) * stride);
  EXPECT_EQ(ef2.select(num_samples - 1), (num_samples - 1) * stride);
}

// =============================================================================
// Edge cases
// =============================================================================

TEST(EliasFano, UniverseEqualsN) {
  std::vector<uint64_t> values = {0, 1, 2, 3, 4};
  auto ef = EliasFano::encode(values, 5);
  ASSERT_EQ(ef.size(), 5u);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(ef.select(i), i);
  }
}

TEST(EliasFano, DeserializeTooSmall) {
  std::vector<uint8_t> buf(4, 0); // Too small for header
  size_t consumed = 0;
  auto ef = EliasFano::deserialize(buf.data(), buf.size(), consumed);
  EXPECT_EQ(ef.size(), 0u);
}

TEST(EliasFano, SizeBytes) {
  std::vector<uint64_t> values = {100, 200, 300, 400, 500};
  auto ef = EliasFano::encode(values, 501);
  EXPECT_GT(ef.serialized_size(), 0u);
  // Header is at least 24 bytes
  EXPECT_GE(ef.serialized_size(), 24u);
}

// =============================================================================
// Randomized stress test
// =============================================================================

TEST(EliasFano, RandomizedRoundtrip) {
  std::mt19937_64 rng(42);

  for (int trial = 0; trial < 20; ++trial) {
    size_t n = 1 + (rng() % 1000);
    uint64_t max_val = 1 + (rng() % 10000000);

    std::vector<uint64_t> values(n);
    for (size_t i = 0; i < n; ++i) {
      values[i] = rng() % (max_val + 1);
    }
    std::sort(values.begin(), values.end());

    auto ef = EliasFano::encode(values, max_val + 1);
    ASSERT_EQ(ef.size(), n) << "trial " << trial;

    for (size_t i = 0; i < n; ++i) {
      EXPECT_EQ(ef.select(i), values[i]) << "trial " << trial << " index " << i;
    }

    // Serialize roundtrip
    std::vector<uint8_t> buf(ef.serialized_size());
    ef.serialize(buf.data());
    size_t consumed = 0;
    auto ef2 = EliasFano::deserialize(buf.data(), buf.size(), consumed);
    ASSERT_EQ(ef2.size(), n) << "trial " << trial;
    for (size_t i = 0; i < n; ++i) {
      EXPECT_EQ(ef2.select(i), values[i]) << "trial " << trial << " index " << i;
    }
  }
}
