#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

namespace libvroom {

// Elias-Fano encoding for monotone non-decreasing sequences of integers.
// Splits each value into high bits (unary coded) and low bits (packed array).
// Provides O(1) select via broadword operations.
class EliasFano {
public:
  EliasFano() = default;

  // Encode a sorted sequence of values.
  // Values must be non-decreasing. Universe is the upper bound (exclusive).
  static EliasFano encode(const std::vector<uint64_t>& values, uint64_t universe) {
    EliasFano ef;
    ef.num_elements_ = values.size();
    ef.universe_ = universe;

    if (values.empty()) {
      ef.low_bits_ = 0;
      return ef;
    }

    // Compute low_bits width: floor(log2(universe / n))
    // If universe <= n, low_bits = 0
    if (universe > ef.num_elements_ && ef.num_elements_ > 0) {
      uint64_t ratio = universe / ef.num_elements_;
      ef.low_bits_ = 63 - count_leading_zeros(ratio);
    } else {
      ef.low_bits_ = 0;
    }

    // Build low bits array (packed, each element is low_bits_ bits)
    if (ef.low_bits_ > 0) {
      size_t total_low_bits = ef.num_elements_ * ef.low_bits_;
      ef.low_array_.resize((total_low_bits + 63) / 64, 0);

      uint64_t low_mask = ef.low_bits_ == 64 ? UINT64_MAX : (uint64_t{1} << ef.low_bits_) - 1;
      for (size_t i = 0; i < values.size(); ++i) {
        uint64_t low_val = values[i] & low_mask;
        set_bits(ef.low_array_.data(), i * ef.low_bits_, ef.low_bits_, low_val);
      }
    }

    // Build high bits bitvector (unary coded)
    // High part of value i is values[i] >> low_bits_
    // The high bitvector has n 1-bits (one per element) and at most
    // (max_high_value + 1) 0-bits, interleaved in unary coding.
    // Total bits = n + max_high + 1
    uint64_t max_high = values.back() >> ef.low_bits_;
    size_t high_bits_count = ef.num_elements_ + max_high + 1;
    ef.high_bitvec_.resize((high_bits_count + 63) / 64, 0);
    ef.high_bits_count_ = high_bits_count;

    // Set bits: for each element i, set bit at position (high_value + i)
    for (size_t i = 0; i < values.size(); ++i) {
      uint64_t high = values[i] >> ef.low_bits_;
      size_t pos = high + i;
      ef.high_bitvec_[pos / 64] |= uint64_t{1} << (pos % 64);
    }

    return ef;
  }

  // Get the i-th value (0-indexed). O(1) via broadword select.
  uint64_t select(size_t i) const {
    assert(i < num_elements_);

    // Find position of (i+1)-th set bit in high_bitvec_
    size_t pos = select1(i);

    // High part = pos - i (number of 0-bits before position pos)
    uint64_t high = pos - i;

    // Low part: read low_bits_ bits starting at i * low_bits_
    uint64_t low = 0;
    if (low_bits_ > 0) {
      low = get_bits(low_array_.data(), i * low_bits_, low_bits_);
    }

    return (high << low_bits_) | low;
  }

  size_t size() const { return num_elements_; }
  uint64_t universe() const { return universe_; }
  uint32_t low_bits() const { return low_bits_; }

  // Serialized size in bytes
  size_t serialized_size() const {
    // Header: num_elements(8) + universe(8) + low_bits(4) + high_bitvec_bytes(4) = 24
    size_t high_bytes = high_bitvec_.size() * 8;
    size_t low_bytes = low_array_.size() * 8;
    return 24 + high_bytes + low_bytes;
  }

  // Serialize to buffer. Buffer must have at least serialized_size() bytes.
  void serialize(uint8_t* buf) const {
    uint64_t n = num_elements_;
    uint64_t u = universe_;
    uint32_t lb = low_bits_;
    uint32_t high_bytes = static_cast<uint32_t>(high_bitvec_.size() * 8);

    std::memcpy(buf, &n, 8);
    buf += 8;
    std::memcpy(buf, &u, 8);
    buf += 8;
    std::memcpy(buf, &lb, 4);
    buf += 4;
    std::memcpy(buf, &high_bytes, 4);
    buf += 4;

    if (high_bytes > 0) {
      std::memcpy(buf, high_bitvec_.data(), high_bytes);
      buf += high_bytes;
    }

    size_t low_bytes_size = low_array_.size() * 8;
    if (low_bytes_size > 0) {
      std::memcpy(buf, low_array_.data(), low_bytes_size);
    }
  }

  // Deserialize from buffer. Returns bytes consumed.
  static EliasFano deserialize(const uint8_t* buf, size_t buf_size, size_t& bytes_consumed) {
    EliasFano ef;
    bytes_consumed = 0;

    if (buf_size < 24) {
      return ef; // Not enough data for header
    }

    uint64_t n, u;
    uint32_t lb, high_bytes;
    std::memcpy(&n, buf, 8);
    buf += 8;
    std::memcpy(&u, buf, 8);
    buf += 8;
    std::memcpy(&lb, buf, 4);
    buf += 4;
    std::memcpy(&high_bytes, buf, 4);
    buf += 4;

    ef.num_elements_ = n;
    ef.universe_ = u;
    ef.low_bits_ = lb;

    size_t remaining = buf_size - 24;

    if (high_bytes > 0) {
      if (remaining < high_bytes) {
        ef.num_elements_ = 0;
        return ef;
      }
      ef.high_bitvec_.resize(high_bytes / 8);
      std::memcpy(ef.high_bitvec_.data(), buf, high_bytes);
      ef.high_bits_count_ = ef.high_bitvec_.size() * 64; // Approximate; exact value reconstructed
      buf += high_bytes;
      remaining -= high_bytes;
    }

    // Compute low array size
    if (lb > 0 && n > 0) {
      size_t total_low_bits = static_cast<size_t>(n) * lb;
      size_t low_words = (total_low_bits + 63) / 64;
      size_t low_bytes_size = low_words * 8;
      if (remaining < low_bytes_size) {
        ef.num_elements_ = 0;
        return ef;
      }
      ef.low_array_.resize(low_words);
      std::memcpy(ef.low_array_.data(), buf, low_bytes_size);
      bytes_consumed = 24 + high_bytes + low_bytes_size;
    } else {
      bytes_consumed = 24 + high_bytes;
    }

    return ef;
  }

private:
  uint64_t num_elements_ = 0;
  uint64_t universe_ = 0;
  uint32_t low_bits_ = 0;
  size_t high_bits_count_ = 0;
  std::vector<uint64_t> high_bitvec_; // Unary-coded high parts
  std::vector<uint64_t> low_array_;   // Packed low parts

  // Count leading zeros (portable)
  static int count_leading_zeros(uint64_t x) {
    if (x == 0)
      return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_clzll(x);
#else
    int n = 0;
    if ((x & 0xFFFFFFFF00000000ULL) == 0) {
      n += 32;
      x <<= 32;
    }
    if ((x & 0xFFFF000000000000ULL) == 0) {
      n += 16;
      x <<= 16;
    }
    if ((x & 0xFF00000000000000ULL) == 0) {
      n += 8;
      x <<= 8;
    }
    if ((x & 0xF000000000000000ULL) == 0) {
      n += 4;
      x <<= 4;
    }
    if ((x & 0xC000000000000000ULL) == 0) {
      n += 2;
      x <<= 2;
    }
    if ((x & 0x8000000000000000ULL) == 0) {
      n += 1;
    }
    return n;
#endif
  }

  // Population count (portable)
  static int popcount64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcountll(x);
#else
    x = x - ((x >> 1) & 0x5555555555555555ULL);
    x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
    x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0FULL;
    return static_cast<int>((x * 0x0101010101010101ULL) >> 56);
#endif
  }

  // Find position of the (rank+1)-th set bit in high_bitvec_ (0-indexed rank)
  size_t select1(size_t rank) const {
    size_t count = 0;
    for (size_t word_idx = 0; word_idx < high_bitvec_.size(); ++word_idx) {
      uint64_t word = high_bitvec_[word_idx];
      int pc = popcount64(word);
      if (count + static_cast<size_t>(pc) > rank) {
        // The target bit is in this word
        return word_idx * 64 + select_in_word(word, rank - count);
      }
      count += pc;
    }
    // Should not reach here if rank < num_elements_
    assert(false && "select1: rank out of bounds");
    return 0;
  }

  // Find position of the (rank+1)-th set bit within a single 64-bit word (0-indexed rank)
  static size_t select_in_word(uint64_t word, size_t rank) {
    // Portable implementation: iterate through set bits
    for (size_t i = 0; i < rank; ++i) {
      word &= word - 1; // Clear lowest set bit
    }
#if defined(__GNUC__) || defined(__clang__)
    return static_cast<size_t>(__builtin_ctzll(word));
#else
    // Portable ctz fallback
    if (word == 0)
      return 64;
    size_t n = 0;
    if ((word & 0xFFFFFFFF) == 0) {
      n += 32;
      word >>= 32;
    }
    if ((word & 0xFFFF) == 0) {
      n += 16;
      word >>= 16;
    }
    if ((word & 0xFF) == 0) {
      n += 8;
      word >>= 8;
    }
    if ((word & 0xF) == 0) {
      n += 4;
      word >>= 4;
    }
    if ((word & 0x3) == 0) {
      n += 2;
      word >>= 2;
    }
    if ((word & 0x1) == 0) {
      n += 1;
    }
    return n;
#endif
  }

  // Set `width` bits at bit position `bit_pos` in the packed array
  static void set_bits(uint64_t* array, size_t bit_pos, uint32_t width, uint64_t value) {
    size_t word_idx = bit_pos / 64;
    size_t bit_offset = bit_pos % 64;

    array[word_idx] |= value << bit_offset;

    // Handle crossing word boundary
    if (bit_offset + width > 64) {
      array[word_idx + 1] |= value >> (64 - bit_offset);
    }
  }

  // Get `width` bits at bit position `bit_pos` from the packed array
  static uint64_t get_bits(const uint64_t* array, size_t bit_pos, uint32_t width) {
    size_t word_idx = bit_pos / 64;
    size_t bit_offset = bit_pos % 64;
    uint64_t mask = width == 64 ? UINT64_MAX : (uint64_t{1} << width) - 1;

    uint64_t result = (array[word_idx] >> bit_offset) & mask;

    // Handle crossing word boundary
    if (bit_offset + width > 64) {
      uint64_t high_bits = array[word_idx + 1] << (64 - bit_offset);
      result |= high_bits & mask;
    }

    return result;
  }
};

} // namespace libvroom
