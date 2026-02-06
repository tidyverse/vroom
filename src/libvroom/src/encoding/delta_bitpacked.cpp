#include "libvroom/vroom.h"

#include <algorithm>
#include <cstring>

namespace libvroom {
namespace writer {
namespace encoding {

// Delta Binary Packed encoding for Parquet
// Encodes integers as deltas from the previous value, then bit-packs the deltas
// Good for sorted or monotonically increasing data

// Block size parameters (Parquet spec recommends these)
constexpr size_t BLOCK_SIZE = 128;                              // Values per block
constexpr size_t MINIBLOCK_COUNT = 4;                           // Mini-blocks per block
constexpr size_t MINIBLOCK_SIZE = BLOCK_SIZE / MINIBLOCK_COUNT; // Values per mini-block

// ZigZag encode signed integers to unsigned
inline uint64_t zigzag_encode(int64_t value) {
  return static_cast<uint64_t>((value << 1) ^ (value >> 63));
}

// Write unsigned varint
size_t write_uvarint(uint64_t value, std::vector<uint8_t>& output) {
  size_t bytes_written = 0;
  while (value >= 0x80) {
    output.push_back((value & 0x7F) | 0x80);
    value >>= 7;
    ++bytes_written;
  }
  output.push_back(value & 0x7F);
  return bytes_written + 1;
}

// Calculate bit width needed for a value
uint8_t bit_width_for_value(uint64_t value) {
  if (value == 0)
    return 0;
  return 64 - __builtin_clzll(value);
}

// Bit-pack values into output
void bit_pack(const uint64_t* values, size_t count, uint8_t bit_width,
              std::vector<uint8_t>& output) {
  if (bit_width == 0)
    return;

  uint64_t buffer = 0;
  int bits_in_buffer = 0;

  for (size_t i = 0; i < count; ++i) {
    buffer |= (values[i] << bits_in_buffer);
    bits_in_buffer += bit_width;

    while (bits_in_buffer >= 8) {
      output.push_back(buffer & 0xFF);
      buffer >>= 8;
      bits_in_buffer -= 8;
    }
  }

  if (bits_in_buffer > 0) {
    output.push_back(buffer & 0xFF);
  }
}

// Encode int32 values using delta binary packing
void encode_int32_delta(const int32_t* values, size_t count, std::vector<uint8_t>& output) {
  if (count == 0)
    return;

  // Write header
  write_uvarint(BLOCK_SIZE, output);
  write_uvarint(MINIBLOCK_COUNT, output);
  write_uvarint(count, output);
  write_uvarint(zigzag_encode(values[0]), output); // First value

  if (count == 1)
    return;

  // Calculate deltas
  std::vector<int64_t> deltas(count - 1);
  for (size_t i = 1; i < count; ++i) {
    deltas[i - 1] = static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1]);
  }

  // Process in blocks
  size_t delta_idx = 0;
  while (delta_idx < deltas.size()) {
    // Process one block
    size_t block_count = std::min(BLOCK_SIZE, deltas.size() - delta_idx);

    // Find min delta in block
    int64_t min_delta = deltas[delta_idx];
    for (size_t i = 0; i < block_count; ++i) {
      min_delta = std::min(min_delta, deltas[delta_idx + i]);
    }

    // Write min delta (zigzag encoded)
    write_uvarint(zigzag_encode(min_delta), output);

    // Calculate bit widths for each mini-block
    std::vector<uint8_t> bit_widths(MINIBLOCK_COUNT, 0);

    for (size_t mb = 0; mb < MINIBLOCK_COUNT; ++mb) {
      size_t mb_start = delta_idx + mb * MINIBLOCK_SIZE;
      size_t mb_end = std::min(mb_start + MINIBLOCK_SIZE, delta_idx + block_count);

      uint64_t max_adjusted = 0;
      for (size_t i = mb_start; i < mb_end; ++i) {
        uint64_t adjusted = static_cast<uint64_t>(deltas[i] - min_delta);
        max_adjusted = std::max(max_adjusted, adjusted);
      }

      bit_widths[mb] = bit_width_for_value(max_adjusted);
    }

    // Write bit widths
    for (uint8_t bw : bit_widths) {
      output.push_back(bw);
    }

    // Write mini-blocks
    for (size_t mb = 0; mb < MINIBLOCK_COUNT; ++mb) {
      size_t mb_start = delta_idx + mb * MINIBLOCK_SIZE;
      size_t mb_end = std::min(mb_start + MINIBLOCK_SIZE, delta_idx + block_count);
      size_t mb_count = mb_end - mb_start;

      if (mb_count == 0)
        continue;

      // Convert to adjusted values
      std::vector<uint64_t> adjusted(mb_count);
      for (size_t i = 0; i < mb_count; ++i) {
        adjusted[i] = static_cast<uint64_t>(deltas[mb_start + i] - min_delta);
      }

      // Bit-pack
      bit_pack(adjusted.data(), mb_count, bit_widths[mb], output);
    }

    delta_idx += block_count;
  }
}

// Encode int64 values using delta binary packing
void encode_int64_delta(const int64_t* values, size_t count, std::vector<uint8_t>& output) {
  if (count == 0)
    return;

  // Write header
  write_uvarint(BLOCK_SIZE, output);
  write_uvarint(MINIBLOCK_COUNT, output);
  write_uvarint(count, output);
  write_uvarint(zigzag_encode(values[0]), output);

  if (count == 1)
    return;

  // Calculate deltas
  std::vector<int64_t> deltas(count - 1);
  for (size_t i = 1; i < count; ++i) {
    deltas[i - 1] = values[i] - values[i - 1];
  }

  // Process in blocks
  size_t delta_idx = 0;
  while (delta_idx < deltas.size()) {
    size_t block_count = std::min(BLOCK_SIZE, deltas.size() - delta_idx);

    int64_t min_delta = deltas[delta_idx];
    for (size_t i = 0; i < block_count; ++i) {
      min_delta = std::min(min_delta, deltas[delta_idx + i]);
    }

    write_uvarint(zigzag_encode(min_delta), output);

    std::vector<uint8_t> bit_widths(MINIBLOCK_COUNT, 0);

    for (size_t mb = 0; mb < MINIBLOCK_COUNT; ++mb) {
      size_t mb_start = delta_idx + mb * MINIBLOCK_SIZE;
      size_t mb_end = std::min(mb_start + MINIBLOCK_SIZE, delta_idx + block_count);

      uint64_t max_adjusted = 0;
      for (size_t i = mb_start; i < mb_end; ++i) {
        uint64_t adjusted = static_cast<uint64_t>(deltas[i] - min_delta);
        max_adjusted = std::max(max_adjusted, adjusted);
      }

      bit_widths[mb] = bit_width_for_value(max_adjusted);
    }

    for (uint8_t bw : bit_widths) {
      output.push_back(bw);
    }

    for (size_t mb = 0; mb < MINIBLOCK_COUNT; ++mb) {
      size_t mb_start = delta_idx + mb * MINIBLOCK_SIZE;
      size_t mb_end = std::min(mb_start + MINIBLOCK_SIZE, delta_idx + block_count);
      size_t mb_count = mb_end - mb_start;

      if (mb_count == 0)
        continue;

      std::vector<uint64_t> adjusted(mb_count);
      for (size_t i = 0; i < mb_count; ++i) {
        adjusted[i] = static_cast<uint64_t>(deltas[mb_start + i] - min_delta);
      }

      bit_pack(adjusted.data(), mb_count, bit_widths[mb], output);
    }

    delta_idx += block_count;
  }
}

} // namespace encoding
} // namespace writer
} // namespace libvroom
