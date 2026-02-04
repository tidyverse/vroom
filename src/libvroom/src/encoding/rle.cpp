#include "libvroom/vroom.h"

#include <cstring>

namespace libvroom {
namespace writer {
namespace encoding {

// Run-Length Encoding (RLE) for Parquet
// Used for definition levels, repetition levels, and boolean data

// Write a varint (variable-length integer)
size_t write_varint(uint32_t value, std::vector<uint8_t>& output) {
  size_t bytes_written = 0;
  while (value >= 0x80) {
    output.push_back((value & 0x7F) | 0x80);
    value >>= 7;
    ++bytes_written;
  }
  output.push_back(value & 0x7F);
  return bytes_written + 1;
}

// Calculate bits needed to represent max_value
uint8_t bits_required(uint32_t max_value) {
  if (max_value == 0)
    return 0;
  uint8_t bits = 0;
  while (max_value > 0) {
    max_value >>= 1;
    ++bits;
  }
  return bits;
}

// RLE/Bit-Packed Hybrid Encoding for definition/repetition levels
// Header format:
//   If LSB is 0: RLE run - (header >> 1) gives repeat count
//   If LSB is 1: Bit-packed run - (header >> 1) gives number of groups (each group is 8 values)

class RleEncoder {
public:
  explicit RleEncoder(uint8_t bit_width) : bit_width_(bit_width) {
    max_run_length_ = (1 << 30); // Arbitrary large limit
  }

  void encode(const uint32_t* values, size_t count, std::vector<uint8_t>& output) {
    if (count == 0 || bit_width_ == 0)
      return;

    size_t i = 0;
    while (i < count) {
      // Look for a run of repeated values
      size_t run_start = i;
      uint32_t run_value = values[i];
      size_t run_length = 1;

      while (i + run_length < count && values[i + run_length] == run_value &&
             run_length < max_run_length_) {
        ++run_length;
      }

      // Decide whether to use RLE or bit-packing
      // RLE is better for runs of 8+ identical values
      if (run_length >= 8) {
        // Use RLE encoding
        write_rle_run(run_value, run_length, output);
        i += run_length;
      } else {
        // Use bit-packing for small runs
        // Collect up to 8 values for a bit-packed group
        size_t group_size = std::min(size_t(8), count - i);
        write_bit_packed_run(values + i, group_size, output);
        i += group_size;
      }
    }
  }

private:
  void write_rle_run(uint32_t value, size_t count, std::vector<uint8_t>& output) {
    // Header: (count << 1) | 0
    uint32_t header = static_cast<uint32_t>(count) << 1;
    write_varint(header, output);

    // Value using minimum bytes for bit_width_
    size_t value_bytes = (bit_width_ + 7) / 8;
    for (size_t b = 0; b < value_bytes; ++b) {
      output.push_back((value >> (b * 8)) & 0xFF);
    }
  }

  void write_bit_packed_run(const uint32_t* values, size_t count, std::vector<uint8_t>& output) {
    // Pad to multiple of 8
    size_t groups = (count + 7) / 8;

    // Header: (groups << 1) | 1
    uint32_t header = (static_cast<uint32_t>(groups) << 1) | 1;
    write_varint(header, output);

    // Bit-pack the values
    uint64_t buffer = 0;
    int bits_in_buffer = 0;

    for (size_t i = 0; i < groups * 8; ++i) {
      uint32_t value = (i < count) ? values[i] : 0;
      buffer |= (static_cast<uint64_t>(value) << bits_in_buffer);
      bits_in_buffer += bit_width_;

      while (bits_in_buffer >= 8) {
        output.push_back(buffer & 0xFF);
        buffer >>= 8;
        bits_in_buffer -= 8;
      }
    }

    // Flush remaining bits
    if (bits_in_buffer > 0) {
      output.push_back(buffer & 0xFF);
    }
  }

  uint8_t bit_width_;
  size_t max_run_length_;
};

// Encode definition levels using RLE
void encode_definition_levels_rle(const std::vector<bool>& null_bitmap, uint8_t max_def_level,
                                  std::vector<uint8_t>& output) {
  if (null_bitmap.empty())
    return;

  uint8_t bit_width = bits_required(max_def_level);
  RleEncoder encoder(bit_width);

  // Convert null bitmap to definition levels
  // def_level = max_def_level if not null, 0 if null
  std::vector<uint32_t> def_levels(null_bitmap.size());
  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    def_levels[i] = null_bitmap[i] ? 0 : max_def_level;
  }

  encoder.encode(def_levels.data(), def_levels.size(), output);
}

} // namespace encoding
} // namespace writer
} // namespace libvroom
