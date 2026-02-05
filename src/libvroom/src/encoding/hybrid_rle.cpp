#include "libvroom/vroom.h"

#include <cstring>

namespace libvroom {
namespace writer {
namespace encoding {

// Hybrid RLE/Bit-Packing encoding
// This is the standard encoding for definition and repetition levels in Parquet
// It combines run-length encoding for repeated values with bit-packing for varied data

// Forward declarations
size_t write_varint(uint32_t value, std::vector<uint8_t>& output);
uint8_t bits_required(uint32_t max_value);

// Encode values using the hybrid RLE/Bit-packing scheme
// This is the complete implementation matching Parquet spec

class HybridRleEncoder {
public:
  explicit HybridRleEncoder(uint8_t bit_width, size_t initial_capacity = 1024)
      : bit_width_(bit_width), bytes_per_value_((bit_width + 7) / 8) {
    output_.reserve(initial_capacity);
    buffered_values_.reserve(8);
  }

  // Add a value to encode
  void put(uint32_t value) {
    if (buffered_values_.empty()) {
      current_value_ = value;
      repeat_count_ = 1;
      buffered_values_.push_back(value);
      return;
    }

    if (value == current_value_) {
      // Continue run
      ++repeat_count_;
      buffered_values_.push_back(value);

      if (repeat_count_ >= 8) {
        // We have a significant run - flush any non-repeated values first
        size_t non_repeated = buffered_values_.size() - repeat_count_;
        if (non_repeated > 0) {
          // Flush non-repeated values as bit-packed
          flush_bit_packed(non_repeated);
          // Keep only the repeated values
          std::vector<uint32_t> repeated(buffered_values_.end() - repeat_count_,
                                         buffered_values_.end());
          buffered_values_ = std::move(repeated);
        }
      }
    } else {
      // Run ended
      if (repeat_count_ >= 8) {
        // Flush the RLE run
        flush_rle_run();
        buffered_values_.clear();
      }

      // Start new potential run
      current_value_ = value;
      repeat_count_ = 1;
      buffered_values_.push_back(value);
    }

    // Flush when buffer is full
    if (buffered_values_.size() >= 64) {
      flush_buffered();
    }
  }

  // Finish encoding and get output
  std::vector<uint8_t> finish() {
    flush_buffered();
    return std::move(output_);
  }

  // Get output without finishing (for inspection)
  const std::vector<uint8_t>& output() const { return output_; }

private:
  void flush_buffered() {
    if (buffered_values_.empty())
      return;

    if (repeat_count_ >= 8 && buffered_values_.size() == repeat_count_) {
      // All buffered values are the same - use RLE
      flush_rle_run();
    } else if (repeat_count_ >= 8) {
      // Some bit-packed values followed by an RLE run
      size_t bit_packed_count = buffered_values_.size() - repeat_count_;
      if (bit_packed_count > 0) {
        flush_bit_packed(bit_packed_count);
      }
      flush_rle_run();
    } else {
      // All bit-packed
      flush_bit_packed(buffered_values_.size());
    }

    buffered_values_.clear();
    repeat_count_ = 0;
  }

  void flush_rle_run() {
    if (repeat_count_ == 0)
      return;

    // Header: (repeat_count << 1) | 0
    uint32_t header = static_cast<uint32_t>(repeat_count_) << 1;
    write_varint(header, output_);

    // Value in minimum bytes
    for (size_t b = 0; b < bytes_per_value_; ++b) {
      output_.push_back((current_value_ >> (b * 8)) & 0xFF);
    }

    repeat_count_ = 0;
  }

  void flush_bit_packed(size_t count) {
    if (count == 0)
      return;

    // Round up to multiple of 8
    size_t groups = (count + 7) / 8;

    // Header: (groups << 1) | 1
    uint32_t header = (static_cast<uint32_t>(groups) << 1) | 1;
    write_varint(header, output_);

    // Bit-pack values
    uint64_t buffer = 0;
    int bits_in_buffer = 0;

    for (size_t i = 0; i < groups * 8; ++i) {
      uint32_t value = (i < count) ? buffered_values_[i] : 0;
      buffer |= (static_cast<uint64_t>(value) << bits_in_buffer);
      bits_in_buffer += bit_width_;

      while (bits_in_buffer >= 8) {
        output_.push_back(buffer & 0xFF);
        buffer >>= 8;
        bits_in_buffer -= 8;
      }
    }

    if (bits_in_buffer > 0) {
      output_.push_back(buffer & 0xFF);
    }
  }

  uint8_t bit_width_;
  size_t bytes_per_value_;
  std::vector<uint8_t> output_;
  std::vector<uint32_t> buffered_values_;
  uint32_t current_value_ = 0;
  size_t repeat_count_ = 0;
};

// Main encoding function
void encode_hybrid_rle(const uint32_t* values, size_t count, uint8_t bit_width,
                       std::vector<uint8_t>& output) {
  if (count == 0)
    return;

  HybridRleEncoder encoder(bit_width);

  for (size_t i = 0; i < count; ++i) {
    encoder.put(values[i]);
  }

  auto encoded = encoder.finish();
  output.insert(output.end(), encoded.begin(), encoded.end());
}

// Convenience function for encoding definition levels
// Optimized version with null_count parameter for fast path when no nulls
void encode_def_levels_hybrid(const std::vector<bool>& null_bitmap, uint8_t max_def_level,
                              std::vector<uint8_t>& output,
                              size_t null_count // Pass null_count to enable fast path
) {
  if (null_bitmap.empty())
    return;

  uint8_t bit_width = bits_required(max_def_level);

  // First write the byte length of the encoded data (placeholder)
  size_t length_pos = output.size();
  output.resize(output.size() + 4); // 4 bytes for length

  size_t data_start = output.size();

  // FAST PATH: When there are no nulls, emit a single RLE run
  // This avoids iterating through the entire null_bitmap
  if (null_count == 0) {
    // All values have def_level = max_def_level
    // Emit as a single RLE run: header = (count << 1) | 0, then value bytes
    size_t count = null_bitmap.size();
    uint32_t header = static_cast<uint32_t>(count) << 1;
    write_varint(header, output);

    // Write value (max_def_level) in minimum bytes based on bit_width
    size_t bytes_per_value = (bit_width + 7) / 8;
    for (size_t b = 0; b < bytes_per_value; ++b) {
      output.push_back((max_def_level >> (b * 8)) & 0xFF);
    }
  } else {
    // Stream directly to encoder without materializing def_levels array
    HybridRleEncoder encoder(bit_width);
    for (size_t i = 0; i < null_bitmap.size(); ++i) {
      // def_level = 0 if null, max_def_level if not null
      encoder.put(null_bitmap[i] ? 0 : max_def_level);
    }
    auto encoded = encoder.finish();
    output.insert(output.end(), encoded.begin(), encoded.end());
  }

  // Write the actual length
  uint32_t encoded_length = static_cast<uint32_t>(output.size() - data_start);
  output[length_pos] = encoded_length & 0xFF;
  output[length_pos + 1] = (encoded_length >> 8) & 0xFF;
  output[length_pos + 2] = (encoded_length >> 16) & 0xFF;
  output[length_pos + 3] = (encoded_length >> 24) & 0xFF;
}

// Legacy version without null_count - must iterate to check
void encode_def_levels_hybrid(const std::vector<bool>& null_bitmap, uint8_t max_def_level,
                              std::vector<uint8_t>& output) {
  // Count nulls to enable fast path
  size_t null_count = 0;
  for (bool is_null : null_bitmap) {
    if (is_null)
      null_count++;
  }
  encode_def_levels_hybrid(null_bitmap, max_def_level, output, null_count);
}

} // namespace encoding
} // namespace writer
} // namespace libvroom
