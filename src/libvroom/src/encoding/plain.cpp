#include "libvroom/arrow_buffer.h"
#include "libvroom/vroom.h"

#include <cstring>
#include <string_view>

namespace libvroom {
namespace writer {
namespace encoding {

// Plain encoding for Parquet
// Just writes raw bytes without any compression or encoding

// Encode int32 values as little-endian bytes
void encode_int32_plain(const int32_t* values, size_t count, std::vector<uint8_t>& output) {
  size_t start = output.size();
  output.resize(start + count * sizeof(int32_t));

  // On little-endian systems, we can memcpy directly
  std::memcpy(output.data() + start, values, count * sizeof(int32_t));
}

// Encode int32 values as little-endian bytes, skipping null values
void encode_int32_plain(const std::vector<int32_t>& values, const std::vector<bool>& null_bitmap,
                        std::vector<uint8_t>& output) {
  // Fast path: if no null bitmap or all values are non-null, use bulk copy
  if (null_bitmap.empty()) {
    encode_int32_plain(values.data(), values.size(), output);
    return;
  }

  // Count non-null values to reserve space
  size_t non_null_count = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      non_null_count++;
    }
  }

  // Another fast path: all values are non-null
  if (non_null_count == values.size()) {
    encode_int32_plain(values.data(), values.size(), output);
    return;
  }

  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(int32_t));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      std::memcpy(dest, &values[i], sizeof(int32_t));
      dest += sizeof(int32_t);
    }
  }
}

// Encode int64 values as little-endian bytes
void encode_int64_plain(const int64_t* values, size_t count, std::vector<uint8_t>& output) {
  size_t start = output.size();
  output.resize(start + count * sizeof(int64_t));

  std::memcpy(output.data() + start, values, count * sizeof(int64_t));
}

// Encode int64 values as little-endian bytes, skipping null values
void encode_int64_plain(const std::vector<int64_t>& values, const std::vector<bool>& null_bitmap,
                        std::vector<uint8_t>& output) {
  // Fast path: if no null bitmap or all values are non-null, use bulk copy
  if (null_bitmap.empty()) {
    encode_int64_plain(values.data(), values.size(), output);
    return;
  }

  // Count non-null values to reserve space
  size_t non_null_count = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      non_null_count++;
    }
  }

  // Another fast path: all values are non-null
  if (non_null_count == values.size()) {
    encode_int64_plain(values.data(), values.size(), output);
    return;
  }

  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(int64_t));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      std::memcpy(dest, &values[i], sizeof(int64_t));
      dest += sizeof(int64_t);
    }
  }
}

// Encode float64 (double) values as little-endian bytes
void encode_float64_plain(const double* values, size_t count, std::vector<uint8_t>& output) {
  size_t start = output.size();
  output.resize(start + count * sizeof(double));

  std::memcpy(output.data() + start, values, count * sizeof(double));
}

// Encode float64 values as little-endian bytes, skipping null values
void encode_float64_plain(const std::vector<double>& values, const std::vector<bool>& null_bitmap,
                          std::vector<uint8_t>& output) {
  // Fast path: if no null bitmap or all values are non-null, use bulk copy
  if (null_bitmap.empty()) {
    encode_float64_plain(values.data(), values.size(), output);
    return;
  }

  // Count non-null values to reserve space
  size_t non_null_count = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      non_null_count++;
    }
  }

  // Another fast path: all values are non-null
  if (non_null_count == values.size()) {
    encode_float64_plain(values.data(), values.size(), output);
    return;
  }

  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(double));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap[i]) {
      std::memcpy(dest, &values[i], sizeof(double));
      dest += sizeof(double);
    }
  }
}

// Encode boolean values as packed bits
void encode_bool_plain(const bool* values, size_t count, std::vector<uint8_t>& output) {
  size_t num_bytes = (count + 7) / 8;
  size_t start = output.size();
  output.resize(start + num_bytes, 0);

  for (size_t i = 0; i < count; ++i) {
    if (values[i]) {
      output[start + i / 8] |= (1 << (i % 8));
    }
  }
}

// Encode boolean values as packed bits, skipping null values
void encode_bool_plain(const std::vector<bool>& values, const std::vector<bool>& null_bitmap,
                       std::vector<uint8_t>& output) {
  // Count non-null values to determine output size
  size_t non_null_count = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_count++;
    }
  }

  size_t num_bytes = (non_null_count + 7) / 8;
  size_t start = output.size();
  output.resize(start + num_bytes, 0);

  size_t bit_idx = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      if (values[i]) {
        output[start + bit_idx / 8] |= (1 << (bit_idx % 8));
      }
      bit_idx++;
    }
  }
}

// Encode string values with length prefixes
// Format: [4-byte length][string bytes] for each string
void encode_string_plain(const std::vector<std::string>& values, std::vector<uint8_t>& output) {
  // Calculate total size needed
  size_t total_size = 0;
  for (const auto& s : values) {
    total_size += 4 + s.size(); // 4-byte length prefix + string data
  }

  size_t start = output.size();
  output.resize(start + total_size);
  uint8_t* dest = output.data() + start;

  for (const auto& s : values) {
    // Write length as 4-byte little-endian using memcpy
    uint32_t len = static_cast<uint32_t>(s.size());
    std::memcpy(dest, &len, sizeof(len));
    dest += sizeof(len);

    // Write string data using memcpy
    std::memcpy(dest, s.data(), s.size());
    dest += s.size();
  }
}

// Encode byte array values (similar to strings but without length prefix array)
void encode_byte_array_plain(const std::vector<std::string>& values,
                             const std::vector<bool>& null_bitmap, std::vector<uint8_t>& output) {
  // Pre-calculate total size for non-null values
  size_t total_size = 0;
  bool has_nulls = !null_bitmap.empty();
  for (size_t i = 0; i < values.size(); ++i) {
    if (!has_nulls || !null_bitmap[i]) {
      total_size += 4 + values[i].size(); // 4-byte length prefix + string data
    }
  }

  size_t start = output.size();
  output.resize(start + total_size);
  uint8_t* dest = output.data() + start;

  // Write directly using memcpy
  for (size_t i = 0; i < values.size(); ++i) {
    if (!has_nulls || !null_bitmap[i]) {
      const auto& s = values[i];
      uint32_t len = static_cast<uint32_t>(s.size());

      // Write length as 4-byte little-endian using memcpy
      std::memcpy(dest, &len, sizeof(len));
      dest += sizeof(len);

      // Write string data using memcpy
      std::memcpy(dest, s.data(), s.size());
      dest += s.size();
    }
  }
}

// ============================================================================
// Arrow buffer encoding functions
// These work directly with contiguous Arrow-style buffers for better performance
// ============================================================================

// Encode int32 values from NumericBuffer, skipping null values using NullBitmap
void encode_int32_plain_arrow(const NumericBuffer<int32_t>& values, const NullBitmap& nulls,
                              std::vector<uint8_t>& output) {
  // Fast path: if no nulls, use bulk copy
  size_t null_count = nulls.null_count_fast();
  if (null_count == 0) {
    size_t start = output.size();
    size_t count = values.size();
    output.resize(start + count * sizeof(int32_t));
    std::memcpy(output.data() + start, values.data(), count * sizeof(int32_t));
    return;
  }

  // With nulls: skip null values
  size_t non_null_count = values.size() - null_count;
  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(int32_t));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      std::memcpy(dest, values.data() + i, sizeof(int32_t));
      dest += sizeof(int32_t);
    }
  }
}

// Encode int64 values from NumericBuffer, skipping null values using NullBitmap
void encode_int64_plain_arrow(const NumericBuffer<int64_t>& values, const NullBitmap& nulls,
                              std::vector<uint8_t>& output) {
  // Fast path: if no nulls, use bulk copy
  size_t null_count = nulls.null_count_fast();
  if (null_count == 0) {
    size_t start = output.size();
    size_t count = values.size();
    output.resize(start + count * sizeof(int64_t));
    std::memcpy(output.data() + start, values.data(), count * sizeof(int64_t));
    return;
  }

  // With nulls: skip null values
  size_t non_null_count = values.size() - null_count;
  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(int64_t));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      std::memcpy(dest, values.data() + i, sizeof(int64_t));
      dest += sizeof(int64_t);
    }
  }
}

// Encode float64 values from NumericBuffer, skipping null values using NullBitmap
void encode_float64_plain_arrow(const NumericBuffer<double>& values, const NullBitmap& nulls,
                                std::vector<uint8_t>& output) {
  // Fast path: if no nulls, use bulk copy
  size_t null_count = nulls.null_count_fast();
  if (null_count == 0) {
    size_t start = output.size();
    size_t count = values.size();
    output.resize(start + count * sizeof(double));
    std::memcpy(output.data() + start, values.data(), count * sizeof(double));
    return;
  }

  // With nulls: skip null values
  size_t non_null_count = values.size() - null_count;
  size_t start = output.size();
  output.resize(start + non_null_count * sizeof(double));

  uint8_t* dest = output.data() + start;
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      std::memcpy(dest, values.data() + i, sizeof(double));
      dest += sizeof(double);
    }
  }
}

// Encode bool values from NumericBuffer<uint8_t>, skipping null values using NullBitmap
void encode_bool_plain_arrow(const NumericBuffer<uint8_t>& values, const NullBitmap& nulls,
                             std::vector<uint8_t>& output) {
  // Count non-null values to determine output size
  size_t null_count = nulls.null_count_fast();
  size_t non_null_count = values.size() - null_count;

  size_t num_bytes = (non_null_count + 7) / 8;
  size_t start = output.size();
  output.resize(start + num_bytes, 0);

  size_t bit_idx = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      if (values.get(i)) {
        output[start + bit_idx / 8] |= (1 << (bit_idx % 8));
      }
      bit_idx++;
    }
  }
}

// Encode string values from StringBuffer, skipping null values using NullBitmap
void encode_byte_array_plain_arrow(const StringBuffer& values, const NullBitmap& nulls,
                                   std::vector<uint8_t>& output) {
  // Pre-calculate total size for non-null values
  size_t total_size = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      total_size += 4 + values.length(i); // 4-byte length prefix + string data
    }
  }

  size_t start = output.size();
  output.resize(start + total_size);
  uint8_t* dest = output.data() + start;

  // Write directly using memcpy
  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      std::string_view s = values.get(i);
      uint32_t len = static_cast<uint32_t>(s.size());

      // Write length as 4-byte little-endian using memcpy
      std::memcpy(dest, &len, sizeof(len));
      dest += sizeof(len);

      // Write string data using memcpy
      std::memcpy(dest, s.data(), s.size());
      dest += s.size();
    }
  }
}

// Forward declarations from rle.cpp
extern size_t write_varint(uint32_t value, std::vector<uint8_t>& output);
extern uint8_t bits_required(uint32_t max_value);

// Encode definition levels from NullBitmap using hybrid RLE
// OPTIMIZED: Direct encoding without intermediate vector<bool>
void encode_def_levels_hybrid_arrow(const NullBitmap& nulls, uint8_t max_def_level,
                                    std::vector<uint8_t>& output, size_t null_count) {
  if (nulls.size() == 0)
    return;

  size_t count = nulls.size();

  // ULTRA-FAST PATH: No nulls with max_def_level=1 (the common case)
  // Pre-compute exact bytes needed: 4-byte length prefix + varint header + 1-byte value
  // For count up to 2^21-1 (~2M rows), varint is at most 3 bytes, so total data is 4 bytes max
  // This avoids function call overhead and loops entirely
  if ((null_count == 0 || !nulls.has_nulls()) && max_def_level == 1) {
    // RLE header = count << 1 (LSB=0 for RLE), value = 1
    uint32_t header = static_cast<uint32_t>(count) << 1;

    // Calculate varint size and total encoded length
    size_t varint_size;
    if (header < 0x80) {
      varint_size = 1;
    } else if (header < 0x4000) {
      varint_size = 2;
    } else if (header < 0x200000) {
      varint_size = 3;
    } else if (header < 0x10000000) {
      varint_size = 4;
    } else {
      varint_size = 5;
    }

    // Total encoded data: varint header + 1 byte value
    uint32_t encoded_length = static_cast<uint32_t>(varint_size + 1);

    // Pre-allocate exact space needed: 4-byte length prefix + encoded data
    size_t start_pos = output.size();
    output.resize(start_pos + 4 + encoded_length);
    uint8_t* ptr = output.data() + start_pos;

    // Write 4-byte little-endian length prefix
    ptr[0] = encoded_length & 0xFF;
    ptr[1] = (encoded_length >> 8) & 0xFF;
    ptr[2] = (encoded_length >> 16) & 0xFF;
    ptr[3] = (encoded_length >> 24) & 0xFF;
    ptr += 4;

    // Write varint header inline
    uint32_t val = header;
    while (val >= 0x80) {
      *ptr++ = (val & 0x7F) | 0x80;
      val >>= 7;
    }
    *ptr++ = val & 0x7F;

    // Write value byte (always 1 for max_def_level=1)
    *ptr = 1;

    return;
  }

  uint8_t bit_width = bits_required(max_def_level);
  size_t bytes_per_value = (bit_width + 7) / 8;

  // Reserve space for length prefix (4 bytes)
  size_t length_pos = output.size();
  output.resize(output.size() + 4);

  size_t data_start = output.size();

  // FAST PATH: When there are no nulls (but max_def_level > 1), emit a single RLE run
  if (null_count == 0 || !nulls.has_nulls()) {
    // All values have def_level = max_def_level
    // Emit as a single RLE run: header = (count << 1) | 0, then value bytes
    uint32_t header = static_cast<uint32_t>(count) << 1;
    write_varint(header, output);

    // Write value (max_def_level) in minimum bytes based on bit_width
    for (size_t b = 0; b < bytes_per_value; ++b) {
      output.push_back((max_def_level >> (b * 8)) & 0xFF);
    }
  } else {
    // SLOW PATH: Has nulls - encode directly from NullBitmap
    // Use a simplified approach: emit values in bit-packed groups
    // This is faster than creating an intermediate vector<bool>

    size_t num_values = nulls.size();
    size_t i = 0;

    while (i < num_values) {
      // Check for a run of identical values
      bool is_valid_start = nulls.is_valid(i);
      uint32_t run_value = is_valid_start ? max_def_level : 0;
      size_t run_length = 1;

      while (i + run_length < num_values && run_length < (1 << 30)) {
        bool is_valid_next = nulls.is_valid(i + run_length);
        uint32_t next_value = is_valid_next ? max_def_level : 0;
        if (next_value != run_value)
          break;
        ++run_length;
      }

      if (run_length >= 8) {
        // Use RLE encoding for runs of 8+
        uint32_t header = static_cast<uint32_t>(run_length) << 1;
        write_varint(header, output);
        for (size_t b = 0; b < bytes_per_value; ++b) {
          output.push_back((run_value >> (b * 8)) & 0xFF);
        }
        i += run_length;
      } else {
        // Use bit-packing for small runs (groups of 8)
        size_t group_count = std::min(size_t(8), num_values - i);
        size_t groups = 1; // Always 1 group of 8 values

        // Header: (groups << 1) | 1
        uint32_t header = (static_cast<uint32_t>(groups) << 1) | 1;
        write_varint(header, output);

        // Bit-pack 8 values
        uint64_t buffer = 0;
        int bits_in_buffer = 0;

        for (size_t j = 0; j < 8; ++j) {
          uint32_t value = 0;
          if (i + j < num_values) {
            value = nulls.is_valid(i + j) ? max_def_level : 0;
          }
          buffer |= (static_cast<uint64_t>(value) << bits_in_buffer);
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

        i += group_count;
      }
    }
  }

  // Write the actual length
  uint32_t encoded_length = static_cast<uint32_t>(output.size() - data_start);
  output[length_pos] = encoded_length & 0xFF;
  output[length_pos + 1] = (encoded_length >> 8) & 0xFF;
  output[length_pos + 2] = (encoded_length >> 16) & 0xFF;
  output[length_pos + 3] = (encoded_length >> 24) & 0xFF;
}

// ============================================================================
// Dictionary encoding functions
// ============================================================================

// Encode string dictionary page (just the unique values)
// Format: [4-byte length][string bytes] for each dictionary entry
void encode_dictionary_page_strings(const std::vector<std::string_view>& dictionary,
                                    std::vector<uint8_t>& output) {
  // Calculate total size
  size_t total_size = 0;
  for (const auto& s : dictionary) {
    total_size += 4 + s.size(); // 4-byte length prefix + string data
  }

  size_t start = output.size();
  output.resize(start + total_size);
  uint8_t* dest = output.data() + start;

  for (const auto& s : dictionary) {
    uint32_t len = static_cast<uint32_t>(s.size());
    std::memcpy(dest, &len, sizeof(len));
    dest += sizeof(len);
    std::memcpy(dest, s.data(), s.size());
    dest += s.size();
  }
}

// Encode dictionary indices using RLE/Bit-Packed Hybrid
// This encodes indices for non-null values only (nulls handled by def levels)
// Format: bit-width byte followed by RLE/bit-packed encoded indices
void encode_dictionary_indices(const std::vector<int32_t>& indices, const NullBitmap& nulls,
                               uint8_t bit_width, std::vector<uint8_t>& output) {
  if (indices.empty())
    return;

  // Count non-null values
  size_t null_count = nulls.null_count_fast();
  size_t non_null_count = indices.size() - null_count;

  if (non_null_count == 0)
    return;

  // Write bit width as first byte (required by Parquet spec for dictionary data pages)
  output.push_back(bit_width);

  // Collect non-null indices
  std::vector<uint32_t> non_null_indices;
  non_null_indices.reserve(non_null_count);

  for (size_t i = 0; i < indices.size(); ++i) {
    if (nulls.is_valid(i)) {
      non_null_indices.push_back(static_cast<uint32_t>(indices[i]));
    }
  }

  // Use the existing HybridRleEncoder from hybrid_rle.cpp
  // We need to implement encoding inline here since HybridRleEncoder is local to that file

  size_t bytes_per_value = (bit_width + 7) / 8;

  // Process indices and encode with RLE/bit-packing
  size_t i = 0;
  while (i < non_null_indices.size()) {
    // Check for a run of identical values
    uint32_t run_value = non_null_indices[i];
    size_t run_length = 1;

    while (i + run_length < non_null_indices.size() &&
           non_null_indices[i + run_length] == run_value && run_length < (1UL << 30)) {
      ++run_length;
    }

    if (run_length >= 8) {
      // Use RLE encoding for runs of 8+
      uint32_t header = static_cast<uint32_t>(run_length) << 1;
      write_varint(header, output);
      for (size_t b = 0; b < bytes_per_value; ++b) {
        output.push_back((run_value >> (b * 8)) & 0xFF);
      }
      i += run_length;
    } else {
      // Use bit-packing for small runs (groups of 8)
      size_t remaining = non_null_indices.size() - i;
      size_t group_count = std::min(size_t(8), remaining);

      // Header: (1 << 1) | 1 = 3 (1 group of 8 values)
      uint32_t header = (1 << 1) | 1;
      write_varint(header, output);

      // Bit-pack 8 values
      uint64_t buffer = 0;
      int bits_in_buffer = 0;

      for (size_t j = 0; j < 8; ++j) {
        uint32_t value = 0;
        if (i + j < non_null_indices.size()) {
          value = non_null_indices[i + j];
        }
        buffer |= (static_cast<uint64_t>(value) << bits_in_buffer);
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

      i += group_count;
    }
  }
}

} // namespace encoding
} // namespace writer
} // namespace libvroom
