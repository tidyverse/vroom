#include "libvroom/dictionary.h"
#include "libvroom/vroom.h"

#include "columns/cow_buffer.h"

#include <cstring>

namespace libvroom {
namespace writer {

// Forward declarations from encoding modules
namespace encoding {
void encode_int32_plain(const int32_t* values, size_t count, std::vector<uint8_t>& output);
void encode_int64_plain(const int64_t* values, size_t count, std::vector<uint8_t>& output);
void encode_float64_plain(const double* values, size_t count, std::vector<uint8_t>& output);
void encode_bool_plain(const bool* values, size_t count, std::vector<uint8_t>& output);
void encode_byte_array_plain(const std::vector<std::string>& values,
                             const std::vector<bool>& null_bitmap, std::vector<uint8_t>& output);
void encode_def_levels_hybrid(const std::vector<bool>& null_bitmap, uint8_t max_def_level,
                              std::vector<uint8_t>& output);
void encode_hybrid_rle(const uint32_t* values, size_t count, uint8_t bit_width,
                       std::vector<uint8_t>& output);
} // namespace encoding

// Forward declarations for compression functions
std::vector<uint8_t> compress(const uint8_t* data, size_t size, Compression codec, int level);
void compress_into(const uint8_t* data, size_t size, Compression codec, int level,
                   std::vector<uint8_t>& output);

// Parquet page types
enum class PageType : uint8_t {
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2,
  DATA_PAGE_V2 = 3
};

// Page header structure (simplified)
struct PageHeader {
  PageType type;
  uint32_t uncompressed_page_size;
  uint32_t compressed_page_size;
  uint32_t num_values;
  Encoding encoding;
  bool has_statistics;
};

// Write a data page - returns CowByteBuffer for zero-copy sharing
CowByteBuffer write_data_page(const uint8_t* data, size_t data_size, size_t num_values,
                              const std::vector<bool>& null_bitmap, Compression compression,
                              int compression_level, Encoding encoding) {
  CowByteBuffer page_data;

  // First, write definition levels if there are nulls
  bool has_nulls = false;
  for (bool is_null : null_bitmap) {
    if (is_null) {
      has_nulls = true;
      break;
    }
  }

  if (has_nulls) {
    encoding::encode_def_levels_hybrid(null_bitmap, 1, page_data.to_mut());
  }

  // Append the actual data
  auto& buf = page_data.to_mut();
  buf.insert(buf.end(), data, data + data_size);

  // Compress if needed using buffer pooling
  if (compression != Compression::NONE) {
    std::vector<uint8_t> compressed;
    compress_into(page_data.data(), page_data.size(), compression, compression_level, compressed);

    // Only use compressed if it's actually smaller
    if (compressed.size() < page_data.size()) {
      return CowByteBuffer(std::move(compressed));
    }
  }

  // No compression or compression didn't help - return as-is (zero-copy sharing possible)
  return page_data;
}

// Write a dictionary page for strings - returns CowByteBuffer for zero-copy sharing
CowByteBuffer write_dictionary_page(const std::vector<std::string>& dictionary,
                                    Compression compression, int compression_level) {
  CowByteBuffer page_data;
  auto& buf = page_data.to_mut();

  // Encode dictionary values as plain byte arrays
  for (const auto& value : dictionary) {
    uint32_t len = static_cast<uint32_t>(value.size());
    buf.push_back(len & 0xFF);
    buf.push_back((len >> 8) & 0xFF);
    buf.push_back((len >> 16) & 0xFF);
    buf.push_back((len >> 24) & 0xFF);
    buf.insert(buf.end(), value.begin(), value.end());
  }

  // Compress if needed using buffer pooling
  if (compression != Compression::NONE) {
    std::vector<uint8_t> compressed;
    compress_into(page_data.data(), page_data.size(), compression, compression_level, compressed);
    if (compressed.size() < page_data.size()) {
      return CowByteBuffer(std::move(compressed));
    }
  }

  return page_data;
}

// Write a dictionary page for int32 values
CowByteBuffer write_int32_dictionary_page(const std::vector<int32_t>& dictionary,
                                          Compression compression, int compression_level) {
  CowByteBuffer page_data;
  auto& buf = page_data.to_mut();

  // Encode dictionary values as plain int32 (little-endian)
  for (int32_t value : dictionary) {
    buf.push_back(value & 0xFF);
    buf.push_back((value >> 8) & 0xFF);
    buf.push_back((value >> 16) & 0xFF);
    buf.push_back((value >> 24) & 0xFF);
  }

  // Compress if needed using buffer pooling
  if (compression != Compression::NONE) {
    std::vector<uint8_t> compressed;
    compress_into(page_data.data(), page_data.size(), compression, compression_level, compressed);
    if (compressed.size() < page_data.size()) {
      return CowByteBuffer(std::move(compressed));
    }
  }

  return page_data;
}

// Write a dictionary page for int64 values
CowByteBuffer write_int64_dictionary_page(const std::vector<int64_t>& dictionary,
                                          Compression compression, int compression_level) {
  CowByteBuffer page_data;
  auto& buf = page_data.to_mut();

  // Encode dictionary values as plain int64 (little-endian)
  for (int64_t value : dictionary) {
    for (int i = 0; i < 8; ++i) {
      buf.push_back((value >> (i * 8)) & 0xFF);
    }
  }

  // Compress if needed using buffer pooling
  if (compression != Compression::NONE) {
    std::vector<uint8_t> compressed;
    compress_into(page_data.data(), page_data.size(), compression, compression_level, compressed);
    if (compressed.size() < page_data.size()) {
      return CowByteBuffer(std::move(compressed));
    }
  }

  return page_data;
}

// Calculate the bit width required for the given maximum value
static uint8_t bits_required_internal(uint32_t max_value) {
  if (max_value == 0)
    return 1; // At least 1 bit needed
  uint8_t bits = 0;
  while (max_value > 0) {
    ++bits;
    max_value >>= 1;
  }
  return bits;
}

// Write dictionary-encoded data page (indices encoded with RLE)
CowByteBuffer write_dictionary_data_page(const std::vector<int32_t>& indices,
                                         const std::vector<bool>& null_bitmap,
                                         size_t dictionary_size, Compression compression,
                                         int compression_level) {
  CowByteBuffer page_data;

  // First, write definition levels if there are nulls
  bool has_nulls = false;
  for (bool is_null : null_bitmap) {
    if (is_null) {
      has_nulls = true;
      break;
    }
  }

  if (has_nulls) {
    encoding::encode_def_levels_hybrid(null_bitmap, 1, page_data.to_mut());
  }

  // Calculate bit width for indices (guard against empty dictionary)
  uint8_t bit_width =
      dictionary_size > 0 ? bits_required_internal(static_cast<uint32_t>(dictionary_size - 1)) : 1;

  // Write bit width as first byte of the data
  page_data.to_mut().push_back(bit_width);

  // Filter out null indices and convert to uint32_t for RLE encoding
  std::vector<uint32_t> non_null_indices;
  non_null_indices.reserve(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_indices.push_back(static_cast<uint32_t>(indices[i]));
    }
  }

  // Encode indices with hybrid RLE
  encoding::encode_hybrid_rle(non_null_indices.data(), non_null_indices.size(), bit_width,
                              page_data.to_mut());

  // Compress if needed using buffer pooling
  if (compression != Compression::NONE) {
    std::vector<uint8_t> compressed;
    compress_into(page_data.data(), page_data.size(), compression, compression_level, compressed);
    if (compressed.size() < page_data.size()) {
      return CowByteBuffer(std::move(compressed));
    }
  }

  return page_data;
}

// Write VarInt to output
static void write_varint_internal(uint32_t value, std::vector<uint8_t>& output) {
  while (value >= 0x80) {
    output.push_back((value & 0x7F) | 0x80);
    value >>= 7;
  }
  output.push_back(value & 0x7F);
}

// Write signed VarInt (zigzag encoded)
static void write_signed_varint_internal(int32_t value, std::vector<uint8_t>& output) {
  uint32_t encoded = static_cast<uint32_t>((value << 1) ^ (value >> 31));
  write_varint_internal(encoded, output);
}

// Write 4-byte little-endian integer
static void write_int32_le_internal(int32_t value, std::vector<uint8_t>& output) {
  output.push_back(value & 0xFF);
  output.push_back((value >> 8) & 0xFF);
  output.push_back((value >> 16) & 0xFF);
  output.push_back((value >> 24) & 0xFF);
}

// Write 8-byte little-endian integer
static void write_int64_le_internal(int64_t value, std::vector<uint8_t>& output) {
  for (int i = 0; i < 8; ++i) {
    output.push_back((value >> (i * 8)) & 0xFF);
  }
}

} // namespace writer
} // namespace libvroom
