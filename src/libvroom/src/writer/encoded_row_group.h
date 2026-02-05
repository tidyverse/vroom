#pragma once

#include "libvroom/vroom.h"

#include "parquet_types.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace libvroom {
namespace writer {

// Pre-encoded column data ready for writing
// This enables parallel encoding followed by sequential writing
struct EncodedColumn {
  std::vector<uint8_t> header_bytes;  // Serialized page header
  std::vector<uint8_t> content_bytes; // Page content (possibly compressed)
  int64_t num_values;
  int64_t null_count;
  int64_t uncompressed_size;
  int64_t compressed_size;
  std::optional<Statistics> statistics;
  CompressionCodec actual_codec;
  DataType data_type;
  std::string column_name;
  bool is_nullable;

  // Dictionary encoding support
  std::optional<std::vector<uint8_t>> dictionary_page_header;
  std::optional<std::vector<uint8_t>> dictionary_page_content;
  bool uses_dictionary = false;
  size_t dictionary_size = 0;
  int64_t dictionary_uncompressed_size = 0;
  int64_t dictionary_compressed_size = 0;
};

// Pre-encoded row group ready for sequential writing
// Contains all encoded column data and metadata needed to write to disk
struct EncodedRowGroup {
  // Encoded columns
  std::vector<EncodedColumn> columns;

  // Row group metadata
  int64_t num_rows;
  int64_t total_byte_size;       // Uncompressed size
  int64_t total_compressed_size; // Compressed size

  // Track source chunk index for ordering
  size_t sequence_number;
};

} // namespace writer
} // namespace libvroom
