#pragma once

#include "thrift_compact.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace libvroom {
namespace writer {

// Parquet physical types (from parquet.thrift)
enum class ParquetType : int32_t {
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3, // deprecated
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7
};

// Field repetition type
enum class FieldRepetitionType : int32_t { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 };

// Encoding types
enum class ParquetEncoding : int32_t {
  PLAIN = 0,
  PLAIN_DICTIONARY = 2,
  RLE = 3,
  BIT_PACKED = 4,
  DELTA_BINARY_PACKED = 5,
  DELTA_LENGTH_BYTE_ARRAY = 6,
  DELTA_BYTE_ARRAY = 7,
  RLE_DICTIONARY = 8,
  BYTE_STREAM_SPLIT = 9
};

// Compression codecs
enum class CompressionCodec : int32_t {
  UNCOMPRESSED = 0,
  SNAPPY = 1,
  GZIP = 2,
  LZO = 3,
  BROTLI = 4,
  LZ4 = 5,
  ZSTD = 6,
  LZ4_RAW = 7
};

// Page types
enum class PageType : int32_t {
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2,
  DATA_PAGE_V2 = 3
};

// Converted types (for backwards compatibility)
enum class ConvertedType : int32_t {
  UTF8 = 0,
  DATE = 6,
  TIMESTAMP_MILLIS = 9,
  TIMESTAMP_MICROS = 10,
  INT_8 = 15,
  INT_16 = 16,
  INT_32 = 17,
  INT_64 = 18
};

// Statistics for columns and pages
struct Statistics {
  std::optional<std::vector<uint8_t>> max;       // field 1
  std::optional<std::vector<uint8_t>> min;       // field 2
  std::optional<int64_t> null_count;             // field 3
  std::optional<int64_t> distinct_count;         // field 4
  std::optional<std::vector<uint8_t>> max_value; // field 5
  std::optional<std::vector<uint8_t>> min_value; // field 6

  void write(ThriftCompactWriter& writer) const;
};

// Schema element (column or group definition)
struct SchemaElement {
  std::optional<ParquetType> type;                    // field 1
  std::optional<int32_t> type_length;                 // field 2
  std::optional<FieldRepetitionType> repetition_type; // field 3
  std::string name;                                   // field 4 (required)
  std::optional<int32_t> num_children;                // field 5
  std::optional<ConvertedType> converted_type;        // field 6

  void write(ThriftCompactWriter& writer) const;
};

// Data page header (V1)
struct DataPageHeader {
  int32_t num_values;                        // field 1 (required)
  ParquetEncoding encoding;                  // field 2 (required)
  ParquetEncoding definition_level_encoding; // field 3 (required)
  ParquetEncoding repetition_level_encoding; // field 4 (required)
  std::optional<Statistics> statistics;      // field 5

  void write(ThriftCompactWriter& writer) const;
};

// Dictionary page header
struct DictionaryPageHeader {
  int32_t num_values;            // field 1 (required)
  ParquetEncoding encoding;      // field 2 (required)
  std::optional<bool> is_sorted; // field 3

  void write(ThriftCompactWriter& writer) const;
};

// Data page header V2
struct DataPageHeaderV2 {
  int32_t num_values;                    // field 1 (required)
  int32_t num_nulls;                     // field 2 (required)
  int32_t num_rows;                      // field 3 (required)
  ParquetEncoding encoding;              // field 4 (required)
  int32_t definition_levels_byte_length; // field 5 (required)
  int32_t repetition_levels_byte_length; // field 6 (required)
  std::optional<bool> is_compressed;     // field 7 (default true)
  std::optional<Statistics> statistics;  // field 8

  void write(ThriftCompactWriter& writer) const;
};

// Page header
struct PageHeader {
  PageType type;                                              // field 1 (required)
  int32_t uncompressed_page_size;                             // field 2 (required)
  int32_t compressed_page_size;                               // field 3 (required)
  std::optional<int32_t> crc;                                 // field 4
  std::optional<DataPageHeader> data_page_header;             // field 5
  std::optional<DictionaryPageHeader> dictionary_page_header; // field 7

  void write(ThriftCompactWriter& writer) const;
};

// Key-value metadata
struct KeyValue {
  std::string key;                  // field 1 (required)
  std::optional<std::string> value; // field 2

  void write(ThriftCompactWriter& writer) const;
};

// Column metadata
struct ColumnMetaData {
  ParquetType type;                              // field 1 (required)
  std::vector<ParquetEncoding> encodings;        // field 2 (required)
  std::vector<std::string> path_in_schema;       // field 3 (required)
  CompressionCodec codec;                        // field 4 (required)
  int64_t num_values;                            // field 5 (required)
  int64_t total_uncompressed_size;               // field 6 (required)
  int64_t total_compressed_size;                 // field 7 (required)
  int64_t data_page_offset;                      // field 9 (required)
  std::optional<int64_t> dictionary_page_offset; // field 11
  std::optional<Statistics> statistics;          // field 12

  void write(ThriftCompactWriter& writer) const;
};

// Column chunk
struct ColumnChunk {
  std::optional<std::string> file_path;    // field 1
  int64_t file_offset = 0;                 // field 2 (required, default 0)
  std::optional<ColumnMetaData> meta_data; // field 3

  void write(ThriftCompactWriter& writer) const;
};

// Row group
struct RowGroup {
  std::vector<ColumnChunk> columns;             // field 1 (required)
  int64_t total_byte_size;                      // field 2 (required)
  int64_t num_rows;                             // field 3 (required)
  std::optional<int64_t> file_offset;           // field 5
  std::optional<int64_t> total_compressed_size; // field 6

  void write(ThriftCompactWriter& writer) const;
};

// Type-defined order for column ordering
struct TypeDefinedOrder {
  void write(ThriftCompactWriter& writer) const;
};

// Column order (union with single variant for simplicity)
struct ColumnOrder {
  TypeDefinedOrder type_order; // field 1

  void write(ThriftCompactWriter& writer) const;
};

// File metadata (root structure)
struct FileMetaData {
  int32_t version;                                         // field 1 (required)
  std::vector<SchemaElement> schema;                       // field 2 (required)
  int64_t num_rows;                                        // field 3 (required)
  std::vector<RowGroup> row_groups;                        // field 4 (required)
  std::optional<std::vector<KeyValue>> key_value_metadata; // field 5
  std::optional<std::string> created_by;                   // field 6
  std::optional<std::vector<ColumnOrder>> column_orders;   // field 7

  void write(ThriftCompactWriter& writer) const;
};

} // namespace writer
} // namespace libvroom
