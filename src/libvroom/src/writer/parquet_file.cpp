#include "libvroom/arrow_column_builder.h"
#include "libvroom/dictionary.h"
#include "libvroom/vroom.h"

#include "BS_thread_pool.hpp"
#include "encoded_row_group.h"
#include "encoded_row_group_queue.h"
#include "parquet_types.h"
#include "thrift_compact.h"

#include <atomic>
#include <cstring>
#include <fstream>
#include <future>
#include <string_view>
#include <thread>

namespace libvroom {

// Parquet magic bytes
static constexpr char PARQUET_MAGIC[] = "PAR1";

// Forward declarations for encoding
namespace writer {
namespace encoding {
void encode_int32_plain(const int32_t* values, size_t count, std::vector<uint8_t>& output);
void encode_int32_plain(const std::vector<int32_t>& values, const std::vector<bool>& null_bitmap,
                        std::vector<uint8_t>& output);
void encode_int64_plain(const int64_t* values, size_t count, std::vector<uint8_t>& output);
void encode_int64_plain(const std::vector<int64_t>& values, const std::vector<bool>& null_bitmap,
                        std::vector<uint8_t>& output);
void encode_float64_plain(const double* values, size_t count, std::vector<uint8_t>& output);
void encode_float64_plain(const std::vector<double>& values, const std::vector<bool>& null_bitmap,
                          std::vector<uint8_t>& output);
void encode_bool_plain(const bool* values, size_t count, std::vector<uint8_t>& output);
void encode_bool_plain(const std::vector<bool>& values, const std::vector<bool>& null_bitmap,
                       std::vector<uint8_t>& output);
void encode_byte_array_plain(const std::vector<std::string>& values,
                             const std::vector<bool>& null_bitmap, std::vector<uint8_t>& output);
void encode_def_levels_hybrid(const std::vector<bool>& null_bitmap, uint8_t max_def_level,
                              std::vector<uint8_t>& output, size_t null_count);

// Arrow buffer encoding functions
void encode_int32_plain_arrow(const NumericBuffer<int32_t>& values, const NullBitmap& nulls,
                              std::vector<uint8_t>& output);
void encode_int64_plain_arrow(const NumericBuffer<int64_t>& values, const NullBitmap& nulls,
                              std::vector<uint8_t>& output);
void encode_float64_plain_arrow(const NumericBuffer<double>& values, const NullBitmap& nulls,
                                std::vector<uint8_t>& output);
void encode_bool_plain_arrow(const NumericBuffer<uint8_t>& values, const NullBitmap& nulls,
                             std::vector<uint8_t>& output);
void encode_byte_array_plain_arrow(const StringBuffer& values, const NullBitmap& nulls,
                                   std::vector<uint8_t>& output);
void encode_def_levels_hybrid_arrow(const NullBitmap& nulls, uint8_t max_def_level,
                                    std::vector<uint8_t>& output, size_t null_count);

// Dictionary encoding functions
void encode_dictionary_page_strings(const std::vector<std::string_view>& dictionary,
                                    std::vector<uint8_t>& output);
void encode_dictionary_indices(const std::vector<int32_t>& indices, const NullBitmap& nulls,
                               uint8_t bit_width, std::vector<uint8_t>& output);

// Helper function (from rle.cpp)
uint8_t bits_required(uint32_t max_value);
} // namespace encoding

// Compression
std::vector<uint8_t> compress(const uint8_t* data, size_t size, Compression codec, int level);

// Convert vroom types to Parquet types
ParquetType to_parquet_type(DataType type) {
  switch (type) {
  case DataType::BOOL:
    return ParquetType::BOOLEAN;
  case DataType::INT32:
    return ParquetType::INT32;
  case DataType::INT64:
    return ParquetType::INT64;
  case DataType::DATE:
    return ParquetType::INT32; // DATE is INT32 days since epoch
  case DataType::TIMESTAMP:
    return ParquetType::INT64; // TIMESTAMP is INT64 microseconds
  case DataType::FLOAT64:
    return ParquetType::DOUBLE;
  case DataType::STRING:
  default:
    return ParquetType::BYTE_ARRAY;
  }
}

CompressionCodec to_parquet_codec(Compression c) {
  switch (c) {
  case Compression::NONE:
    return CompressionCodec::UNCOMPRESSED;
  case Compression::GZIP:
    return CompressionCodec::GZIP;
  case Compression::SNAPPY:
    return CompressionCodec::SNAPPY;
  case Compression::ZSTD:
    return CompressionCodec::ZSTD;
  case Compression::LZ4:
    return CompressionCodec::LZ4;
  default:
    return CompressionCodec::UNCOMPRESSED;
  }
}

// Type alias for statistics value variant
using StatValue = std::variant<std::monostate, bool, int32_t, int64_t, double, std::string>;

// Helper to serialize statistics for min/max values
std::vector<uint8_t> serialize_stat_value(const StatValue& val, DataType type) {
  std::vector<uint8_t> result;
  std::visit(
      [&result](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          // Empty - do nothing
        } else if constexpr (std::is_same_v<T, int32_t>) {
          result.resize(4);
          std::memcpy(result.data(), &v, 4);
        } else if constexpr (std::is_same_v<T, int64_t>) {
          result.resize(8);
          std::memcpy(result.data(), &v, 8);
        } else if constexpr (std::is_same_v<T, double>) {
          result.resize(8);
          std::memcpy(result.data(), &v, 8);
        } else if constexpr (std::is_same_v<T, bool>) {
          result.push_back(v ? 1 : 0);
        } else if constexpr (std::is_same_v<T, std::string>) {
          result.insert(result.end(), v.begin(), v.end());
        }
      },
      val);
  return result;
}

// Column write result containing offset and size info
struct ColumnWriteResult {
  int64_t data_page_offset;
  int64_t total_compressed_size;
  int64_t total_uncompressed_size;
  int64_t num_values;
  int64_t null_count;
  std::optional<Statistics> statistics;
  CompressionCodec actual_codec; // Track actual codec used
};

} // namespace writer

// Encode a single column (thread-safe, no I/O)
// Returns encoded data ready to be written to disk
// Uses chunk-based access to avoid expensive cache concatenation
static writer::EncodedColumn encode_column(const ColumnBuilder& column,
                                           const ColumnSchema& col_schema,
                                           const ParquetOptions& options) {
  writer::EncodedColumn result;
  result.num_values = static_cast<int64_t>(column.size());
  result.null_count = 0;
  result.data_type = column.type();
  result.column_name = col_schema.name;
  result.is_nullable = col_schema.nullable;

  // Finalize to ensure all data is in chunks
  const_cast<ColumnBuilder&>(column).finalize();

  // Count nulls by iterating over chunks (avoids cache concatenation)
  const size_t num_chunks = column.num_chunks();
  for (size_t c = 0; c < num_chunks; ++c) {
    const auto& null_bitmap = column.chunk_null_bitmap(c);
    for (bool is_null : null_bitmap) {
      if (is_null)
        result.null_count++;
    }
  }

  // Encode the data based on type
  std::vector<uint8_t> encoded_data;
  std::vector<uint8_t> def_levels;

  // For OPTIONAL (nullable) columns, we must always encode definition levels
  // We need to build a combined null bitmap for the RLE encoder
  bool is_nullable_col = col_schema.nullable;
  if (is_nullable_col && num_chunks > 0) {
    // Build combined null bitmap for def level encoding
    // This is needed because the RLE encoder expects contiguous data
    std::vector<bool> combined_nulls;
    combined_nulls.reserve(column.size());
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& chunk_nulls = column.chunk_null_bitmap(c);
      combined_nulls.insert(combined_nulls.end(), chunk_nulls.begin(), chunk_nulls.end());
    }
    writer::encoding::encode_def_levels_hybrid(combined_nulls, 1, def_levels, result.null_count);
  }

  DataType type = column.type();

  // Pre-calculate output size to reserve buffer (avoids reallocations)
  size_t estimated_size = 0;
  size_t non_null_count = result.num_values - result.null_count;

  switch (type) {
  case DataType::STRING: {
    // For strings: 4 bytes length prefix + string data per non-null value
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values =
          *static_cast<const std::vector<std::string>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      for (size_t i = 0; i < values.size(); ++i) {
        if (nulls.empty() || !nulls[i]) {
          estimated_size += 4 + values[i].size();
        }
      }
    }
    break;
  }
  case DataType::INT32:
  case DataType::DATE:
    estimated_size = non_null_count * sizeof(int32_t);
    break;
  case DataType::INT64:
  case DataType::TIMESTAMP:
    estimated_size = non_null_count * sizeof(int64_t);
    break;
  case DataType::FLOAT64:
    estimated_size = non_null_count * sizeof(double);
    break;
  case DataType::BOOL:
    estimated_size = (non_null_count + 7) / 8; // Bit-packed
    break;
  default:
    break;
  }
  encoded_data.reserve(estimated_size);

  // Encode each chunk and append to encoded_data
  // This avoids the expensive cache concatenation for raw values
  switch (type) {
  case DataType::STRING: {
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values =
          *static_cast<const std::vector<std::string>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_byte_array_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::INT32: {
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<int32_t>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_int32_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::INT64: {
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<int64_t>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_int64_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::DATE: {
    // DATE is stored as INT32 (days since epoch)
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<int32_t>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_int32_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::TIMESTAMP: {
    // TIMESTAMP is stored as INT64 (microseconds since epoch)
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<int64_t>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_int64_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::FLOAT64: {
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<double>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_float64_plain(values, nulls, encoded_data);
    }
    break;
  }
  case DataType::BOOL: {
    for (size_t c = 0; c < num_chunks; ++c) {
      const auto& values = *static_cast<const std::vector<bool>*>(column.chunk_raw_values(c));
      const auto& nulls = column.chunk_null_bitmap(c);
      writer::encoding::encode_bool_plain(values, nulls, encoded_data);
    }
    break;
  }
  default:
    break;
  }

  // Combine definition levels and data
  std::vector<uint8_t> page_content;
  page_content.reserve(def_levels.size() + encoded_data.size());
  page_content.insert(page_content.end(), def_levels.begin(), def_levels.end());
  page_content.insert(page_content.end(), encoded_data.begin(), encoded_data.end());

  int32_t uncompressed_size = static_cast<int32_t>(page_content.size());
  int32_t compressed_size = uncompressed_size;

  // Compress if needed
  std::vector<uint8_t> compressed_content;
  result.actual_codec = writer::CompressionCodec::UNCOMPRESSED;
  if (options.compression != Compression::NONE) {
    compressed_content = writer::compress(page_content.data(), page_content.size(),
                                          options.compression, options.compression_level);
    if (compressed_content.size() < page_content.size()) {
      compressed_size = static_cast<int32_t>(compressed_content.size());
      result.actual_codec = writer::to_parquet_codec(options.compression);
    } else {
      compressed_content.clear();
    }
  }

  // Build page header
  writer::PageHeader page_header;
  page_header.type = writer::PageType::DATA_PAGE;
  page_header.uncompressed_page_size = uncompressed_size;
  page_header.compressed_page_size = compressed_size;

  writer::DataPageHeader data_header;
  data_header.num_values = static_cast<int32_t>(column.size());
  data_header.encoding = writer::ParquetEncoding::PLAIN;
  data_header.definition_level_encoding = writer::ParquetEncoding::RLE;
  data_header.repetition_level_encoding = writer::ParquetEncoding::RLE;

  // Add statistics if enabled
  if (options.write_statistics) {
    auto col_stats = column.statistics();
    writer::Statistics stats;
    stats.null_count = col_stats.null_count;

    if (col_stats.min_value.index() != 0 || std::holds_alternative<bool>(col_stats.min_value)) {
      stats.min_value = writer::serialize_stat_value(col_stats.min_value, type);
      stats.max_value = writer::serialize_stat_value(col_stats.max_value, type);
    }

    data_header.statistics = stats;
    result.statistics = stats;
  }

  page_header.data_page_header = data_header;

  // Serialize page header
  writer::ThriftCompactWriter header_writer(result.header_bytes);
  page_header.write(header_writer);

  // Store content (use compressed if smaller)
  result.content_bytes =
      compressed_content.empty() ? std::move(page_content) : std::move(compressed_content);
  result.uncompressed_size = uncompressed_size;
  result.compressed_size = compressed_size;

  return result;
}

// Encode a single Arrow column (thread-safe, no I/O)
// Returns encoded data ready to be written to disk
static writer::EncodedColumn encode_column_arrow(const ArrowColumnBuilder& column,
                                                 const ColumnSchema& col_schema,
                                                 const ParquetOptions& options) {
  writer::EncodedColumn result;
  result.num_values = static_cast<int64_t>(column.size());
  result.null_count = static_cast<int64_t>(column.null_count());
  result.data_type = column.type();
  result.column_name = col_schema.name;
  result.is_nullable = col_schema.nullable;

  const NullBitmap& nulls = column.null_bitmap();

  DataType type = column.type();
  size_t non_null_count = result.num_values - result.null_count;
  bool is_nullable_col = col_schema.nullable;

  // Track the encoding used for the data page
  writer::ParquetEncoding data_encoding = writer::ParquetEncoding::PLAIN;

  // Pre-calculate output size to reserve buffer for page_content
  // This includes space for both def_levels and encoded_data
  size_t estimated_data_size = 0;
  switch (type) {
  case DataType::STRING: {
    const auto& builder = static_cast<const ArrowStringColumnBuilder&>(column);
    const auto& values = builder.values();
    // For strings: 4 bytes length prefix + string data per non-null value
    for (size_t i = 0; i < values.size(); ++i) {
      if (nulls.is_valid(i)) {
        estimated_data_size += 4 + values.length(i);
      }
    }
    break;
  }
  case DataType::INT32:
  case DataType::DATE:
    estimated_data_size = non_null_count * sizeof(int32_t);
    break;
  case DataType::INT64:
  case DataType::TIMESTAMP:
    estimated_data_size = non_null_count * sizeof(int64_t);
    break;
  case DataType::FLOAT64:
    estimated_data_size = non_null_count * sizeof(double);
    break;
  case DataType::BOOL:
    estimated_data_size = (non_null_count + 7) / 8;
    break;
  default:
    break;
  }

  // Estimate def_levels size: 4 byte length prefix + varint header (up to 5 bytes) + value bytes
  // For RLE encoding with no nulls, this is minimal (~10 bytes)
  // With nulls, worst case is bit-packed which is ~(num_values/8 + overhead)
  size_t estimated_def_levels_size = is_nullable_col ? (16 + (result.num_values + 7) / 8) : 0;

  // Create single page_content buffer and write directly to it
  std::vector<uint8_t> page_content;
  page_content.reserve(estimated_def_levels_size + estimated_data_size);

  // For OPTIONAL (nullable) columns, encode definition levels directly to page_content
  if (is_nullable_col) {
    writer::encoding::encode_def_levels_hybrid_arrow(nulls, 1, page_content, result.null_count);
  }

  // Encode data directly to page_content based on type
  switch (type) {
  case DataType::STRING: {
    const auto& builder = static_cast<const ArrowStringColumnBuilder&>(column);
    const auto& string_values = builder.values();

    // Analyze dictionary viability if dictionary encoding is enabled
    if (options.enable_dictionary && string_values.size() > 0) {
      auto analysis = writer::analyze_string_dictionary_arrow(string_values, nulls,
                                                              options.dictionary_ratio_threshold);

      if (analysis.should_use_dictionary) {
        // Use dictionary encoding
        // 1. Create dictionary
        auto [dict_entries, indices] = writer::create_string_dictionary_arrow(string_values, nulls);

        // 2. Encode dictionary page (unique values)
        std::vector<uint8_t> dict_page_raw;
        std::vector<std::string_view> dict_views;
        dict_views.reserve(dict_entries.size());
        for (const auto& s : dict_entries) {
          dict_views.push_back(s);
        }
        writer::encoding::encode_dictionary_page_strings(dict_views, dict_page_raw);

        // 3. Compress dictionary page if needed
        int32_t dict_uncompressed = static_cast<int32_t>(dict_page_raw.size());
        int32_t dict_compressed = dict_uncompressed;
        std::vector<uint8_t> dict_page_compressed;

        if (options.compression != Compression::NONE) {
          dict_page_compressed = writer::compress(dict_page_raw.data(), dict_page_raw.size(),
                                                  options.compression, options.compression_level);
          if (dict_page_compressed.size() < dict_page_raw.size()) {
            dict_compressed = static_cast<int32_t>(dict_page_compressed.size());
          } else {
            dict_page_compressed.clear();
          }
        }

        // 4. Build dictionary page header
        writer::PageHeader dict_page_header;
        dict_page_header.type = writer::PageType::DICTIONARY_PAGE;
        dict_page_header.uncompressed_page_size = dict_uncompressed;
        dict_page_header.compressed_page_size = dict_compressed;

        writer::DictionaryPageHeader dict_header;
        dict_header.num_values = static_cast<int32_t>(dict_entries.size());
        dict_header.encoding = writer::ParquetEncoding::PLAIN;
        dict_page_header.dictionary_page_header = dict_header;

        // Serialize dictionary page header
        result.dictionary_page_header.emplace();
        writer::ThriftCompactWriter dict_header_writer(*result.dictionary_page_header);
        dict_page_header.write(dict_header_writer);

        // Store dictionary page content
        result.dictionary_page_content = dict_page_compressed.empty()
                                             ? std::move(dict_page_raw)
                                             : std::move(dict_page_compressed);
        result.dictionary_uncompressed_size = dict_uncompressed;
        result.dictionary_compressed_size = dict_compressed;
        result.uses_dictionary = true;
        result.dictionary_size = dict_entries.size();

        // 5. Encode data page with indices using RLE
        uint8_t bit_width = writer::encoding::bits_required(
            dict_entries.empty() ? 0 : static_cast<uint32_t>(dict_entries.size() - 1));
        // Ensure at least 1 bit width even for single value dictionary
        if (bit_width == 0)
          bit_width = 1;

        writer::encoding::encode_dictionary_indices(indices, nulls, bit_width, page_content);

        data_encoding = writer::ParquetEncoding::RLE_DICTIONARY;
        break;
      }
    }

    // Fallthrough to plain encoding if dictionary not beneficial or disabled
    writer::encoding::encode_byte_array_plain_arrow(string_values, nulls, page_content);
    break;
  }
  case DataType::INT32: {
    const auto& builder = static_cast<const ArrowInt32ColumnBuilder&>(column);
    writer::encoding::encode_int32_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  case DataType::INT64: {
    const auto& builder = static_cast<const ArrowInt64ColumnBuilder&>(column);
    writer::encoding::encode_int64_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  case DataType::DATE: {
    const auto& builder = static_cast<const ArrowDateColumnBuilder&>(column);
    writer::encoding::encode_int32_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  case DataType::TIMESTAMP: {
    const auto& builder = static_cast<const ArrowTimestampColumnBuilder&>(column);
    writer::encoding::encode_int64_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  case DataType::FLOAT64: {
    const auto& builder = static_cast<const ArrowFloat64ColumnBuilder&>(column);
    writer::encoding::encode_float64_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  case DataType::BOOL: {
    const auto& builder = static_cast<const ArrowBoolColumnBuilder&>(column);
    writer::encoding::encode_bool_plain_arrow(builder.values(), nulls, page_content);
    break;
  }
  default:
    break;
  }

  int32_t uncompressed_size = static_cast<int32_t>(page_content.size());
  int32_t compressed_size = uncompressed_size;

  // Compress if needed
  std::vector<uint8_t> compressed_content;
  result.actual_codec = writer::CompressionCodec::UNCOMPRESSED;
  if (options.compression != Compression::NONE) {
    compressed_content = writer::compress(page_content.data(), page_content.size(),
                                          options.compression, options.compression_level);
    if (compressed_content.size() < page_content.size()) {
      compressed_size = static_cast<int32_t>(compressed_content.size());
      result.actual_codec = writer::to_parquet_codec(options.compression);
    } else {
      compressed_content.clear();
    }
  }

  // Build page header
  writer::PageHeader page_header;
  page_header.type = writer::PageType::DATA_PAGE;
  page_header.uncompressed_page_size = uncompressed_size;
  page_header.compressed_page_size = compressed_size;

  writer::DataPageHeader data_header;
  data_header.num_values = static_cast<int32_t>(column.size());
  data_header.encoding = data_encoding;
  data_header.definition_level_encoding = writer::ParquetEncoding::RLE;
  data_header.repetition_level_encoding = writer::ParquetEncoding::RLE;

  page_header.data_page_header = data_header;

  // Serialize page header
  writer::ThriftCompactWriter header_writer(result.header_bytes);
  page_header.write(header_writer);

  // Store content (use compressed if smaller)
  result.content_bytes =
      compressed_content.empty() ? std::move(page_content) : std::move(compressed_content);
  result.uncompressed_size = uncompressed_size;
  result.compressed_size = compressed_size;

  return result;
}

// Encode all columns of a row group in parallel
// Returns EncodedRowGroup ready for sequential writing
static writer::EncodedRowGroup
encode_row_group(const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns,
                 const std::vector<ColumnSchema>& schema, const ParquetOptions& options,
                 BS::thread_pool& pool, size_t sequence_number) {
  writer::EncodedRowGroup result;
  result.num_rows = columns.empty() ? 0 : static_cast<int64_t>(columns[0]->size());
  result.sequence_number = sequence_number;
  result.total_byte_size = 0;
  result.total_compressed_size = 0;

  const size_t num_columns = columns.size();
  result.columns.resize(num_columns);

  if (num_columns > 1) {
    // Parallel encoding
    std::vector<std::future<void>> futures;
    futures.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i) {
      futures.push_back(pool.submit_task([&columns, &result, &schema, &options, i]() {
        result.columns[i] = encode_column_arrow(*columns[i], schema[i], options);
      }));
    }

    for (auto& f : futures) {
      f.get();
    }
  } else {
    // Serial encoding for single column
    for (size_t i = 0; i < num_columns; ++i) {
      result.columns[i] = encode_column_arrow(*columns[i], schema[i], options);
    }
  }

  // Sum up sizes
  for (const auto& col : result.columns) {
    result.total_byte_size += col.uncompressed_size;
    result.total_compressed_size += col.compressed_size;
  }

  return result;
}

// ParquetWriter implementation
struct ParquetWriter::Impl {
  ParquetOptions options;
  std::string path;
  std::ofstream file;
  std::vector<ColumnSchema> schema;
  bool is_open = false;

  // Current file position (for offset tracking)
  int64_t current_offset = 0;

  // Row groups collected during writing
  std::vector<writer::RowGroup> row_groups;
  int64_t total_rows = 0;

  // Reusable thread pool for parallel encoding (created on first use)
  std::unique_ptr<BS::thread_pool> encoding_pool;

  // Pipeline state
  std::unique_ptr<writer::EncodedRowGroupQueue> pipeline_queue_;
  std::unique_ptr<std::thread> writer_thread_;
  std::atomic<bool> pipeline_error_{false};
  std::string pipeline_error_message_;
  size_t next_sequence_number_ = 0;

  BS::thread_pool& get_encoding_pool() {
    if (!encoding_pool) {
      size_t num_threads = std::thread::hardware_concurrency();
      if (num_threads == 0)
        num_threads = 4;
      encoding_pool = std::make_unique<BS::thread_pool>(num_threads);
    }
    return *encoding_pool;
  }

  // Create dictionary options from parquet options
  // Note: Dictionary encoding integration is available through dictionary.h
  // but not yet fully integrated into the new Thrift-based writer
  writer::DictionaryOptions get_dict_options() const {
    writer::DictionaryOptions opts;
    opts.enable_dictionary = options.enable_dictionary;
    opts.ratio_threshold = options.dictionary_ratio_threshold;
    return opts;
  }

  void write_bytes(const void* data, size_t size) {
    file.write(static_cast<const char*>(data), size);
    current_offset += size;
  }

  void write_magic() { write_bytes(PARQUET_MAGIC, 4); }

  // Write a pre-encoded row group to disk
  // Must be called sequentially in order
  void write_encoded_row_group(const writer::EncodedRowGroup& encoded) {
    writer::RowGroup row_group;
    row_group.num_rows = encoded.num_rows;
    row_group.file_offset = current_offset;
    row_group.total_byte_size = 0;
    row_group.total_compressed_size = 0;

    for (size_t i = 0; i < encoded.columns.size(); ++i) {
      const auto& col = encoded.columns[i];

      std::optional<int64_t> dictionary_page_offset;
      int64_t total_written = 0;
      int64_t total_uncompressed = 0;

      // Write dictionary page if present
      if (col.uses_dictionary && col.dictionary_page_header.has_value() &&
          col.dictionary_page_content.has_value()) {
        dictionary_page_offset = current_offset;

        write_bytes(col.dictionary_page_header->data(), col.dictionary_page_header->size());
        write_bytes(col.dictionary_page_content->data(), col.dictionary_page_content->size());

        total_written += static_cast<int64_t>(col.dictionary_page_header->size() +
                                              col.dictionary_page_content->size());
        total_uncompressed += static_cast<int64_t>(col.dictionary_page_header->size() +
                                                   col.dictionary_uncompressed_size);
      }

      // Record data page offset
      int64_t data_page_offset = current_offset;

      // Write data page
      write_bytes(col.header_bytes.data(), col.header_bytes.size());
      write_bytes(col.content_bytes.data(), col.content_bytes.size());

      total_written += static_cast<int64_t>(col.header_bytes.size() + col.content_bytes.size());
      total_uncompressed += static_cast<int64_t>(col.header_bytes.size() + col.uncompressed_size);

      // Build column metadata
      writer::ColumnMetaData meta;
      meta.type = writer::to_parquet_type(col.data_type);

      if (col.uses_dictionary) {
        meta.encodings = {writer::ParquetEncoding::PLAIN, writer::ParquetEncoding::RLE_DICTIONARY,
                          writer::ParquetEncoding::RLE};
        meta.dictionary_page_offset = dictionary_page_offset;
      } else {
        meta.encodings = {writer::ParquetEncoding::PLAIN, writer::ParquetEncoding::RLE};
      }

      meta.path_in_schema = {col.column_name};
      meta.codec = col.actual_codec;
      meta.num_values = col.num_values;
      meta.total_uncompressed_size = total_uncompressed;
      meta.total_compressed_size = total_written;
      meta.data_page_offset = data_page_offset;

      if (col.statistics.has_value()) {
        meta.statistics = col.statistics;
      }

      writer::ColumnChunk chunk;
      chunk.file_offset = 0;
      chunk.meta_data = meta;

      row_group.columns.push_back(chunk);
      row_group.total_byte_size += meta.total_uncompressed_size;
      if (row_group.total_compressed_size.has_value()) {
        row_group.total_compressed_size = row_group.total_compressed_size.value() + total_written;
      } else {
        row_group.total_compressed_size = total_written;
      }
    }

    total_rows += encoded.num_rows;
    row_groups.push_back(row_group);
  }

  // Write a page header and return the bytes written
  size_t write_page_header(const writer::PageHeader& header) {
    std::vector<uint8_t> buffer;
    writer::ThriftCompactWriter writer(buffer);
    header.write(writer);
    write_bytes(buffer.data(), buffer.size());
    return buffer.size();
  }

  // Write column data and return result with offsets/sizes
  writer::ColumnWriteResult write_column(const ColumnBuilder& column,
                                         const ColumnSchema& col_schema) {
    writer::ColumnWriteResult result;
    result.data_page_offset = current_offset;
    result.num_values = static_cast<int64_t>(column.size());
    result.null_count = 0;

    const auto& null_bitmap = column.null_bitmap();
    for (bool is_null : null_bitmap) {
      if (is_null)
        result.null_count++;
    }

    // Encode the data based on type
    std::vector<uint8_t> encoded_data;
    std::vector<uint8_t> def_levels;

    // For OPTIONAL (nullable) columns, we must always encode definition levels
    // even when there are no nulls - the reader expects them based on schema
    bool is_nullable = col_schema.nullable;
    if (is_nullable) {
      writer::encoding::encode_def_levels_hybrid(null_bitmap, 1, def_levels, result.null_count);
    }

    DataType type = column.type();
    const void* raw = column.raw_values();

    switch (type) {
    case DataType::STRING: {
      const auto& values = *static_cast<const std::vector<std::string>*>(raw);
      writer::encoding::encode_byte_array_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::INT32: {
      const auto& values = *static_cast<const std::vector<int32_t>*>(raw);
      writer::encoding::encode_int32_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::INT64: {
      const auto& values = *static_cast<const std::vector<int64_t>*>(raw);
      writer::encoding::encode_int64_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::DATE: {
      // DATE is stored as INT32 (days since epoch)
      const auto& values = *static_cast<const std::vector<int32_t>*>(raw);
      writer::encoding::encode_int32_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::TIMESTAMP: {
      // TIMESTAMP is stored as INT64 (microseconds since epoch)
      const auto& values = *static_cast<const std::vector<int64_t>*>(raw);
      writer::encoding::encode_int64_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::FLOAT64: {
      const auto& values = *static_cast<const std::vector<double>*>(raw);
      writer::encoding::encode_float64_plain(values, null_bitmap, encoded_data);
      break;
    }
    case DataType::BOOL: {
      const auto& values = *static_cast<const std::vector<bool>*>(raw);
      writer::encoding::encode_bool_plain(values, null_bitmap, encoded_data);
      break;
    }
    default:
      break;
    }

    // Combine definition levels and data
    std::vector<uint8_t> page_content;
    page_content.insert(page_content.end(), def_levels.begin(), def_levels.end());
    page_content.insert(page_content.end(), encoded_data.begin(), encoded_data.end());

    int32_t uncompressed_size = static_cast<int32_t>(page_content.size());
    int32_t compressed_size = uncompressed_size;

    // Compress if needed
    std::vector<uint8_t> compressed_content;
    result.actual_codec = writer::CompressionCodec::UNCOMPRESSED; // Default to uncompressed
    if (options.compression != Compression::NONE) {
      compressed_content = writer::compress(page_content.data(), page_content.size(),
                                            options.compression, options.compression_level);
      if (compressed_content.size() < page_content.size()) {
        compressed_size = static_cast<int32_t>(compressed_content.size());
        result.actual_codec = writer::to_parquet_codec(options.compression);
      } else {
        compressed_content.clear(); // Don't use compression - keep uncompressed codec
      }
    }

    // Build page header
    writer::PageHeader page_header;
    page_header.type = writer::PageType::DATA_PAGE;
    page_header.uncompressed_page_size = uncompressed_size;
    page_header.compressed_page_size = compressed_size;

    writer::DataPageHeader data_header;
    data_header.num_values = static_cast<int32_t>(column.size());
    data_header.encoding = writer::ParquetEncoding::PLAIN;
    data_header.definition_level_encoding = writer::ParquetEncoding::RLE;
    data_header.repetition_level_encoding = writer::ParquetEncoding::RLE;

    // Add statistics if enabled
    if (options.write_statistics) {
      auto col_stats = column.statistics();
      writer::Statistics stats;
      stats.null_count = col_stats.null_count;

      if (col_stats.min_value.index() != 0 || std::holds_alternative<bool>(col_stats.min_value)) {
        stats.min_value = writer::serialize_stat_value(col_stats.min_value, type);
        stats.max_value = writer::serialize_stat_value(col_stats.max_value, type);
      }

      data_header.statistics = stats;
    }

    page_header.data_page_header = data_header;

    // Write page header
    size_t header_size = write_page_header(page_header);

    // Write page content
    const auto& content_to_write = compressed_content.empty() ? page_content : compressed_content;
    write_bytes(content_to_write.data(), content_to_write.size());

    result.total_compressed_size = static_cast<int64_t>(header_size + content_to_write.size());
    result.total_uncompressed_size = static_cast<int64_t>(header_size + uncompressed_size);

    // Copy statistics to result
    if (options.write_statistics && data_header.statistics.has_value()) {
      result.statistics = data_header.statistics;
    }

    return result;
  }
};

ParquetWriter::ParquetWriter(const ParquetOptions& options) : impl_(std::make_unique<Impl>()) {
  impl_->options = options;
}

ParquetWriter::~ParquetWriter() {
  if (impl_->is_open) {
    close();
  }
}

Result<bool> ParquetWriter::open(const std::string& path) {
  impl_->path = path;
  impl_->file.open(path, std::ios::binary);

  if (!impl_->file.is_open()) {
    return Result<bool>::failure("Failed to open file for writing: " + path);
  }

  // Write magic number
  impl_->write_magic();
  impl_->is_open = true;

  return Result<bool>::success(true);
}

void ParquetWriter::set_schema(const std::vector<ColumnSchema>& schema) {
  impl_->schema = schema;
}

Result<bool> ParquetWriter::write(const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns) {
  if (!impl_->is_open) {
    return Result<bool>::failure("Writer not open");
  }

  if (columns.empty()) {
    return Result<bool>::success(true);
  }

  // Encode the row group (CPU work)
  auto encoded =
      encode_row_group(columns, impl_->schema, impl_->options, impl_->get_encoding_pool(),
                       impl_->row_groups.size() // sequence number
      );

  // Write to disk (I/O work) - currently sequential
  impl_->write_encoded_row_group(encoded);

  return Result<bool>::success(true);
}

Result<bool> ParquetWriter::start_pipeline() {
  if (!impl_->is_open) {
    return Result<bool>::failure("Writer not open");
  }

  // Create queue with capacity for 4 encoded row groups
  impl_->pipeline_queue_ = std::make_unique<writer::EncodedRowGroupQueue>(4);
  impl_->next_sequence_number_ = 0;
  impl_->pipeline_error_ = false;
  impl_->pipeline_error_message_.clear();

  // Start writer thread
  impl_->writer_thread_ = std::make_unique<std::thread>([this]() {
    while (auto encoded = impl_->pipeline_queue_->pop()) {
      try {
        impl_->write_encoded_row_group(*encoded);
      } catch (const std::exception& e) {
        impl_->pipeline_error_ = true;
        impl_->pipeline_error_message_ = e.what();
        // Close the queue to unblock any waiting producers
        impl_->pipeline_queue_->close();
        break;
      }
    }
  });

  return Result<bool>::success(true);
}

Result<bool>
ParquetWriter::submit_row_group(std::vector<std::unique_ptr<ArrowColumnBuilder>> columns) {
  if (!impl_->pipeline_queue_) {
    return Result<bool>::failure("Pipeline not started");
  }

  // Check for early error - don't read error message here (data race)
  // The actual error message will be returned from finish_pipeline() after thread joins
  if (impl_->pipeline_error_) {
    return Result<bool>::failure("Pipeline writer encountered an error");
  }

  if (columns.empty()) {
    return Result<bool>::success(true);
  }

  // Encode the row group (CPU work, can happen in parallel with I/O)
  auto encoded = encode_row_group(columns, impl_->schema, impl_->options,
                                  impl_->get_encoding_pool(), impl_->next_sequence_number_++);

  // Submit to queue (blocks if queue full - provides backpressure)
  if (!impl_->pipeline_queue_->push(std::move(encoded))) {
    return Result<bool>::failure("Pipeline closed unexpectedly");
  }

  return Result<bool>::success(true);
}

Result<bool> ParquetWriter::finish_pipeline() {
  if (!impl_->pipeline_queue_) {
    return Result<bool>::failure("Pipeline not started");
  }

  // Signal no more row groups
  impl_->pipeline_queue_->close();

  // Wait for writer thread to finish
  if (impl_->writer_thread_ && impl_->writer_thread_->joinable()) {
    impl_->writer_thread_->join();
  }

  // Clean up
  impl_->pipeline_queue_.reset();
  impl_->writer_thread_.reset();

  if (impl_->pipeline_error_) {
    return Result<bool>::failure(impl_->pipeline_error_message_);
  }

  return Result<bool>::success(true);
}

Result<bool> ParquetWriter::close() {
  if (!impl_->is_open) {
    return Result<bool>::success(true);
  }

  // Build file metadata
  writer::FileMetaData file_meta;
  file_meta.version = 1;
  file_meta.num_rows = impl_->total_rows;
  file_meta.row_groups = impl_->row_groups;
  file_meta.created_by = "vroom (C++ CSV to Parquet converter)";

  // Build schema elements
  // First element is the root
  writer::SchemaElement root;
  root.name = "schema";
  root.num_children = static_cast<int32_t>(impl_->schema.size());
  file_meta.schema.push_back(root);

  // Add column elements
  for (const auto& col : impl_->schema) {
    writer::SchemaElement elem;
    elem.type = writer::to_parquet_type(col.type);
    elem.repetition_type = col.nullable ? writer::FieldRepetitionType::OPTIONAL
                                        : writer::FieldRepetitionType::REQUIRED;
    elem.name = col.name;

    // Add converted type for strings (UTF8)
    if (col.type == DataType::STRING) {
      elem.converted_type = writer::ConvertedType::UTF8;
    }

    file_meta.schema.push_back(elem);
  }

  // Add column orders
  std::vector<writer::ColumnOrder> column_orders;
  for (size_t i = 0; i < impl_->schema.size(); ++i) {
    writer::ColumnOrder order;
    column_orders.push_back(order);
  }
  file_meta.column_orders = column_orders;

  // Serialize file metadata
  std::vector<uint8_t> metadata_buffer;
  writer::ThriftCompactWriter thrift_writer(metadata_buffer);
  file_meta.write(thrift_writer);

  // Write metadata
  impl_->write_bytes(metadata_buffer.data(), metadata_buffer.size());

  // Write metadata length (4 bytes, little-endian)
  int32_t metadata_len = static_cast<int32_t>(metadata_buffer.size());
  impl_->write_bytes(&metadata_len, 4);

  // Write final magic bytes
  impl_->write_bytes(PARQUET_MAGIC, 4);

  impl_->file.close();
  impl_->is_open = false;

  return Result<bool>::success(true);
}

} // namespace libvroom
