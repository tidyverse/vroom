#include "libvroom/dictionary.h"
#include "libvroom/statistics.h"
#include "libvroom/vroom.h"

#include "columns/cow_buffer.h"

#include <cstring>

namespace libvroom {
namespace writer {

// Forward declarations
void write_int32_le(int32_t value, std::vector<uint8_t>& output);
void write_int64_le(int64_t value, std::vector<uint8_t>& output);

CowByteBuffer write_data_page(const uint8_t* data, size_t data_size, size_t num_values,
                              const std::vector<bool>& null_bitmap, Compression compression,
                              int compression_level, Encoding encoding);

CowByteBuffer write_dictionary_page(const std::vector<std::string>& dictionary,
                                    Compression compression, int compression_level);

CowByteBuffer write_int32_dictionary_page(const std::vector<int32_t>& dictionary,
                                          Compression compression, int compression_level);

CowByteBuffer write_int64_dictionary_page(const std::vector<int64_t>& dictionary,
                                          Compression compression, int compression_level);

CowByteBuffer write_dictionary_data_page(const std::vector<int32_t>& indices,
                                         const std::vector<bool>& null_bitmap,
                                         size_t dictionary_size, Compression compression,
                                         int compression_level);

namespace encoding {
void encode_int32_plain(const int32_t* values, size_t count, std::vector<uint8_t>& output);
void encode_int64_plain(const int64_t* values, size_t count, std::vector<uint8_t>& output);
void encode_float64_plain(const double* values, size_t count, std::vector<uint8_t>& output);
void encode_bool_plain(const bool* values, size_t count, std::vector<uint8_t>& output);
void encode_byte_array_plain(const std::vector<std::string>& values,
                             const std::vector<bool>& null_bitmap, std::vector<uint8_t>& output);
} // namespace encoding

// Column chunk data - uses CowByteBuffer for zero-copy sharing
struct ColumnChunkData {
  CowByteBuffer data; // Zero-copy buffer for page data
  int64_t total_compressed_size = 0;
  int64_t total_uncompressed_size = 0;
  int64_t num_values = 0;
  int64_t null_count = 0;
  DataType type;
  ColumnStatistics statistics;
  bool uses_dictionary = false;
  Encoding encoding = Encoding::PLAIN;
};

// Write a column of int32 values with optional dictionary encoding and pre-computed statistics
ColumnChunkData write_int32_column(const std::vector<int32_t>& values,
                                   const std::vector<bool>& null_bitmap,
                                   const ColumnStatistics* precomputed_stats,
                                   Compression compression, int compression_level, size_t page_size,
                                   const DictionaryOptions& dict_opts) {
  ColumnChunkData result;
  result.type = DataType::INT32;
  result.num_values = static_cast<int64_t>(values.size());

  // Use pre-computed statistics if available, otherwise compute using SIMD
  if (precomputed_stats) {
    result.statistics = *precomputed_stats;
    result.null_count = precomputed_stats->null_count;
  } else {
    // Compute statistics using SIMD-optimized batch operation
    Int32Statistics stats;
    stats.update_batch_with_nulls(values.data(), null_bitmap, values.size());

    result.null_count = stats.null_count();
    result.statistics.has_null = stats.has_null();
    result.statistics.null_count = stats.null_count();
    if (stats.has_value()) {
      result.statistics.min_value = stats.min();
      result.statistics.max_value = stats.max();
    }
  }

  // Check if dictionary encoding is beneficial
  if (dict_opts.enable_dictionary && values.size() > 0) {
    auto analysis = analyze_int32_dictionary(values, null_bitmap, dict_opts.ratio_threshold);

    if (analysis.should_use_dictionary) {
      // Create dictionary and indices
      auto [dictionary, indices] = create_int32_dictionary(values, null_bitmap);

      // Write dictionary page
      auto dict_page = write_int32_dictionary_page(dictionary, compression, compression_level);

      // Write data page with RLE-encoded indices
      auto data_page = write_dictionary_data_page(indices, null_bitmap, dictionary.size(),
                                                  compression, compression_level);

      // Combine pages - share buffers where possible
      auto& output = result.data.to_mut();
      output.insert(output.end(), dict_page.data(), dict_page.data() + dict_page.size());
      output.insert(output.end(), data_page.data(), data_page.data() + data_page.size());
      result.total_compressed_size = result.data.size();
      result.total_uncompressed_size = result.data.size(); // Simplified
      result.uses_dictionary = true;
      result.encoding = Encoding::DICTIONARY;

      return result;
    }
  }

  // Fall back to plain encoding
  std::vector<int32_t> non_null_values;
  non_null_values.reserve(values.size() - static_cast<size_t>(result.null_count));
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_values.push_back(values[i]);
    }
  }

  std::vector<uint8_t> encoded_data;
  encoding::encode_int32_plain(non_null_values.data(), non_null_values.size(), encoded_data);

  result.total_uncompressed_size = static_cast<int64_t>(encoded_data.size());

  result.data = write_data_page(encoded_data.data(), encoded_data.size(), values.size(),
                                null_bitmap, compression, compression_level, Encoding::PLAIN);

  result.total_compressed_size = static_cast<int64_t>(result.data.size());
  result.encoding = Encoding::PLAIN;

  return result;
}

// Write a column of int64 values with optional dictionary encoding and pre-computed statistics
ColumnChunkData write_int64_column(const std::vector<int64_t>& values,
                                   const std::vector<bool>& null_bitmap,
                                   const ColumnStatistics* precomputed_stats,
                                   Compression compression, int compression_level, size_t page_size,
                                   const DictionaryOptions& dict_opts) {
  ColumnChunkData result;
  result.type = DataType::INT64;
  result.num_values = static_cast<int64_t>(values.size());

  if (precomputed_stats) {
    result.statistics = *precomputed_stats;
    result.null_count = precomputed_stats->null_count;
  } else {
    Int64Statistics stats;
    stats.update_batch_with_nulls(values.data(), null_bitmap, values.size());

    result.null_count = stats.null_count();
    result.statistics.has_null = stats.has_null();
    result.statistics.null_count = stats.null_count();
    if (stats.has_value()) {
      result.statistics.min_value = stats.min();
      result.statistics.max_value = stats.max();
    }
  }

  // Check if dictionary encoding is beneficial
  if (dict_opts.enable_dictionary && values.size() > 0) {
    auto analysis = analyze_int64_dictionary(values, null_bitmap, dict_opts.ratio_threshold);

    if (analysis.should_use_dictionary) {
      // Create dictionary and indices
      auto [dictionary, indices] = create_int64_dictionary(values, null_bitmap);

      // Write dictionary page
      auto dict_page = write_int64_dictionary_page(dictionary, compression, compression_level);

      // Write data page with RLE-encoded indices
      auto data_page = write_dictionary_data_page(indices, null_bitmap, dictionary.size(),
                                                  compression, compression_level);

      // Combine pages
      auto& output = result.data.to_mut();
      output.insert(output.end(), dict_page.data(), dict_page.data() + dict_page.size());
      output.insert(output.end(), data_page.data(), data_page.data() + data_page.size());
      result.total_compressed_size = result.data.size();
      result.total_uncompressed_size = result.data.size();
      result.uses_dictionary = true;
      result.encoding = Encoding::DICTIONARY;

      return result;
    }
  }

  // Fall back to plain encoding
  std::vector<int64_t> non_null_values;
  non_null_values.reserve(values.size() - static_cast<size_t>(result.null_count));
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_values.push_back(values[i]);
    }
  }

  std::vector<uint8_t> encoded_data;
  encoding::encode_int64_plain(non_null_values.data(), non_null_values.size(), encoded_data);

  result.total_uncompressed_size = static_cast<int64_t>(encoded_data.size());

  result.data = write_data_page(encoded_data.data(), encoded_data.size(), values.size(),
                                null_bitmap, compression, compression_level, Encoding::PLAIN);

  result.total_compressed_size = static_cast<int64_t>(result.data.size());
  result.encoding = Encoding::PLAIN;

  return result;
}

// Write a column of float64 values with pre-computed statistics
// Note: Float columns typically don't benefit from dictionary encoding
ColumnChunkData write_float64_column(const std::vector<double>& values,
                                     const std::vector<bool>& null_bitmap,
                                     const ColumnStatistics* precomputed_stats,
                                     Compression compression, int compression_level,
                                     size_t page_size) {
  ColumnChunkData result;
  result.type = DataType::FLOAT64;
  result.num_values = static_cast<int64_t>(values.size());

  if (precomputed_stats) {
    result.statistics = *precomputed_stats;
    result.null_count = precomputed_stats->null_count;
  } else {
    Float64Statistics stats;
    stats.update_batch_with_nulls(values.data(), null_bitmap, values.size());

    result.null_count = stats.null_count();
    result.statistics.has_null = stats.has_null();
    result.statistics.null_count = stats.null_count();
    if (stats.has_value()) {
      result.statistics.min_value = stats.min();
      result.statistics.max_value = stats.max();
    }
  }

  std::vector<double> non_null_values;
  non_null_values.reserve(values.size() - static_cast<size_t>(result.null_count));
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_values.push_back(values[i]);
    }
  }

  std::vector<uint8_t> encoded_data;
  encoding::encode_float64_plain(non_null_values.data(), non_null_values.size(), encoded_data);

  result.total_uncompressed_size = static_cast<int64_t>(encoded_data.size());

  result.data = write_data_page(encoded_data.data(), encoded_data.size(), values.size(),
                                null_bitmap, compression, compression_level, Encoding::PLAIN);

  result.total_compressed_size = static_cast<int64_t>(result.data.size());
  result.encoding = Encoding::PLAIN;

  return result;
}

// Backwards compatible overload
ColumnChunkData write_float64_column(const std::vector<double>& values,
                                     const std::vector<bool>& null_bitmap, Compression compression,
                                     int compression_level, size_t page_size) {
  return write_float64_column(values, null_bitmap, nullptr, compression, compression_level,
                              page_size);
}

// Write a column of string values with optional dictionary encoding and pre-computed statistics
ColumnChunkData write_string_column(const std::vector<std::string>& values,
                                    const std::vector<bool>& null_bitmap,
                                    const ColumnStatistics* precomputed_stats,
                                    Compression compression, int compression_level,
                                    size_t page_size, const DictionaryOptions& dict_opts) {
  ColumnChunkData result;
  result.type = DataType::STRING;
  result.num_values = static_cast<int64_t>(values.size());

  // Handle statistics
  if (precomputed_stats) {
    result.statistics = *precomputed_stats;
    result.null_count = precomputed_stats->null_count;
  } else {
    for (bool is_null : null_bitmap) {
      if (is_null)
        ++result.null_count;
    }
  }

  // Check if dictionary encoding is beneficial
  if (dict_opts.enable_dictionary && values.size() > 0) {
    auto analysis = analyze_string_dictionary(values, null_bitmap, dict_opts.ratio_threshold);

    if (analysis.should_use_dictionary) {
      // Create dictionary and indices
      auto [dictionary, indices] = create_string_dictionary(values, null_bitmap);

      // Write dictionary page
      auto dict_page = write_dictionary_page(dictionary, compression, compression_level);

      // Write data page with RLE-encoded indices
      auto data_page = write_dictionary_data_page(indices, null_bitmap, dictionary.size(),
                                                  compression, compression_level);

      // Combine pages - share buffers where possible
      auto& output = result.data.to_mut();
      output.insert(output.end(), dict_page.data(), dict_page.data() + dict_page.size());
      output.insert(output.end(), data_page.data(), data_page.data() + data_page.size());
      result.total_compressed_size = static_cast<int64_t>(result.data.size());
      result.total_uncompressed_size = static_cast<int64_t>(result.data.size());
      result.uses_dictionary = true;
      result.encoding = Encoding::DICTIONARY;

      return result;
    }
  }

  // Fall back to plain encoding
  std::vector<uint8_t> encoded_data;
  encoding::encode_byte_array_plain(values, null_bitmap, encoded_data);

  result.total_uncompressed_size = static_cast<int64_t>(encoded_data.size());

  result.data = write_data_page(encoded_data.data(), encoded_data.size(), values.size(),
                                null_bitmap, compression, compression_level, Encoding::PLAIN);

  result.total_compressed_size = static_cast<int64_t>(result.data.size());
  result.encoding = Encoding::PLAIN;

  return result;
}

// Write a column of boolean values with pre-computed statistics
// Note: Boolean columns don't benefit from dictionary encoding
ColumnChunkData write_bool_column(const std::vector<bool>& values,
                                  const std::vector<bool>& null_bitmap,
                                  const ColumnStatistics* precomputed_stats,
                                  Compression compression, int compression_level,
                                  size_t page_size) {
  ColumnChunkData result;
  result.type = DataType::BOOL;
  result.num_values = static_cast<int64_t>(values.size());

  if (precomputed_stats) {
    result.statistics = *precomputed_stats;
    result.null_count = precomputed_stats->null_count;
  } else {
    BoolStatistics stats;
    stats.update_batch_with_nulls(values, null_bitmap);

    result.null_count = stats.null_count();
    result.statistics.has_null = stats.has_null();
    result.statistics.null_count = stats.null_count();
    if (stats.has_value()) {
      result.statistics.min_value = stats.min();
      result.statistics.max_value = stats.max();
    }
  }

  // Convert std::vector<bool> to raw bool array
  std::vector<bool> non_null_values;
  non_null_values.reserve(values.size() - static_cast<size_t>(result.null_count));
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      non_null_values.push_back(values[i]);
    }
  }

  // Encode as bit-packed
  std::vector<uint8_t> encoded_data;
  size_t num_bytes = (non_null_values.size() + 7) / 8;
  encoded_data.resize(num_bytes, 0);

  for (size_t i = 0; i < non_null_values.size(); ++i) {
    if (non_null_values[i]) {
      encoded_data[i / 8] |= (1 << (i % 8));
    }
  }

  result.total_uncompressed_size = static_cast<int64_t>(encoded_data.size());

  result.data = write_data_page(encoded_data.data(), encoded_data.size(), values.size(),
                                null_bitmap, compression, compression_level, Encoding::PLAIN);

  result.total_compressed_size = static_cast<int64_t>(result.data.size());
  result.encoding = Encoding::PLAIN;

  return result;
}

// Backwards compatible overload
ColumnChunkData write_bool_column(const std::vector<bool>& values,
                                  const std::vector<bool>& null_bitmap, Compression compression,
                                  int compression_level, size_t page_size) {
  return write_bool_column(values, null_bitmap, nullptr, compression, compression_level, page_size);
}

} // namespace writer
} // namespace libvroom
