#pragma once

#include "arrow_buffer.h"
#include "types.h"

#include <string>
#include <utility>
#include <vector>

namespace libvroom {
namespace writer {

// Smart dictionary heuristics from Polars
// Only create dictionary if cardinality benefits from it

// Cardinality thresholds (from Polars)
// These are the maximum dictionary sizes for different index types
constexpr size_t DICT_THRESHOLD_I8 = 16;
constexpr size_t DICT_THRESHOLD_I16 = 256;
constexpr size_t DICT_THRESHOLD_I32 = 512;
constexpr size_t DICT_THRESHOLD_I64 = 2048;

// Default dictionary ratio threshold
// Dictionary is beneficial if cardinality/length < this ratio
constexpr double DICT_RATIO_THRESHOLD = 0.75;

// Result of dictionary analysis
struct DictionaryAnalysis {
  bool should_use_dictionary = false;
  size_t cardinality = 0;
  size_t total_values = 0;
  double ratio = 1.0;
  // Suggested index storage type size in bits (8, 16, 32, or 64).
  // This indicates the minimum integer type that can represent all indices,
  // not the actual bit-width needed for encoding (which is calculated separately
  // based on cardinality using bits_required()).
  uint8_t index_bit_width = 32;
};

// Dictionary encoding options (subset of ParquetOptions relevant to dictionary)
struct DictionaryOptions {
  bool enable_dictionary = true;
  double ratio_threshold = DICT_RATIO_THRESHOLD;
};

// Analyze string column for dictionary encoding viability
DictionaryAnalysis analyze_string_dictionary(const std::vector<std::string>& values,
                                             const std::vector<bool>& null_bitmap,
                                             double ratio_threshold = DICT_RATIO_THRESHOLD);

// Analyze string column from Arrow StringBuffer for dictionary encoding viability
DictionaryAnalysis analyze_string_dictionary_arrow(const StringBuffer& values,
                                                   const NullBitmap& nulls,
                                                   double ratio_threshold = DICT_RATIO_THRESHOLD);

// Analyze int32 column for dictionary encoding viability
DictionaryAnalysis analyze_int32_dictionary(const std::vector<int32_t>& values,
                                            const std::vector<bool>& null_bitmap,
                                            double ratio_threshold = DICT_RATIO_THRESHOLD);

// Analyze int64 column for dictionary encoding viability
DictionaryAnalysis analyze_int64_dictionary(const std::vector<int64_t>& values,
                                            const std::vector<bool>& null_bitmap,
                                            double ratio_threshold = DICT_RATIO_THRESHOLD);

// Create string dictionary
// Returns: (dictionary entries, indices into dictionary)
// Index of -1 indicates null value
std::pair<std::vector<std::string>, std::vector<int32_t>>
create_string_dictionary(const std::vector<std::string>& values,
                         const std::vector<bool>& null_bitmap);

// Create string dictionary from Arrow StringBuffer
// Returns: (dictionary entries as strings, indices into dictionary)
// Index of -1 indicates null value
std::pair<std::vector<std::string>, std::vector<int32_t>>
create_string_dictionary_arrow(const StringBuffer& values, const NullBitmap& nulls);

// Create int32 dictionary
// Returns: (dictionary entries, indices into dictionary)
std::pair<std::vector<int32_t>, std::vector<int32_t>>
create_int32_dictionary(const std::vector<int32_t>& values, const std::vector<bool>& null_bitmap);

// Create int64 dictionary
// Returns: (dictionary entries, indices into dictionary)
std::pair<std::vector<int64_t>, std::vector<int32_t>>
create_int64_dictionary(const std::vector<int64_t>& values, const std::vector<bool>& null_bitmap);

} // namespace writer
} // namespace libvroom
