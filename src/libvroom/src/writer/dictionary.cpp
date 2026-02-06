#include "libvroom/dictionary.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace libvroom {
namespace writer {

// Analyze string column for dictionary potential
DictionaryAnalysis analyze_string_dictionary(const std::vector<std::string>& values,
                                             const std::vector<bool>& null_bitmap,
                                             double ratio_threshold) {
  DictionaryAnalysis result;
  result.total_values = values.size();

  if (values.empty()) {
    return result;
  }

  // Count unique values (excluding nulls)
  std::unordered_map<std::string, size_t> unique_values;

  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      unique_values[values[i]]++;
    }
  }

  result.cardinality = unique_values.size();
  result.ratio = static_cast<double>(result.cardinality) / result.total_values;

  // Determine if dictionary is beneficial
  // Very low cardinality always benefits from dictionary (8-bit indices)
  if (result.cardinality <= DICT_THRESHOLD_I8) {
    result.should_use_dictionary = true;
    result.index_bit_width = 8;
  } else if (result.cardinality <= DICT_THRESHOLD_I16 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 16;
  } else if (result.cardinality <= DICT_THRESHOLD_I32 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 32;
  } else if (result.cardinality <= DICT_THRESHOLD_I64 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 64;
  }

  return result;
}

// Analyze string column from Arrow StringBuffer for dictionary potential
DictionaryAnalysis analyze_string_dictionary_arrow(const StringBuffer& values,
                                                   const NullBitmap& nulls,
                                                   double ratio_threshold) {
  DictionaryAnalysis result;
  result.total_values = values.size();

  if (values.empty()) {
    return result;
  }

  // Count unique values using hash set (excluding nulls)
  std::unordered_set<std::string_view> unique_values;

  for (size_t i = 0; i < values.size(); ++i) {
    if (nulls.is_valid(i)) {
      unique_values.insert(values.get(i));
    }
  }

  result.cardinality = unique_values.size();
  result.ratio = static_cast<double>(result.cardinality) / result.total_values;

  // Determine if dictionary is beneficial
  // Very low cardinality always benefits from dictionary (8-bit indices)
  if (result.cardinality <= DICT_THRESHOLD_I8) {
    result.should_use_dictionary = true;
    result.index_bit_width = 8;
  } else if (result.cardinality <= DICT_THRESHOLD_I16 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 16;
  } else if (result.cardinality <= DICT_THRESHOLD_I32 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 32;
  } else if (result.cardinality <= DICT_THRESHOLD_I64 && result.ratio < ratio_threshold) {
    result.should_use_dictionary = true;
    result.index_bit_width = 64;
  }

  return result;
}

// Create dictionary from string values
// Returns: (dictionary entries, indices)
std::pair<std::vector<std::string>, std::vector<int32_t>>
create_string_dictionary(const std::vector<std::string>& values,
                         const std::vector<bool>& null_bitmap) {
  std::vector<std::string> dictionary;
  std::vector<int32_t> indices(values.size());

  std::unordered_map<std::string, int32_t> value_to_index;

  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap.empty() && null_bitmap[i]) {
      // Null value - use -1 to indicate null
      indices[i] = -1;
      continue;
    }

    const auto& value = values[i];
    auto it = value_to_index.find(value);

    if (it != value_to_index.end()) {
      indices[i] = it->second;
    } else {
      int32_t new_index = static_cast<int32_t>(dictionary.size());
      dictionary.push_back(value);
      value_to_index[value] = new_index;
      indices[i] = new_index;
    }
  }

  return {std::move(dictionary), std::move(indices)};
}

// Create dictionary from Arrow StringBuffer
// Returns: (dictionary entries, indices)
std::pair<std::vector<std::string>, std::vector<int32_t>>
create_string_dictionary_arrow(const StringBuffer& values, const NullBitmap& nulls) {
  std::unordered_map<std::string_view, int32_t> value_to_index;
  std::vector<std::string> dictionary;
  std::vector<int32_t> indices;
  indices.reserve(values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    if (!nulls.is_valid(i)) {
      indices.push_back(-1); // Null marker
      continue;
    }

    std::string_view sv = values.get(i);
    auto it = value_to_index.find(sv);
    if (it != value_to_index.end()) {
      indices.push_back(it->second);
    } else {
      int32_t idx = static_cast<int32_t>(dictionary.size());
      dictionary.emplace_back(sv);
      value_to_index[sv] = idx;
      indices.push_back(idx);
    }
  }

  return {std::move(dictionary), std::move(indices)};
}

// Analyze int32 column for dictionary potential
DictionaryAnalysis analyze_int32_dictionary(const std::vector<int32_t>& values,
                                            const std::vector<bool>& null_bitmap,
                                            double ratio_threshold) {
  DictionaryAnalysis result;
  result.total_values = values.size();

  if (values.empty()) {
    return result;
  }

  std::unordered_map<int32_t, size_t> unique_values;

  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      unique_values[values[i]]++;
    }
  }

  result.cardinality = unique_values.size();
  result.ratio = static_cast<double>(result.cardinality) / result.total_values;

  // Integer columns typically don't benefit as much from dictionary
  // Use stricter thresholds (0.5x the ratio threshold)
  double strict_ratio = ratio_threshold * 0.5;

  if (result.cardinality <= DICT_THRESHOLD_I8) {
    result.should_use_dictionary = true;
    result.index_bit_width = 8;
  } else if (result.cardinality <= DICT_THRESHOLD_I16 && result.ratio < strict_ratio) {
    result.should_use_dictionary = true;
    result.index_bit_width = 16;
  }

  return result;
}

// Create dictionary from int32 values
std::pair<std::vector<int32_t>, std::vector<int32_t>>
create_int32_dictionary(const std::vector<int32_t>& values, const std::vector<bool>& null_bitmap) {
  std::vector<int32_t> dictionary;
  std::vector<int32_t> indices(values.size());

  std::unordered_map<int32_t, int32_t> value_to_index;

  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap.empty() && null_bitmap[i]) {
      indices[i] = -1;
      continue;
    }

    const auto value = values[i];
    auto it = value_to_index.find(value);

    if (it != value_to_index.end()) {
      indices[i] = it->second;
    } else {
      int32_t new_index = static_cast<int32_t>(dictionary.size());
      dictionary.push_back(value);
      value_to_index[value] = new_index;
      indices[i] = new_index;
    }
  }

  return {std::move(dictionary), std::move(indices)};
}

// Analyze int64 column for dictionary potential
DictionaryAnalysis analyze_int64_dictionary(const std::vector<int64_t>& values,
                                            const std::vector<bool>& null_bitmap,
                                            double ratio_threshold) {
  DictionaryAnalysis result;
  result.total_values = values.size();

  if (values.empty()) {
    return result;
  }

  std::unordered_map<int64_t, size_t> unique_values;

  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      unique_values[values[i]]++;
    }
  }

  result.cardinality = unique_values.size();
  result.ratio = static_cast<double>(result.cardinality) / result.total_values;

  // Int64 uses same strict thresholds as int32
  double strict_ratio = ratio_threshold * 0.5;

  if (result.cardinality <= DICT_THRESHOLD_I8) {
    result.should_use_dictionary = true;
    result.index_bit_width = 8;
  } else if (result.cardinality <= DICT_THRESHOLD_I16 && result.ratio < strict_ratio) {
    result.should_use_dictionary = true;
    result.index_bit_width = 16;
  }

  return result;
}

// Create dictionary from int64 values
std::pair<std::vector<int64_t>, std::vector<int32_t>>
create_int64_dictionary(const std::vector<int64_t>& values, const std::vector<bool>& null_bitmap) {
  std::vector<int64_t> dictionary;
  std::vector<int32_t> indices(values.size());

  std::unordered_map<int64_t, int32_t> value_to_index;

  for (size_t i = 0; i < values.size(); ++i) {
    if (!null_bitmap.empty() && null_bitmap[i]) {
      indices[i] = -1;
      continue;
    }

    const auto value = values[i];
    auto it = value_to_index.find(value);

    if (it != value_to_index.end()) {
      indices[i] = it->second;
    } else {
      int32_t new_index = static_cast<int32_t>(dictionary.size());
      dictionary.push_back(value);
      value_to_index[value] = new_index;
      indices[i] = new_index;
    }
  }

  return {std::move(dictionary), std::move(indices)};
}

} // namespace writer
} // namespace libvroom
