#include "libvroom/vroom.h"

#include <cstring>

namespace libvroom {
namespace writer {
namespace encoding {

// Forward declaration from delta_bitpacked.cpp
void encode_int32_delta(const int32_t* values, size_t count, std::vector<uint8_t>& output);

// Delta Length Byte Array encoding for Parquet
// Encodes string lengths using delta encoding, then concatenates all string bytes
// Good for strings with similar lengths

void encode_delta_length_byte_array(const std::vector<std::string>& values,
                                    const std::vector<bool>& null_bitmap,
                                    std::vector<uint8_t>& output) {
  if (values.empty())
    return;

  // Collect lengths (only non-null values)
  std::vector<int32_t> lengths;
  lengths.reserve(values.size());

  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      lengths.push_back(static_cast<int32_t>(values[i].size()));
    }
  }

  // Encode lengths using delta encoding
  std::vector<uint8_t> length_encoded;
  encode_int32_delta(lengths.data(), lengths.size(), length_encoded);

  // Write length-encoded data
  output.insert(output.end(), length_encoded.begin(), length_encoded.end());

  // Write concatenated string data
  for (size_t i = 0; i < values.size(); ++i) {
    if (null_bitmap.empty() || !null_bitmap[i]) {
      output.insert(output.end(), values[i].begin(), values[i].end());
    }
  }
}

} // namespace encoding
} // namespace writer
} // namespace libvroom
