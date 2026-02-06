#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace libvroom {

// Type hierarchy: BOOL < INT32 < INT64 < FLOAT64 < STRING
// Lower types can be promoted to higher types during inference
enum class DataType : uint8_t {
  UNKNOWN = 0,
  BOOL = 1,
  INT32 = 2,
  INT64 = 3,
  FLOAT64 = 4,
  STRING = 5,
  DATE = 6,      // ISO8601 date
  TIMESTAMP = 7, // ISO8601 timestamp
  NA = 255       // Null/missing value
};

// Check if one type can be promoted to another
inline bool can_promote(DataType from, DataType to) {
  if (from == DataType::NA || from == DataType::UNKNOWN)
    return true;
  if (to == DataType::STRING)
    return true;
  return static_cast<uint8_t>(from) <= static_cast<uint8_t>(to);
}

// Get the wider type between two types
inline DataType wider_type(DataType a, DataType b) {
  if (a == DataType::NA || a == DataType::UNKNOWN)
    return b;
  if (b == DataType::NA || b == DataType::UNKNOWN)
    return a;
  // STRING is the universal fallback
  if (a == DataType::STRING || b == DataType::STRING)
    return DataType::STRING;
  // DATE/TIMESTAMP don't promote to numeric types
  if ((a == DataType::DATE || a == DataType::TIMESTAMP) &&
      (b >= DataType::BOOL && b <= DataType::FLOAT64))
    return DataType::STRING;
  if ((b == DataType::DATE || b == DataType::TIMESTAMP) &&
      (a >= DataType::BOOL && a <= DataType::FLOAT64))
    return DataType::STRING;
  return static_cast<uint8_t>(a) > static_cast<uint8_t>(b) ? a : b;
}

// String representation of types
inline const char* type_name(DataType type) {
  switch (type) {
  case DataType::UNKNOWN:
    return "UNKNOWN";
  case DataType::BOOL:
    return "BOOL";
  case DataType::INT32:
    return "INT32";
  case DataType::INT64:
    return "INT64";
  case DataType::FLOAT64:
    return "FLOAT64";
  case DataType::STRING:
    return "STRING";
  case DataType::DATE:
    return "DATE";
  case DataType::TIMESTAMP:
    return "TIMESTAMP";
  case DataType::NA:
    return "NA";
  default:
    return "INVALID";
  }
}

// Compression codec for Parquet
enum class Compression : uint8_t { NONE = 0, ZSTD = 1, SNAPPY = 2, LZ4 = 3, GZIP = 4 };

inline const char* compression_name(Compression c) {
  switch (c) {
  case Compression::NONE:
    return "none";
  case Compression::ZSTD:
    return "zstd";
  case Compression::SNAPPY:
    return "snappy";
  case Compression::LZ4:
    return "lz4";
  case Compression::GZIP:
    return "gzip";
  default:
    return "unknown";
  }
}

// Parquet encoding types
enum class Encoding : uint8_t {
  PLAIN = 0,
  RLE = 1,
  DELTA_BINARY_PACKED = 2,
  DELTA_LENGTH_BYTE_ARRAY = 3,
  DICTIONARY = 4
};

// A view into a field in the CSV
struct FieldView {
  const char* data = nullptr;
  size_t size = 0;
  bool quoted = false;

  std::string_view view() const { return {data, size}; }
  bool empty() const { return size == 0; }
};

// Chunk boundary information
struct ChunkBoundary {
  size_t start_offset = 0;    // Byte offset of chunk start
  size_t end_offset = 0;      // Byte offset of chunk end (exclusive)
  size_t row_count = 0;       // Number of rows in this chunk
  bool ends_in_quote = false; // True if chunk ends inside a quoted field
};

// Column schema information
struct ColumnSchema {
  std::string name;
  DataType type = DataType::STRING;
  bool nullable = true;
  size_t index = 0; // Original column index in CSV
};

// Result type for operations that can fail
template <typename T> struct Result {
  T value;
  std::string error;
  bool ok = true;

  static Result success(T&& val) { return {std::move(val), "", true}; }
  static Result failure(std::string err) { return {{}, std::move(err), false}; }

  explicit operator bool() const { return ok; }
};

// Specialization for void result (operations that succeed or fail with no value)
template <> struct Result<void> {
  std::string error;
  bool ok = true;

  static Result success() { return {"", true}; }
  static Result failure(std::string err) { return {std::move(err), false}; }

  explicit operator bool() const { return ok; }
};

// Statistics for a column chunk
struct ColumnStatistics {
  bool has_null = false;
  int64_t null_count = 0;
  int64_t distinct_count = 0;

  // Min/max as variants (type-specific)
  std::variant<std::monostate, bool, int32_t, int64_t, double, std::string> min_value;
  std::variant<std::monostate, bool, int32_t, int64_t, double, std::string> max_value;
};

} // namespace libvroom
