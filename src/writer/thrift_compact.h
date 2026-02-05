#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace libvroom {
namespace writer {

/**
 * Thrift Compact Protocol Writer
 *
 * Implements the Thrift Compact Protocol (THRIFT-110) for serializing
 * Parquet metadata structures. This is a minimal implementation supporting
 * only the types needed for Parquet:
 * - bool, i16, i32, i64, double
 * - string, binary
 * - struct, list
 *
 * Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
 */
class ThriftCompactWriter {
public:
  explicit ThriftCompactWriter(std::vector<uint8_t>& buffer);

  // Struct operations
  void write_struct_begin();
  void write_struct_end();

  // Field operations (for structs)
  // type_id: Thrift compact type ID
  void write_field_begin(int16_t field_id, uint8_t type_id);
  void write_field_stop();

  // Basic types
  void write_bool(bool value);
  void write_i16(int16_t value);
  void write_i32(int32_t value);
  void write_i64(int64_t value);
  void write_double(double value);
  void write_string(const std::string& value);
  void write_binary(const std::vector<uint8_t>& value);
  void write_binary(const uint8_t* data, size_t size);

  // List operations
  void write_list_begin(uint8_t element_type, int32_t size);
  void write_list_end();

  // Helper to get current buffer size
  size_t size() const;

  // Thrift compact type IDs
  static constexpr uint8_t TYPE_BOOL_TRUE = 1;
  static constexpr uint8_t TYPE_BOOL_FALSE = 2;
  static constexpr uint8_t TYPE_BYTE = 3;
  static constexpr uint8_t TYPE_I16 = 4;
  static constexpr uint8_t TYPE_I32 = 5;
  static constexpr uint8_t TYPE_I64 = 6;
  static constexpr uint8_t TYPE_DOUBLE = 7;
  static constexpr uint8_t TYPE_BINARY = 8; // Also used for strings
  static constexpr uint8_t TYPE_LIST = 9;
  static constexpr uint8_t TYPE_SET = 10;
  static constexpr uint8_t TYPE_MAP = 11;
  static constexpr uint8_t TYPE_STRUCT = 12;

private:
  std::vector<uint8_t>& buffer_;
  int16_t last_field_id_;
  std::vector<int16_t> field_id_stack_; // For nested structs

  // Write unsigned varint (ULEB128)
  void write_varint(uint64_t value);

  // Write signed varint (zigzag encoded)
  void write_signed_varint(int64_t value);

  // Zigzag encode signed integer to unsigned
  static uint64_t zigzag_encode(int64_t value);
};

/**
 * Helper class to serialize a struct field with RAII
 * Automatically handles field begin/end
 */
class ThriftFieldScope {
public:
  ThriftFieldScope(ThriftCompactWriter& writer, int16_t field_id, uint8_t type_id)
      : writer_(writer), has_value_(true) {
    writer_.write_field_begin(field_id, type_id);
  }

  // For optional fields - only write if value exists
  ThriftFieldScope(ThriftCompactWriter& writer, int16_t field_id, uint8_t type_id, bool has_value)
      : writer_(writer), has_value_(has_value) {
    if (has_value_) {
      writer_.write_field_begin(field_id, type_id);
    }
  }

  bool should_write() const { return has_value_; }

private:
  ThriftCompactWriter& writer_;
  bool has_value_;
};

} // namespace writer
} // namespace libvroom
