#include "thrift_compact.h"

#include <cstring>

namespace libvroom {
namespace writer {

ThriftCompactWriter::ThriftCompactWriter(std::vector<uint8_t>& buffer)
    : buffer_(buffer), last_field_id_(0) {}

void ThriftCompactWriter::write_struct_begin() {
  // Save current field ID for nested structs
  field_id_stack_.push_back(last_field_id_);
  last_field_id_ = 0;
}

void ThriftCompactWriter::write_struct_end() {
  // Write struct terminator (field stop)
  write_field_stop();
  // Restore previous field ID
  if (!field_id_stack_.empty()) {
    last_field_id_ = field_id_stack_.back();
    field_id_stack_.pop_back();
  }
}

void ThriftCompactWriter::write_field_begin(int16_t field_id, uint8_t type_id) {
  int16_t delta = field_id - last_field_id_;

  if (delta > 0 && delta <= 15) {
    // Use short form: (delta << 4) | type
    buffer_.push_back(static_cast<uint8_t>((delta << 4) | type_id));
  } else {
    // Use long form: type byte, then field ID as zigzag varint
    buffer_.push_back(type_id);
    write_signed_varint(field_id);
  }

  last_field_id_ = field_id;
}

void ThriftCompactWriter::write_field_stop() {
  buffer_.push_back(0); // Field stop marker
}

void ThriftCompactWriter::write_bool(bool value) {
  // For bool fields, the type ID encodes the value:
  // TYPE_BOOL_TRUE (1) or TYPE_BOOL_FALSE (2)
  // But when writing standalone, we use a single byte
  buffer_.push_back(value ? 1 : 0);
}

void ThriftCompactWriter::write_i16(int16_t value) {
  write_signed_varint(value);
}

void ThriftCompactWriter::write_i32(int32_t value) {
  write_signed_varint(value);
}

void ThriftCompactWriter::write_i64(int64_t value) {
  write_signed_varint(value);
}

void ThriftCompactWriter::write_double(double value) {
  // Doubles are written as 8 bytes, little-endian
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  for (int i = 0; i < 8; ++i) {
    buffer_.push_back(static_cast<uint8_t>(bits >> (i * 8)));
  }
}

void ThriftCompactWriter::write_string(const std::string& value) {
  write_varint(value.size());
  buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void ThriftCompactWriter::write_binary(const std::vector<uint8_t>& value) {
  write_varint(value.size());
  buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void ThriftCompactWriter::write_binary(const uint8_t* data, size_t size) {
  write_varint(size);
  buffer_.insert(buffer_.end(), data, data + size);
}

void ThriftCompactWriter::write_list_begin(uint8_t element_type, int32_t size) {
  if (size < 15) {
    // Short form: (size << 4) | element_type
    buffer_.push_back(static_cast<uint8_t>((size << 4) | element_type));
  } else {
    // Long form: 0xF | element_type, then size as varint
    buffer_.push_back(static_cast<uint8_t>(0xF0 | element_type));
    write_varint(static_cast<uint64_t>(size));
  }
}

void ThriftCompactWriter::write_list_end() {
  // No action needed - lists don't have terminators
}

size_t ThriftCompactWriter::size() const {
  return buffer_.size();
}

void ThriftCompactWriter::write_varint(uint64_t value) {
  // ULEB128 encoding
  while (value >= 0x80) {
    buffer_.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  buffer_.push_back(static_cast<uint8_t>(value));
}

void ThriftCompactWriter::write_signed_varint(int64_t value) {
  write_varint(zigzag_encode(value));
}

uint64_t ThriftCompactWriter::zigzag_encode(int64_t value) {
  // Zigzag encoding: positive numbers become even, negative become odd
  // n -> (n << 1) ^ (n >> 63)
  return static_cast<uint64_t>((value << 1) ^ (value >> 63));
}

} // namespace writer
} // namespace libvroom
