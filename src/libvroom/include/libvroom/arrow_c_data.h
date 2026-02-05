#pragma once

#include <cstdint>

namespace libvroom {

// Arrow C Data Interface flag constants
constexpr int64_t ARROW_FLAG_DICTIONARY_ORDERED = 1;
constexpr int64_t ARROW_FLAG_NULLABLE = 2;
constexpr int64_t ARROW_FLAG_MAP_KEYS_SORTED = 4;

// Arrow format strings for each type
// See: https://arrow.apache.org/docs/format/CDataInterface.html#format-strings
namespace arrow_format {
constexpr const char* INT32 = "i";           // 32-bit signed integer
constexpr const char* INT64 = "l";           // 64-bit signed integer
constexpr const char* FLOAT64 = "g";         // 64-bit IEEE floating point
constexpr const char* BOOL = "b";            // boolean (1 bit per value, packed)
constexpr const char* UTF8 = "u";            // UTF-8 string (32-bit offsets)
constexpr const char* DATE32 = "tdD";        // date32 (days since Unix epoch)
constexpr const char* TIMESTAMP_US = "tsu:"; // timestamp (microseconds, no timezone)
constexpr const char* STRUCT = "+s";         // struct (for table export)
} // namespace arrow_format

// Arrow C Data Interface structures
// These must match the Arrow spec exactly for FFI compatibility
// See: https://arrow.apache.org/docs/format/CDataInterface.html

struct ArrowSchema {
  // Format string describing the type
  const char* format;
  // Optional name
  const char* name;
  // Optional metadata (null for us)
  const char* metadata;
  // Flags (ARROW_FLAG_NULLABLE, etc.)
  int64_t flags;
  // Number of children for nested types
  int64_t n_children;
  // Array of child schemas
  ArrowSchema** children;
  // Optional dictionary schema
  ArrowSchema* dictionary;
  // Release callback - MUST be called by consumer when done
  void (*release)(ArrowSchema*);
  // Private data for release callback
  void* private_data;
};

struct ArrowArray {
  // Length of this array (number of elements)
  int64_t length;
  // Number of null values
  int64_t null_count;
  // Offset into buffers (0 for us)
  int64_t offset;
  // Number of buffers
  int64_t n_buffers;
  // Number of children
  int64_t n_children;
  // Array of buffer pointers
  const void** buffers;
  // Array of child arrays
  ArrowArray** children;
  // Optional dictionary array
  ArrowArray* dictionary;
  // Release callback - MUST be called by consumer when done
  void (*release)(ArrowArray*);
  // Private data for release callback
  void* private_data;
};

struct ArrowArrayStream {
  // Callback to get schema
  int (*get_schema)(ArrowArrayStream*, ArrowSchema* out);
  // Callback to get next batch (returns 0 and sets out->release=nullptr when done)
  int (*get_next)(ArrowArrayStream*, ArrowArray* out);
  // Callback to get error message
  const char* (*get_last_error)(ArrowArrayStream*);
  // Release callback
  void (*release)(ArrowArrayStream*);
  // Private data
  void* private_data;
};

// Helper to initialize an empty (released) schema
inline void init_empty_schema(ArrowSchema* schema) {
  schema->format = nullptr;
  schema->name = nullptr;
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->release = nullptr;
  schema->private_data = nullptr;
}

// Helper to initialize an empty (released) array
inline void init_empty_array(ArrowArray* array) {
  array->length = 0;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 0;
  array->n_children = 0;
  array->buffers = nullptr;
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = nullptr;
  array->private_data = nullptr;
}

} // namespace libvroom
