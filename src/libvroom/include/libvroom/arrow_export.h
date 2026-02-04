#pragma once

#include "arrow_c_data.h"
#include "types.h"

#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Forward declaration
class ArrowColumnBuilder;

// Private data for column ArrowArray - prevents column destruction while consumer holds reference
struct ArrowColumnPrivate {
  // Keep column alive via raw pointer (Table owns the unique_ptr)
  // This works because the stream keeps the Table alive
  std::vector<const void*> buffers;
  std::string name_storage; // Storage for column name string

  // For string columns, we need child schema/array for offsets
  std::unique_ptr<ArrowSchema> child_schema;
  std::unique_ptr<ArrowArray> child_array;
  ArrowSchema* child_schema_ptr = nullptr;
  ArrowArray* child_array_ptr = nullptr;
};

// Private data for ArrowSchema
struct ArrowSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<ArrowSchema>> child_schemas;
  std::vector<ArrowSchema*> child_schema_ptrs;
};

// Get Arrow format string for a DataType
inline const char* get_arrow_format(DataType type) {
  switch (type) {
  case DataType::INT32:
    return arrow_format::INT32;
  case DataType::INT64:
    return arrow_format::INT64;
  case DataType::FLOAT64:
    return arrow_format::FLOAT64;
  case DataType::BOOL:
    return arrow_format::BOOL;
  case DataType::STRING:
    return arrow_format::UTF8;
  case DataType::DATE:
    return arrow_format::DATE32;
  case DataType::TIMESTAMP:
    return arrow_format::TIMESTAMP_US;
  default:
    return arrow_format::UTF8; // Default to string
  }
}

// Release callback for ArrowSchema
inline void release_arrow_schema(ArrowSchema* schema) {
  if (schema->release == nullptr)
    return; // Already released

  if (schema->private_data) {
    delete static_cast<ArrowSchemaPrivate*>(schema->private_data);
  }

  // Mark as released
  schema->release = nullptr;
}

// Release callback for column ArrowArray
inline void release_arrow_array(ArrowArray* array) {
  if (array->release == nullptr)
    return; // Already released

  if (array->private_data) {
    delete static_cast<ArrowColumnPrivate*>(array->private_data);
  }

  // Mark as released
  array->release = nullptr;
}

} // namespace libvroom
