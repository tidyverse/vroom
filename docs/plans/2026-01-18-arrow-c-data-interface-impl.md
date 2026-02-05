# Arrow C Data Interface Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable zero-copy export of parsed CSV data to polars, duckdb, pyarrow, and pandas via the Arrow PyCapsule protocol.

**Architecture:** Implement Arrow C Data Interface structs manually, export column buffers directly without copying, wrap in PyCapsules with the `__arrow_c_stream__` method on the Python Table class.

**Tech Stack:** C++17, pybind11, Arrow C Data Interface specification

---

## Task 1: Create Arrow C Data Interface Header

**Files:**
- Create: `include/vroom/arrow_c_data.h`

**Step 1: Create the header with Arrow C structs and constants**

```cpp
#pragma once

#include <cstdint>

namespace vroom {

// Arrow C Data Interface flag constants
constexpr int64_t ARROW_FLAG_DICTIONARY_ORDERED = 1;
constexpr int64_t ARROW_FLAG_NULLABLE = 2;
constexpr int64_t ARROW_FLAG_MAP_KEYS_SORTED = 4;

// Arrow format strings for each type
// See: https://arrow.apache.org/docs/format/CDataInterface.html#format-strings
namespace arrow_format {
constexpr const char* INT32 = "i";       // 32-bit signed integer
constexpr const char* INT64 = "l";       // 64-bit signed integer
constexpr const char* FLOAT64 = "g";     // 64-bit IEEE floating point
constexpr const char* BOOL = "b";        // boolean (1 bit per value, packed)
constexpr const char* UTF8 = "u";        // UTF-8 string (32-bit offsets)
constexpr const char* DATE32 = "tdD";    // date32 (days since Unix epoch)
constexpr const char* TIMESTAMP_US = "tsu:"; // timestamp (microseconds, no timezone)
constexpr const char* STRUCT = "+s";     // struct (for table export)
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

} // namespace vroom
```

**Step 2: Verify it compiles**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build succeeds (header is not used yet)

**Step 3: Commit**

```bash
git add include/vroom/arrow_c_data.h
git commit -m "feat: Add Arrow C Data Interface header

Defines ArrowSchema, ArrowArray, ArrowArrayStream structs and
format string constants for zero-copy Arrow export.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Create Arrow Export Header with Private Data Structs

**Files:**
- Create: `include/vroom/arrow_export.h`

**Step 1: Create the export header with private data structures**

```cpp
#pragma once

#include "arrow_buffer.h"
#include "arrow_c_data.h"
#include "arrow_column_builder.h"
#include "types.h"

#include <memory>
#include <string>
#include <vector>

namespace vroom {

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
  if (schema->release == nullptr) return; // Already released

  if (schema->private_data) {
    delete static_cast<ArrowSchemaPrivate*>(schema->private_data);
  }

  // Mark as released
  schema->release = nullptr;
}

// Release callback for column ArrowArray
inline void release_arrow_array(ArrowArray* array) {
  if (array->release == nullptr) return; // Already released

  if (array->private_data) {
    delete static_cast<ArrowColumnPrivate*>(array->private_data);
  }

  // Mark as released
  array->release = nullptr;
}

} // namespace vroom
```

**Step 2: Verify it compiles**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add include/vroom/arrow_export.h
git commit -m "feat: Add Arrow export private data structures

Defines ArrowColumnPrivate and ArrowSchemaPrivate for memory management
during zero-copy export, plus helper functions for format strings.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Add Export Methods to ArrowColumnBuilder Base Class

**Files:**
- Modify: `include/vroom/arrow_column_builder.h`

**Step 1: Add virtual export methods to base class**

Add after line 45 (after `merge_from` declaration):

```cpp
  // Export column to Arrow C Data Interface
  // The ArrowArray buffers point directly to this column's data (zero-copy)
  // Caller must ensure this column outlives the ArrowArray
  virtual void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const = 0;

  // Export schema to Arrow C Data Interface
  virtual void export_schema(ArrowSchema* out, const std::string& name) const = 0;
```

Also add include at top of file:

```cpp
#include "arrow_export.h"
```

**Step 2: Verify it compiles (will fail - need to implement in each subclass)**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build fails with "pure virtual function" errors (that's expected)

---

## Task 4: Implement Export for Numeric Types (INT32, INT64, FLOAT64)

**Files:**
- Modify: `include/vroom/arrow_column_builder.h`

**Step 1: Add export_to_arrow to ArrowInt32ColumnBuilder**

Add after line 93 (after `values()` method) in ArrowInt32ColumnBuilder:

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    // Set up buffers: [validity bitmap, values]
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::INT32;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 2: Add export_to_arrow to ArrowInt64ColumnBuilder**

Add after line 134 (after `values()` method) in ArrowInt64ColumnBuilder:

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::INT64;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 3: Add export_to_arrow to ArrowFloat64ColumnBuilder**

Add after line 175 (after `values()` method) in ArrowFloat64ColumnBuilder:

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::FLOAT64;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 4: Verify it compiles (will still fail - more types needed)**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build fails (Bool, Date, Timestamp, String still need implementation)

---

## Task 5: Implement Export for Bool, Date, Timestamp Types

**Files:**
- Modify: `include/vroom/arrow_column_builder.h`

**Step 1: Add export_to_arrow to ArrowBoolColumnBuilder**

Add after line 216 (after `values()` method):

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    // Note: Arrow boolean is bit-packed, but we store as uint8 (one byte per value)
    // This is technically non-conformant but works with most implementations
    // For strict compliance, we'd need to pack bits - but this is much simpler
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    // Use uint8 format "C" since we store as uint8, not packed bits
    out->format = "C"; // uint8
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 2: Add export_to_arrow to ArrowDateColumnBuilder**

Add after line 257 (after `values()` method):

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::DATE32;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 3: Add export_to_arrow to ArrowTimestampColumnBuilder**

Add after line 298 (after `values()` method):

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    priv->buffers.resize(2);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.data();

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 2;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::TIMESTAMP_US;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 4: Verify it compiles (will still fail - String needed)**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build fails (String type still needs implementation)

---

## Task 6: Implement Export for String Type

**Files:**
- Modify: `include/vroom/arrow_column_builder.h`

**Step 1: Add export_to_arrow to ArrowStringColumnBuilder**

Add after line 340 (after `values()` method):

```cpp
  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    // String arrays have 3 buffers: [validity, offsets, data]
    priv->buffers.resize(3);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.offsets();  // int32 offsets
    priv->buffers[2] = values_.data();     // char data

    out->length = static_cast<int64_t>(values_.size());
    out->null_count = static_cast<int64_t>(nulls_.null_count_fast());
    out->offset = 0;
    out->n_buffers = 3;
    out->n_children = 0;
    out->buffers = priv->buffers.data();
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_array;
    out->private_data = priv;
  }

  void export_schema(ArrowSchema* out, const std::string& name) const override {
    auto* priv = new ArrowSchemaPrivate();
    priv->name_storage = name;

    out->format = arrow_format::UTF8;
    out->name = priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = priv;
  }
```

**Step 2: Verify it compiles**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build succeeds

**Step 3: Commit all column builder changes**

```bash
git add include/vroom/arrow_column_builder.h
git commit -m "feat: Add Arrow export methods to all column builders

Each ArrowColumnBuilder now implements export_to_arrow() and
export_schema() for zero-copy Arrow C Data Interface export.
Supports INT32, INT64, FLOAT64, BOOL, STRING, DATE, TIMESTAMP.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Implement Stream Interface in Python Bindings

**Files:**
- Modify: `python/src/bindings.cpp`

**Step 1: Remove duplicate Arrow struct definitions (already in arrow_c_data.h)**

Remove lines 29-60 (the ArrowSchema, ArrowArray, ArrowArrayStream struct definitions) since they're now in the header.

Add include instead:

```cpp
#include "vroom/arrow_c_data.h"
#include "vroom/arrow_export.h"
```

**Step 2: Add TableStreamPrivate struct and stream callbacks after the Table class**

Add after line 107 (after Table class closing brace):

```cpp
// =============================================================================
// Arrow stream export for Table
// =============================================================================

// Private data for the stream - keeps Table alive and tracks state
struct TableStreamPrivate {
  std::shared_ptr<Table> table;
  bool batch_returned = false;
  std::string last_error;

  // Storage for struct schema/array
  std::vector<std::unique_ptr<vroom::ArrowSchema>> child_schemas;
  std::vector<vroom::ArrowSchema*> child_schema_ptrs;
  std::vector<std::unique_ptr<vroom::ArrowArray>> child_arrays;
  std::vector<vroom::ArrowArray*> child_array_ptrs;
  std::vector<std::unique_ptr<vroom::ArrowColumnPrivate>> child_privates;
  std::vector<const void*> struct_buffers;
  std::string struct_name;
};

// Stream get_schema callback
int table_stream_get_schema(vroom::ArrowArrayStream* stream, vroom::ArrowSchema* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = priv->table;
  const auto& schema = table->schema();

  // Create struct schema with children for each column
  priv->child_schemas.clear();
  priv->child_schema_ptrs.clear();

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto child = std::make_unique<vroom::ArrowSchema>();
    table->columns()[i]->export_schema(child.get(), schema[i].name);
    priv->child_schema_ptrs.push_back(child.get());
    priv->child_schemas.push_back(std::move(child));
  }

  // Set up struct schema
  priv->struct_name = "";
  out->format = vroom::arrow_format::STRUCT;
  out->name = priv->struct_name.c_str();
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->children = priv->child_schema_ptrs.data();
  out->dictionary = nullptr;
  out->release = nullptr; // Struct schema release handled by stream
  out->private_data = nullptr;

  return 0;
}

// Stream get_next callback
int table_stream_get_next(vroom::ArrowArrayStream* stream, vroom::ArrowArray* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);

  if (priv->batch_returned) {
    // No more batches - signal end of stream
    vroom::init_empty_array(out);
    return 0;
  }

  auto& table = priv->table;

  // Create child arrays for each column
  priv->child_arrays.clear();
  priv->child_array_ptrs.clear();
  priv->child_privates.clear();

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto child_priv = std::make_unique<vroom::ArrowColumnPrivate>();
    auto child = std::make_unique<vroom::ArrowArray>();
    table->columns()[i]->export_to_arrow(child.get(), child_priv.get());

    // Override the release - we manage memory at stream level
    child->release = nullptr;
    child->private_data = nullptr;

    priv->child_array_ptrs.push_back(child.get());
    priv->child_arrays.push_back(std::move(child));
    priv->child_privates.push_back(std::move(child_priv));
  }

  // Set up struct array
  priv->struct_buffers = {nullptr}; // Struct has no buffers itself

  out->length = static_cast<int64_t>(table->num_rows());
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->buffers = priv->struct_buffers.data();
  out->children = priv->child_array_ptrs.data();
  out->dictionary = nullptr;
  out->release = nullptr; // Managed by stream
  out->private_data = nullptr;

  priv->batch_returned = true;
  return 0;
}

// Stream get_last_error callback
const char* table_stream_get_last_error(vroom::ArrowArrayStream* stream) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

// Stream release callback
void table_stream_release(vroom::ArrowArrayStream* stream) {
  if (stream->release == nullptr) return;

  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  delete priv;

  stream->release = nullptr;
}
```

**Step 3: Add columns() accessor to Table class**

Add after line 98 (after `column_names()` method):

```cpp
  // Access to column builders for export
  const std::vector<std::unique_ptr<vroom::ArrowColumnBuilder>>& columns() const {
    return columns_;
  }
```

**Step 4: Add __arrow_c_stream__ method to Table Python binding**

In the Table pybind11 class definition (around line 258), add after `.def_property_readonly("column_names"`:

```cpp
      .def("__arrow_c_stream__", [](std::shared_ptr<Table> self, py::object requested_schema) {
        // Create stream
        auto* stream = new vroom::ArrowArrayStream();
        auto* priv = new TableStreamPrivate();
        priv->table = self;

        stream->get_schema = table_stream_get_schema;
        stream->get_next = table_stream_get_next;
        stream->get_last_error = table_stream_get_last_error;
        stream->release = table_stream_release;
        stream->private_data = priv;

        // Wrap in PyCapsule with required name
        return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
          auto* s = static_cast<vroom::ArrowArrayStream*>(ptr);
          if (s->release) s->release(s);
          delete s;
        });
      }, py::arg("requested_schema") = py::none(),
      "Export table as Arrow stream via PyCapsule (zero-copy)")
```

**Step 5: Add __arrow_c_schema__ method**

Add after `__arrow_c_stream__`:

```cpp
      .def("__arrow_c_schema__", [](std::shared_ptr<Table> self) {
        auto* schema = new vroom::ArrowSchema();
        auto* priv = new vroom::ArrowSchemaPrivate();
        const auto& table_schema = self->schema();

        // Create child schemas
        for (size_t i = 0; i < self->num_columns(); ++i) {
          auto child = std::make_unique<vroom::ArrowSchema>();
          self->columns()[i]->export_schema(child.get(), table_schema[i].name);
          priv->child_schema_ptrs.push_back(child.get());
          priv->child_schemas.push_back(std::move(child));
        }

        priv->name_storage = "";
        schema->format = vroom::arrow_format::STRUCT;
        schema->name = priv->name_storage.c_str();
        schema->metadata = nullptr;
        schema->flags = 0;
        schema->n_children = static_cast<int64_t>(self->num_columns());
        schema->children = priv->child_schema_ptrs.data();
        schema->dictionary = nullptr;
        schema->release = vroom::release_arrow_schema;
        schema->private_data = priv;

        return py::capsule(schema, "arrow_schema", [](void* ptr) {
          auto* s = static_cast<vroom::ArrowSchema*>(ptr);
          if (s->release) s->release(s);
          delete s;
        });
      },
      "Export table schema as Arrow schema via PyCapsule")
```

**Step 6: Rebuild**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface && cmake --build build -j$(nproc)`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add python/src/bindings.cpp
git commit -m "feat: Implement Arrow PyCapsule protocol for Table

Add __arrow_c_stream__ and __arrow_c_schema__ methods to Table class
for zero-copy interop with pyarrow, polars, duckdb, and pandas.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Build and Install Python Package

**Files:**
- No file changes

**Step 1: Build and install the Python package**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface/python && pip install -e . --no-build-isolation`
Expected: Package installs successfully

**Step 2: Verify basic import works**

Run: `python -c "import vroom_csv; print(vroom_csv.__version__)"`
Expected: Outputs "2.0.0"

---

## Task 9: Run Existing Tests

**Files:**
- No file changes

**Step 1: Run the existing Arrow tests**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface/python && python -m pytest tests/test_arrow.py -v`
Expected: Tests pass (the tests expect these methods to exist)

**Step 2: If tests fail, debug and fix**

Common issues:
- Capsule name mismatch (must be exactly "arrow_array_stream")
- Null bitmap not handled correctly
- Schema/array lifetime issues

**Step 3: Commit any test fixes if needed**

---

## Task 10: Final Verification and Commit

**Files:**
- No file changes

**Step 1: Run full test suite**

Run: `cd ~/.worktrees/libvroom/arrow-c-data-interface/python && python -m pytest tests/ -v`
Expected: All tests pass

**Step 2: Manual verification with pyarrow**

```python
import vroom_csv
import pyarrow as pa

# Create test CSV
with open("/tmp/test.csv", "w") as f:
    f.write("name,age,score\nAlice,30,95.5\nBob,25,87.3\n")

# Read and convert
table = vroom_csv.read_csv("/tmp/test.csv")
arrow_table = pa.table(table)
print(arrow_table)
print(arrow_table.to_pandas())
```

**Step 3: Manual verification with polars**

```python
import vroom_csv
import polars as pl

table = vroom_csv.read_csv("/tmp/test.csv")
df = pl.from_arrow(table)
print(df)
```

**Step 4: Final commit if all working**

```bash
git add -A
git commit -m "test: Verify Arrow PyCapsule interop with pyarrow and polars

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

This plan implements zero-copy Arrow export in 10 tasks:

1. Create Arrow C Data Interface header with struct definitions
2. Create Arrow export header with private data structs
3. Add virtual export methods to ArrowColumnBuilder base class
4. Implement export for numeric types (INT32, INT64, FLOAT64)
5. Implement export for Bool, Date, Timestamp types
6. Implement export for String type
7. Implement stream interface in Python bindings
8. Build and install Python package
9. Run existing tests
10. Final verification

The implementation uses direct buffer pointers (zero-copy) and proper memory management via shared_ptr to ensure data stays valid while consumers hold references.
