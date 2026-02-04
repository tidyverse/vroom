# Arrow C Data Interface for Zero-Copy Python Export

**Date:** 2026-01-18
**Status:** Approved
**Author:** Jim Hester

## Overview

Implement the Arrow C Data Interface to enable zero-copy handoff of parsed CSV data from libvroom to Python libraries (polars, duckdb, pyarrow, pandas).

## Goals

- Zero-copy export: no data duplication when handing off to other libraries
- Standard interface: use Arrow PyCapsule protocol (`__arrow_c_stream__`)
- No new dependencies: implement C Data Interface manually (no Arrow C++ library)
- All 7 column types: INT32, INT64, FLOAT64, BOOL, STRING, DATE, TIMESTAMP

## Non-Goals (for now)

- Import from other libraries (only export)
- True streaming (single batch in stream interface for now)
- Dictionary encoding

## Arrow C Data Interface Structs

Three C structs defined by the Arrow spec, placed in `include/vroom/arrow_c_data.h`:

```cpp
struct ArrowSchema {
  const char* format;      // Type format string
  const char* name;        // Column name
  const char* metadata;    // Optional metadata (null)
  int64_t flags;           // ARROW_FLAG_NULLABLE = 2
  int64_t n_children;      // 0 for primitives
  ArrowSchema** children;  // Child schemas
  ArrowSchema* dictionary; // null (no dictionary encoding)
  void (*release)(ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;          // Number of rows
  int64_t null_count;      // Number of nulls
  int64_t offset;          // Start offset (0)
  int64_t n_buffers;       // 2 for primitives, 3 for strings
  int64_t n_children;      // 0 for primitives
  const void** buffers;    // [null_bitmap, values] or [null_bitmap, offsets, data]
  ArrowArray** children;
  ArrowArray* dictionary;  // null
  void (*release)(ArrowArray*);
  void* private_data;
};

struct ArrowArrayStream {
  int (*get_schema)(ArrowArrayStream*, ArrowSchema*);
  int (*get_next)(ArrowArrayStream*, ArrowArray*);
  const char* (*get_last_error)(ArrowArrayStream*);
  void (*release)(ArrowArrayStream*);
  void* private_data;
};
```

## Type Format Strings

| libvroom Type | Arrow Format | Buffers |
|---------------|--------------|---------|
| INT32 | `"i"` | [null_bitmap, int32 values] |
| INT64 | `"l"` | [null_bitmap, int64 values] |
| FLOAT64 | `"g"` | [null_bitmap, float64 values] |
| BOOL | `"b"` | [null_bitmap, packed bits] |
| STRING | `"u"` | [null_bitmap, int32 offsets, char data] |
| DATE | `"tdD"` | [null_bitmap, int32 days since epoch] |
| TIMESTAMP | `"tsu:"` | [null_bitmap, int64 microseconds] |

## Memory Ownership Model

1. `Table` owns `ArrowColumnBuilder` objects which own the data buffers
2. Export creates ArrowArray structs pointing to these buffers (no copy)
3. Consumer calls `release()` when done, decrementing reference count
4. Shared ownership via `std::shared_ptr` prevents premature deallocation

```cpp
struct ArrowArrayPrivate {
  std::shared_ptr<ArrowColumnBuilder> column;
  std::vector<const void*> buffers_storage;
};

void release_arrow_array(ArrowArray* array) {
  if (array->release == nullptr) return;
  auto* priv = static_cast<ArrowArrayPrivate*>(array->private_data);
  delete priv;
  array->release = nullptr;
}
```

## Table Export as Struct Array

Tables export as Arrow struct arrays (children = columns):

```
ArrowArray (struct, format="+s")
├── length = num_rows
├── n_children = num_columns
├── children[0] → column 0 ArrowArray
├── children[1] → column 1 ArrowArray
└── ...
```

## Stream Interface

Single-batch stream wrapping the table export:

```cpp
int stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* priv = static_cast<ArrowStreamPrivate*>(stream->private_data);

  if (priv->batch_returned) {
    out->release = nullptr;  // Signal end of stream
    return 0;
  }

  priv->table->export_to_c(out);
  priv->batch_returned = true;
  return 0;
}
```

## Python PyCapsule Binding

```cpp
.def("__arrow_c_stream__", [](Table& self, py::object requested_schema) {
  auto* stream = new ArrowArrayStream(self.export_to_stream());

  return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
    auto* s = static_cast<ArrowArrayStream*>(ptr);
    if (s->release) s->release(s);
    delete s;
  });
}, py::arg("requested_schema") = py::none())
```

## File Organization

**New files:**
- `include/vroom/arrow_c_data.h` - C Data Interface structs
- `include/vroom/arrow_export.h` - Export functions
- `test/arrow_export_test.cpp` - C++ tests

**Modified files:**
- `include/vroom/arrow_column_builder.h` - Add `export_to_c()` methods
- `python/src/bindings.cpp` - Add `__arrow_c_stream__()`
- `python/tests/test_arrow.py` - Integration tests
- `CMakeLists.txt` - Add test file

## Usage

```python
import libvroom
import polars as pl
import pyarrow as pa

table = libvroom.read_csv("data.csv")

# All zero-copy:
df = pl.from_arrow(table)
pa_table = pa.table(table)
```

## Testing Strategy

1. **C++ unit tests**: Verify ArrowArray/ArrowSchema struct contents
2. **Python integration tests**:
   - `polars.from_arrow()` roundtrip
   - `pyarrow.table()` roundtrip
   - Verify data integrity across all 7 types
   - Null handling verification

## References

- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
- [Polars Arrow interop](https://docs.pola.rs/user-guide/misc/arrow/)
