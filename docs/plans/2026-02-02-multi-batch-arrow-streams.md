# Multi-Batch Arrow Streams Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate chunk merge overhead by emitting each parsed chunk as a separate RecordBatch in the ArrowArrayStream, making Table construction O(1) instead of O(n).

**Architecture:** The current Python `Table` class merges all parsed chunks into a single set of column builders via O(n) `merge_from()` calls before export. Instead, `Table` will store chunks separately and the ArrowArrayStream will iterate over chunks, emitting one RecordBatch per chunk. This turns `from_parsed_chunks()` into an O(1) move operation and eliminates the dominant cost for large files.

**Tech Stack:** C++20, Arrow C Data Interface (ArrowArrayStream/ArrowArray/ArrowSchema), pybind11, Google Test, Google Benchmark

---

## Summary of Changes

The implementation is entirely in `python/src/bindings.cpp`. There is no separate `table.h`/`table.cpp` - the `Table` class and Arrow stream callbacks are local to the Python bindings. The core library (`ParsedChunks`, `ArrowColumnBuilder`) is unchanged.

### Files Modified

| File | Change |
|------|--------|
| `python/src/bindings.cpp` | Refactor Table to store chunks separately; update stream callbacks |
| `test/table_test.cpp` | **New file** - C++ tests for multi-batch stream export |
| `CMakeLists.txt` | Add table_test executable |

---

### Task 1: Write failing tests for multi-batch Table

**Files:**
- Create: `test/table_test.cpp`
- Modify: `CMakeLists.txt` (add test executable)

**Step 1: Create the test file**

Create `test/table_test.cpp` with tests that exercise the multi-batch Table API. These tests use the CsvReader directly (no Python), construct a Table from ParsedChunks, then consume the ArrowArrayStream to verify multi-batch output.

Since the Table class currently lives only in `python/src/bindings.cpp`, we need to extract the Table + stream code into a header. Create `include/libvroom/table.h` with the Table class and stream callbacks.

```cpp
// test/table_test.cpp
#include "libvroom.h"
#include "libvroom/table.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

// Helper to create temp CSV files
class TempFile {
public:
  explicit TempFile(const std::string& content, const std::string& ext = ".csv") {
    static int counter = 0;
    path_ = "/tmp/table_test_" + std::to_string(getpid()) + "_" +
            std::to_string(counter++) + ext;
    std::ofstream f(path_);
    f << content;
  }
  ~TempFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }
private:
  std::string path_;
};

// Helper: parse a CSV file and return a Table
std::shared_ptr<libvroom::Table> parse_to_table(const std::string& path,
                                                 size_t num_threads = 0) {
  libvroom::CsvOptions opts;
  if (num_threads > 0)
    opts.num_threads = num_threads;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(path);
  if (!open_result.ok)
    return nullptr;
  auto read_result = reader.read_all();
  if (!read_result.ok)
    return nullptr;
  return libvroom::Table::from_parsed_chunks(reader.schema(),
                                              std::move(read_result.value));
}

// =============================================================================
// Table Construction Tests
// =============================================================================

TEST(TableTest, SingleChunkConstruction) {
  TempFile csv("a,b\n1,2\n3,4\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 2);
  EXPECT_EQ(table->num_columns(), 2);
  EXPECT_GE(table->num_chunks(), 1);
}

TEST(TableTest, MultiChunkConstruction) {
  // Create a large-enough CSV to trigger multiple chunks with parallel parsing
  std::string content = "x,y,z\n";
  for (int i = 0; i < 10000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 2) + "," +
               std::to_string(i * 3) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 10000);
  EXPECT_EQ(table->num_columns(), 3);
  // With 4 threads on 10K rows, we likely get multiple chunks
  // But don't assert exact count - depends on chunking strategy
}

TEST(TableTest, EmptyTable) {
  TempFile csv("a,b,c\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 0);
  EXPECT_EQ(table->num_columns(), 3);
  EXPECT_EQ(table->num_chunks(), 0);
}

TEST(TableTest, SchemaPreserved) {
  TempFile csv("name,age,score\nAlice,30,95.5\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  const auto& schema = table->schema();
  ASSERT_EQ(schema.size(), 3);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "age");
  EXPECT_EQ(schema[2].name, "score");
}

// =============================================================================
// Arrow Stream Tests
// =============================================================================

TEST(TableStreamTest, StreamSchemaCorrect) {
  TempFile csv("a,b\n1,hello\n2,world\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Get schema
  libvroom::ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);

  // Verify struct schema
  EXPECT_STREQ(schema.format, "+s");
  EXPECT_EQ(schema.n_children, 2);
  EXPECT_STREQ(schema.children[0]->name, "a");
  EXPECT_STREQ(schema.children[1]->name, "b");

  // Clean up
  schema.release(&schema);
  stream.release(&stream);
}

TEST(TableStreamTest, SingleChunkStream) {
  TempFile csv("x,y\n1,2\n3,4\n5,6\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Count batches and total rows
  size_t total_rows = 0;
  size_t num_batches = 0;

  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break; // End of stream

    EXPECT_GT(batch.length, 0);
    EXPECT_EQ(batch.n_children, 2);
    total_rows += static_cast<size_t>(batch.length);
    num_batches++;

    batch.release(&batch);
  }

  EXPECT_EQ(total_rows, 3);
  EXPECT_GE(num_batches, 1);

  stream.release(&stream);
}

TEST(TableStreamTest, MultiBatchStream) {
  // Create a large CSV that will produce multiple chunks
  std::string content = "a,b,c\n";
  for (int i = 0; i < 10000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 2) + ",str" +
               std::to_string(i) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Consume all batches
  size_t total_rows = 0;
  size_t num_batches = 0;

  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break;

    EXPECT_GT(batch.length, 0);
    EXPECT_EQ(batch.n_children, 3);
    total_rows += static_cast<size_t>(batch.length);
    num_batches++;

    batch.release(&batch);
  }

  EXPECT_EQ(total_rows, 10000);
  // Verify we got multiple batches (the whole point)
  EXPECT_EQ(num_batches, table->num_chunks());

  stream.release(&stream);
}

TEST(TableStreamTest, EmptyStream) {
  TempFile csv("a,b\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Schema should still be available
  libvroom::ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  EXPECT_EQ(schema.n_children, 2);
  schema.release(&schema);

  // First get_next should signal end of stream
  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  EXPECT_EQ(batch.release, nullptr); // End of stream

  stream.release(&stream);
}

TEST(TableStreamTest, ChunkRowCountsMatchTotal) {
  std::string content = "id,val\n";
  for (int i = 0; i < 5000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 10) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  // Verify chunk_rows accessor matches stream output
  size_t sum_from_accessor = 0;
  for (size_t i = 0; i < table->num_chunks(); ++i) {
    sum_from_accessor += table->chunk_rows(i);
  }
  EXPECT_EQ(sum_from_accessor, table->num_rows());

  // Verify stream output matches
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  size_t sum_from_stream = 0;
  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break;
    sum_from_stream += static_cast<size_t>(batch.length);
    batch.release(&batch);
  }
  EXPECT_EQ(sum_from_stream, table->num_rows());

  stream.release(&stream);
}

TEST(TableStreamTest, StreamCanBeConsumedOnlyOnce) {
  TempFile csv("a\n1\n2\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Consume the stream
  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  batch.release(&batch);

  // Next call should signal end
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  EXPECT_EQ(batch.release, nullptr);

  // Can get a new stream from same table
  stream.release(&stream);

  libvroom::ArrowArrayStream stream2;
  table->export_to_stream(&stream2);

  // Should be able to consume again
  ASSERT_EQ(stream2.get_next(&stream2, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  batch.release(&batch);

  stream2.release(&stream2);
}
```

**Step 2: Add test to CMakeLists.txt**

Add to `CMakeLists.txt` inside the `if(BUILD_TESTING)` section, after the `vroom_api_test` block:

```cmake
# Table multi-batch Arrow stream tests
add_executable(table_test
    test/table_test.cpp
)
target_link_libraries(table_test PRIVATE
    vroom
    GTest::gtest_main
    pthread
)
target_include_directories(table_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
add_dependencies(table_test copy_test_data)
gtest_discover_tests(table_test)
```

**Step 3: Try to build - should fail (RED)**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) --target table_test 2>&1 | head -30`

Expected: Compilation error - `libvroom/table.h: No such file or directory`

**Step 4: Commit the test skeleton**

```bash
git add test/table_test.cpp CMakeLists.txt
git commit -m "test: add multi-batch Arrow stream table tests (RED)"
```

---

### Task 2: Create Table class header with multi-batch storage

**Files:**
- Create: `include/libvroom/table.h`

**Step 1: Create the Table class header**

This is the core data structure change. The Table stores chunks separately (no merge) and exposes an ArrowArrayStream that iterates over them.

```cpp
// include/libvroom/table.h
#pragma once

#include "arrow_c_data.h"
#include "arrow_column_builder.h"
#include "arrow_export.h"
#include "types.h"

#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Forward declaration
struct ParsedChunks;

/// Table - holds parsed CSV data as multiple chunks for zero-copy Arrow export.
///
/// Instead of merging all parsed chunks into a single set of column builders
/// (O(n) data copy), the Table stores each chunk separately and the Arrow
/// stream iterates over chunks (O(1) construction).
class Table : public std::enable_shared_from_this<Table> {
public:
  /// Construct a Table from schema and pre-built chunks.
  /// Takes ownership of the column builders via move.
  Table(std::vector<ColumnSchema> schema,
        std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> chunks,
        std::vector<size_t> chunk_row_counts, size_t total_rows)
      : schema_(std::move(schema)), chunks_(std::move(chunks)),
        chunk_row_counts_(std::move(chunk_row_counts)), total_rows_(total_rows) {}

  // Non-copyable (unique_ptr members)
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  /// Create a Table from ParsedChunks (O(1) - just moves vectors).
  static std::shared_ptr<Table> from_parsed_chunks(
      const std::vector<ColumnSchema>& schema, ParsedChunks&& parsed);

  // --- Accessors ---

  size_t num_rows() const { return total_rows_; }
  size_t num_columns() const { return schema_.size(); }
  size_t num_chunks() const { return chunks_.size(); }

  /// Number of rows in a specific chunk.
  size_t chunk_rows(size_t chunk_idx) const { return chunk_row_counts_[chunk_idx]; }

  const std::vector<ColumnSchema>& schema() const { return schema_; }

  std::vector<std::string> column_names() const {
    std::vector<std::string> names;
    names.reserve(schema_.size());
    for (const auto& col : schema_) {
      names.push_back(col.name);
    }
    return names;
  }

  /// Access columns for a specific chunk (for direct use).
  const std::vector<std::unique_ptr<ArrowColumnBuilder>>&
  chunk_columns(size_t chunk_idx) const {
    return chunks_[chunk_idx];
  }

  /// Export as ArrowArrayStream. The stream iterates over chunks,
  /// emitting one RecordBatch per chunk.
  /// The caller must call stream->release(stream) when done.
  /// The Table must outlive the stream (ensured via shared_ptr in private data).
  void export_to_stream(ArrowArrayStream* stream);

private:
  std::vector<ColumnSchema> schema_;
  std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> chunks_;
  std::vector<size_t> chunk_row_counts_;
  size_t total_rows_;
};

} // namespace libvroom
```

**Step 2: Try to build - should still fail**

The tests reference `Table::from_parsed_chunks` and `Table::export_to_stream` which are declared but not defined. Need implementation.

**Step 3: Commit**

```bash
git add include/libvroom/table.h
git commit -m "feat: add Table class header with multi-batch storage"
```

---

### Task 3: Implement Table class (from_parsed_chunks + stream export)

**Files:**
- Create: `src/table.cpp`
- Modify: `CMakeLists.txt` (add to VROOM_SOURCES)

**Step 1: Implement table.cpp**

```cpp
// src/table.cpp
#include "libvroom/table.h"
#include "libvroom/vroom.h" // for ParsedChunks

namespace libvroom {

// =============================================================================
// Table::from_parsed_chunks - O(1) construction, no merge
// =============================================================================

std::shared_ptr<Table> Table::from_parsed_chunks(
    const std::vector<ColumnSchema>& schema, ParsedChunks&& parsed) {
  std::vector<size_t> chunk_row_counts;
  chunk_row_counts.reserve(parsed.chunks.size());

  for (const auto& chunk : parsed.chunks) {
    // Each chunk's columns should all have the same size
    size_t rows = chunk.empty() ? 0 : chunk[0]->size();
    chunk_row_counts.push_back(rows);
  }

  return std::make_shared<Table>(schema, std::move(parsed.chunks),
                                  std::move(chunk_row_counts),
                                  parsed.total_rows);
}

// =============================================================================
// Arrow stream callbacks
// =============================================================================

namespace {

// Private data for the stream - keeps Table alive and tracks chunk position
struct TableStreamPrivate {
  std::shared_ptr<Table> table;
  size_t current_chunk = 0;
  std::string last_error;
};

// Private data for struct schema - owns child schemas
struct StructSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<ArrowSchema>> child_schemas;
  std::vector<ArrowSchema*> child_schema_ptrs;
};

// Release callback for struct schema
void release_struct_schema(ArrowSchema* schema) {
  if (schema->release == nullptr)
    return;

  if (schema->children) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
      if (schema->children[i] && schema->children[i]->release) {
        schema->children[i]->release(schema->children[i]);
      }
    }
  }

  if (schema->private_data) {
    delete static_cast<StructSchemaPrivate*>(schema->private_data);
  }
  schema->release = nullptr;
}

// Private data for struct array - owns child arrays and keeps table alive
struct StructArrayPrivate {
  std::shared_ptr<Table> table;
  std::vector<std::unique_ptr<ArrowArray>> child_arrays;
  std::vector<ArrowArray*> child_array_ptrs;
  std::vector<const void*> struct_buffers;
};

// Release callback for struct array
void release_struct_array(ArrowArray* array) {
  if (array->release == nullptr)
    return;

  if (array->children) {
    for (int64_t i = 0; i < array->n_children; ++i) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }
  }

  if (array->private_data) {
    delete static_cast<StructArrayPrivate*>(array->private_data);
  }
  array->release = nullptr;
}

// get_schema callback
int table_stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = priv->table;
  const auto& table_schema = table->schema();

  auto* schema_priv = new StructSchemaPrivate();
  schema_priv->name_storage = "";

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto child = std::make_unique<ArrowSchema>();
    // Need a column builder to export schema - use first non-empty chunk,
    // or create a temporary one from the schema type
    if (table->num_chunks() > 0) {
      table->chunk_columns(0)[i]->export_schema(child.get(), table_schema[i].name);
    } else {
      // Empty table - create a temporary builder just for schema export
      auto temp = ArrowColumnBuilder::create(table_schema[i].type);
      temp->export_schema(child.get(), table_schema[i].name);
    }
    schema_priv->child_schema_ptrs.push_back(child.get());
    schema_priv->child_schemas.push_back(std::move(child));
  }

  out->format = arrow_format::STRUCT;
  out->name = schema_priv->name_storage.c_str();
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->children = schema_priv->child_schema_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_schema;
  out->private_data = schema_priv;

  return 0;
}

// get_next callback - iterates over chunks
int table_stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* stream_priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = stream_priv->table;

  if (stream_priv->current_chunk >= table->num_chunks()) {
    // No more batches - signal end of stream
    init_empty_array(out);
    return 0;
  }

  size_t chunk_idx = stream_priv->current_chunk++;
  const auto& columns = table->chunk_columns(chunk_idx);
  size_t num_rows = table->chunk_rows(chunk_idx);

  // Create private data to own child arrays and keep table alive
  auto* array_priv = new StructArrayPrivate();
  array_priv->table = table;

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto* child_priv = new ArrowColumnPrivate();
    auto child = std::make_unique<ArrowArray>();
    columns[i]->export_to_arrow(child.get(), child_priv);

    array_priv->child_array_ptrs.push_back(child.get());
    array_priv->child_arrays.push_back(std::move(child));
  }

  // Struct array
  array_priv->struct_buffers = {nullptr};

  out->length = static_cast<int64_t>(num_rows);
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->buffers = array_priv->struct_buffers.data();
  out->children = array_priv->child_array_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_array;
  out->private_data = array_priv;

  return 0;
}

// get_last_error callback
const char* table_stream_get_last_error(ArrowArrayStream* stream) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

// release callback
void table_stream_release(ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;

  if (stream->private_data) {
    delete static_cast<TableStreamPrivate*>(stream->private_data);
  }
  stream->release = nullptr;
}

} // anonymous namespace

// =============================================================================
// Table::export_to_stream
// =============================================================================

void Table::export_to_stream(ArrowArrayStream* stream) {
  auto* priv = new TableStreamPrivate();
  priv->table = shared_from_this();

  stream->get_schema = table_stream_get_schema;
  stream->get_next = table_stream_get_next;
  stream->get_last_error = table_stream_get_last_error;
  stream->release = table_stream_release;
  stream->private_data = priv;
}

} // namespace libvroom
```

**Step 2: Add table.cpp to CMakeLists.txt VROOM_SOURCES**

Add `src/table.cpp` to the `VROOM_SOURCES` list in `CMakeLists.txt`:

```cmake
set(VROOM_SOURCES
    src/table.cpp              # <-- ADD THIS LINE
    src/error.cpp
    ... (rest unchanged)
)
```

**Step 3: Build and run tests (GREEN)**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) --target table_test && ./build/table_test`

Expected: All tests pass.

**Step 4: Commit**

```bash
git add src/table.cpp CMakeLists.txt include/libvroom/table.h
git commit -m "feat: implement Table with multi-batch Arrow stream export"
```

---

### Task 4: Update Python bindings to use new Table class

**Files:**
- Modify: `python/src/bindings.cpp`
- Modify: `include/libvroom.h` (add table.h include)

**Step 1: Add table.h to the main header**

Add `#include "libvroom/table.h"` to `include/libvroom.h`.

**Step 2: Refactor python/src/bindings.cpp**

Replace the local `Table` class and all stream callbacks with usage of the new `libvroom::Table`. The key changes:

1. Remove local `Table` class definition (lines ~38-73)
2. Remove local `TableStreamPrivate`, `StructSchemaPrivate`, `StructArrayPrivate` structs
3. Remove all stream callback functions (`table_stream_get_schema`, `table_stream_get_next`, etc.)
4. Remove `release_struct_schema`, `release_struct_array` functions
5. Use `libvroom::Table` directly
6. Update `read_csv()` to use `Table::from_parsed_chunks()` instead of merging
7. Update `__arrow_c_stream__` to use `Table::export_to_stream()`
8. Update `__arrow_c_schema__` to use chunk_columns(0) or temp builder for schema

The updated `read_csv()` function:

```cpp
std::shared_ptr<libvroom::Table> read_csv(const std::string& path, ...) {
  // ... (options setup unchanged) ...

  auto read_result = reader.read_all();
  // ... (error handling unchanged) ...

  auto schema = reader.schema();
  return libvroom::Table::from_parsed_chunks(schema, std::move(read_result.value));
}
```

The updated `__arrow_c_stream__`:

```cpp
.def(
    "__arrow_c_stream__",
    [](std::shared_ptr<libvroom::Table> self, py::object requested_schema) {
      auto* stream = new libvroom::ArrowArrayStream();
      self->export_to_stream(stream);

      return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
        auto* s = static_cast<libvroom::ArrowArrayStream*>(ptr);
        if (s->release)
          s->release(s);
        delete s;
      });
    },
    py::arg("requested_schema") = py::none(),
    "Export table as Arrow stream via PyCapsule (zero-copy)")
```

The updated `__arrow_c_schema__`:

```cpp
.def(
    "__arrow_c_schema__",
    [](std::shared_ptr<libvroom::Table> self) {
      // Use export_to_stream to get schema, then extract it
      auto* stream = new libvroom::ArrowArrayStream();
      self->export_to_stream(stream);

      auto* schema = new libvroom::ArrowSchema();
      stream->get_schema(stream, schema);

      // Release stream (we only needed the schema)
      stream->release(stream);
      delete stream;

      return py::capsule(schema, "arrow_schema", [](void* ptr) {
        auto* s = static_cast<libvroom::ArrowSchema*>(ptr);
        if (s->release)
          s->release(s);
        delete s;
      });
    },
    "Export table schema as Arrow schema via PyCapsule")
```

**Step 3: Build and verify**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc)`

Expected: Clean build.

**Step 4: Run all tests**

Run: `cd build && ctest --output-on-failure -j$(nproc)`

Expected: All tests pass.

**Step 5: Commit**

```bash
git add include/libvroom.h python/src/bindings.cpp
git commit -m "refactor: update Python bindings to use libvroom::Table multi-batch stream"
```

---

### Task 5: Add benchmark to verify merge overhead eliminated

**Files:**
- Create: `benchmark/table_benchmarks.cpp`
- Modify: `CMakeLists.txt` (add to BENCHMARK_SOURCES)

**Step 1: Create table_benchmarks.cpp**

```cpp
// benchmark/table_benchmarks.cpp
#include "libvroom.h"
#include "libvroom/table.h"

#include <benchmark/benchmark.h>
#include <cstdio>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>

namespace {

// Generate CSV with specified dimensions
std::string generate_csv(size_t num_rows, size_t num_cols) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, 9999);

  std::ostringstream oss;
  for (size_t c = 0; c < num_cols; ++c) {
    if (c > 0) oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  for (size_t r = 0; r < num_rows; ++r) {
    for (size_t c = 0; c < num_cols; ++c) {
      if (c > 0) oss << ',';
      oss << dist(rng);
    }
    oss << '\n';
  }
  return oss.str();
}

// Write CSV to a temp file
std::string write_temp_csv(const std::string& content) {
  static int counter = 0;
  std::string path = "/tmp/bench_table_" + std::to_string(getpid()) +
                     "_" + std::to_string(counter++) + ".csv";
  std::ofstream f(path);
  f << content;
  return path;
}

} // namespace

// Benchmark: CsvReader::read_all() only (parsing baseline)
static void BM_ReadAll(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv);

  for (auto _ : state) {
    libvroom::CsvOptions opts;
    libvroom::CsvReader reader(opts);
    reader.open(path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  std::remove(path.c_str());
  state.SetBytesProcessed(static_cast<int64_t>(csv.size()) * state.iterations());
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
}

// Benchmark: read_all() + Table construction (should be ~same as parse)
static void BM_ReadAllPlusTable(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv);

  for (auto _ : state) {
    libvroom::CsvOptions opts;
    libvroom::CsvReader reader(opts);
    reader.open(path);
    auto result = reader.read_all();
    auto table = libvroom::Table::from_parsed_chunks(
        reader.schema(), std::move(result.value));
    benchmark::DoNotOptimize(table);
  }

  std::remove(path.c_str());
  state.SetBytesProcessed(static_cast<int64_t>(csv.size()) * state.iterations());
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
}

// Benchmark: read_all() + Table + Arrow stream consumption
static void BM_ReadAllPlusArrowStream(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  std::string csv = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv);

  for (auto _ : state) {
    libvroom::CsvOptions opts;
    libvroom::CsvReader reader(opts);
    reader.open(path);
    auto result = reader.read_all();
    auto table = libvroom::Table::from_parsed_chunks(
        reader.schema(), std::move(result.value));

    // Consume the Arrow stream
    libvroom::ArrowArrayStream stream;
    table->export_to_stream(&stream);

    while (true) {
      libvroom::ArrowArray batch;
      stream.get_next(&stream, &batch);
      if (batch.release == nullptr)
        break;
      benchmark::DoNotOptimize(batch);
      batch.release(&batch);
    }
    stream.release(&stream);
  }

  std::remove(path.c_str());
  state.SetBytesProcessed(static_cast<int64_t>(csv.size()) * state.iterations());
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
}

// Registration: {rows, cols}
BENCHMARK(BM_ReadAll)
    ->Args({10000, 9})
    ->Args({100000, 30})
    ->Args({1000000, 9})
    ->Args({1000000, 30})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_ReadAllPlusTable)
    ->Args({10000, 9})
    ->Args({100000, 30})
    ->Args({1000000, 9})
    ->Args({1000000, 30})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_ReadAllPlusArrowStream)
    ->Args({10000, 9})
    ->Args({100000, 30})
    ->Args({1000000, 9})
    ->Args({1000000, 30})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
```

**Step 2: Add to BENCHMARK_SOURCES in CMakeLists.txt**

Add `benchmark/table_benchmarks.cpp` to the `BENCHMARK_SOURCES` list.

**Step 3: Build benchmarks**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON && cmake --build build -j$(nproc) --target libvroom_benchmark`

**Step 4: Run benchmarks and capture results**

Run: `./build/libvroom_benchmark --benchmark_filter="BM_ReadAll"`

Expected: `BM_ReadAllPlusTable` and `BM_ReadAllPlusArrowStream` should show near-zero overhead compared to `BM_ReadAll` (vs the 5-15x overhead in the issue's table).

**Step 5: Commit**

```bash
git add benchmark/table_benchmarks.cpp CMakeLists.txt
git commit -m "bench: add Table multi-batch Arrow stream benchmarks"
```

---

### Task 6: Run full test suite and verify

**Step 1: Run all tests**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`

Expected: All tests pass.

**Step 2: Run benchmarks for PR description**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON && cmake --build build -j$(nproc) --target libvroom_benchmark && ./build/libvroom_benchmark --benchmark_filter="BM_ReadAll"`

Capture the benchmark output for the PR description.

---

## Expected Final State

After all tasks, the repository will have:

1. **`include/libvroom/table.h`** - Table class with multi-batch storage and Arrow stream export
2. **`src/table.cpp`** - Implementation of Table::from_parsed_chunks and stream callbacks
3. **`python/src/bindings.cpp`** - Simplified to use libvroom::Table (no local Table class)
4. **`test/table_test.cpp`** - Comprehensive tests for multi-batch behavior
5. **`benchmark/table_benchmarks.cpp`** - Benchmarks proving merge overhead eliminated
6. **`CMakeLists.txt`** - Updated with new source file and test

The key metric: `Table::from_parsed_chunks()` is O(1) (moves vectors), and Arrow stream export adds near-zero overhead compared to parsing alone.
