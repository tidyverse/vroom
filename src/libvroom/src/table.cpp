#include "libvroom/table.h"

#include "libvroom/vroom.h"

#include <cassert>
#include <numeric>

namespace libvroom {

// =============================================================================
// Table::from_parsed_chunks - O(1) construction, no merge
// =============================================================================

std::shared_ptr<Table> Table::from_parsed_chunks(const std::vector<ColumnSchema>& schema,
                                                 ParsedChunks&& parsed) {
  std::vector<std::vector<std::unique_ptr<ArrowColumnBuilder>>> non_empty_chunks;
  std::vector<size_t> chunk_row_counts;

  for (auto& chunk : parsed.chunks) {
    size_t rows = chunk.empty() ? 0 : chunk[0]->size();
    if (rows > 0) {
      chunk_row_counts.push_back(rows);
      non_empty_chunks.push_back(std::move(chunk));
    }
  }

  assert(std::accumulate(chunk_row_counts.begin(), chunk_row_counts.end(), size_t{0}) ==
         parsed.total_rows);

  return std::make_shared<Table>(schema, std::move(non_empty_chunks), std::move(chunk_row_counts),
                                 parsed.total_rows);
}

// =============================================================================
// Arrow stream callbacks
// =============================================================================

namespace {

struct TableStreamPrivate {
  std::shared_ptr<Table> table;
  size_t current_chunk = 0;
  std::string last_error;
};

struct StructSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<ArrowSchema>> child_schemas;
  std::vector<ArrowSchema*> child_schema_ptrs;
};

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

struct StructArrayPrivate {
  std::shared_ptr<Table> table;
  std::vector<std::unique_ptr<ArrowArray>> child_arrays;
  std::vector<ArrowArray*> child_array_ptrs;
  std::vector<const void*> struct_buffers;
};

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

int table_stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = priv->table;
  const auto& table_schema = table->schema();

  auto* schema_priv = new StructSchemaPrivate();
  schema_priv->name_storage = "";

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto child = std::make_unique<ArrowSchema>();
    if (table->num_chunks() > 0) {
      table->chunk_columns(0)[i]->export_schema(child.get(), table_schema[i].name);
    } else {
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

int table_stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* stream_priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = stream_priv->table;

  if (stream_priv->current_chunk >= table->num_chunks()) {
    init_empty_array(out);
    return 0;
  }

  size_t chunk_idx = stream_priv->current_chunk++;
  const auto& columns = table->chunk_columns(chunk_idx);
  size_t num_rows = table->chunk_rows(chunk_idx);

  auto* array_priv = new StructArrayPrivate();
  array_priv->table = table;

  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto* child_priv = new ArrowColumnPrivate();
    auto child = std::make_unique<ArrowArray>();
    columns[i]->export_to_arrow(child.get(), child_priv);

    array_priv->child_array_ptrs.push_back(child.get());
    array_priv->child_arrays.push_back(std::move(child));
  }

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

const char* table_stream_get_last_error(ArrowArrayStream* stream) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

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
