#include <cpp11.hpp>
#include <libvroom/arrow_c_data.h>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/arrow_export.h>
#include <libvroom/encoding.h>
#include <libvroom/types.h>
#include <libvroom/vroom.h>

#include <memory>
#include <string>
#include <vector>

// =============================================================================
// Streaming Arrow stream: parsing runs in background, get_next() returns chunks
// as they become available via the streaming CsvReader API.
//
// Flow:
//   1. reader.open()            -- synchronous (fast: header + type inference)
//   2. reader.start_streaming() -- SIMD analysis, dispatches parse tasks
//   3. Return stream to R       -- R can set up ImportRecordBatchReader
//   4. get_next() calls         -- each call returns the next parsed chunk
//                                  (blocks only if that chunk isn't ready yet)
// =============================================================================

namespace {

struct StreamingStreamPrivate {
  // Schema (available immediately after open)
  std::vector<libvroom::ColumnSchema> schema;

  // Reader (owns the streaming state and background parse tasks)
  std::shared_ptr<libvroom::CsvReader> reader;

  // Error tracking
  std::string last_error;
};

// Schema for a struct (record batch) wrapping the column schemas
struct StructSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<libvroom::ArrowSchema>> child_schemas;
  std::vector<libvroom::ArrowSchema*> child_schema_ptrs;
};

void release_struct_schema(libvroom::ArrowSchema* schema) {
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

// Struct array wrapping column arrays for one record batch.
// Owns the chunk's column builders directly (no Table needed).
struct StructArrayPrivate {
  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> chunk_columns;
  std::vector<std::unique_ptr<libvroom::ArrowArray>> child_arrays;
  std::vector<libvroom::ArrowArray*> child_array_ptrs;
  std::vector<const void*> struct_buffers;
};

void release_struct_array(libvroom::ArrowArray* array) {
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

int streaming_get_schema(libvroom::ArrowArrayStream* stream,
                         libvroom::ArrowSchema* out) {
  auto* priv = static_cast<StreamingStreamPrivate*>(stream->private_data);

  auto* schema_priv = new StructSchemaPrivate();
  schema_priv->name_storage = "";

  for (size_t i = 0; i < priv->schema.size(); ++i) {
    auto child = std::make_unique<libvroom::ArrowSchema>();
    // Build schema from type info (no data needed)
    auto temp = libvroom::ArrowColumnBuilder::create(priv->schema[i].type);
    temp->export_schema(child.get(), priv->schema[i].name);
    schema_priv->child_schema_ptrs.push_back(child.get());
    schema_priv->child_schemas.push_back(std::move(child));
  }

  out->format = libvroom::arrow_format::STRUCT;
  out->name = schema_priv->name_storage.c_str();
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<int64_t>(priv->schema.size());
  out->children = schema_priv->child_schema_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_schema;
  out->private_data = schema_priv;

  return 0;
}

int streaming_get_next(libvroom::ArrowArrayStream* stream,
                       libvroom::ArrowArray* out) {
  auto* priv = static_cast<StreamingStreamPrivate*>(stream->private_data);

  // Get next chunk from streaming parser (blocks if not ready yet)
  auto chunk = priv->reader->next_chunk();
  if (!chunk.has_value()) {
    // All chunks consumed — signal end of stream
    libvroom::init_empty_array(out);
    return 0;
  }

  auto& columns = chunk.value();
  if (columns.empty()) {
    libvroom::init_empty_array(out);
    return 0;
  }

  size_t num_rows = columns[0]->size();

  auto* array_priv = new StructArrayPrivate();

  for (size_t i = 0; i < columns.size(); ++i) {
    auto* child_priv = new libvroom::ArrowColumnPrivate();
    auto child = std::make_unique<libvroom::ArrowArray>();
    columns[i]->export_to_arrow(child.get(), child_priv);

    array_priv->child_array_ptrs.push_back(child.get());
    array_priv->child_arrays.push_back(std::move(child));
  }

  // Transfer ownership of the chunk columns to StructArrayPrivate
  // so they stay alive as long as the ArrowArray does
  array_priv->chunk_columns = std::move(columns);
  array_priv->struct_buffers = {nullptr};

  out->length = static_cast<int64_t>(num_rows);
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(array_priv->child_array_ptrs.size());
  out->buffers = array_priv->struct_buffers.data();
  out->children = array_priv->child_array_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_array;
  out->private_data = array_priv;

  return 0;
}

const char* streaming_get_last_error(libvroom::ArrowArrayStream* stream) {
  auto* priv = static_cast<StreamingStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

void streaming_release(libvroom::ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;

  auto* priv = static_cast<StreamingStreamPrivate*>(stream->private_data);
  if (priv) {
    delete priv;
  }
  stream->release = nullptr;
}

} // anonymous namespace

// Prevent the ArrowArrayStream from leaking if R errors during import.
struct StreamGuard {
  libvroom::ArrowArrayStream* stream = nullptr;
  ~StreamGuard() {
    if (stream && stream->release) {
      stream->release(stream);
    }
    delete stream;
  }
  void release() { stream = nullptr; } // transfer ownership
};

[[cpp11::register]] cpp11::sexp vroom_arrow_(
    const std::string& path,
    const std::string& delim,
    char quote,
    bool has_header,
    int skip,
    const std::string& comment,
    bool skip_empty_rows,
    const std::string& na_values,
    int num_threads) {

  libvroom::CsvOptions opts;
  if (!delim.empty())
    opts.separator = delim;
  opts.quote = quote;
  opts.has_header = has_header;
  opts.skip_empty_rows = skip_empty_rows;
  if (!comment.empty())
    opts.comment = comment[0];
  if (!na_values.empty())
    opts.null_values = na_values;
  if (num_threads > 0)
    opts.num_threads = static_cast<size_t>(num_threads);

  // Skip full-file encoding detection (simdutf::validate_utf8 scans entire
  // file). R already handles encoding at the connection level.
  opts.encoding = libvroom::CharEncoding::UTF8;

  // Phase 1: Open file (synchronous - reads header, detects dialect,
  // infers types). This is fast and gives us the schema.
  auto reader = std::make_shared<libvroom::CsvReader>(opts);

  auto open_result = reader->open(path);
  if (!open_result) {
    cpp11::stop("Failed to open file: %s", open_result.error.c_str());
  }

  // Capture schema before starting streaming
  auto schema = reader->schema();

  // Phase 2: Start streaming — runs SIMD analysis phases 1-2 synchronously,
  // then dispatches chunk parsing to background threads.
  auto stream_result = reader->start_streaming();
  if (!stream_result) {
    cpp11::stop("Failed to start streaming: %s", stream_result.error.c_str());
  }

  // Phase 3: Set up ArrowArrayStream with streaming callbacks.
  // get_schema() works immediately (uses pre-captured schema).
  // get_next() calls next_chunk() which blocks only until that specific
  // chunk is ready — true pipeline parallelism.
  auto* priv = new StreamingStreamPrivate();
  priv->schema = schema;
  priv->reader = reader;

  auto* stream = new libvroom::ArrowArrayStream();
  StreamGuard guard;
  guard.stream = stream;

  stream->get_schema = streaming_get_schema;
  stream->get_next = streaming_get_next;
  stream->get_last_error = streaming_get_last_error;
  stream->release = streaming_release;
  stream->private_data = priv;

  // Encode stream pointer as R double (following DuckDB pattern)
  cpp11::sexp stream_ptr_sexp(Rf_ScalarReal(
      static_cast<double>(reinterpret_cast<uintptr_t>(stream))));

  // Call arrow::ImportRecordBatchReader(stream_ptr)
  cpp11::function get_namespace = cpp11::package("base")["getNamespace"];
  cpp11::sexp arrow_ns = get_namespace("arrow");
  cpp11::function import_rbr(
      Rf_findFun(Rf_install("ImportRecordBatchReader"), arrow_ns));

  // Transfer stream ownership to the arrow package
  guard.release();

  return import_rbr(stream_ptr_sexp);
}
