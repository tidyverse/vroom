#pragma once

#include "arrow_buffer.h"
#include "arrow_export.h"
#include "fast_arrow_context.h"
#include "types.h"

#include <memory>
#include <string_view>

namespace libvroom {

// ArrowColumnBuilder - Column builder using Arrow-style buffers
// Key benefits:
// 1. Packed null bitmap (8x less memory for nulls)
// 2. Contiguous string storage (no per-string allocation)
// 3. Cache-friendly memory layout
class ArrowColumnBuilder {
public:
  virtual ~ArrowColumnBuilder() = default;

  // Core interface
  virtual DataType type() const = 0;
  virtual size_t size() const = 0;
  virtual void reserve(size_t capacity) = 0;
  virtual void clear() = 0;

  // Get null bitmap for Parquet writing
  virtual const NullBitmap& null_bitmap() const = 0;
  virtual size_t null_count() const = 0;

  // Create FastArrowContext for this column
  virtual FastArrowContext create_context() = 0;

  // Get statistics (minimal implementation - returns null count only)
  virtual ColumnStatistics statistics() const {
    ColumnStatistics stats;
    stats.null_count = static_cast<int64_t>(null_count());
    stats.has_null = (stats.null_count > 0);
    return stats;
  }

  // Merge another builder into this one (for parallel processing)
  // The other builder must be of the same type
  // O(n) operation - appends data from other to this
  virtual void merge_from(ArrowColumnBuilder& other) = 0;

  // Export column to Arrow C Data Interface
  // The ArrowArray buffers point directly to this column's data (zero-copy)
  // Caller must ensure this column outlives the ArrowArray
  virtual void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const = 0;

  // Export schema to Arrow C Data Interface
  virtual void export_schema(ArrowSchema* out, const std::string& name) const = 0;

  // Factory methods
  static std::unique_ptr<ArrowColumnBuilder> create(DataType type);
  static std::unique_ptr<ArrowColumnBuilder> create_int32();
  static std::unique_ptr<ArrowColumnBuilder> create_int64();
  static std::unique_ptr<ArrowColumnBuilder> create_float64();
  static std::unique_ptr<ArrowColumnBuilder> create_bool();
  static std::unique_ptr<ArrowColumnBuilder> create_date();
  static std::unique_ptr<ArrowColumnBuilder> create_timestamp();
  static std::unique_ptr<ArrowColumnBuilder> create_string();
};

// Int32 column
class ArrowInt32ColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::INT32; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.int32_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_int32;
    ctx.append_null_fn = FastArrowContext::append_null_int32;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowInt32ColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  // Direct access for writing
  const NumericBuffer<int32_t>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::INT32;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<int32_t> values_;
  NullBitmap nulls_;
};

// Int64 column
class ArrowInt64ColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::INT64; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.int64_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_int64;
    ctx.append_null_fn = FastArrowContext::append_null_int64;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowInt64ColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const NumericBuffer<int64_t>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::INT64;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<int64_t> values_;
  NullBitmap nulls_;
};

// Float64 column
class ArrowFloat64ColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::FLOAT64; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.float64_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_float64;
    ctx.append_null_fn = FastArrowContext::append_null_float64;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowFloat64ColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const NumericBuffer<double>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::FLOAT64;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<double> values_;
  NullBitmap nulls_;
};

// Bool column (stored as uint8)
class ArrowBoolColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::BOOL; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.bool_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_bool;
    ctx.append_null_fn = FastArrowContext::append_null_bool;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowBoolColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const NumericBuffer<uint8_t>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    // Use uint8 "C" format since we store as uint8, not packed bits
    out->format = "C";
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<uint8_t> values_;
  NullBitmap nulls_;
};

// Date column (stored as int32 days since epoch)
class ArrowDateColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::DATE; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.int32_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_date;
    ctx.append_null_fn = FastArrowContext::append_null_date;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowDateColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const NumericBuffer<int32_t>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::DATE32;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<int32_t> values_;
  NullBitmap nulls_;
};

// Timestamp column (stored as int64 microseconds since epoch)
class ArrowTimestampColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::TIMESTAMP; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    values_.reserve(capacity);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.int64_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_timestamp;
    ctx.append_null_fn = FastArrowContext::append_null_timestamp;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowTimestampColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const NumericBuffer<int64_t>& values() const { return values_; }

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::TIMESTAMP_US;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  NumericBuffer<int64_t> values_;
  NullBitmap nulls_;
};

// String column (contiguous buffer + offsets)
class ArrowStringColumnBuilder : public ArrowColumnBuilder {
public:
  DataType type() const override { return DataType::STRING; }
  size_t size() const override { return values_.size(); }

  void reserve(size_t capacity) override {
    // Estimate 32 bytes average per string
    values_.reserve(capacity, capacity * 32);
    nulls_.reserve(capacity);
  }

  void clear() override {
    values_.clear();
    nulls_.clear();
  }

  const NullBitmap& null_bitmap() const override { return nulls_; }
  size_t null_count() const override { return nulls_.null_count_fast(); }

  FastArrowContext create_context() override {
    FastArrowContext ctx;
    ctx.string_buffer = &values_;
    ctx.null_bitmap = &nulls_;
    ctx.append_fn = FastArrowContext::append_string;
    ctx.append_null_fn = FastArrowContext::append_null_string;
    return ctx;
  }

  void merge_from(ArrowColumnBuilder& other) override {
    auto& typed_other = static_cast<ArrowStringColumnBuilder&>(other);
    values_.append_from(typed_other.values_);
    nulls_.append_from(typed_other.nulls_);
  }

  const StringBuffer& values() const { return values_; }

  void export_to_arrow(ArrowArray* out, ArrowColumnPrivate* priv) const override {
    // String arrays have 3 buffers: [validity, offsets, data]
    priv->buffers.resize(3);
    priv->buffers[0] = nulls_.has_nulls() ? nulls_.data() : nullptr;
    priv->buffers[1] = values_.offsets(); // int32 offsets
    priv->buffers[2] = values_.data();    // char data

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
    auto* schema_priv = new ArrowSchemaPrivate();
    schema_priv->name_storage = name;

    out->format = arrow_format::UTF8;
    out->name = schema_priv->name_storage.c_str();
    out->metadata = nullptr;
    out->flags = ARROW_FLAG_NULLABLE;
    out->n_children = 0;
    out->children = nullptr;
    out->dictionary = nullptr;
    out->release = release_arrow_schema;
    out->private_data = schema_priv;
  }

private:
  StringBuffer values_;
  NullBitmap nulls_;
};

// Factory implementations
inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create(DataType type) {
  switch (type) {
  case DataType::INT32:
    return create_int32();
  case DataType::INT64:
    return create_int64();
  case DataType::FLOAT64:
    return create_float64();
  case DataType::BOOL:
    return create_bool();
  case DataType::DATE:
    return create_date();
  case DataType::TIMESTAMP:
    return create_timestamp();
  case DataType::STRING:
  default:
    return create_string();
  }
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_int32() {
  return std::make_unique<ArrowInt32ColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_int64() {
  return std::make_unique<ArrowInt64ColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_float64() {
  return std::make_unique<ArrowFloat64ColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_bool() {
  return std::make_unique<ArrowBoolColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_date() {
  return std::make_unique<ArrowDateColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_timestamp() {
  return std::make_unique<ArrowTimestampColumnBuilder>();
}

inline std::unique_ptr<ArrowColumnBuilder> ArrowColumnBuilder::create_string() {
  return std::make_unique<ArrowStringColumnBuilder>();
}

} // namespace libvroom
