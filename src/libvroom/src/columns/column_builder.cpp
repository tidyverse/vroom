#include "libvroom/data_chunk.h"
#include "libvroom/statistics.h"
#include "libvroom/vroom.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cmath>
#include <fast_float/fast_float.h>
#include <limits>

namespace libvroom {

// ============================================================================
// ChunkedColumnBuilder - base template for all chunked column types
// Uses ChunkedStorage for O(1) merge_from() performance
// ============================================================================

template <typename T, typename Derived> class ChunkedColumnBuilder : public ColumnBuilder {
protected:
  ChunkedStorage<T> storage_;
  mutable bool finalized_ = false;

  // Cached concatenated view (lazy - only created when chunk-spanning reads needed)
  mutable std::vector<T> cached_values_;
  mutable std::vector<bool> cached_null_bitmap_;
  mutable bool cache_valid_ = false;

  void invalidate_cache() {
    cache_valid_ = false;
    finalized_ = false;
  }

  void ensure_cache() const {
    if (cache_valid_)
      return;

    // Must finalize before accessing
    if (!finalized_) {
      const_cast<ChunkedColumnBuilder*>(this)->finalize();
    }

    // Concatenate all chunks into cached vectors
    cached_values_.clear();
    cached_null_bitmap_.clear();

    size_t total = storage_.size();
    cached_values_.reserve(total);
    cached_null_bitmap_.reserve(total);

    for (const auto& chunk : storage_.chunks()) {
      cached_values_.insert(cached_values_.end(), chunk->values.begin(), chunk->values.end());
      cached_null_bitmap_.insert(cached_null_bitmap_.end(), chunk->null_bitmap.begin(),
                                 chunk->null_bitmap.end());
    }

    cache_valid_ = true;
  }

public:
  size_t size() const override { return storage_.size(); }

  void reserve(size_t capacity) override { storage_.reserve(capacity); }

  void finalize() override {
    if (!finalized_) {
      storage_.finalize_active();
      finalized_ = true;
    }
  }

  // ========================================================================
  // Chunked access (efficient O(1) per chunk)
  // ========================================================================

  size_t num_chunks() const override {
    if (!finalized_) {
      const_cast<ChunkedColumnBuilder*>(this)->finalize();
    }
    return storage_.num_chunks();
  }

  size_t chunk_size(size_t chunk_idx) const override {
    if (!finalized_) {
      const_cast<ChunkedColumnBuilder*>(this)->finalize();
    }
    return storage_.chunks()[chunk_idx]->size();
  }

  const void* chunk_raw_values(size_t chunk_idx) const override {
    if (!finalized_) {
      const_cast<ChunkedColumnBuilder*>(this)->finalize();
    }
    return &storage_.chunks()[chunk_idx]->values;
  }

  const std::vector<bool>& chunk_null_bitmap(size_t chunk_idx) const override {
    if (!finalized_) {
      const_cast<ChunkedColumnBuilder*>(this)->finalize();
    }
    return storage_.chunks()[chunk_idx]->null_bitmap;
  }

  // ========================================================================
  // Direct access to active vectors for FastColumnContext
  // This is where parsing writes - no virtualization overhead
  // ========================================================================

  const void* raw_values() const override {
    // If we have chunks (from merges), need to concatenate
    if (storage_.num_chunks() > 0) {
      ensure_cache();
      return &cached_values_;
    }
    // Otherwise return active vectors directly
    return &storage_.active_values();
  }

  const std::vector<bool>& null_bitmap() const override {
    // If we have chunks (from merges), need to concatenate
    if (storage_.num_chunks() > 0) {
      ensure_cache();
      return cached_null_bitmap_;
    }
    // Otherwise return active vectors directly
    return storage_.active_null_bitmap();
  }

  void* raw_values_mutable() override {
    // Mutable access goes to active vectors - this is where parsing writes
    invalidate_cache();
    return &storage_.active_values();
  }

  std::vector<bool>& null_bitmap_mutable() override {
    // Mutable access goes to active vectors - this is where parsing writes
    invalidate_cache();
    return storage_.active_null_bitmap();
  }

  // ========================================================================
  // O(1) merge - just moves chunk pointers!
  // ========================================================================

  void merge_from(ColumnBuilder& other) override {
    assert(type() == other.type() && "Cannot merge column builders of different types");
    auto& other_typed = static_cast<Derived&>(other);
    storage_.merge_from(other_typed.storage_);
    invalidate_cache();
  }
};

// ============================================================================
// String Column Builder (with incremental statistics)
// ============================================================================

class StringColumnBuilder : public ChunkedColumnBuilder<std::string, StringColumnBuilder> {
public:
  void append(std::string_view value) override {
    storage_.append(std::string(value), false);
    stats_.update(std::string(value));
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(std::string{}, true);
    stats_.update_null();
    invalidate_cache();
  }

  DataType type() const override { return DataType::STRING; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    // Count nulls across all chunks
    result.null_count = 0;
    if (!finalized_) {
      const_cast<StringColumnBuilder*>(this)->finalize();
    }
    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
    }
    result.has_null = result.null_count > 0;

    if (stats_.has_value()) {
      result.min_value = stats_.min();
      result.max_value = stats_.max();
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<StringColumnBuilder>();
  }

private:
  StringStatistics stats_;
};

// ============================================================================
// Int32 Column Builder (with incremental statistics)
// ============================================================================

class Int32ColumnBuilder : public ChunkedColumnBuilder<int32_t, Int32ColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(0, true);
      invalidate_cache();
      return;
    }

    int32_t result;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);

    if (ec == std::errc() && ptr == value.data() + value.size()) {
      storage_.append(result, false);
    } else {
      storage_.append(0, true);
    }
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(0, true);
    invalidate_cache();
  }

  DataType type() const override { return DataType::INT32; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<Int32ColumnBuilder*>(this)->finalize();
    }

    int32_t min_val = std::numeric_limits<int32_t>::max();
    int32_t max_val = std::numeric_limits<int32_t>::min();
    bool found = false;

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
      for (size_t i = 0; i < chunk->values.size(); ++i) {
        if (!chunk->null_bitmap[i]) {
          min_val = std::min(min_val, chunk->values[i]);
          max_val = std::max(max_val, chunk->values[i]);
          found = true;
        }
      }
    }

    result.has_null = result.null_count > 0;
    if (found) {
      result.min_value = min_val;
      result.max_value = max_val;
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<Int32ColumnBuilder>();
  }
};

// ============================================================================
// Int64 Column Builder (with incremental statistics)
// ============================================================================

class Int64ColumnBuilder : public ChunkedColumnBuilder<int64_t, Int64ColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(0, true);
      invalidate_cache();
      return;
    }

    int64_t result;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);

    if (ec == std::errc() && ptr == value.data() + value.size()) {
      storage_.append(result, false);
    } else {
      storage_.append(0, true);
    }
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(0, true);
    invalidate_cache();
  }

  DataType type() const override { return DataType::INT64; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<Int64ColumnBuilder*>(this)->finalize();
    }

    int64_t min_val = std::numeric_limits<int64_t>::max();
    int64_t max_val = std::numeric_limits<int64_t>::min();
    bool found = false;

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
      for (size_t i = 0; i < chunk->values.size(); ++i) {
        if (!chunk->null_bitmap[i]) {
          min_val = std::min(min_val, chunk->values[i]);
          max_val = std::max(max_val, chunk->values[i]);
          found = true;
        }
      }
    }

    result.has_null = result.null_count > 0;
    if (found) {
      result.min_value = min_val;
      result.max_value = max_val;
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<Int64ColumnBuilder>();
  }
};

// ============================================================================
// Float64 Column Builder (with incremental statistics)
// ============================================================================

class Float64ColumnBuilder : public ChunkedColumnBuilder<double, Float64ColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(std::numeric_limits<double>::quiet_NaN(), true);
      invalidate_cache();
      return;
    }

    double result;
    auto [ptr, ec] = fast_float::from_chars(value.data(), value.data() + value.size(), result);

    if (ec == std::errc() && ptr == value.data() + value.size()) {
      storage_.append(result, false);
    } else {
      storage_.append(std::numeric_limits<double>::quiet_NaN(), true);
    }
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(std::numeric_limits<double>::quiet_NaN(), true);
    invalidate_cache();
  }

  DataType type() const override { return DataType::FLOAT64; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<Float64ColumnBuilder*>(this)->finalize();
    }

    double min_val = std::numeric_limits<double>::infinity();
    double max_val = -std::numeric_limits<double>::infinity();
    bool found = false;

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
      for (size_t i = 0; i < chunk->values.size(); ++i) {
        if (!chunk->null_bitmap[i] && !std::isnan(chunk->values[i])) {
          min_val = std::min(min_val, chunk->values[i]);
          max_val = std::max(max_val, chunk->values[i]);
          found = true;
        }
      }
    }

    result.has_null = result.null_count > 0;
    if (found) {
      result.min_value = min_val;
      result.max_value = max_val;
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<Float64ColumnBuilder>();
  }
};

// ============================================================================
// Date Column Builder (stores days since epoch as int32)
// ============================================================================

// Forward declarations from type_parsers.cpp
bool parse_date(std::string_view value, int32_t& days_since_epoch);
bool parse_timestamp(std::string_view value, int64_t& micros_since_epoch);

class DateColumnBuilder : public ChunkedColumnBuilder<int32_t, DateColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(0, true);
      invalidate_cache();
      return;
    }

    int32_t days;
    if (parse_date(value, days)) {
      storage_.append(days, false);
    } else {
      storage_.append(0, true);
    }
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(0, true);
    invalidate_cache();
  }

  DataType type() const override { return DataType::DATE; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<DateColumnBuilder*>(this)->finalize();
    }

    int32_t min_val = std::numeric_limits<int32_t>::max();
    int32_t max_val = std::numeric_limits<int32_t>::min();
    bool found = false;

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
      for (size_t i = 0; i < chunk->values.size(); ++i) {
        if (!chunk->null_bitmap[i]) {
          min_val = std::min(min_val, chunk->values[i]);
          max_val = std::max(max_val, chunk->values[i]);
          found = true;
        }
      }
    }

    result.has_null = result.null_count > 0;
    if (found) {
      result.min_value = min_val;
      result.max_value = max_val;
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<DateColumnBuilder>();
  }
};

// ============================================================================
// Timestamp Column Builder (stores microseconds since epoch as int64)
// ============================================================================

class TimestampColumnBuilder : public ChunkedColumnBuilder<int64_t, TimestampColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(0, true);
      invalidate_cache();
      return;
    }

    int64_t micros;
    if (parse_timestamp(value, micros)) {
      storage_.append(micros, false);
    } else {
      storage_.append(0, true);
    }
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(0, true);
    invalidate_cache();
  }

  DataType type() const override { return DataType::TIMESTAMP; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<TimestampColumnBuilder*>(this)->finalize();
    }

    int64_t min_val = std::numeric_limits<int64_t>::max();
    int64_t max_val = std::numeric_limits<int64_t>::min();
    bool found = false;

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
      for (size_t i = 0; i < chunk->values.size(); ++i) {
        if (!chunk->null_bitmap[i]) {
          min_val = std::min(min_val, chunk->values[i]);
          max_val = std::max(max_val, chunk->values[i]);
          found = true;
        }
      }
    }

    result.has_null = result.null_count > 0;
    if (found) {
      result.min_value = min_val;
      result.max_value = max_val;
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<TimestampColumnBuilder>();
  }
};

// ============================================================================
// Bool Column Builder (with incremental statistics)
// ============================================================================

class BoolColumnBuilder : public ChunkedColumnBuilder<bool, BoolColumnBuilder> {
public:
  void append(std::string_view value) override {
    if (value.empty()) {
      storage_.append(false, true);
      stats_.update_null();
      invalidate_cache();
      return;
    }

    // Check for common true values
    if (value == "true" || value == "TRUE" || value == "True" || value == "1" || value == "yes" ||
        value == "YES") {
      storage_.append(true, false);
      stats_.update(true);
      invalidate_cache();
      return;
    }

    // Check for common false values
    if (value == "false" || value == "FALSE" || value == "False" || value == "0" || value == "no" ||
        value == "NO") {
      storage_.append(false, false);
      stats_.update(false);
      invalidate_cache();
      return;
    }

    // Unknown value - treat as null
    storage_.append(false, true);
    stats_.update_null();
    invalidate_cache();
  }

  void append_null() override {
    storage_.append(false, true);
    stats_.update_null();
    invalidate_cache();
  }

  DataType type() const override { return DataType::BOOL; }

  ColumnStatistics statistics() const override {
    ColumnStatistics result;
    result.null_count = 0;

    if (!finalized_) {
      const_cast<BoolColumnBuilder*>(this)->finalize();
    }

    for (const auto& chunk : storage_.chunks()) {
      result.null_count += std::count(chunk->null_bitmap.begin(), chunk->null_bitmap.end(), true);
    }

    result.has_null = result.null_count > 0;

    if (stats_.has_value()) {
      result.min_value = stats_.min();
      result.max_value = stats_.max();
    }

    return result;
  }

  std::unique_ptr<ColumnBuilder> clone_empty() const override {
    return std::make_unique<BoolColumnBuilder>();
  }

private:
  BoolStatistics stats_;
};

// ============================================================================
// Factory Methods
// ============================================================================

std::unique_ptr<ColumnBuilder> ColumnBuilder::create(DataType type) {
  switch (type) {
  case DataType::BOOL:
    return std::make_unique<BoolColumnBuilder>();
  case DataType::INT32:
    return std::make_unique<Int32ColumnBuilder>();
  case DataType::INT64:
    return std::make_unique<Int64ColumnBuilder>();
  case DataType::FLOAT64:
    return std::make_unique<Float64ColumnBuilder>();
  case DataType::DATE:
    return std::make_unique<DateColumnBuilder>();
  case DataType::TIMESTAMP:
    return std::make_unique<TimestampColumnBuilder>();
  case DataType::STRING:
  default:
    return std::make_unique<StringColumnBuilder>();
  }
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_string() {
  return std::make_unique<StringColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_int32() {
  return std::make_unique<Int32ColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_int64() {
  return std::make_unique<Int64ColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_float64() {
  return std::make_unique<Float64ColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_bool() {
  return std::make_unique<BoolColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_date() {
  return std::make_unique<DateColumnBuilder>();
}

std::unique_ptr<ColumnBuilder> ColumnBuilder::create_timestamp() {
  return std::make_unique<TimestampColumnBuilder>();
}

} // namespace libvroom
