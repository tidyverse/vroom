#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

// Cross-platform inline/noinline macros
#ifdef _MSC_VER
#define VROOM_FORCE_INLINE __forceinline
#define VROOM_NOINLINE __declspec(noinline)
#define VROOM_LIKELY(x) (x)
#define VROOM_UNLIKELY(x) (x)
#else
#define VROOM_FORCE_INLINE __attribute__((always_inline)) inline
#define VROOM_NOINLINE __attribute__((noinline))
#define VROOM_LIKELY(x) __builtin_expect(!!(x), 1)
#define VROOM_UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

namespace libvroom {

// Packed null bitmap - stores 8 null flags per byte
// Bit is SET (1) when value is VALID (non-null)
// Bit is CLEAR (0) when value is NULL
// This is the Arrow convention
// Uses LAZY INITIALIZATION (like Polars): no allocation until first null
class NullBitmap {
public:
  NullBitmap() = default;

  // Reserve capacity for n values (doesn't allocate yet - wait for null)
  void reserve(size_t n) { reserved_capacity_ = n; }

  // Resize to hold n values (initializes to all valid)
  void resize(size_t n) {
    data_.resize((n + 7) / 8, 0xFF); // All bits set = all valid
    size_ = n;
    has_nulls_ = false; // Resized means no nulls tracked
  }

  // Append a validity flag (true = valid, false = null)
  // OPTIMIZED: For valid values with no prior nulls, just increment counter
  // Force inline for hot path performance
  VROOM_FORCE_INLINE void push_back(bool valid) {
    if (VROOM_LIKELY(valid)) {
      push_back_valid();
    } else {
      push_back_null();
    }
  }

  // Specialized method for valid values - optimized for inlining
  // CRITICAL OPTIMIZATION: This is called millions of times, so it must be:
  // 1. Small enough to inline (compiler will inline small functions)
  // 2. Have the common case (no nulls) as the fast path
  // 3. Use branch prediction hints
  VROOM_FORCE_INLINE void push_back_valid() {
    if (VROOM_LIKELY(!has_nulls_)) {
      // Fast path: no nulls yet - just count, no bitmap work
      size_++;
      return;
    }
    // Slow path: has nulls - need to set the bit
    push_back_valid_slow();
  }

  // Specialized method for null values - keeps push_back_valid() small
  void push_back_null() {
    // Null value - lazy init if needed
    if (!has_nulls_) {
      init_bitmap_with_all_valid();
      has_nulls_ = true;
    }
    // Append null - ensure byte exists and bit is 0
    size_t byte_idx = size_ / 8;
    size_t bit_idx = size_ % 8;
    if (byte_idx >= data_.size()) {
      data_.push_back(0);
    } else {
      // Clear the bit (might be set from init_bitmap_with_all_valid)
      data_[byte_idx] &= ~(1 << bit_idx);
    }
    size_++;
    null_count_++;
  }

private:
  // Slow path for push_back_valid when we already have nulls
  // Separated to keep push_back_valid() small for better inlining
  VROOM_NOINLINE void push_back_valid_slow() {
    size_t byte_idx = size_ / 8;
    size_t bit_idx = size_ % 8;
    if (byte_idx >= data_.size()) {
      data_.push_back(0);
    }
    data_[byte_idx] |= (1 << bit_idx);
    size_++;
  }

public:
  // Append without bounds checking (same logic, slightly faster)
  void push_back_unchecked(bool valid) { push_back(valid); }

  // Set validity at index
  void set(size_t idx, bool valid) {
    size_t byte_idx = idx / 8;
    size_t bit_idx = idx % 8;

    if (valid) {
      data_[byte_idx] |= (1 << bit_idx);
    } else {
      data_[byte_idx] &= ~(1 << bit_idx);
    }
  }

  // Check if value at index is valid (non-null)
  bool is_valid(size_t idx) const {
    if (!has_nulls_)
      return true; // No nulls = all valid
    size_t byte_idx = idx / 8;
    size_t bit_idx = idx % 8;
    return (data_[byte_idx] & (1 << bit_idx)) != 0;
  }

  // Check if value at index is null
  bool is_null(size_t idx) const { return !is_valid(idx); }

  // Count null values
  size_t null_count() const {
    size_t count = 0;
    for (size_t i = 0; i < size_; ++i) {
      if (is_null(i))
        count++;
    }
    return count;
  }

  // Count null values using popcount (or cached value)
  size_t null_count_fast() const {
    if (!has_nulls_)
      return 0;         // Fast path: no nulls
    return null_count_; // Use cached count
  }

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  // Access raw data for serialization
  const uint8_t* data() const { return data_.data(); }
  size_t data_size() const { return data_.size(); }

  void clear() {
    data_.clear();
    size_ = 0;
    has_nulls_ = false;
    null_count_ = 0;
    reserved_capacity_ = 0;
  }

  // Check if any nulls have been added
  bool has_nulls() const { return has_nulls_; }

  // Get cached null count (only valid after nulls have been added)
  size_t cached_null_count() const { return null_count_; }

  // Append all values from another bitmap
  void append_from(const NullBitmap& other) {
    if (other.size_ == 0)
      return;

    if (!other.has_nulls_) {
      // Other has no nulls - just add valid values
      if (!has_nulls_) {
        // Neither has nulls - just update size
        size_ += other.size_;
      } else {
        // We have nulls - need to set bits for other's valid values
        for (size_t i = 0; i < other.size_; ++i) {
          push_back(true);
        }
      }
    } else {
      // Other has nulls - copy bit by bit
      for (size_t i = 0; i < other.size_; ++i) {
        push_back(other.is_valid(i));
      }
    }
  }

  // Finalize bitmap if needed (for encoding - ensures data_ is populated)
  void finalize() {
    if (!has_nulls_ && size_ > 0) {
      // No nulls - create all-valid bitmap
      data_.resize((size_ + 7) / 8, 0xFF);
    }
  }

private:
  // Initialize bitmap with all-valid bits for current size_
  void init_bitmap_with_all_valid() {
    size_t bytes_needed = (size_ + 7) / 8;
    data_.resize(bytes_needed, 0xFF); // All bits set = all valid
  }

  std::vector<uint8_t> data_;
  size_t size_ = 0;
  size_t reserved_capacity_ = 0;
  size_t null_count_ = 0;
  bool has_nulls_ = false;
};

// Contiguous buffer for strings with offsets
// Layout: [data buffer] + [offsets array]
// String i is at data[offsets[i]] with length (offsets[i+1] - offsets[i])
class StringBuffer {
public:
  StringBuffer() {
    offsets_.push_back(0); // First offset is always 0
  }

  // Reserve capacity for n strings with estimated total length
  void reserve(size_t n_strings, size_t estimated_total_len = 0) {
    offsets_.reserve(n_strings + 1);
    if (estimated_total_len > 0) {
      data_.reserve(estimated_total_len);
    }
  }

  // Append a string
  void push_back(std::string_view str) {
    data_.insert(data_.end(), str.begin(), str.end());
    offsets_.push_back(static_cast<uint32_t>(data_.size()));
  }

  // Append without bounds checking
  void push_back_unchecked(std::string_view str) {
    size_t old_size = data_.size();
    size_t new_size = old_size + str.size();
    // Assume data_ has capacity
    std::memcpy(data_.data() + old_size, str.data(), str.size());
    data_.resize(new_size); // Just updates size, no realloc if reserved
    offsets_.push_back(static_cast<uint32_t>(new_size));
  }

  // Append empty string (for null values)
  void push_back_empty() { offsets_.push_back(static_cast<uint32_t>(data_.size())); }

  // Get string at index
  std::string_view get(size_t idx) const {
    uint32_t start = offsets_[idx];
    uint32_t end = offsets_[idx + 1];
    return std::string_view(data_.data() + start, end - start);
  }

  // Get string length at index
  size_t length(size_t idx) const { return offsets_[idx + 1] - offsets_[idx]; }

  size_t size() const { return offsets_.size() - 1; }
  bool empty() const { return size() == 0; }

  // Access raw data for serialization
  const char* data() const { return data_.data(); }
  size_t data_size() const { return data_.size(); }
  const uint32_t* offsets() const { return offsets_.data(); }
  size_t offsets_size() const { return offsets_.size(); }

  void clear() {
    data_.clear();
    offsets_.clear();
    offsets_.push_back(0);
  }

  // Append all strings from another buffer
  void append_from(const StringBuffer& other) {
    // Reserve space
    reserve(size() + other.size(), data_.size() + other.data_size());

    // Append data
    uint32_t base_offset = static_cast<uint32_t>(data_.size());
    data_.insert(data_.end(), other.data_.begin(), other.data_.end());

    // Append offsets (adjusted by base_offset)
    // Skip the first offset (0) from other since we already have our own
    for (size_t i = 1; i < other.offsets_.size(); ++i) {
      offsets_.push_back(other.offsets_[i] + base_offset);
    }
  }

private:
  std::vector<char> data_;
  std::vector<uint32_t> offsets_; // n+1 offsets for n strings
};

// Contiguous buffer for numeric values
template <typename T> class NumericBuffer {
public:
  NumericBuffer() = default;

  // Reserve capacity
  void reserve(size_t n) { data_.reserve(n); }

  // Append value - force inline for hot path
  VROOM_FORCE_INLINE void push_back(T value) { data_.push_back(value); }

  // Append without bounds checking
  void push_back_unchecked(T value) {
    data_.push_back(value); // vector::push_back is already efficient when reserved
  }

  // Get value at index
  T get(size_t idx) const { return data_[idx]; }

  // Set value at index
  void set(size_t idx, T value) { data_[idx] = value; }

  size_t size() const { return data_.size(); }
  bool empty() const { return data_.empty(); }

  // Access raw data
  const T* data() const { return data_.data(); }
  T* data() { return data_.data(); }

  void clear() { data_.clear(); }

  // Resize with default value
  void resize(size_t n, T default_val = T{}) { data_.resize(n, default_val); }

  // Append all values from another buffer
  void append_from(const NumericBuffer& other) {
    data_.insert(data_.end(), other.data_.begin(), other.data_.end());
  }

private:
  std::vector<T> data_;
};

// Arrow-style column storage combining values and null bitmap
template <typename BufferType> class ArrowColumn {
public:
  ArrowColumn() = default;

  // Reserve capacity
  void reserve(size_t n) {
    values_.reserve(n);
    nulls_.reserve(n);
  }

  // For numeric types
  template <typename T> void push_back(T value, bool is_null = false) {
    values_.push_back(value);
    nulls_.push_back(!is_null); // Arrow: 1 = valid, 0 = null
  }

  // For string types
  void push_back(std::string_view value, bool is_null = false) {
    if (is_null) {
      values_.push_back_empty();
    } else {
      values_.push_back(value);
    }
    nulls_.push_back(!is_null);
  }

  size_t size() const { return nulls_.size(); }
  bool empty() const { return nulls_.empty(); }

  BufferType& values() { return values_; }
  const BufferType& values() const { return values_; }

  NullBitmap& nulls() { return nulls_; }
  const NullBitmap& nulls() const { return nulls_; }

  size_t null_count() const { return nulls_.null_count_fast(); }

  void clear() {
    values_.clear();
    nulls_.clear();
  }

private:
  BufferType values_;
  NullBitmap nulls_;
};

// Type aliases for common column types
using Int32Column = ArrowColumn<NumericBuffer<int32_t>>;
using Int64Column = ArrowColumn<NumericBuffer<int64_t>>;
using Float64Column = ArrowColumn<NumericBuffer<double>>;
using BoolColumn = ArrowColumn<NumericBuffer<uint8_t>>; // Use uint8 for bool to avoid vector<bool>
using StringColumn = ArrowColumn<StringBuffer>;

} // namespace libvroom
