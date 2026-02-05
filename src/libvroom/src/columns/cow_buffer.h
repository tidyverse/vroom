#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

namespace libvroom {

/**
 * CowBuffer - Copy-on-Write Buffer
 *
 * A buffer that supports shared ownership with copy-on-write semantics.
 * Data is shared between copies until a mutation is requested, at which
 * point the buffer is copied if shared with other instances.
 *
 * This pattern is critical for performance in the Parquet writer, where
 * data often needs to be passed through multiple stages without copying.
 *
 * Inspired by Polars' CowBuffer implementation.
 *
 * Ownership tracking uses shared_ptr::use_count() exclusively:
 * - use_count() == 1: exclusive ownership, mutations are direct
 * - use_count() > 1: shared, mutations trigger copy-on-write
 *
 * Thread Safety:
 * - Different CowBuffer instances can be used from different threads
 * - The same CowBuffer instance must not be accessed concurrently from
 *   multiple threads (typical COW semantics, same as std::string pre-C++11)
 * - Passing copies between threads is safe (they become independent)
 */
template <typename T> class CowBuffer {
public:
  // Default constructor - creates an empty buffer
  CowBuffer() : data_(std::make_shared<std::vector<T>>()) {}

  // Construct with initial capacity
  explicit CowBuffer(size_t capacity) : data_(std::make_shared<std::vector<T>>()) {
    data_->reserve(capacity);
  }

  // Construct from existing vector (takes ownership)
  explicit CowBuffer(std::vector<T> vec)
      : data_(std::make_shared<std::vector<T>>(std::move(vec))) {}

  // Construct from raw data (copies)
  CowBuffer(const T* data, size_t size)
      : data_(std::make_shared<std::vector<T>>(data, data + size)) {}

  // Copy constructor - shares data via shared_ptr
  CowBuffer(const CowBuffer& other) = default;

  // Move constructor
  CowBuffer(CowBuffer&& other) noexcept = default;

  // Copy assignment - shares data via shared_ptr
  CowBuffer& operator=(const CowBuffer& other) = default;

  // Move assignment
  CowBuffer& operator=(CowBuffer&& other) noexcept = default;

  /**
   * Share the buffer without copying.
   * Returns a new CowBuffer that shares the same underlying data.
   * Both buffers track sharing via use_count().
   */
  CowBuffer share() const { return CowBuffer(*this); }

  /**
   * Get const access to data (no copy needed).
   */
  const T* data() const { return data_ ? data_->data() : nullptr; }

  /**
   * Get mutable access to data.
   * If the buffer is shared (use_count > 1), this will first copy
   * the data to ensure exclusive ownership.
   */
  T* mutable_data() {
    ensure_unique();
    return data_ ? data_->data() : nullptr;
  }

  /**
   * Get mutable reference to the underlying vector.
   * Ensures exclusive ownership first.
   */
  std::vector<T>& to_mut() {
    ensure_unique();
    return *data_;
  }

  /**
   * Get const reference to the underlying vector.
   */
  const std::vector<T>& as_vec() const {
    static const std::vector<T> empty;
    return data_ ? *data_ : empty;
  }

  /**
   * Move the underlying vector out of this buffer.
   * If shared, copies first.
   */
  std::vector<T> into_vec() {
    if (!data_) {
      return {};
    }
    ensure_unique();
    return std::move(*data_);
  }

  // Size and capacity
  size_t size() const { return data_ ? data_->size() : 0; }
  size_t capacity() const { return data_ ? data_->capacity() : 0; }
  bool empty() const { return !data_ || data_->empty(); }

  // Check if this buffer owns the data exclusively (use_count == 1)
  bool is_owned() const { return data_ && data_.use_count() == 1; }

  // Check if the buffer is shared (multiple references)
  bool is_shared() const { return data_ && data_.use_count() > 1; }

  // Get the reference count
  long use_count() const { return data_ ? data_.use_count() : 0; }

  // Reserve capacity (may copy if shared)
  void reserve(size_t new_cap) {
    ensure_unique();
    if (data_) {
      data_->reserve(new_cap);
    }
  }

  // Resize (may copy if shared)
  void resize(size_t new_size) {
    ensure_unique();
    if (data_) {
      data_->resize(new_size);
    }
  }

  // Resize with value (may copy if shared)
  void resize(size_t new_size, const T& value) {
    ensure_unique();
    if (data_) {
      data_->resize(new_size, value);
    }
  }

  // Clear the buffer (may copy if shared)
  void clear() {
    ensure_unique();
    if (data_) {
      data_->clear();
    }
  }

  // Push back (may copy if shared)
  void push_back(const T& value) {
    ensure_unique();
    if (data_) {
      data_->push_back(value);
    }
  }

  void push_back(T&& value) {
    ensure_unique();
    if (data_) {
      data_->push_back(std::move(value));
    }
  }

  // Emplace back (may copy if shared)
  template <typename... Args> T& emplace_back(Args&&... args) {
    ensure_unique();
    // ensure_unique() guarantees data_ is valid
    return data_->emplace_back(std::forward<Args>(args)...);
  }

  // Element access (const)
  const T& operator[](size_t idx) const { return (*data_)[idx]; }
  const T& at(size_t idx) const { return data_->at(idx); }

  // Element access (mutable, may copy if shared)
  T& operator[](size_t idx) {
    ensure_unique();
    return (*data_)[idx];
  }

  T& at(size_t idx) {
    ensure_unique();
    return data_->at(idx);
  }

  // Iterators (const)
  typename std::vector<T>::const_iterator begin() const {
    return data_ ? data_->begin() : typename std::vector<T>::const_iterator{};
  }
  typename std::vector<T>::const_iterator end() const {
    return data_ ? data_->end() : typename std::vector<T>::const_iterator{};
  }
  typename std::vector<T>::const_iterator cbegin() const { return begin(); }
  typename std::vector<T>::const_iterator cend() const { return end(); }

  // Swap
  void swap(CowBuffer& other) noexcept { std::swap(data_, other.data_); }

private:
  /**
   * Ensure this buffer has exclusive ownership of the data.
   * If shared (use_count > 1), creates a copy.
   * If data_ is null, allocates a new vector.
   */
  void ensure_unique() {
    if (!data_) {
      data_ = std::make_shared<std::vector<T>>();
      return;
    }

    if (data_.use_count() > 1) {
      // Copy the data to get exclusive ownership
      data_ = std::make_shared<std::vector<T>>(*data_);
    }
  }

  std::shared_ptr<std::vector<T>> data_;
};

// Specialization for uint8_t (byte buffer) which is commonly used in Parquet
using CowByteBuffer = CowBuffer<uint8_t>;

// Swap free function
template <typename T> void swap(CowBuffer<T>& a, CowBuffer<T>& b) noexcept {
  a.swap(b);
}

} // namespace libvroom
