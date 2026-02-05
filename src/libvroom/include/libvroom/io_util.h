/**
 * @file io_util.h
 * @brief File I/O utilities for loading CSV files with SIMD-aligned buffers.
 *
 * This header provides utilities for loading files into memory with proper
 * alignment and padding for efficient SIMD processing. The functions ensure
 * that buffers are cache-line aligned (64 bytes) and include sufficient
 * padding to allow SIMD operations to safely read beyond the actual data
 * length without bounds checking.
 *
 * All functions return memory managed via RAII wrappers (AlignedBuffer)
 * that automatically free memory when they go out of scope.
 */

#ifndef LIBVROOM_IO_UTIL_H
#define LIBVROOM_IO_UTIL_H

#include "common_defs.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <malloc.h> // _aligned_malloc, _aligned_free
#endif

namespace libvroom {

/**
 * @brief RAII wrapper for aligned memory buffers.
 *
 * Manages memory that is aligned to 64-byte boundaries for efficient SIMD
 * operations. Includes padding bytes beyond the logical data size to allow
 * SIMD operations to read past the end without bounds checking.
 *
 * The buffer is automatically freed when the AlignedBuffer goes out of scope.
 */
class AlignedBuffer {
public:
  AlignedBuffer() = default;

  /// Move constructor
  AlignedBuffer(AlignedBuffer&& other) noexcept
      : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
    other.data_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
  }

  /// Move assignment
  AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
    if (this != &other) {
      free_data();
      data_ = other.data_;
      size_ = other.size_;
      capacity_ = other.capacity_;
      other.data_ = nullptr;
      other.size_ = 0;
      other.capacity_ = 0;
    }
    return *this;
  }

  // Non-copyable
  AlignedBuffer(const AlignedBuffer&) = delete;
  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  ~AlignedBuffer() { free_data(); }

  /// @return Pointer to the buffer data
  uint8_t* data() { return data_; }
  const uint8_t* data() const { return data_; }

  /// @return Logical size of the data (not including padding)
  size_t size() const { return size_; }

  /// @return Total allocated capacity (including padding)
  size_t capacity() const { return capacity_; }

  /// @return true if the buffer is valid (not null)
  bool valid() const { return data_ != nullptr; }

  /// @return true if the buffer is empty
  bool empty() const { return size_ == 0; }

  /// Explicit bool conversion
  explicit operator bool() const { return valid(); }

  /// Create an aligned buffer with the given size and padding
  static AlignedBuffer allocate(size_t size, size_t padding = LIBVROOM_PADDING) {
    AlignedBuffer buf;
    buf.capacity_ = size + padding;
    buf.size_ = size;

#ifdef _WIN32
    buf.data_ = static_cast<uint8_t*>(_aligned_malloc(buf.capacity_, 64));
#else
    int rc = posix_memalign(reinterpret_cast<void**>(&buf.data_), 64, buf.capacity_);
    if (rc != 0) {
      buf.data_ = nullptr;
    }
#endif

    if (buf.data_ == nullptr) {
      throw std::runtime_error("Failed to allocate aligned memory");
    }

    // Zero the padding bytes
    if (padding > 0) {
      std::memset(buf.data_ + size, 0, padding);
    }

    return buf;
  }

private:
  void free_data() {
    if (data_) {
#ifdef _WIN32
      _aligned_free(data_);
#else
      std::free(data_);
#endif
      data_ = nullptr;
      size_ = 0;
      capacity_ = 0;
    }
  }

  uint8_t* data_ = nullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
};

/**
 * @brief Allocate raw aligned memory (64-byte alignment).
 *
 * Portable wrapper around platform-specific aligned allocation.
 * Caller must free with aligned_free_portable().
 *
 * @param size Number of bytes to allocate.
 * @return Pointer to allocated memory, or nullptr on failure.
 */
inline void* aligned_alloc_portable(size_t size) {
#ifdef _WIN32
  return _aligned_malloc(size, 64);
#else
  void* ptr = nullptr;
  if (posix_memalign(&ptr, 64, size) != 0) {
    return nullptr;
  }
  return ptr;
#endif
}

/**
 * @brief Free memory allocated with aligned_alloc_portable().
 */
inline void aligned_free_portable(void* ptr) {
#ifdef _WIN32
  _aligned_free(ptr);
#else
  std::free(ptr);
#endif
}

/**
 * @brief Load a file into an aligned buffer.
 *
 * Reads the complete contents of a file into a newly allocated buffer that
 * is cache-line aligned (64 bytes) with additional padding bytes. This
 * enables efficient SIMD processing of the file contents without bounds
 * checking at the end of the buffer.
 *
 * @param filename The path to the file to load.
 * @param padding The number of extra bytes to allocate beyond the file size
 *                for safe SIMD overreads.
 * @return An AlignedBuffer containing the file contents.
 * @throws std::runtime_error If the file cannot be opened or read.
 */
AlignedBuffer load_file_to_ptr(const std::string& filename, size_t padding = LIBVROOM_PADDING);

/**
 * @brief Read all data from stdin into an aligned buffer.
 *
 * Reads the complete contents of standard input into a newly allocated buffer
 * that is cache-line aligned (64 bytes) with additional padding bytes.
 *
 * @param padding The number of extra bytes to allocate beyond the data size.
 * @return An AlignedBuffer containing the stdin contents.
 * @throws std::runtime_error If reading fails or memory allocation fails.
 */
AlignedBuffer read_stdin_to_ptr(size_t padding = LIBVROOM_PADDING);

} // namespace libvroom

#endif // LIBVROOM_IO_UTIL_H
