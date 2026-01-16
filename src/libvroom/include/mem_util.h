/**
 * @file mem_util.h
 * @brief Portable aligned memory allocation utilities.
 *
 * This header provides cross-platform aligned memory allocation and deallocation
 * functions. These are used internally by libvroom to ensure buffers are properly
 * aligned for SIMD operations (typically 64-byte cache line alignment).
 *
 * The functions handle platform differences:
 * - POSIX: posix_memalign()
 * - MSVC: _aligned_malloc() / _aligned_free()
 * - MinGW: __mingw_aligned_malloc() / __mingw_aligned_free()
 *
 * ## Memory Management Options
 *
 * libvroom provides three memory management approaches:
 *
 * ### 1. RAII with FileBuffer (Recommended for files)
 * @code
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 * parser.parse(buffer.data(), buffer.size());
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * ### 2. RAII with AlignedPtr (Recommended for custom allocations)
 * @code
 * AlignedPtr buffer = make_aligned_ptr(1024, 64);
 * // ... use buffer.get() ...
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * ### 3. Direct allocation with read_file() (Low-level)
 * @code
 * auto [buffer, size] = read_file("data.csv", 64);
 * // ... use buffer.get() ...
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @note All memory allocated with aligned_malloc() must be freed with
 *       aligned_free(). Do NOT use standard free() or delete.
 *
 * @see allocate_padded_buffer() in io_util.h for higher-level allocation.
 * @see FileBuffer in libvroom.h for RAII memory management of files.
 * @see AlignedPtr for RAII management of raw aligned buffers.
 */

#ifndef MEM_UTIL_H
#define MEM_UTIL_H

#include <memory>
#include <stdlib.h>

/**
 * @brief Allocate memory with specified alignment.
 *
 * Allocates a block of memory with the specified alignment. This is a
 * portable wrapper around platform-specific aligned allocation functions.
 *
 * @param alignment Required alignment in bytes (must be power of 2).
 *                  Typically 64 for cache line alignment.
 * @param size Number of bytes to allocate.
 *
 * @return Pointer to the allocated memory, or nullptr if allocation fails.
 *
 * @note The returned pointer must be freed using aligned_free(), not free().
 *
 * @warning **Production code must check for nullptr return.** This function
 *          can fail due to insufficient memory, especially when allocating
 *          large buffers for CSV parsing. Always check the return value
 *          before using the pointer to avoid undefined behavior.
 *
 * @example
 * @code
 * // Allocate 1KB aligned to 64-byte cache line
 * void* buffer = aligned_malloc(64, 1024);
 * if (buffer) {
 *     // Use buffer...
 *     aligned_free(buffer);
 * }
 * @endcode
 *
 * @see aligned_free() To deallocate memory from this function.
 */
static inline void* aligned_malloc(size_t alignment, size_t size) {
  void* p;
#ifdef _MSC_VER
  p = _aligned_malloc(size, alignment);
#elif defined(__MINGW32__) || defined(__MINGW64__)
  p = __mingw_aligned_malloc(size, alignment);
#else
  // somehow, if this is used before including "x86intrin.h", it creates an
  // implicit defined warning.
  if (posix_memalign(&p, alignment, size) != 0) {
    return nullptr;
  }
#endif
  return p;
}

/**
 * @brief Free memory allocated with aligned_malloc().
 *
 * Frees a block of memory that was allocated with aligned_malloc() or
 * allocate_padded_buffer(). This is a portable wrapper around platform-specific
 * aligned deallocation functions.
 *
 * @param memblock Pointer to the memory block to free. If nullptr, no action
 *                 is taken (safe to call with null pointers).
 *
 * @warning Do NOT use this function to free memory allocated with standard
 *          malloc(), new, or other non-aligned allocation functions.
 *
 * @warning Do NOT use standard free() or delete to free memory allocated
 *          with aligned_malloc() - this may cause undefined behavior on
 *          some platforms (particularly Windows).
 *
 * @example
 * @code
 * void* buffer = aligned_malloc(64, 1024);
 * // ... use buffer ...
 * aligned_free(buffer);  // Safe even if buffer is nullptr
 * @endcode
 *
 * @see aligned_malloc() To allocate memory that this function frees.
 */
static inline void aligned_free(void* memblock) {
  if (memblock == nullptr) {
    return;
  }
#ifdef _MSC_VER
  _aligned_free(memblock);
#elif defined(__MINGW32__) || defined(__MINGW64__)
  __mingw_aligned_free(memblock);
#else
  free(memblock);
#endif
}

/**
 * @brief Custom deleter for std::unique_ptr that uses aligned_free().
 *
 * This deleter is designed for use with std::unique_ptr to provide RAII
 * memory management for buffers allocated with aligned_malloc() or
 * allocate_padded_buffer().
 *
 * @example
 * @code
 * #include "mem_util.h"
 *
 * // Create a unique_ptr with AlignedDeleter
 * uint8_t* raw = static_cast<uint8_t*>(aligned_malloc(64, 1024));
 * std::unique_ptr<uint8_t[], AlignedDeleter> buffer(raw);
 *
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see AlignedPtr For a convenient type alias.
 * @see make_aligned_ptr() For a factory function.
 */
struct AlignedDeleter {
  /**
   * @brief Deletes the given uint8_t array using aligned_free().
   * @param ptr Pointer to free. Safe to call with nullptr.
   *
   * @note This deleter only accepts uint8_t* to avoid ambiguity with nullptr.
   *       Use static_cast if you need to delete other pointer types.
   */
  void operator()(uint8_t* ptr) const noexcept { aligned_free(ptr); }
};

/**
 * @brief Smart pointer type for SIMD-aligned memory buffers.
 *
 * AlignedPtr is a std::unique_ptr with a custom deleter that calls
 * aligned_free(). Use this for RAII management of buffers allocated
 * with aligned_malloc() or allocate_padded_buffer().
 *
 * @example
 * @code
 * #include "mem_util.h"
 *
 * // Allocate aligned buffer with RAII
 * AlignedPtr buffer = make_aligned_ptr(1024, 64);
 *
 * if (buffer) {
 *     // Use buffer.get() to access the raw pointer
 *     memset(buffer.get(), 0, 1024);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see make_aligned_ptr() For convenient allocation.
 * @see AlignedDeleter For the custom deleter used.
 */
using AlignedPtr = std::unique_ptr<uint8_t[], AlignedDeleter>;

/**
 * @brief Allocates an aligned memory buffer with RAII management.
 *
 * This function combines allocate_padded_buffer() with RAII ownership,
 * returning a smart pointer that automatically frees the memory when
 * it goes out of scope.
 *
 * @param length The number of bytes of actual data that will be stored.
 * @param padding The number of additional bytes to allocate beyond length
 *                for safe SIMD overreads. Typically 32-64 bytes for AVX/AVX2.
 *
 * @return An AlignedPtr owning the allocated buffer, or an empty AlignedPtr
 *         if allocation fails (can be checked with if(ptr) or ptr.get()).
 *
 * @note The padding bytes are not initialized and may contain garbage values.
 *
 * @example
 * @code
 * #include "mem_util.h"
 *
 * // Allocate 1KB with 64-byte padding for SIMD
 * AlignedPtr buffer = make_aligned_ptr(1024, 64);
 *
 * if (buffer) {
 *     // Copy data into buffer
 *     memcpy(buffer.get(), my_data, 1024);
 *
 *     // SIMD operations can safely read up to 1088 bytes
 *     // ... process buffer with SIMD ...
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see allocate_padded_buffer() For manual memory management.
 * @see AlignedPtr For the returned type.
 */
inline AlignedPtr make_aligned_ptr(size_t length, size_t padding) {
  // Check for integer overflow before allocation
  if (length > SIZE_MAX - padding) {
    return AlignedPtr(nullptr);
  }
  size_t total = length + padding;
  uint8_t* ptr = static_cast<uint8_t*>(aligned_malloc(64, total));
  return AlignedPtr(ptr);
}

#endif
