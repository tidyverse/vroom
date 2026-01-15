#ifndef MMAP_UTIL_H
#define MMAP_UTIL_H

/**
 * @file mmap_util.h
 * @brief Cross-platform memory-mapped file utilities.
 *
 * This header provides a portable RAII wrapper for memory-mapped files,
 * supporting both POSIX (mmap/munmap) and Windows (CreateFileMapping/MapViewOfFile).
 *
 * Primary use case: Memory-mapping cached index files for direct pointer access
 * without copying data into heap-allocated memory.
 */

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace libvroom {

/**
 * @brief RAII wrapper for cross-platform memory-mapped files.
 *
 * MmapBuffer provides a safe, RAII-based interface for memory-mapping files
 * as read-only. When the buffer goes out of scope, the mapping is automatically
 * released.
 *
 * Features:
 * - Cross-platform: Works on POSIX (Linux, macOS) and Windows
 * - Move-only: Non-copyable but moveable for efficient transfers
 * - Safe: RAII ensures proper cleanup even on exceptions
 * - Read-only: Files are mapped with read-only permissions
 *
 * @example
 * @code
 * MmapBuffer buf;
 * if (buf.open("data.vidx")) {
 *     const uint8_t* data = buf.data();
 *     size_t size = buf.size();
 *     // Use data...
 * } // Automatically unmapped when buf goes out of scope
 * @endcode
 */
class MmapBuffer {
public:
  /**
   * @brief Default constructor. Creates an empty, invalid buffer.
   */
  MmapBuffer() = default;

  /**
   * @brief Destructor. Unmaps the file if mapped.
   */
  ~MmapBuffer() { unmap(); }

  // Non-copyable
  MmapBuffer(const MmapBuffer&) = delete;
  MmapBuffer& operator=(const MmapBuffer&) = delete;

  /**
   * @brief Move constructor.
   *
   * Transfers ownership of the memory mapping from another MmapBuffer.
   * The source buffer is left in a valid but empty state.
   *
   * @param other The MmapBuffer to move from.
   */
  MmapBuffer(MmapBuffer&& other) noexcept;

  /**
   * @brief Move assignment operator.
   *
   * Releases any existing mapping and takes ownership from another MmapBuffer.
   *
   * @param other The MmapBuffer to move from.
   * @return Reference to this MmapBuffer.
   */
  MmapBuffer& operator=(MmapBuffer&& other) noexcept;

  /**
   * @brief Open and memory-map a file for reading.
   *
   * Maps the entire file into the process's address space with read-only
   * permissions. The file must exist and be non-empty.
   *
   * If a file is already mapped, it is unmapped before opening the new file.
   *
   * @param path Path to the file to map.
   * @return true if the file was successfully mapped, false otherwise.
   *
   * @note On failure, the buffer remains in an invalid state.
   * @note Empty files return false (cannot be memory-mapped).
   */
  bool open(const std::string& path);

  /**
   * @brief Get a pointer to the mapped data.
   *
   * @return Pointer to the start of the mapped data, or nullptr if invalid.
   */
  const uint8_t* data() const { return static_cast<const uint8_t*>(data_); }

  /**
   * @brief Get the size of the mapped data in bytes.
   *
   * @return Size of the mapped data, or 0 if invalid.
   */
  size_t size() const { return size_; }

  /**
   * @brief Check if the buffer contains a valid mapping.
   *
   * @return true if a file is currently mapped, false otherwise.
   */
  bool valid() const { return data_ != nullptr; }

private:
  /**
   * @brief Unmap the current file.
   *
   * Releases all resources associated with the current mapping.
   * Safe to call even if no file is mapped.
   */
  void unmap();

  void* data_ = nullptr;
  size_t size_ = 0;

#ifdef _WIN32
  HANDLE file_handle_ = INVALID_HANDLE_VALUE;
  HANDLE map_handle_ = nullptr;
#else
  int fd_ = -1;
#endif
};

/**
 * @brief Get source file metadata for cache validation.
 *
 * This structure holds metadata about a source CSV file that can be used
 * to determine if a cached index is still valid.
 */
struct SourceMetadata {
  uint64_t mtime = 0; ///< Modification time (seconds since epoch)
  uint64_t size = 0;  ///< File size in bytes
  bool valid = false; ///< True if metadata was successfully retrieved

  /**
   * @brief Retrieve metadata from a file.
   *
   * @param path Path to the file.
   * @return SourceMetadata with valid=true if successful, valid=false otherwise.
   */
  static SourceMetadata from_file(const std::string& path);
};

/**
 * @brief Generate cache file path for a source file.
 *
 * @param source_path Path to the source CSV file.
 * @return Cache file path (source_path + ".vidx").
 */
std::string get_cache_path(const std::string& source_path);

} // namespace libvroom

#endif // MMAP_UTIL_H
