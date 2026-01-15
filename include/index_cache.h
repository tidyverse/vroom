/**
 * @file index_cache.h
 * @brief Cache management utilities for index caching.
 *
 * This header provides utilities for computing cache paths, validating cache
 * freshness, and handling atomic writes for persistent index caching. Index
 * caching allows parsed CSV field indexes to be stored on disk and reloaded
 * on subsequent runs, avoiding the cost of re-parsing large files.
 *
 * ## Cache Path Resolution Strategy
 *
 * The cache system supports three location modes:
 * 1. **SAME_DIR** (default): Cache file adjacent to source (e.g., data.csv.vidx)
 * 2. **XDG_CACHE**: Uses ~/.cache/libvroom/<hash>.vidx for read-only source dirs
 * 3. **CUSTOM**: User-specified directory
 *
 * When using SAME_DIR, if the source directory is not writable, the system
 * automatically falls back to XDG_CACHE to avoid permission errors.
 *
 * ## Cache Validation
 *
 * Cache validity is determined by comparing the source file's modification
 * time and size with the values stored in the cache header. If either has
 * changed, the cache is considered stale and must be regenerated.
 *
 * ## Atomic Writes
 *
 * Cache files are written atomically using a temp file + rename pattern.
 * This ensures that readers never see partially-written cache files.
 *
 * @see two_pass.h for ParseIndex structure
 */

#ifndef LIBVROOM_INDEX_CACHE_H
#define LIBVROOM_INDEX_CACHE_H

#include "two_pass.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace libvroom {

/**
 * @brief Error codes for cache operations.
 *
 * CacheError provides specific error codes for cache load and write operations,
 * enabling callers to distinguish between different failure modes and provide
 * informative user-facing messages.
 */
enum class CacheError {
  None,             ///< No error, operation succeeded
  Corrupted,        ///< Cache file exists but is corrupted or unreadable
  PermissionDenied, ///< Insufficient permissions to read/write cache file
  DiskFull,         ///< Disk is full, cannot write cache file
  VersionMismatch,  ///< Cache file format version doesn't match current version
  SourceChanged,    ///< Source file has changed since cache was created
  IoError,          ///< General I/O error during cache operation
  NotFound          ///< Cache file does not exist
};

/**
 * @brief Convert a CacheError to its string representation.
 *
 * @param error The CacheError to convert
 * @return C-string name of the error code (e.g., "Corrupted", "PermissionDenied")
 */
inline const char* cache_error_to_string(CacheError error) {
  switch (error) {
  case CacheError::None:
    return "None";
  case CacheError::Corrupted:
    return "Corrupted";
  case CacheError::PermissionDenied:
    return "PermissionDenied";
  case CacheError::DiskFull:
    return "DiskFull";
  case CacheError::VersionMismatch:
    return "VersionMismatch";
  case CacheError::SourceChanged:
    return "SourceChanged";
  case CacheError::IoError:
    return "IoError";
  case CacheError::NotFound:
    return "NotFound";
  default:
    return "Unknown";
  }
}

/**
 * @brief Result of a cache load operation.
 *
 * CacheLoadResult provides detailed information about a cache load attempt,
 * including the loaded index (on success), error code, and descriptive message.
 * This enables callers to distinguish between different failure modes and
 * provide informative user-facing messages.
 *
 * @example
 * @code
 * auto result = IndexCache::load("data.csv.vidx", source_meta);
 * if (result.success()) {
 *     // Use result.index
 *     ParseIndex idx = std::move(*result.index);
 * } else {
 *     std::cerr << "Cache load failed: " << result.message << std::endl;
 *     if (result.error == CacheError::SourceChanged) {
 *         // Re-parse the file
 *     }
 * }
 * @endcode
 */
struct CacheLoadResult {
  std::unique_ptr<ParseIndex> index; ///< The loaded index (present only on success)
  CacheError error;                  ///< Error code indicating the type of failure
  std::string message;               ///< Human-readable description of the result

  /// Default constructor creates a failed result with no index.
  CacheLoadResult() : index(nullptr), error(CacheError::NotFound), message("No cache loaded") {}

  /// Constructor for success with an index.
  CacheLoadResult(ParseIndex idx, CacheError err, std::string msg)
      : index(new ParseIndex(std::move(idx))), error(err), message(std::move(msg)) {}

  /// Constructor for failure without an index.
  CacheLoadResult(std::nullptr_t, CacheError err, std::string msg)
      : index(nullptr), error(err), message(std::move(msg)) {}

  // Move-only type (ParseIndex is move-only)
  CacheLoadResult(CacheLoadResult&&) = default;
  CacheLoadResult& operator=(CacheLoadResult&&) = default;
  CacheLoadResult(const CacheLoadResult&) = delete;
  CacheLoadResult& operator=(const CacheLoadResult&) = delete;

  /**
   * @brief Check if the load operation succeeded.
   * @return true if the index was successfully loaded.
   */
  bool success() const { return error == CacheError::None && index != nullptr; }

  /**
   * @brief Check if an index is present.
   * @return true if the index was loaded (regardless of error state).
   */
  bool has_index() const { return index != nullptr; }

  /**
   * @brief Create a successful result with a loaded index.
   * @param idx The successfully loaded ParseIndex.
   * @return CacheLoadResult indicating success.
   */
  static CacheLoadResult ok(ParseIndex idx) {
    return CacheLoadResult(std::move(idx), CacheError::None, "Cache loaded successfully");
  }

  /**
   * @brief Create a failed result with the specified error.
   * @param err The error code.
   * @param msg Human-readable error message.
   * @return CacheLoadResult indicating failure.
   */
  static CacheLoadResult fail(CacheError err, const std::string& msg) {
    return CacheLoadResult(nullptr, err, msg);
  }
};

/**
 * @brief Result of a cache write operation.
 *
 * CacheWriteResult provides detailed information about a cache write attempt,
 * including success status, error code, and descriptive message. This enables
 * callers to understand why a write failed and take appropriate action.
 *
 * @example
 * @code
 * auto result = IndexCache::write("data.csv.vidx", parsed_index, source_meta);
 * if (!result.success()) {
 *     if (result.error == CacheError::DiskFull) {
 *         std::cerr << "Warning: Disk full, cache not written" << std::endl;
 *     } else if (result.error == CacheError::PermissionDenied) {
 *         std::cerr << "Warning: Permission denied, cache not written" << std::endl;
 *     }
 * }
 * @endcode
 */
struct CacheWriteResult {
  bool successful;     ///< Whether the write operation succeeded
  CacheError error;    ///< Error code indicating the type of failure
  std::string message; ///< Human-readable description of the result

  /**
   * @brief Check if the write operation succeeded.
   * @return true if the cache was successfully written.
   */
  bool success() const { return successful; }

  /**
   * @brief Create a successful result.
   * @return CacheWriteResult indicating success.
   */
  static CacheWriteResult ok() {
    return CacheWriteResult{true, CacheError::None, "Cache written successfully"};
  }

  /**
   * @brief Create a failed result with the specified error.
   * @param err The error code.
   * @param msg Human-readable error message.
   * @return CacheWriteResult indicating failure.
   */
  static CacheWriteResult fail(CacheError err, const std::string& msg) {
    return CacheWriteResult{false, err, msg};
  }
};

/// Index cache format version (v1 includes source file metadata for validation)
constexpr uint8_t INDEX_CACHE_VERSION = 1;

/**
 * @brief Callback type for cache warning messages.
 *
 * This callback is invoked when cache operations encounter non-fatal issues
 * that users may want to be aware of for debugging or logging purposes.
 *
 * Warning scenarios include:
 * - Cache file corruption requiring deletion and re-parsing
 * - Cache write failures due to storage constraints
 * - Location fallback (e.g., from source directory to XDG_CACHE)
 * - Version mismatch invalidating cached data
 * - Permission errors when accessing cache files
 *
 * @param message A human-readable description of the warning
 *
 * @example
 * @code
 * CacheConfig config = CacheConfig::defaults();
 * config.warning_callback = [](const std::string& msg) {
 *     std::cerr << "[cache warning] " << msg << std::endl;
 * };
 * @endcode
 */
using CacheWarningCallback = std::function<void(const std::string&)>;

/**
 * @brief Configuration for cache location resolution.
 *
 * CacheConfig controls where cache files are stored. The default SAME_DIR
 * mode places cache files adjacent to source files for maximum locality
 * and simplicity. XDG_CACHE mode uses the standard XDG cache directory
 * (~/.cache/libvroom/) which is useful when source directories are read-only.
 */
struct CacheConfig {
  /**
   * @brief Cache location mode.
   */
  enum Location {
    /**
     * @brief Store cache adjacent to source file (e.g., data.csv.vidx).
     *
     * This is the default and preferred mode. Falls back to XDG_CACHE
     * if the source directory is not writable.
     */
    SAME_DIR,

    /**
     * @brief Store cache in XDG cache directory (~/.cache/libvroom/).
     *
     * Uses a hash of the source file's absolute path to generate a unique
     * filename, avoiding collisions between files with the same name in
     * different directories.
     */
    XDG_CACHE,

    /**
     * @brief Store cache in a custom user-specified directory.
     *
     * When this mode is selected, custom_path must be set to a valid
     * directory path.
     */
    CUSTOM
  };

  /// The cache location mode to use.
  Location location = SAME_DIR;

  /// Custom directory path (only used when location == CUSTOM).
  std::string custom_path;

  /**
   * @brief Whether to resolve symlinks when computing cache paths.
   *
   * When true (default), symlinks in the source file path are resolved to
   * their canonical paths before computing the cache location. This ensures
   * that files accessed through different symlink paths share a single cache
   * file rather than creating duplicate caches.
   *
   * Set to false if you want separate caches for different symlink paths
   * pointing to the same file, or if symlink resolution causes issues in
   * your environment.
   */
  bool resolve_symlinks = true;

  /// Extension used for cache files.
  static constexpr const char* CACHE_EXTENSION = ".vidx";

  /// Optional callback for warning messages during cache operations.
  CacheWarningCallback warning_callback;

  /**
   * @brief Create default configuration (SAME_DIR mode).
   */
  static CacheConfig defaults() { return CacheConfig{}; }

  /**
   * @brief Create configuration for XDG cache directory.
   */
  static CacheConfig xdg_cache() {
    CacheConfig config;
    config.location = XDG_CACHE;
    return config;
  }

  /**
   * @brief Create configuration for a custom directory.
   * @param path Path to the custom cache directory.
   */
  static CacheConfig custom(const std::string& path) {
    CacheConfig config;
    config.location = CUSTOM;
    config.custom_path = path;
    return config;
  }
};

/**
 * @brief Cache management utilities for persistent index storage.
 *
 * IndexCache provides static methods for computing cache paths, validating
 * cache freshness, and performing atomic cache writes. This class is
 * stateless - all methods are static and operate on the paths provided.
 *
 * @example Basic usage
 * @code
 * #include "index_cache.h"
 *
 * // Compute cache path for a source file
 * std::string cache_path = IndexCache::compute_path("data.csv",
 *                                                    CacheConfig::defaults());
 *
 * // Check if existing cache is valid
 * if (IndexCache::is_valid("data.csv", cache_path)) {
 *     // Load from cache (see Phase 4 for loading API)
 * } else {
 *     // Parse file and write cache
 *     auto result = parser.parse(buf, len);
 *     if (IndexCache::write_atomic(cache_path, result.idx, "data.csv")) {
 *         std::cout << "Cache written successfully\n";
 *     }
 * }
 * @endcode
 */
class IndexCache {
public:
  /**
   * @brief Compute cache path for a source file.
   *
   * Resolves the cache path based on the source file path and configuration.
   * For SAME_DIR mode, this simply appends ".vidx" to the source path.
   * For XDG_CACHE mode, this generates a hash-based filename in ~/.cache/libvroom/.
   * For CUSTOM mode, this places the cache file in the configured directory.
   *
   * @param source_path Path to the source CSV file.
   * @param config Cache configuration specifying location mode.
   * @return The computed cache file path.
   *
   * @note For SAME_DIR mode with unwritable source directories, consider
   *       using try_compute_writable_path() instead for automatic fallback.
   */
  static std::string compute_path(const std::string& source_path, const CacheConfig& config);

  /**
   * @brief Compute a writable cache path with automatic fallback.
   *
   * Similar to compute_path(), but for SAME_DIR mode, if the source directory
   * is not writable, automatically falls back to XDG_CACHE mode.
   *
   * @param source_path Path to the source CSV file.
   * @param config Cache configuration specifying location mode.
   * @return A pair of (cache_path, success). If success is false, no writable
   *         location could be found.
   */
  static std::pair<std::string, bool> try_compute_writable_path(const std::string& source_path,
                                                                const CacheConfig& config);

  /**
   * @brief Check if a cache file is valid for the given source file.
   *
   * Reads the cache file header and compares the stored mtime and size
   * with the current source file metadata. The cache is valid only if:
   * 1. The cache file exists and is readable
   * 2. The cache file has a valid header with matching version
   * 3. The stored mtime matches the source file's mtime
   * 4. The stored size matches the source file's size
   *
   * @param source_path Path to the source CSV file.
   * @param cache_path Path to the cache file.
   * @return true if the cache is valid, false otherwise.
   *
   * @note This method does not fully validate the cache contents beyond
   *       the header. A corrupted cache body may still return true.
   */
  static bool is_valid(const std::string& source_path, const std::string& cache_path);

  /**
   * @brief Write a ParseIndex to a cache file atomically.
   *
   * Writes the index to a temporary file, then atomically renames it to
   * the target path. This ensures readers never see partially-written files.
   * The cache header includes the source file's mtime and size for validation.
   *
   * Cache file format:
   * - [version: 1 byte] Cache format version (INDEX_CACHE_VERSION)
   * - [mtime: 8 bytes] Source file modification time (seconds since epoch)
   * - [size: 8 bytes] Source file size in bytes
   * - [columns: 8 bytes] Number of columns in the CSV
   * - [n_threads: 2 bytes] Number of threads used for parsing
   * - [n_indexes: 8 * n_threads bytes] Array of index counts per thread
   * - [indexes: 8 * total_indexes bytes] Array of field separator positions
   *
   * @param path Path to write the cache file.
   * @param index The ParseIndex to serialize.
   * @param source_path Path to the source file (for metadata extraction).
   * @return true if the cache was written successfully, false otherwise.
   *
   * @warning If this returns false, no cache file was created or modified.
   *          Any temporary file is cleaned up automatically.
   */
  static bool write_atomic(const std::string& path, const ParseIndex& index,
                           const std::string& source_path);

  /**
   * @brief Validate a cache file and load if valid, with detailed error reporting.
   *
   * This method combines is_valid() and ParseIndex::from_mmap() functionality
   * into a single call that returns detailed error information. It validates
   * the cache against the source file and loads it if valid.
   *
   * @param source_path Path to the source CSV file.
   * @param cache_path Path to the cache file.
   * @return CacheLoadResult with the loaded index on success, or detailed
   *         error information on failure.
   *
   * @example
   * @code
   * auto result = IndexCache::validate_and_load("data.csv", "data.csv.vidx");
   * if (result.success()) {
   *     ParseIndex idx = std::move(*result.index);
   *     // Use the loaded index
   * } else if (result.error == CacheError::SourceChanged) {
   *     // Re-parse the file, source has been modified
   * } else if (result.error == CacheError::VersionMismatch) {
   *     // Cache format is outdated, re-parse
   * }
   * @endcode
   */
  static CacheLoadResult validate_and_load(const std::string& source_path,
                                           const std::string& cache_path);

  /**
   * @brief Write a ParseIndex to a cache file atomically, with detailed error reporting.
   *
   * Like write_atomic(), but returns a CacheWriteResult with detailed error
   * information instead of a simple boolean.
   *
   * @param path Path to write the cache file.
   * @param index The ParseIndex to serialize.
   * @param source_path Path to the source file (for metadata extraction).
   * @return CacheWriteResult indicating success or detailed failure information.
   *
   * @example
   * @code
   * auto result = IndexCache::write_atomic_result(cache_path, idx, source_path);
   * if (!result.success()) {
   *     if (result.error == CacheError::DiskFull) {
   *         std::cerr << "Disk full: " << result.message << std::endl;
   *     } else if (result.error == CacheError::PermissionDenied) {
   *         std::cerr << "Permission denied: " << result.message << std::endl;
   *     }
   * }
   * @endcode
   */
  static CacheWriteResult write_atomic_result(const std::string& path, const ParseIndex& index,
                                              const std::string& source_path);

  /**
   * @brief Get source file metadata (modification time and size).
   *
   * Retrieves the modification time and size of a file for cache validation.
   *
   * @param source_path Path to the source file.
   * @return A pair of (mtime, size). If the file cannot be stat'd,
   *         returns (0, 0).
   *
   * @note The mtime is in seconds since the Unix epoch (time_t).
   */
  static std::pair<uint64_t, uint64_t> get_source_metadata(const std::string& source_path);

  /**
   * @brief Check if a directory is writable.
   *
   * Attempts to determine if files can be created in the given directory.
   *
   * @param dir_path Path to the directory to check.
   * @return true if the directory exists and is writable, false otherwise.
   */
  static bool is_directory_writable(const std::string& dir_path);

  /**
   * @brief Get the XDG cache directory for libvroom.
   *
   * Returns the path to ~/.cache/libvroom/, creating it if necessary.
   *
   * @return Path to the cache directory, or empty string if it cannot be
   *         created.
   */
  static std::string get_xdg_cache_dir();

  /**
   * @brief Hash a file path to generate a unique cache filename.
   *
   * Uses a simple hash algorithm to convert a file path to a fixed-length
   * hexadecimal string suitable for use as a cache filename.
   *
   * @param path The file path to hash.
   * @return A hexadecimal hash string.
   */
  static std::string hash_path(const std::string& path);

  /**
   * @brief Resolve symlinks in a file path to get the canonical path.
   *
   * Uses std::filesystem::canonical() to resolve all symlinks, '.', and '..'
   * components in the path. If resolution fails (e.g., file doesn't exist,
   * permission denied, or too many symlink levels), gracefully falls back
   * to the original path.
   *
   * @param path The file path to resolve.
   * @return The canonical path if resolution succeeds, or the original path
   *         if resolution fails.
   *
   * @note This function is used internally by compute_path() when
   *       CacheConfig::resolve_symlinks is true.
   */
  static std::string resolve_path(const std::string& path);

  /**
   * @brief Cache file header size in bytes.
   *
   * Layout: [version:1][mtime:8][size:8][columns:8][n_threads:2] = 27 bytes
   */
  static constexpr size_t HEADER_SIZE = 1 + 8 + 8 + 8 + 2;

  /**
   * @brief Result of a cache load operation.
   *
   * Contains the loaded index (if successful) and status information about
   * why a load might have failed.
   */
  struct LoadResult {
    ParseIndex index;          ///< The loaded index (check is_valid() for success)
    bool was_corrupted{false}; ///< True if cache file was corrupted and deleted
    bool file_deleted{false};  ///< True if corrupted cache file was deleted
    std::string error_message; ///< Description of any error encountered

    /// @return true if the index was loaded successfully
    bool success() const { return index.is_valid(); }
  };

  /**
   * @brief Load a cached index with automatic corruption handling.
   *
   * Attempts to load a cached ParseIndex from disk. If the cache file is
   * corrupted (invalid version, truncated, or fails validation), it is
   * automatically deleted to allow re-caching on the next parse.
   *
   * Corruption conditions that trigger automatic cleanup:
   * - File is too small to contain a valid header
   * - Version byte is not the expected v3 format
   * - File is truncated (indexes extend beyond file boundary)
   *
   * Note: Stale caches (where source file metadata has changed) are NOT deleted,
   * only true corruption is cleaned up. Stale caches are simply re-parsed.
   *
   * @param cache_path Path to the cache file.
   * @param source_path Path to the source CSV file (for metadata validation).
   * @return LoadResult containing the index and status information.
   *
   * @example
   * @code
   * auto result = IndexCache::load("data.csv.vidx", "data.csv");
   * if (result.success()) {
   *     // Use result.index
   * } else if (result.was_corrupted) {
   *     std::cerr << "Cache was corrupted and deleted: " << result.error_message << "\n";
   *     // Re-parse the file and write new cache
   * } else {
   *     // Cache miss (file doesn't exist or source changed)
   * }
   * @endcode
   *
   * @see is_valid() for validation-only checks without loading
   * @see write_atomic() for writing cache files
   */
  static LoadResult load(const std::string& cache_path, const std::string& source_path);
};

} // namespace libvroom

#endif // LIBVROOM_INDEX_CACHE_H
