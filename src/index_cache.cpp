/**
 * @file index_cache.cpp
 * @brief Implementation of cache management utilities.
 */

#include "index_cache.h"

#include "mmap_util.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <windows.h>
#define mkdir(dir, mode) _mkdir(dir)
#define access _access
#define W_OK 2
#define getpid _getpid
// Windows compatibility for POSIX stat macros
#ifndef S_ISREG
#define S_ISREG(m) (((m) & _S_IFMT) == _S_IFREG)
#endif
#ifndef S_ISDIR
#define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#endif
#else
#include <unistd.h>
#endif

namespace libvroom {

std::string IndexCache::compute_path(const std::string& source_path, const CacheConfig& config) {
  // Resolve symlinks if enabled to ensure files accessed through different
  // symlink paths share a single cache file
  std::string resolved_path = config.resolve_symlinks ? resolve_path(source_path) : source_path;

  switch (config.location) {
  case CacheConfig::SAME_DIR:
    // For SAME_DIR mode, use the original path for the cache location
    // (keep cache adjacent to the symlink, not the target)
    return source_path + CacheConfig::CACHE_EXTENSION;

  case CacheConfig::XDG_CACHE: {
    std::string cache_dir = get_xdg_cache_dir();
    if (cache_dir.empty()) {
      // Fallback to same-dir if XDG cache unavailable
      return source_path + CacheConfig::CACHE_EXTENSION;
    }
    // Use resolved path for hashing to ensure symlinks share the same cache
    return cache_dir + "/" + hash_path(resolved_path) + CacheConfig::CACHE_EXTENSION;
  }

  case CacheConfig::CUSTOM: {
    if (config.custom_path.empty()) {
      // No custom path specified, fallback to same-dir
      return source_path + CacheConfig::CACHE_EXTENSION;
    }
    // Extract filename from resolved path for custom directory
    // Use resolved path to ensure consistent cache filenames
    size_t last_sep = resolved_path.find_last_of("/\\");
    std::string filename =
        (last_sep == std::string::npos) ? resolved_path : resolved_path.substr(last_sep + 1);
    return config.custom_path + "/" + filename + CacheConfig::CACHE_EXTENSION;
  }

  default:
    return source_path + CacheConfig::CACHE_EXTENSION;
  }
}

std::pair<std::string, bool> IndexCache::try_compute_writable_path(const std::string& source_path,
                                                                   const CacheConfig& config) {
  // Helper to emit warnings if callback is set
  auto warn = [&config](const std::string& message) {
    if (config.warning_callback) {
      config.warning_callback(message);
    }
  };

  std::string path = compute_path(source_path, config);

  if (config.location == CacheConfig::SAME_DIR) {
    // Check if source directory is writable
    size_t last_sep = source_path.find_last_of("/\\");
    std::string dir = (last_sep == std::string::npos) ? "." : source_path.substr(0, last_sep);

    if (!is_directory_writable(dir)) {
      // Fall back to XDG cache - use resolved path for consistent hashing
      std::string xdg_dir = get_xdg_cache_dir();
      if (!xdg_dir.empty()) {
        std::string resolved_path =
            config.resolve_symlinks ? resolve_path(source_path) : source_path;
        path = xdg_dir + "/" + hash_path(resolved_path) + CacheConfig::CACHE_EXTENSION;
        if (is_directory_writable(xdg_dir)) {
          warn("Source directory '" + dir +
               "' is not writable, falling back to XDG cache: " + path);
          return {path, true};
        }
        warn("Source directory '" + dir + "' is not writable and XDG cache directory '" + xdg_dir +
             "' is also not writable; cache disabled");
      } else {
        warn("Source directory '" + dir +
             "' is not writable and XDG cache directory could not be created; cache disabled");
      }
      return {"", false};
    }
  } else if (config.location == CacheConfig::XDG_CACHE) {
    std::string xdg_dir = get_xdg_cache_dir();
    if (xdg_dir.empty()) {
      warn("XDG cache directory could not be created; cache disabled");
      return {"", false};
    }
    if (!is_directory_writable(xdg_dir)) {
      warn("XDG cache directory '" + xdg_dir + "' is not writable; cache disabled");
      return {"", false};
    }
  } else if (config.location == CacheConfig::CUSTOM) {
    if (config.custom_path.empty()) {
      warn("Custom cache path is empty; cache disabled");
      return {"", false};
    }
    if (!is_directory_writable(config.custom_path)) {
      warn("Custom cache directory '" + config.custom_path + "' is not writable; cache disabled");
      return {"", false};
    }
  }

  return {path, true};
}

bool IndexCache::is_valid(const std::string& source_path, const std::string& cache_path) {
  // Get current source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    // Source file doesn't exist or can't be read
    return false;
  }

  // Open cache file
  std::FILE* fp = std::fopen(cache_path.c_str(), "rb");
  if (!fp) {
    return false;
  }

  bool valid = false;

  // Read and validate header - v3 format: version(1) + padding(7) + mtime(8) + size(8)
  // Total: 24 bytes to get to mtime and size
  static constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;

  uint8_t version = 0;
  if (std::fread(&version, 1, 1, fp) != 1 || version != INDEX_FORMAT_VERSION_V3) {
    std::fclose(fp);
    return false;
  }

  // Skip 7 bytes of alignment padding
  uint8_t padding[7];
  if (std::fread(padding, 1, 7, fp) != 7) {
    std::fclose(fp);
    return false;
  }

  uint64_t cached_mtime = 0;
  uint64_t cached_size = 0;
  if (std::fread(&cached_mtime, 8, 1, fp) != 1 || std::fread(&cached_size, 8, 1, fp) != 1) {
    std::fclose(fp);
    return false;
  }

  // Compare metadata
  if (cached_mtime == source_meta.mtime && cached_size == source_meta.size) {
    valid = true;
  }

  std::fclose(fp);
  return valid;
}

bool IndexCache::write_atomic(const std::string& path, const ParseIndex& index,
                              const std::string& source_path) {
  // Get source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    // Can't get source metadata
    return false;
  }

  // Delegate to ParseIndex::write which writes v3 format with atomic rename
  // Need to const_cast because write() is not const (but doesn't modify data)
  try {
    const_cast<ParseIndex&>(index).write(path, source_meta);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

CacheLoadResult IndexCache::validate_and_load(const std::string& source_path,
                                              const std::string& cache_path) {
  // Get current source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    return CacheLoadResult::fail(CacheError::IoError,
                                 "Cannot read source file metadata: " + source_path);
  }

  // Check if cache file exists
  struct stat cache_stat;
  if (stat(cache_path.c_str(), &cache_stat) != 0) {
    if (errno == ENOENT) {
      return CacheLoadResult::fail(CacheError::NotFound, "Cache file not found: " + cache_path);
    } else if (errno == EACCES) {
      return CacheLoadResult::fail(CacheError::PermissionDenied,
                                   "Permission denied reading cache file: " + cache_path);
    }
    return CacheLoadResult::fail(CacheError::IoError, "Cannot stat cache file: " + cache_path +
                                                          " (errno: " + std::to_string(errno) +
                                                          ")");
  }

  // Open cache file to validate header
  std::FILE* fp = std::fopen(cache_path.c_str(), "rb");
  if (!fp) {
    if (errno == EACCES) {
      return CacheLoadResult::fail(CacheError::PermissionDenied,
                                   "Permission denied opening cache file: " + cache_path);
    }
    return CacheLoadResult::fail(CacheError::IoError, "Cannot open cache file: " + cache_path +
                                                          " (errno: " + std::to_string(errno) +
                                                          ")");
  }

  // Read and validate header - v3 format: version(1) + padding(7) + mtime(8) + size(8)
  static constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;

  uint8_t version = 0;
  if (std::fread(&version, 1, 1, fp) != 1) {
    std::fclose(fp);
    return CacheLoadResult::fail(CacheError::Corrupted,
                                 "Cannot read cache file version: " + cache_path);
  }

  if (version != INDEX_FORMAT_VERSION_V3) {
    std::fclose(fp);
    return CacheLoadResult::fail(CacheError::VersionMismatch,
                                 "Cache file version mismatch: expected " +
                                     std::to_string(INDEX_FORMAT_VERSION_V3) + ", got " +
                                     std::to_string(version));
  }

  // Skip 7 bytes of alignment padding
  uint8_t padding[7];
  if (std::fread(padding, 1, 7, fp) != 7) {
    std::fclose(fp);
    return CacheLoadResult::fail(CacheError::Corrupted,
                                 "Cannot read cache file header padding: " + cache_path);
  }

  uint64_t cached_mtime = 0;
  uint64_t cached_size = 0;
  if (std::fread(&cached_mtime, 8, 1, fp) != 1 || std::fread(&cached_size, 8, 1, fp) != 1) {
    std::fclose(fp);
    return CacheLoadResult::fail(CacheError::Corrupted,
                                 "Cannot read cache file metadata: " + cache_path);
  }

  std::fclose(fp);

  // Compare metadata
  if (cached_mtime != source_meta.mtime || cached_size != source_meta.size) {
    return CacheLoadResult::fail(
        CacheError::SourceChanged,
        "Source file has changed since cache was created (mtime: " + std::to_string(cached_mtime) +
            " vs " + std::to_string(source_meta.mtime) + ", size: " + std::to_string(cached_size) +
            " vs " + std::to_string(source_meta.size) + ")");
  }

  // Cache is valid, load via mmap
  ParseIndex idx = ParseIndex::from_mmap(cache_path, source_meta);
  if (!idx.is_valid()) {
    return CacheLoadResult::fail(CacheError::Corrupted,
                                 "Cache file is corrupted or incomplete: " + cache_path);
  }

  return CacheLoadResult::ok(std::move(idx));
}

CacheWriteResult IndexCache::write_atomic_result(const std::string& path, const ParseIndex& index,
                                                 const std::string& source_path) {
  // Get source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    return CacheWriteResult::fail(CacheError::IoError,
                                  "Cannot read source file metadata: " + source_path);
  }

  // Check if target directory is writable
  size_t last_sep = path.find_last_of("/\\");
  std::string dir = (last_sep == std::string::npos) ? "." : path.substr(0, last_sep);
  if (!is_directory_writable(dir)) {
    return CacheWriteResult::fail(CacheError::PermissionDenied,
                                  "Cannot write to cache directory: " + dir);
  }

  // Delegate to ParseIndex::write which writes v3 format with atomic rename
  // Need to const_cast because write() is not const (but doesn't modify data)
  try {
    const_cast<ParseIndex&>(index).write(path, source_meta);
    return CacheWriteResult::ok();
  } catch (const std::exception& e) {
    std::string error_msg = e.what();

    // Classify error based on exception message content
    // Note: We only check the message text, not errno, because errno may be stale
    // after the exception propagates through multiple system calls
    if (error_msg.find("permission") != std::string::npos ||
        error_msg.find("Permission") != std::string::npos) {
      return CacheWriteResult::fail(CacheError::PermissionDenied,
                                    "Permission denied writing cache file: " + error_msg);
    }
    if (error_msg.find("space") != std::string::npos ||
        error_msg.find("disk") != std::string::npos) {
      return CacheWriteResult::fail(CacheError::DiskFull,
                                    "Disk full, cannot write cache file: " + error_msg);
    }

    return CacheWriteResult::fail(CacheError::IoError, "Failed to write cache file: " + error_msg);
  }
}

std::pair<uint64_t, uint64_t> IndexCache::get_source_metadata(const std::string& source_path) {
  struct stat st;
  if (stat(source_path.c_str(), &st) != 0) {
    return {0, 0};
  }

  // Only regular files have meaningful metadata for caching
  if (!S_ISREG(st.st_mode)) {
    return {0, 0};
  }

  return {static_cast<uint64_t>(st.st_mtime), static_cast<uint64_t>(st.st_size)};
}

bool IndexCache::is_directory_writable(const std::string& dir_path) {
  if (dir_path.empty()) {
    return false;
  }

  // Check if directory exists
  struct stat st;
  if (stat(dir_path.c_str(), &st) != 0) {
    return false;
  }

  if (!S_ISDIR(st.st_mode)) {
    return false;
  }

  // Check write access
  return access(dir_path.c_str(), W_OK) == 0;
}

std::string IndexCache::get_xdg_cache_dir() {
  std::string cache_dir;

  // Try XDG_CACHE_HOME first
  const char* xdg_cache = std::getenv("XDG_CACHE_HOME");
  if (xdg_cache && xdg_cache[0] != '\0') {
    cache_dir = xdg_cache;
  } else {
    // Fall back to ~/.cache
    const char* home = std::getenv("HOME");
#ifdef _WIN32
    if (!home) {
      home = std::getenv("USERPROFILE");
    }
#endif
    if (!home || home[0] == '\0') {
      return "";
    }
    cache_dir = std::string(home) + "/.cache";
  }

  // Append libvroom subdirectory
  cache_dir += "/libvroom";

  // Create directory if it doesn't exist
  struct stat st;
  if (stat(cache_dir.c_str(), &st) != 0) {
    // Directory doesn't exist, try to create it
#ifdef _WIN32
    // On Windows, create parent directories recursively
    std::string parent = cache_dir.substr(0, cache_dir.find_last_of("/\\"));
    if (stat(parent.c_str(), &st) != 0) {
      if (mkdir(parent.c_str(), 0755) != 0 && errno != EEXIST) {
        return "";
      }
    }
#else
    // On Unix, parent directory should exist if ~/.cache exists
    std::string parent = cache_dir.substr(0, cache_dir.find_last_of('/'));
    if (stat(parent.c_str(), &st) != 0) {
      // Create ~/.cache first
      if (mkdir(parent.c_str(), 0755) != 0 && errno != EEXIST) {
        return "";
      }
    }
#endif
    if (mkdir(cache_dir.c_str(), 0755) != 0 && errno != EEXIST) {
      return "";
    }
  }

  return cache_dir;
}

std::string IndexCache::hash_path(const std::string& path) {
  // Simple FNV-1a hash for path hashing
  // This is a well-known, fast hash suitable for filenames
  constexpr uint64_t FNV_PRIME = 0x100000001b3ULL;
  constexpr uint64_t FNV_OFFSET = 0xcbf29ce484222325ULL;

  uint64_t hash = FNV_OFFSET;
  for (char c : path) {
    hash ^= static_cast<uint8_t>(c);
    hash *= FNV_PRIME;
  }

  // Convert to hexadecimal string
  char buf[17]; // 16 hex digits + null terminator
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(hash));
  return std::string(buf);
}

IndexCache::LoadResult IndexCache::load(const std::string& cache_path,
                                        const std::string& source_path) {
  LoadResult result;

  // Check if cache file exists
  struct stat cache_st;
  if (stat(cache_path.c_str(), &cache_st) != 0) {
    // Cache file doesn't exist - not corruption, just a miss
    result.error_message = "Cache file does not exist";
    return result;
  }

  // Get source file metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    // Source file doesn't exist or can't be read - not corruption
    result.error_message = "Source file does not exist or cannot be read";
    return result;
  }

  // Check minimum file size for header
  // V3 header is 40 bytes minimum
  static constexpr size_t INDEX_V3_HEADER_SIZE = 40;
  if (static_cast<size_t>(cache_st.st_size) < INDEX_V3_HEADER_SIZE) {
    result.was_corrupted = true;
    result.error_message = "Cache file is too small to contain a valid header (expected at least " +
                           std::to_string(INDEX_V3_HEADER_SIZE) + " bytes, got " +
                           std::to_string(cache_st.st_size) + ")";

    // Delete corrupted cache file
    if (std::remove(cache_path.c_str()) == 0) {
      result.file_deleted = true;
    }
    return result;
  }

  // Open and read version byte to check magic/format
  std::FILE* fp = std::fopen(cache_path.c_str(), "rb");
  if (!fp) {
    result.error_message = "Failed to open cache file";
    return result;
  }

  uint8_t version = 0;
  if (std::fread(&version, 1, 1, fp) != 1) {
    std::fclose(fp);
    result.was_corrupted = true;
    result.error_message = "Failed to read version byte from cache file";
    if (std::remove(cache_path.c_str()) == 0) {
      result.file_deleted = true;
    }
    return result;
  }

  // Expected v3 format version
  static constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;
  if (version != INDEX_FORMAT_VERSION_V3) {
    std::fclose(fp);
    result.was_corrupted = true;
    result.error_message = "Invalid cache version byte (expected " +
                           std::to_string(INDEX_FORMAT_VERSION_V3) + ", got " +
                           std::to_string(version) + ")";
    if (std::remove(cache_path.c_str()) == 0) {
      result.file_deleted = true;
    }
    return result;
  }

  std::fclose(fp);

  // Version is correct, try to load the full index via mmap
  // ParseIndex::from_mmap performs additional validation (size bounds, overflow checks)
  result.index = ParseIndex::from_mmap(cache_path, source_meta);

  if (!result.index.is_valid()) {
    // Loading failed - this could be:
    // 1. Source file changed (stale cache) - mtime/size mismatch
    // 2. Truncated/corrupted index data

    // Re-read the header to determine if it's corruption vs staleness
    // We need to check if mtime/size match to distinguish
    fp = std::fopen(cache_path.c_str(), "rb");
    if (fp) {
      uint8_t header_buf[24]; // version(1) + padding(7) + mtime(8) + size(8)
      if (std::fread(header_buf, 1, 24, fp) == 24) {
        uint64_t cached_mtime;
        uint64_t cached_size;
        std::memcpy(&cached_mtime, header_buf + 8, sizeof(uint64_t));
        std::memcpy(&cached_size, header_buf + 16, sizeof(uint64_t));

        if (cached_mtime != source_meta.mtime || cached_size != source_meta.size) {
          // Stale cache - source file changed, not corruption
          std::fclose(fp);
          result.error_message = "Cache is stale (source file has changed)";
          return result;
        }
      }
      std::fclose(fp);
    }

    // If we get here, metadata matches but index load still failed
    // This means the index data itself is corrupted (truncated arrays, etc)
    result.was_corrupted = true;
    result.error_message = "Cache file is corrupted (truncated or invalid index data)";
    if (std::remove(cache_path.c_str()) == 0) {
      result.file_deleted = true;
    }
    return result;
  }

  // Success!
  return result;
}

std::string IndexCache::resolve_path(const std::string& path) {
  if (path.empty()) {
    return path;
  }

  try {
    std::filesystem::path fs_path(path);
    std::filesystem::path canonical = std::filesystem::canonical(fs_path);
    return canonical.string();
  } catch (const std::filesystem::filesystem_error&) {
    // Resolution failed (file doesn't exist, permission denied, etc.)
    // Gracefully fall back to the original path
    return path;
  }
}

} // namespace libvroom
