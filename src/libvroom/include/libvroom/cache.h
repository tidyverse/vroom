#pragma once

#include "elias_fano.h"
#include "types.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace libvroom {

// Cache file magic bytes: "VIDX"
static constexpr uint32_t VIDX_MAGIC = 0x58444956; // "VIDX" in little-endian
static constexpr uint8_t VIDX_VERSION = 1;
static constexpr size_t VIDX_HEADER_SIZE = 48;

// Configuration for index cache location and behavior
struct CacheConfig {
  enum Location { SAME_DIR, XDG_CACHE, CUSTOM };

  Location location = SAME_DIR;
  std::string custom_path;       // Only used when location == CUSTOM
  bool resolve_symlinks = true;  // Resolve symlinks before computing cache path
  uint16_t sample_interval = 32; // Every Kth row sampled (default 32)

  static CacheConfig defaults() { return CacheConfig{}; }

  static CacheConfig xdg_cache() {
    CacheConfig cfg;
    cfg.location = XDG_CACHE;
    return cfg;
  }

  static CacheConfig custom(const std::string& path) {
    CacheConfig cfg;
    cfg.location = CUSTOM;
    cfg.custom_path = path;
    return cfg;
  }
};

// Per-chunk analysis metadata (persisted Phase 1 output)
struct ChunkMeta {
  uint32_t row_count = 0;
  bool ends_inside_starting_outside = false;
};

// Complete cached index for a CSV file
struct CachedIndex {
  // Header fields
  uint64_t source_mtime = 0;
  uint64_t source_size = 0;
  size_t header_end_offset = 0;
  uint32_t num_columns = 0;
  uint64_t total_rows = 0;
  uint16_t sample_interval = 32;

  // Layer 0: Chunk metadata
  std::vector<std::pair<size_t, size_t>> chunk_boundaries; // (start, end) offsets
  std::vector<ChunkMeta> chunk_analysis;

  // Layer 1: Sampled row offsets (Elias-Fano encoded)
  EliasFano sampled_offsets;
  std::vector<uint8_t> sample_quote_states; // Packed bit array: 1 bit per sample

  // Schema
  std::vector<ColumnSchema> schema;
};

// Error types for cache operations
enum class CacheError { None, NotFound, Corrupted, VersionMismatch, SourceChanged, IoError };

// Result type for cache operations
struct CacheResult {
  CacheError error = CacheError::None;
  std::string message;
  CachedIndex index;

  bool ok() const { return error == CacheError::None; }

  static CacheResult success(CachedIndex idx) { return {CacheError::None, "", std::move(idx)}; }

  static CacheResult failure(CacheError err, const std::string& msg) { return {err, msg, {}}; }
};

// Index cache I/O and validation
class IndexCache {
public:
  // Compute the cache file path for a given source file
  static std::string compute_path(const std::string& source_path, const CacheConfig& cfg);

  // Load a cached index, validating against the source file
  static CacheResult load(const std::string& cache_path, const std::string& source_path);

  // Write a cached index atomically (temp file + rename)
  static bool write_atomic(const std::string& cache_path, const CachedIndex& index,
                           const std::string& source_path);

  // Check if the cached mtime/size match the current source file
  static bool is_fresh(const std::string& source_path, uint64_t cached_mtime, uint64_t cached_size);

  // Get the XDG cache directory (~/.cache/libvroom/)
  static std::string get_xdg_cache_dir();

  // Hash a file path to a short hex string (for XDG/CUSTOM cache filenames)
  static std::string hash_path(const std::string& path);

  // Check if a directory is writable
  static bool is_directory_writable(const std::string& dir);

private:
  // Serialize a CachedIndex to bytes
  static std::vector<uint8_t> serialize(const CachedIndex& index, const std::string& source_path);

  // Deserialize bytes to a CachedIndex
  static CacheResult deserialize(const uint8_t* data, size_t size);
};

} // namespace libvroom
