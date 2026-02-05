#include "libvroom/cache.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <sys/stat.h>
#include <unistd.h>

namespace libvroom {

namespace fs = std::filesystem;

// =============================================================================
// Path resolution
// =============================================================================

std::string IndexCache::compute_path(const std::string& source_path, const CacheConfig& cfg) {
  if (source_path.empty()) {
    return "";
  }

  std::string resolved = source_path;
  if (cfg.resolve_symlinks) {
    std::error_code ec;
    auto canonical = fs::canonical(source_path, ec);
    if (!ec) {
      resolved = canonical.string();
    }
  }

  switch (cfg.location) {
  case CacheConfig::SAME_DIR: {
    // Place .vidx next to the source file (using the accessed path, not resolved)
    std::string cache_path = source_path + ".vidx";

    // Check if directory is writable; fall back to XDG_CACHE if not
    fs::path parent = fs::path(source_path).parent_path();
    if (parent.empty())
      parent = ".";
    if (!is_directory_writable(parent.string())) {
      // Fall back to XDG_CACHE
      std::string xdg_dir = get_xdg_cache_dir();
      if (xdg_dir.empty())
        return "";
      return xdg_dir + "/" + hash_path(resolved) + ".vidx";
    }
    return cache_path;
  }

  case CacheConfig::XDG_CACHE: {
    std::string xdg_dir = get_xdg_cache_dir();
    if (xdg_dir.empty())
      return "";
    return xdg_dir + "/" + hash_path(resolved) + ".vidx";
  }

  case CacheConfig::CUSTOM: {
    if (cfg.custom_path.empty())
      return "";
    return cfg.custom_path + "/" + hash_path(resolved) + ".vidx";
  }
  }

  return "";
}

// =============================================================================
// Staleness detection
// =============================================================================

bool IndexCache::is_fresh(const std::string& source_path, uint64_t cached_mtime,
                          uint64_t cached_size) {
  struct stat st;
  if (stat(source_path.c_str(), &st) != 0) {
    return false;
  }

  auto current_mtime = static_cast<uint64_t>(st.st_mtime);
  auto current_size = static_cast<uint64_t>(st.st_size);

  return current_mtime == cached_mtime && current_size == cached_size;
}

// =============================================================================
// Load
// =============================================================================

CacheResult IndexCache::load(const std::string& cache_path, const std::string& source_path) {
  // Read cache file
  std::ifstream file(cache_path, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    return CacheResult::failure(CacheError::NotFound, "Cache file not found: " + cache_path);
  }

  auto file_size = static_cast<size_t>(file.tellg());
  if (file_size < VIDX_HEADER_SIZE) {
    return CacheResult::failure(CacheError::Corrupted, "Cache file too small");
  }

  std::vector<uint8_t> data(file_size);
  file.seekg(0);
  file.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(file_size));
  if (!file) {
    return CacheResult::failure(CacheError::IoError, "Failed to read cache file");
  }
  file.close();

  // Deserialize
  auto result = deserialize(data.data(), data.size());
  if (!result.ok()) {
    return result;
  }

  // Check freshness
  if (!is_fresh(source_path, result.index.source_mtime, result.index.source_size)) {
    return CacheResult::failure(CacheError::SourceChanged, "Source file has changed");
  }

  return result;
}

// =============================================================================
// Write (atomic: write to temp, then rename)
// =============================================================================

bool IndexCache::write_atomic(const std::string& cache_path, const CachedIndex& index,
                              const std::string& source_path) {
  if (cache_path.empty())
    return false;

  // Ensure parent directory exists
  fs::path parent = fs::path(cache_path).parent_path();
  if (!parent.empty()) {
    std::error_code ec;
    fs::create_directories(parent, ec);
    if (ec)
      return false;
  }

  auto data = serialize(index, source_path);
  if (data.empty())
    return false;

  // Write to temp file first (use PID + random to avoid races)
  std::string tmp_path = cache_path + ".tmp." + std::to_string(getpid()) + "." +
                         std::to_string(std::random_device{}());
  {
    std::ofstream tmp(tmp_path, std::ios::binary | std::ios::trunc);
    if (!tmp.is_open())
      return false;
    tmp.write(reinterpret_cast<const char*>(data.data()),
              static_cast<std::streamsize>(data.size()));
    if (!tmp)
      return false;
  }

  // Atomic rename
  std::error_code ec;
  fs::rename(tmp_path, cache_path, ec);
  if (ec) {
    fs::remove(tmp_path, ec); // Clean up temp file
    return false;
  }

  return true;
}

// =============================================================================
// Serialization
// =============================================================================

std::vector<uint8_t> IndexCache::serialize(const CachedIndex& index,
                                           const std::string& source_path) {
  // Get source file stats
  struct stat st;
  uint64_t mtime = index.source_mtime;
  uint64_t fsize = index.source_size;
  if (!source_path.empty() && stat(source_path.c_str(), &st) == 0) {
    mtime = static_cast<uint64_t>(st.st_mtime);
    fsize = static_cast<uint64_t>(st.st_size);
  }

  // Calculate total size
  uint32_t num_chunks = static_cast<uint32_t>(index.chunk_boundaries.size());

  // Section 1: Chunk boundaries (16 * num_chunks)
  size_t sect1_size = num_chunks * 16;
  // Section 2: Chunk analysis (5 * num_chunks)
  size_t sect2_size = num_chunks * 5;
  // Section 3: Elias-Fano sampled offsets
  size_t sect3_size = index.sampled_offsets.serialized_size();
  // Section 4: Sample quote states
  size_t num_samples = index.sampled_offsets.size();
  size_t sect4_size = (num_samples + 7) / 8;
  // Section 5: Schema
  size_t sect5_size = 0;
  for (const auto& col : index.schema) {
    sect5_size += 1 + 2 + col.name.size(); // type(1) + name_len(2) + name
  }

  size_t total_size =
      VIDX_HEADER_SIZE + sect1_size + sect2_size + sect3_size + sect4_size + sect5_size;
  std::vector<uint8_t> buf(total_size, 0);
  uint8_t* ptr = buf.data();

  // Header (48 bytes)
  uint32_t magic = VIDX_MAGIC;
  std::memcpy(ptr + 0, &magic, 4);
  ptr[4] = VIDX_VERSION;
  ptr[5] = 0; // flags
  uint16_t sample_interval = index.sample_interval;
  std::memcpy(ptr + 6, &sample_interval, 2);
  std::memcpy(ptr + 8, &mtime, 8);
  std::memcpy(ptr + 16, &fsize, 8);
  uint64_t header_end = index.header_end_offset;
  std::memcpy(ptr + 24, &header_end, 8);
  uint32_t ncols = index.num_columns;
  std::memcpy(ptr + 32, &ncols, 4);
  std::memcpy(ptr + 36, &num_chunks, 4);
  uint64_t total_rows = index.total_rows;
  std::memcpy(ptr + 40, &total_rows, 8);
  ptr += VIDX_HEADER_SIZE;

  // Section 1: Chunk boundaries
  for (uint32_t i = 0; i < num_chunks; ++i) {
    uint64_t start = index.chunk_boundaries[i].first;
    uint64_t end = index.chunk_boundaries[i].second;
    std::memcpy(ptr, &start, 8);
    ptr += 8;
    std::memcpy(ptr, &end, 8);
    ptr += 8;
  }

  // Section 2: Chunk analysis
  for (uint32_t i = 0; i < num_chunks; ++i) {
    uint32_t rc = index.chunk_analysis[i].row_count;
    std::memcpy(ptr, &rc, 4);
    ptr += 4;
    *ptr = index.chunk_analysis[i].ends_inside_starting_outside ? 1 : 0;
    ptr += 1;
  }

  // Section 3: Elias-Fano sampled offsets
  index.sampled_offsets.serialize(ptr);
  ptr += sect3_size;

  // Section 4: Sample quote states
  if (sect4_size > 0) {
    if (index.sample_quote_states.size() >= sect4_size) {
      std::memcpy(ptr, index.sample_quote_states.data(), sect4_size);
    }
    ptr += sect4_size;
  }

  // Section 5: Schema
  for (const auto& col : index.schema) {
    *ptr = static_cast<uint8_t>(col.type);
    ptr += 1;
    uint16_t name_len = static_cast<uint16_t>(col.name.size());
    std::memcpy(ptr, &name_len, 2);
    ptr += 2;
    if (name_len > 0) {
      std::memcpy(ptr, col.name.data(), name_len);
      ptr += name_len;
    }
  }

  return buf;
}

CacheResult IndexCache::deserialize(const uint8_t* data, size_t size) {
  if (size < VIDX_HEADER_SIZE) {
    return CacheResult::failure(CacheError::Corrupted, "File too small for header");
  }

  const uint8_t* ptr = data;

  // Read header
  uint32_t magic;
  std::memcpy(&magic, ptr, 4);
  if (magic != VIDX_MAGIC) {
    return CacheResult::failure(CacheError::Corrupted, "Bad magic number");
  }

  uint8_t version = ptr[4];
  if (version != VIDX_VERSION) {
    return CacheResult::failure(CacheError::VersionMismatch,
                                "Version mismatch: expected " + std::to_string(VIDX_VERSION) +
                                    ", got " + std::to_string(version));
  }

  CachedIndex index;
  std::memcpy(&index.sample_interval, ptr + 6, 2);
  std::memcpy(&index.source_mtime, ptr + 8, 8);
  std::memcpy(&index.source_size, ptr + 16, 8);
  uint64_t header_end;
  std::memcpy(&header_end, ptr + 24, 8);
  index.header_end_offset = static_cast<size_t>(header_end);
  std::memcpy(&index.num_columns, ptr + 32, 4);
  uint32_t num_chunks;
  std::memcpy(&num_chunks, ptr + 36, 4);
  std::memcpy(&index.total_rows, ptr + 40, 8);
  ptr += VIDX_HEADER_SIZE;
  size_t remaining = size - VIDX_HEADER_SIZE;

  // Section 1: Chunk boundaries
  size_t sect1_size = static_cast<size_t>(num_chunks) * 16;
  if (remaining < sect1_size) {
    return CacheResult::failure(CacheError::Corrupted, "Truncated chunk boundaries");
  }
  index.chunk_boundaries.resize(num_chunks);
  for (uint32_t i = 0; i < num_chunks; ++i) {
    uint64_t start, end;
    std::memcpy(&start, ptr, 8);
    ptr += 8;
    std::memcpy(&end, ptr, 8);
    ptr += 8;
    index.chunk_boundaries[i] = {static_cast<size_t>(start), static_cast<size_t>(end)};
  }
  remaining -= sect1_size;

  // Section 2: Chunk analysis
  size_t sect2_size = static_cast<size_t>(num_chunks) * 5;
  if (remaining < sect2_size) {
    return CacheResult::failure(CacheError::Corrupted, "Truncated chunk analysis");
  }
  index.chunk_analysis.resize(num_chunks);
  for (uint32_t i = 0; i < num_chunks; ++i) {
    std::memcpy(&index.chunk_analysis[i].row_count, ptr, 4);
    ptr += 4;
    index.chunk_analysis[i].ends_inside_starting_outside = (*ptr != 0);
    ptr += 1;
  }
  remaining -= sect2_size;

  // Section 3: Elias-Fano sampled offsets
  size_t ef_consumed = 0;
  index.sampled_offsets = EliasFano::deserialize(ptr, remaining, ef_consumed);
  if (ef_consumed == 0 && remaining > 0) {
    return CacheResult::failure(CacheError::Corrupted, "Failed to deserialize Elias-Fano data");
  }
  ptr += ef_consumed;
  remaining -= ef_consumed;

  // Section 4: Sample quote states
  size_t num_samples = index.sampled_offsets.size();
  size_t sect4_size = (num_samples + 7) / 8;
  if (remaining < sect4_size) {
    return CacheResult::failure(CacheError::Corrupted, "Truncated sample quote states");
  }
  if (sect4_size > 0) {
    index.sample_quote_states.resize(sect4_size);
    std::memcpy(index.sample_quote_states.data(), ptr, sect4_size);
    ptr += sect4_size;
    remaining -= sect4_size;
  }

  // Section 5: Schema
  index.schema.clear();
  for (uint32_t i = 0; i < index.num_columns && remaining >= 3; ++i) {
    ColumnSchema col;
    col.type = static_cast<DataType>(*ptr);
    ptr += 1;
    uint16_t name_len;
    std::memcpy(&name_len, ptr, 2);
    ptr += 2;
    remaining -= 3;

    if (remaining < name_len) {
      return CacheResult::failure(CacheError::Corrupted, "Truncated schema");
    }
    col.name = std::string(reinterpret_cast<const char*>(ptr), name_len);
    ptr += name_len;
    remaining -= name_len;
    col.index = i;
    index.schema.push_back(std::move(col));
  }

  return CacheResult::success(std::move(index));
}

// =============================================================================
// Utility functions
// =============================================================================

std::string IndexCache::get_xdg_cache_dir() {
  std::string cache_home;

  const char* xdg = std::getenv("XDG_CACHE_HOME");
  if (xdg && xdg[0] != '\0') {
    cache_home = xdg;
  } else {
    const char* home = std::getenv("HOME");
    if (!home || home[0] == '\0')
      return "";
    cache_home = std::string(home) + "/.cache";
  }

  std::string dir = cache_home + "/libvroom";

  // Create directory if it doesn't exist
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec)
    return "";

  return dir;
}

std::string IndexCache::hash_path(const std::string& path) {
  // Simple FNV-1a hash
  uint64_t hash = 14695981039346656037ULL;
  for (char c : path) {
    hash ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
    hash *= 1099511628211ULL;
  }

  // Convert to hex string
  char buf[17];
  snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(hash));
  return std::string(buf);
}

bool IndexCache::is_directory_writable(const std::string& dir) {
  std::error_code ec;
  auto status = fs::status(dir, ec);
  if (ec)
    return false;

  if (!fs::is_directory(status))
    return false;

  // Try to actually create a temp file to check writability
  // Use PID + random suffix to avoid races with concurrent processes
  std::string test_path = dir + "/.vidx_write_test_" + std::to_string(getpid()) + "_" +
                          std::to_string(std::random_device{}());
  std::ofstream test(test_path, std::ios::binary);
  if (test.is_open()) {
    test.close();
    fs::remove(test_path, ec);
    return true;
  }
  return false;
}

} // namespace libvroom
