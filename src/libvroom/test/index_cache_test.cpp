/**
 * @file index_cache_test.cpp
 * @brief Unit tests for index cache management utilities.
 */

#include "libvroom.h"

#include "index_cache.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace libvroom;

// =============================================================================
// Test Fixture
// =============================================================================

class IndexCacheTest : public ::testing::Test {
protected:
  std::string temp_dir;
  std::vector<std::string> temp_files;

  void SetUp() override {
    // Create temp directory for test files
    temp_dir =
        (fs::temp_directory_path() / ("index_cache_test_" + std::to_string(getpid()))).string();
    fs::create_directories(temp_dir);
  }

  void TearDown() override {
    // Clean up temp files and directory
    if (fs::exists(temp_dir)) {
      fs::remove_all(temp_dir);
    }
  }

  std::string createTempFile(const std::string& filename, const std::string& content) {
    std::string path = temp_dir + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
    // Explicit flush before close to ensure data is visible to subsequent readers.
    // This is important on macOS where aggressive caching can cause race conditions
    // between file writes and subsequent reads from a different file handle.
    file.flush();
    file.close();
    return path;
  }

  std::string createTempDir(const std::string& dirname) {
    std::string path = temp_dir + "/" + dirname;
    fs::create_directories(path);
    return path;
  }

  std::string createTempFileInDir(const std::string& dir, const std::string& filename,
                                  const std::string& content) {
    std::string path = dir + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
    file.flush();
    file.close();
    return path;
  }
};

// =============================================================================
// CacheConfig Tests
// =============================================================================

TEST_F(IndexCacheTest, CacheConfig_Defaults) {
  CacheConfig config = CacheConfig::defaults();
  EXPECT_EQ(config.location, CacheConfig::SAME_DIR);
  EXPECT_TRUE(config.custom_path.empty());
}

TEST_F(IndexCacheTest, CacheConfig_XdgCache) {
  CacheConfig config = CacheConfig::xdg_cache();
  EXPECT_EQ(config.location, CacheConfig::XDG_CACHE);
  EXPECT_TRUE(config.custom_path.empty());
}

TEST_F(IndexCacheTest, CacheConfig_Custom) {
  CacheConfig config = CacheConfig::custom("/custom/path");
  EXPECT_EQ(config.location, CacheConfig::CUSTOM);
  EXPECT_EQ(config.custom_path, "/custom/path");
}

// =============================================================================
// IndexCache::compute_path Tests
// =============================================================================

TEST_F(IndexCacheTest, ComputePath_SameDir) {
  std::string source = "/path/to/data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "/path/to/data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_SameDir_NoPath) {
  std::string source = "data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_SameDir_Windows) {
  std::string source = "C:\\Users\\data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "C:\\Users\\data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_XdgCache) {
  std::string source = temp_dir + "/data.csv";
  createTempFile("data.csv", "a,b\n1,2\n");

  std::string cache_path = IndexCache::compute_path(source, CacheConfig::xdg_cache());

  // Should contain the XDG cache directory
  EXPECT_TRUE(cache_path.find(".cache/libvroom") != std::string::npos ||
              cache_path.find("libvroom") != std::string::npos);
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);
}

TEST_F(IndexCacheTest, ComputePath_XdgCache_DifferentFilesGetDifferentPaths) {
  std::string source1 = "/path/to/file1.csv";
  std::string source2 = "/path/to/file2.csv";

  std::string cache1 = IndexCache::compute_path(source1, CacheConfig::xdg_cache());
  std::string cache2 = IndexCache::compute_path(source2, CacheConfig::xdg_cache());

  EXPECT_NE(cache1, cache2);
}

TEST_F(IndexCacheTest, ComputePath_Custom) {
  std::string custom_dir = createTempDir("custom_cache");
  std::string source = "/path/to/data.csv";

  CacheConfig config = CacheConfig::custom(custom_dir);
  std::string cache_path = IndexCache::compute_path(source, config);

  EXPECT_TRUE(cache_path.find(custom_dir) != std::string::npos);
  EXPECT_TRUE(cache_path.find("data.csv.vidx") != std::string::npos);
}

TEST_F(IndexCacheTest, ComputePath_Custom_EmptyPath) {
  CacheConfig config = CacheConfig::custom("");
  std::string source = "/path/to/data.csv";

  std::string cache_path = IndexCache::compute_path(source, config);

  // Should fall back to same-dir mode
  EXPECT_EQ(cache_path, "/path/to/data.csv.vidx");
}

// =============================================================================
// IndexCache::get_source_metadata Tests
// =============================================================================

TEST_F(IndexCacheTest, GetSourceMetadata_ValidFile) {
  std::string content = "hello,world\n1,2,3\n";
  std::string path = createTempFile("meta_test.csv", content);

  auto [mtime, size] = IndexCache::get_source_metadata(path);

  EXPECT_GT(mtime, 0u);
  EXPECT_EQ(size, content.size());
}

TEST_F(IndexCacheTest, GetSourceMetadata_NonexistentFile) {
  auto [mtime, size] = IndexCache::get_source_metadata("/nonexistent/file.csv");

  EXPECT_EQ(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

TEST_F(IndexCacheTest, GetSourceMetadata_Directory) {
  std::string dir = createTempDir("not_a_file");

  auto [mtime, size] = IndexCache::get_source_metadata(dir);

  // Directories should not have valid metadata for caching
  EXPECT_EQ(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

TEST_F(IndexCacheTest, GetSourceMetadata_EmptyFile) {
  std::string path = createTempFile("empty.csv", "");

  auto [mtime, size] = IndexCache::get_source_metadata(path);

  EXPECT_GT(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

// =============================================================================
// IndexCache::is_directory_writable Tests
// =============================================================================

TEST_F(IndexCacheTest, IsDirectoryWritable_WritableDir) {
  std::string dir = createTempDir("writable");

  EXPECT_TRUE(IndexCache::is_directory_writable(dir));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_NonexistentDir) {
  EXPECT_FALSE(IndexCache::is_directory_writable("/nonexistent/directory"));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_EmptyPath) {
  EXPECT_FALSE(IndexCache::is_directory_writable(""));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_FileNotDir) {
  std::string path = createTempFile("not_a_dir.txt", "content");

  EXPECT_FALSE(IndexCache::is_directory_writable(path));
}

#ifndef _WIN32
TEST_F(IndexCacheTest, IsDirectoryWritable_ReadOnlyDir) {
  std::string dir = createTempDir("readonly");

  // Make directory read-only
  chmod(dir.c_str(), 0555);

  EXPECT_FALSE(IndexCache::is_directory_writable(dir));

  // Restore permissions for cleanup
  chmod(dir.c_str(), 0755);
}
#endif

// =============================================================================
// IndexCache::hash_path Tests
// =============================================================================

TEST_F(IndexCacheTest, HashPath_Basic) {
  std::string hash = IndexCache::hash_path("/path/to/file.csv");

  EXPECT_EQ(hash.length(), 16u); // 64-bit hash = 16 hex chars
}

TEST_F(IndexCacheTest, HashPath_DifferentPathsDifferentHashes) {
  std::string hash1 = IndexCache::hash_path("/path/to/file1.csv");
  std::string hash2 = IndexCache::hash_path("/path/to/file2.csv");

  EXPECT_NE(hash1, hash2);
}

TEST_F(IndexCacheTest, HashPath_SamePathSameHash) {
  std::string hash1 = IndexCache::hash_path("/path/to/file.csv");
  std::string hash2 = IndexCache::hash_path("/path/to/file.csv");

  EXPECT_EQ(hash1, hash2);
}

TEST_F(IndexCacheTest, HashPath_EmptyPath) {
  std::string hash = IndexCache::hash_path("");

  EXPECT_EQ(hash.length(), 16u);
}

TEST_F(IndexCacheTest, HashPath_OnlyHexChars) {
  std::string hash = IndexCache::hash_path("/some/path");

  for (char c : hash) {
    EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }
}

// =============================================================================
// IndexCache::get_xdg_cache_dir Tests
// =============================================================================

TEST_F(IndexCacheTest, GetXdgCacheDir_ReturnsNonEmpty) {
  std::string cache_dir = IndexCache::get_xdg_cache_dir();

  // Should return a valid path (assuming HOME is set)
  if (!cache_dir.empty()) {
    EXPECT_TRUE(cache_dir.find("libvroom") != std::string::npos);
    EXPECT_TRUE(fs::exists(cache_dir) || fs::create_directories(cache_dir));
  }
}

// =============================================================================
// IndexCache::write_atomic Tests
// =============================================================================

TEST_F(IndexCacheTest, WriteAtomic_BasicWrite) {
  // Create a source file
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string source_path = createTempFile("source.csv", content);
  std::string cache_path = temp_dir + "/source.csv.vidx";

  // Parse the file to get a ParseIndex
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  ASSERT_TRUE(result.success());

  // Write the cache
  bool success = IndexCache::write_atomic(cache_path, result.idx, source_path);
  EXPECT_TRUE(success);

  // Verify cache file exists
  EXPECT_TRUE(fs::exists(cache_path));

  // Verify cache file has content
  EXPECT_GT(fs::file_size(cache_path), IndexCache::HEADER_SIZE);
}

TEST_F(IndexCacheTest, WriteAtomic_AtomicNoPartialWrites) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("atomic.csv", content);
  std::string cache_path = temp_dir + "/atomic.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write the cache
  IndexCache::write_atomic(cache_path, result.idx, source_path);

  // No temp files should remain
  for (const auto& entry : fs::directory_iterator(temp_dir)) {
    std::string filename = entry.path().filename().string();
    EXPECT_TRUE(filename.find(".tmp.") == std::string::npos)
        << "Temp file should be cleaned up: " << filename;
  }
}

TEST_F(IndexCacheTest, WriteAtomic_NonexistentSource) {
  ParseIndex empty_idx;
  std::string cache_path = temp_dir + "/cache.vidx";

  bool success = IndexCache::write_atomic(cache_path, empty_idx, "/nonexistent/source.csv");

  EXPECT_FALSE(success);
  EXPECT_FALSE(fs::exists(cache_path));
}

TEST_F(IndexCacheTest, WriteAtomic_InvalidCachePath) {
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("source.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Try to write to a nonexistent directory
  std::string cache_path = "/nonexistent/dir/cache.vidx";
  bool success = IndexCache::write_atomic(cache_path, result.idx, source_path);

  EXPECT_FALSE(success);
}

// =============================================================================
// IndexCache::is_valid Tests
// =============================================================================

TEST_F(IndexCacheTest, IsValid_ValidCache) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("valid.csv", content);
  std::string cache_path = temp_dir + "/valid.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Cache should be valid
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_InvalidAfterModification) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("modified.csv", content);
  std::string cache_path = temp_dir + "/modified.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Modify source file (change content and wait for different mtime)
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* content = "a,b,c\n1,2,3\n4,5,6\n";
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  // Cache should now be invalid
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_NonexistentCache) {
  std::string source_path = createTempFile("nocache.csv", "a,b\n");

  EXPECT_FALSE(IndexCache::is_valid(source_path, "/nonexistent/cache.vidx"));
}

TEST_F(IndexCacheTest, IsValid_NonexistentSource) {
  std::string cache_path = createTempFile("orphan.vidx", "dummy content");

  EXPECT_FALSE(IndexCache::is_valid("/nonexistent/source.csv", cache_path));
}

TEST_F(IndexCacheTest, IsValid_CorruptedHeader) {
  std::string source_path = createTempFile("corrupt_source.csv", "a,b\n1,2\n");
  std::string cache_path = createTempFile("corrupt.vidx", "not a valid cache file");

  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_WrongVersion) {
  std::string source_path = createTempFile("version.csv", "a,b\n");

  // Create a cache file with wrong version
  std::string cache_path = temp_dir + "/version.vidx";
  std::ofstream file(cache_path, std::ios::binary);
  uint8_t wrong_version = 255; // Invalid version
  file.write(reinterpret_cast<char*>(&wrong_version), 1);
  file.close();

  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

// =============================================================================
// IndexCache::try_compute_writable_path Tests
// =============================================================================

TEST_F(IndexCacheTest, TryComputeWritablePath_WritableDir) {
  std::string source_path = createTempFile("writable.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::defaults());

  EXPECT_TRUE(success);
  EXPECT_EQ(cache_path, source_path + ".vidx");
}

TEST_F(IndexCacheTest, TryComputeWritablePath_XdgCache) {
  std::string source_path = temp_dir + "/xdg.csv";
  createTempFile("xdg.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::xdg_cache());

  if (!IndexCache::get_xdg_cache_dir().empty()) {
    EXPECT_TRUE(success);
    EXPECT_TRUE(cache_path.find("libvroom") != std::string::npos);
  }
}

TEST_F(IndexCacheTest, TryComputeWritablePath_CustomWritable) {
  std::string custom_dir = createTempDir("custom_writable");
  std::string source_path = temp_dir + "/custom.csv";
  createTempFile("custom.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::custom(custom_dir));

  EXPECT_TRUE(success);
  EXPECT_TRUE(cache_path.find(custom_dir) != std::string::npos);
}

TEST_F(IndexCacheTest, TryComputeWritablePath_CustomNonexistent) {
  std::string source_path = temp_dir + "/source.csv";

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::custom("/nonexistent/dir"));

  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST_F(IndexCacheTest, Integration_FullCacheWorkflow) {
  // Create source file
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n";
  std::string source_path = createTempFile("workflow.csv", content);
  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

  // Initial state: no cache
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));

  // Parse and write cache
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(result.success());

  EXPECT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Cache should now be valid
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));

  // Verify cache file structure (v3 format with alignment padding)
  // Layout: version(1) + padding(7) + mtime(8) + size(8)
  std::ifstream cache_file(cache_path, std::ios::binary);
  uint8_t version;
  cache_file.read(reinterpret_cast<char*>(&version), 1);
  constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;
  EXPECT_EQ(version, INDEX_FORMAT_VERSION_V3);

  // Skip 7 bytes of alignment padding
  char padding[7];
  cache_file.read(padding, 7);

  uint64_t mtime, size;
  cache_file.read(reinterpret_cast<char*>(&mtime), 8);
  cache_file.read(reinterpret_cast<char*>(&size), 8);

  auto [actual_mtime, actual_size] = IndexCache::get_source_metadata(source_path);
  EXPECT_EQ(mtime, actual_mtime);
  EXPECT_EQ(size, actual_size);
}

TEST_F(IndexCacheTest, Integration_MultipleFiles) {
  // Create multiple source files
  std::vector<std::string> source_paths;
  for (int i = 0; i < 3; ++i) {
    std::string content = "col" + std::to_string(i) + "\n" + std::to_string(i * 10) + "\n";
    source_paths.push_back(createTempFile("multi" + std::to_string(i) + ".csv", content));
  }

  // Parse and cache each file
  libvroom::Parser parser;
  for (const auto& source_path : source_paths) {
    std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(result.success());

    EXPECT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
    EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
  }
}

TEST_F(IndexCacheTest, Integration_CacheOverwrite) {
  std::string content1 = "a\n1\n";
  std::string source_path = createTempFile("overwrite.csv", content1);
  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

  libvroom::Parser parser;

  // Write first cache
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
  }

  size_t first_cache_size = fs::file_size(cache_path);

  // Modify source and wait for mtime change
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* content = "a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n";
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  // Cache should be invalid now
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));

  // Write new cache (overwrites old)
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
  }

  // New cache should be valid and larger
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
  EXPECT_GT(fs::file_size(cache_path), first_cache_size);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(IndexCacheTest, EdgeCase_VeryLongPath) {
  std::string long_name(200, 'x');
  std::string source_path = temp_dir + "/" + long_name + ".csv";

  // Create the file (may fail on some filesystems)
  std::ofstream file(source_path, std::ios::binary);
  if (!file.is_open()) {
    GTEST_SKIP() << "Filesystem doesn't support long filenames";
  }
  const char* content = "a,b\n1,2\n";
  file.write(content, static_cast<std::streamsize>(strlen(content)));
  file.flush();
  file.close();

  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);

  // XDG cache should use hash (fixed length)
  std::string xdg_path = IndexCache::compute_path(source_path, CacheConfig::xdg_cache());
  // Hash is 16 chars, so XDG path should be shorter than same-dir path
  if (!IndexCache::get_xdg_cache_dir().empty()) {
    EXPECT_LT(xdg_path.length(), cache_path.length());
  }
}

TEST_F(IndexCacheTest, EdgeCase_SpecialCharactersInPath) {
  // Test with path containing special characters
  std::string hash1 = IndexCache::hash_path("/path/with spaces/file.csv");
  std::string hash2 = IndexCache::hash_path("/path/with-dashes/file.csv");
  std::string hash3 = IndexCache::hash_path("/path/with_underscores/file.csv");

  EXPECT_EQ(hash1.length(), 16u);
  EXPECT_EQ(hash2.length(), 16u);
  EXPECT_EQ(hash3.length(), 16u);

  // All should be different
  EXPECT_NE(hash1, hash2);
  EXPECT_NE(hash2, hash3);
  EXPECT_NE(hash1, hash3);
}

TEST_F(IndexCacheTest, EdgeCase_UnicodeInPath) {
  std::string hash = IndexCache::hash_path("/path/to/日本語.csv");

  EXPECT_EQ(hash.length(), 16u);

  // Should be consistent
  std::string hash2 = IndexCache::hash_path("/path/to/日本語.csv");
  EXPECT_EQ(hash, hash2);
}

TEST_F(IndexCacheTest, EdgeCase_EmptyIndex) {
  std::string content = "";
  std::string source_path = createTempFile("empty.csv", content);
  std::string cache_path = temp_dir + "/empty.csv.vidx";

  // Create an empty ParseIndex
  ParseIndex empty_idx;
  empty_idx.columns = 0;
  empty_idx.n_threads = 0;
  empty_idx.n_indexes = nullptr;
  empty_idx.indexes = nullptr;

  // Should still write (header only)
  bool success = IndexCache::write_atomic(cache_path, empty_idx, source_path);

  // This might fail because source has size 0 - that's ok
  if (success) {
    EXPECT_TRUE(fs::exists(cache_path));
  }
}

// =============================================================================
// Parser API Integration Tests
// =============================================================================

TEST_F(IndexCacheTest, ParserApi_WithCacheFactory) {
  // Test ParseOptions::with_cache factory method
  std::string content = "name,age\nAlice,30\nBob,25\n";
  std::string source_path = createTempFile("api_factory.csv", content);

  auto opts = libvroom::ParseOptions::with_cache(source_path);

  EXPECT_TRUE(opts.cache.has_value());
  EXPECT_EQ(opts.cache->location, CacheConfig::SAME_DIR);
  EXPECT_EQ(opts.source_path, source_path);
}

TEST_F(IndexCacheTest, ParserApi_WithCacheDirFactory) {
  // Test ParseOptions::with_cache_dir factory method
  std::string custom_dir = createTempDir("custom_cache_api");
  std::string source_path = "/path/to/file.csv";

  auto opts = libvroom::ParseOptions::with_cache_dir(source_path, custom_dir);

  EXPECT_TRUE(opts.cache.has_value());
  EXPECT_EQ(opts.cache->location, CacheConfig::CUSTOM);
  EXPECT_EQ(opts.cache->custom_path, custom_dir);
  EXPECT_EQ(opts.source_path, source_path);
}

TEST_F(IndexCacheTest, ParserApi_CacheMissWritesFile) {
  // First parse should write cache file
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
  std::string source_path = createTempFile("cache_miss.csv", content);
  std::string expected_cache_path = source_path + ".vidx";

  // Ensure no cache exists
  ASSERT_FALSE(fs::exists(expected_cache_path));

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  auto opts = libvroom::ParseOptions::with_cache(source_path);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache); // Cache miss
  EXPECT_FALSE(result.cache_path.empty());
  EXPECT_TRUE(fs::exists(result.cache_path)); // Cache was written
}

TEST_F(IndexCacheTest, ParserApi_CacheHitLoadsMmap) {
  // First parse creates cache, second parse should load from cache
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
  std::string source_path = createTempFile("cache_hit.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - cache miss
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_FALSE(result1.used_cache);
  EXPECT_TRUE(fs::exists(result1.cache_path));

  // Second parse - cache hit
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache); // Cache hit!
  EXPECT_EQ(result2.cache_path, result1.cache_path);
}

TEST_F(IndexCacheTest, ParserApi_CacheResultsCorrect) {
  // Verify that cached results produce correct data access
  std::string content = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
  std::string source_path = createTempFile("cache_verify.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - cache miss
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_EQ(result1.num_rows(), 3);

  // Second parse - cache hit
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache);
  EXPECT_EQ(result2.num_rows(), 3); // Same row count

  // Verify we can still access data
  auto names = result2.column_string(0);
  EXPECT_EQ(names.size(), 3u);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
  EXPECT_EQ(names[2], "Charlie");
}

TEST_F(IndexCacheTest, ParserApi_NoCacheByDefault) {
  // Parsing without cache options should not create cache
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("no_cache.csv", content);
  std::string cache_path = source_path + ".vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Parse without cache options
  auto result = parser.parse(buffer.data(), buffer.size);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.empty()); // No cache path set
  EXPECT_FALSE(fs::exists(cache_path));   // No cache file created
}

TEST_F(IndexCacheTest, ParserApi_ForceCacheRefresh) {
  // Test force_cache_refresh option
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("force_refresh.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - creates cache
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_FALSE(result1.used_cache);

  // Get cache file mtime
  auto cache_path = result1.cache_path;
  auto mtime1 = fs::last_write_time(cache_path);

  // Wait a bit to ensure different mtime
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Second parse with force_cache_refresh
  opts.force_cache_refresh = true;
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_FALSE(result2.used_cache); // Force refresh means cache miss

  // Cache file should have been rewritten (newer mtime)
  auto mtime2 = fs::last_write_time(cache_path);
  EXPECT_GE(mtime2, mtime1);
}

TEST_F(IndexCacheTest, ParserApi_CacheInvalidAfterSourceChange) {
  // Cache should become invalid if source file changes
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("change.csv", content);

  libvroom::Parser parser;
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - creates cache
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    ASSERT_TRUE(result.success());
    EXPECT_FALSE(result.used_cache);
  }

  // Modify source file and wait for mtime change
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* content = "a,b,c\n1,2,3\n4,5,6\n";
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  // Second parse - cache should be invalid
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    EXPECT_TRUE(result.success());
    EXPECT_FALSE(result.used_cache); // Cache was stale
    EXPECT_EQ(result.num_rows(), 2); // New content has 2 data rows
  }
}

TEST_F(IndexCacheTest, ParserApi_CustomCacheDir) {
  // Test caching to custom directory
  std::string custom_dir = createTempDir("custom_api_dir");
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("custom_dir.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache_dir(source_path, custom_dir);

  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.find(custom_dir) != std::string::npos);
  EXPECT_TRUE(fs::exists(result.cache_path));
}

TEST_F(IndexCacheTest, ParserApi_EmptySourcePathDisablesCache) {
  // If source_path is empty, caching should be silently disabled
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("empty_source.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Create options with cache but empty source_path
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.source_path = ""; // Empty - caching disabled

  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.empty()); // No cache due to empty source_path
}

TEST_F(IndexCacheTest, ParserApi_DialectDetectionWithCache) {
  // Auto-detected dialect should work with cached results
  std::string content = "name\tage\nAlice\t30\nBob\t25\n"; // TSV format
  std::string source_path = createTempFile("tsv_cache.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Parse with cache and auto-detection
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.source_path = source_path;
  // dialect is nullopt (default) - auto-detect

  // First parse
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_EQ(result1.dialect.delimiter, '\t'); // Detected TSV

  // Second parse - cache hit, dialect should still be detected
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache);
  EXPECT_EQ(result2.dialect.delimiter, '\t'); // Still TSV
}

// =============================================================================
// Warning Callback Tests
// =============================================================================

TEST_F(IndexCacheTest, WarningCallback_NotCalledOnSuccess) {
  // Warning callback should NOT be called when everything works normally
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("no_warn.csv", content);

  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::defaults();
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_TRUE(success);
  EXPECT_FALSE(cache_path.empty());
  EXPECT_TRUE(warnings.empty()); // No warnings for successful operation
}

TEST_F(IndexCacheTest, WarningCallback_FallbackToXdg) {
  // Warning should be emitted when falling back from SAME_DIR to XDG_CACHE
  std::string ro_dir = createTempDir("readonly_dir");

  // Create a file in read-only directory
  std::string source_path = ro_dir + "/data.csv";
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* content = "a,b\n1,2\n";
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  // Make directory read-only
  chmod(ro_dir.c_str(), 0555);

  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::defaults();
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  // Restore permissions for cleanup
  chmod(ro_dir.c_str(), 0755);

  // Should fall back to XDG cache and emit a warning
  if (success) {
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_TRUE(warnings[0].find("falling back to XDG cache") != std::string::npos);
    EXPECT_TRUE(cache_path.find(".cache/libvroom") != std::string::npos);
  } else {
    // If XDG is also not writable, we get a different warning
    ASSERT_GE(warnings.size(), 1u);
    EXPECT_TRUE(warnings[0].find("not writable") != std::string::npos);
  }
}

TEST_F(IndexCacheTest, WarningCallback_CustomDirNotWritable) {
  // Warning should be emitted when custom directory is not writable
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("/nonexistent/directory");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
  ASSERT_EQ(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("not writable") != std::string::npos);
  EXPECT_TRUE(warnings[0].find("/nonexistent/directory") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_EmptyCustomPath) {
  // Warning should be emitted when custom path is empty
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_FALSE(success);
  ASSERT_EQ(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("empty") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_NullCallbackDoesNotCrash) {
  // Null callback should not cause issues
  CacheConfig config = CacheConfig::custom("/nonexistent/directory");
  // warning_callback is not set (nullptr by default)

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  // Should complete without crash, just no warnings emitted
  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
}

TEST_F(IndexCacheTest, WarningCallback_ParserApi_CacheWriteFailure) {
  // Test that warning is emitted when cache write fails
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("write_fail.csv", content);
  std::string ro_dir = createTempDir("readonly_cache");

  // Make directory read-only after creation
  chmod(ro_dir.c_str(), 0555);

  std::vector<std::string> warnings;
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::custom(ro_dir);
  opts.cache->warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };
  opts.source_path = source_path;

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  // Restore permissions for cleanup
  chmod(ro_dir.c_str(), 0755);

  // Parsing should succeed
  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);

  // Warning should be emitted about cache write failure or directory not writable
  ASSERT_GE(warnings.size(), 1u);
  // Either "not writable" (path computation) or "Failed to write" (write attempt)
  bool has_relevant_warning = false;
  for (const auto& w : warnings) {
    if (w.find("not writable") != std::string::npos ||
        w.find("Failed to write") != std::string::npos) {
      has_relevant_warning = true;
      break;
    }
  }
  EXPECT_TRUE(has_relevant_warning);
}

TEST_F(IndexCacheTest, WarningCallback_ParserApi_InvalidCache) {
  // Test that warning is emitted when cache is invalid
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("invalid_cache.csv", content);
  std::string cache_path = source_path + ".vidx";

  // Create an invalid cache file (wrong version or corrupted)
  {
    std::ofstream cache_file(cache_path, std::ios::binary);
    const char* content_data = "invalid_cache_data";
    cache_file.write(content_data, static_cast<std::streamsize>(strlen(content_data)));
    cache_file.flush();
    cache_file.close();
  }

  std::vector<std::string> warnings;
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.cache->warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };
  opts.source_path = source_path;

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  // Parsing should succeed (re-parsing after cache miss)
  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);

  // Warning should be emitted about invalid cache
  ASSERT_GE(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("invalid") != std::string::npos ||
              warnings[0].find("version mismatch") != std::string::npos ||
              warnings[0].find("corruption") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_MultipleWarnings) {
  // Test that multiple warnings can be collected
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("/nonexistent/path1");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  // This should emit a warning
  auto [path1, success1] = IndexCache::try_compute_writable_path("/test1.csv", config);
  EXPECT_FALSE(success1);

  // Update config and try again
  config.custom_path = "/nonexistent/path2";
  auto [path2, success2] = IndexCache::try_compute_writable_path("/test2.csv", config);
  EXPECT_FALSE(success2);

  // Both warnings should be collected
  EXPECT_EQ(warnings.size(), 2u);
}

// =============================================================================
// CacheError Enum Tests
// =============================================================================

TEST(CacheErrorTest, CacheErrorToString) {
  EXPECT_STREQ(cache_error_to_string(CacheError::None), "None");
  EXPECT_STREQ(cache_error_to_string(CacheError::Corrupted), "Corrupted");
  EXPECT_STREQ(cache_error_to_string(CacheError::PermissionDenied), "PermissionDenied");
  EXPECT_STREQ(cache_error_to_string(CacheError::DiskFull), "DiskFull");
  EXPECT_STREQ(cache_error_to_string(CacheError::VersionMismatch), "VersionMismatch");
  EXPECT_STREQ(cache_error_to_string(CacheError::SourceChanged), "SourceChanged");
  EXPECT_STREQ(cache_error_to_string(CacheError::IoError), "IoError");
  EXPECT_STREQ(cache_error_to_string(CacheError::NotFound), "NotFound");
}

// =============================================================================
// CacheLoadResult Tests
// =============================================================================

TEST(CacheLoadResultTest, DefaultConstructor) {
  CacheLoadResult result;
  EXPECT_FALSE(result.success());
  EXPECT_FALSE(result.has_index());
  EXPECT_EQ(result.error, CacheError::NotFound);
}

TEST(CacheLoadResultTest, FailFactory) {
  auto result = CacheLoadResult::fail(CacheError::Corrupted, "Test error message");
  EXPECT_FALSE(result.success());
  EXPECT_FALSE(result.has_index());
  EXPECT_EQ(result.error, CacheError::Corrupted);
  EXPECT_EQ(result.message, "Test error message");
}

TEST(CacheLoadResultTest, OkFactory) {
  ParseIndex idx;
  idx.columns = 5;
  idx.n_threads = 1;

  auto result = CacheLoadResult::ok(std::move(idx));
  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.has_index());
  EXPECT_EQ(result.error, CacheError::None);
  EXPECT_EQ(result.index->columns, 5u);
}

TEST(CacheLoadResultTest, MoveSemantics) {
  ParseIndex idx;
  idx.columns = 10;

  auto result1 = CacheLoadResult::ok(std::move(idx));
  EXPECT_TRUE(result1.success());

  auto result2 = std::move(result1);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.has_index());
  EXPECT_EQ(result2.index->columns, 10u);
}

// =============================================================================
// CacheWriteResult Tests
// =============================================================================

TEST(CacheWriteResultTest, OkFactory) {
  auto result = CacheWriteResult::ok();
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.error, CacheError::None);
  EXPECT_EQ(result.message, "Cache written successfully");
}

TEST(CacheWriteResultTest, FailFactory) {
  auto result = CacheWriteResult::fail(CacheError::DiskFull, "No space left on device");
  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::DiskFull);
  EXPECT_EQ(result.message, "No space left on device");
}

TEST(CacheWriteResultTest, FailPermissionDenied) {
  auto result = CacheWriteResult::fail(CacheError::PermissionDenied, "Access denied");
  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::PermissionDenied);
}

// =============================================================================
// IndexCache::validate_and_load Tests
// =============================================================================

TEST_F(IndexCacheTest, ValidateAndLoad_NotFound) {
  std::string source_path = createTempFile("source.csv", "a,b\n1,2\n");
  std::string cache_path = temp_dir + "/nonexistent.vidx";

  auto result = IndexCache::validate_and_load(source_path, cache_path);

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::NotFound);
  EXPECT_FALSE(result.has_index());
  EXPECT_TRUE(result.message.find("not found") != std::string::npos);
}

TEST_F(IndexCacheTest, ValidateAndLoad_VersionMismatch) {
  std::string source_path = createTempFile("version_mismatch.csv", "a,b\n1,2\n");
  std::string cache_path = temp_dir + "/version_mismatch.vidx";

  // Create a cache file with wrong version
  std::ofstream file(cache_path, std::ios::binary);
  uint8_t wrong_version = 255;
  file.write(reinterpret_cast<char*>(&wrong_version), 1);
  file.close();

  auto result = IndexCache::validate_and_load(source_path, cache_path);

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::VersionMismatch);
  EXPECT_TRUE(result.message.find("version mismatch") != std::string::npos);
}

TEST_F(IndexCacheTest, ValidateAndLoad_Corrupted) {
  std::string source_path = createTempFile("corrupted.csv", "a,b\n1,2\n");
  std::string cache_path = createTempFile("corrupted.vidx", "not a valid cache");

  auto result = IndexCache::validate_and_load(source_path, cache_path);

  EXPECT_FALSE(result.success());
  // Should be either VersionMismatch (if it reads version) or Corrupted
  EXPECT_TRUE(result.error == CacheError::VersionMismatch || result.error == CacheError::Corrupted);
}

TEST_F(IndexCacheTest, ValidateAndLoad_SourceChanged) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("changed.csv", content);
  std::string cache_path = temp_dir + "/changed.csv.vidx";

  // Create a valid cache
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, parse_result.idx, source_path));

  // Modify source file
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* new_content = "a,b,c,d\n1,2,3,4\n";
    file.write(new_content, static_cast<std::streamsize>(strlen(new_content)));
    file.flush();
    file.close();
  }

  // Cache should report source changed
  auto result = IndexCache::validate_and_load(source_path, cache_path);

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::SourceChanged);
  EXPECT_TRUE(result.message.find("changed") != std::string::npos);
}

TEST_F(IndexCacheTest, ValidateAndLoad_Success) {
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string source_path = createTempFile("valid_load.csv", content);
  std::string cache_path = temp_dir + "/valid_load.csv.vidx";

  // Create a valid cache
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, parse_result.idx, source_path));

  // Load cache
  auto result = IndexCache::validate_and_load(source_path, cache_path);

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.error, CacheError::None);
  EXPECT_TRUE(result.has_index());
  EXPECT_TRUE(result.index->is_valid());
}

TEST_F(IndexCacheTest, ValidateAndLoad_IoError_NonexistentSource) {
  std::string cache_path = temp_dir + "/orphan.vidx";

  auto result = IndexCache::validate_and_load("/nonexistent/source.csv", cache_path);

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::IoError);
}

// =============================================================================
// IndexCache::write_atomic_result Tests
// =============================================================================

TEST_F(IndexCacheTest, WriteAtomicResult_Success) {
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("write_success.csv", content);
  std::string cache_path = temp_dir + "/write_success.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  auto result = IndexCache::write_atomic_result(cache_path, parse_result.idx, source_path);

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.error, CacheError::None);
  EXPECT_TRUE(fs::exists(cache_path));
}

TEST_F(IndexCacheTest, WriteAtomicResult_NonexistentSource) {
  ParseIndex idx;
  std::string cache_path = temp_dir + "/orphan.vidx";

  auto result = IndexCache::write_atomic_result(cache_path, idx, "/nonexistent/source.csv");

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::IoError);
}

TEST_F(IndexCacheTest, WriteAtomicResult_NonWritableDir) {
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("write_perm.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  // Try to write to a directory that doesn't exist
  auto result =
      IndexCache::write_atomic_result("/nonexistent/dir/cache.vidx", parse_result.idx, source_path);

  EXPECT_FALSE(result.success());
  // Could be IoError or PermissionDenied depending on system
  EXPECT_TRUE(result.error == CacheError::IoError || result.error == CacheError::PermissionDenied);
}

// =============================================================================
// IndexCache::resolve_path Tests
// =============================================================================

TEST_F(IndexCacheTest, ResolvePath_EmptyPath) {
  std::string result = IndexCache::resolve_path("");
  EXPECT_EQ(result, "");
}

TEST_F(IndexCacheTest, ResolvePath_NonexistentPath) {
  std::string result = IndexCache::resolve_path("/nonexistent/path/to/file.csv");
  // Should return the original path since it doesn't exist
  EXPECT_EQ(result, "/nonexistent/path/to/file.csv");
}

TEST_F(IndexCacheTest, ResolvePath_ExistingFile) {
  std::string source = createTempFile("resolve_test.csv", "a,b\n1,2\n");
  std::string resolved = IndexCache::resolve_path(source);

  // Resolved path should be non-empty and exist
  EXPECT_FALSE(resolved.empty());
  EXPECT_TRUE(fs::exists(resolved));
}

#ifndef _WIN32
// Symlink tests only work on Unix-like systems
TEST_F(IndexCacheTest, ResolvePath_Symlink) {
  std::string source = createTempFile("original.csv", "a,b\n1,2\n");
  std::string link = temp_dir + "/link.csv";

  // Create a symlink
  ASSERT_EQ(symlink(source.c_str(), link.c_str()), 0);

  std::string resolved_source = IndexCache::resolve_path(source);
  std::string resolved_link = IndexCache::resolve_path(link);

  // Both should resolve to the same canonical path
  EXPECT_EQ(resolved_source, resolved_link);
}

TEST_F(IndexCacheTest, ResolvePath_NestedSymlinks) {
  // Create: subdir/file.csv, link1 -> subdir, link2 -> link1
  std::string subdir = temp_dir + "/subdir";
  fs::create_directory(subdir);
  std::string source = createTempFileInDir(subdir, "file.csv", "a,b\n1,2\n");

  std::string link1 = temp_dir + "/link1";
  std::string link2 = temp_dir + "/link2";

  ASSERT_EQ(symlink(subdir.c_str(), link1.c_str()), 0);
  ASSERT_EQ(symlink(link1.c_str(), link2.c_str()), 0);

  std::string path_via_link2 = link2 + "/file.csv";

  std::string resolved_direct = IndexCache::resolve_path(source);
  std::string resolved_via_link2 = IndexCache::resolve_path(path_via_link2);

  EXPECT_EQ(resolved_direct, resolved_via_link2);
}

TEST_F(IndexCacheTest, ResolvePath_SymlinkedDir) {
  std::string subdir = temp_dir + "/real_subdir";
  fs::create_directory(subdir);
  std::string source = createTempFileInDir(subdir, "file.csv", "a,b\n1,2\n");

  std::string dir_link = temp_dir + "/linked_subdir";
  ASSERT_EQ(symlink(subdir.c_str(), dir_link.c_str()), 0);

  std::string path_via_link = dir_link + "/file.csv";

  std::string resolved_direct = IndexCache::resolve_path(source);
  std::string resolved_via_link = IndexCache::resolve_path(path_via_link);

  EXPECT_EQ(resolved_direct, resolved_via_link);
}

#endif // _WIN32

// =============================================================================
// IndexCache::load Tests (Corruption detection with auto-cleanup)
// =============================================================================

TEST_F(IndexCacheTest, Load_NonexistentCache) {
  std::string source_path = createTempFile("source.csv", "a,b\n1,2\n");
  std::string cache_path = temp_dir + "/nonexistent.vidx";

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_FALSE(result.was_corrupted);
  EXPECT_FALSE(result.file_deleted);
  EXPECT_FALSE(result.error_message.empty());
}

TEST_F(IndexCacheTest, Load_NonexistentSource) {
  std::string cache_path = createTempFile("cache.vidx", "some content");

  auto result = IndexCache::load(cache_path, "/nonexistent/source.csv");

  EXPECT_FALSE(result.success());
  EXPECT_FALSE(result.was_corrupted);
  EXPECT_FALSE(result.file_deleted);
}

TEST_F(IndexCacheTest, Load_ValidCache) {
  // Create source file and valid cache
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string source_path = createTempFile("valid_source.csv", content);
  std::string cache_path = temp_dir + "/valid_source.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, parse_result.idx, source_path));

  // Load should succeed
  auto result = IndexCache::load(cache_path, source_path);
  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.was_corrupted);
  EXPECT_FALSE(result.file_deleted);
  EXPECT_TRUE(result.index.is_valid());
}

TEST_F(IndexCacheTest, Load_TruncatedHeader_DeletesFile) {
  std::string source_path = createTempFile("truncated.csv", "a,b\n1,2\n");

  // Create a cache file that's too small (less than 40-byte header)
  std::string cache_path = temp_dir + "/truncated.vidx";
  {
    std::ofstream file(cache_path, std::ios::binary);
    const char* content = "short"; // Only 5 bytes, need at least 40
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  ASSERT_TRUE(fs::exists(cache_path));

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(result.was_corrupted);
  EXPECT_TRUE(result.file_deleted);
  EXPECT_FALSE(fs::exists(cache_path)); // File should be deleted
  EXPECT_TRUE(result.error_message.find("too small") != std::string::npos);
}

TEST_F(IndexCacheTest, Load_WrongVersion_DeletesFile) {
  std::string source_path = createTempFile("wrongver.csv", "a,b\n1,2\n");

  // Create a cache file with wrong version byte
  std::string cache_path = temp_dir + "/wrongver.vidx";
  std::ofstream file(cache_path, std::ios::binary);

  // Write invalid version (255 instead of 3)
  uint8_t wrong_version = 255;
  file.write(reinterpret_cast<char*>(&wrong_version), 1);

  // Pad to minimum header size (40 bytes)
  char padding[39] = {0};
  file.write(padding, 39);
  file.close();

  ASSERT_TRUE(fs::exists(cache_path));
  ASSERT_GE(fs::file_size(cache_path), 40u);

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(result.was_corrupted);
  EXPECT_TRUE(result.file_deleted);
  EXPECT_FALSE(fs::exists(cache_path));
  EXPECT_TRUE(result.error_message.find("version") != std::string::npos);
}

TEST_F(IndexCacheTest, Load_TruncatedIndexData_DeletesFile) {
  // Create source file and valid cache first
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string source_path = createTempFile("truncated_data.csv", content);
  std::string cache_path = temp_dir + "/truncated_data.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  // Write valid cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, parse_result.idx, source_path));
  size_t original_size = fs::file_size(cache_path);

  // Truncate the cache file (remove some index data)
  fs::resize_file(cache_path, original_size - 20);

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(result.was_corrupted);
  EXPECT_TRUE(result.file_deleted);
  EXPECT_FALSE(fs::exists(cache_path));
}

TEST_F(IndexCacheTest, Load_StaleCache_DoesNotDelete) {
  // Create source file and valid cache
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("stale.csv", content);
  std::string cache_path = temp_dir + "/stale.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, parse_result.idx, source_path));

  // Modify source file to make cache stale
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    std::ofstream file(source_path, std::ios::binary);
    const char* new_content = "a,b,c,d\n1,2,3,4\n5,6,7,8\n"; // Different content
    file.write(new_content, static_cast<std::streamsize>(strlen(new_content)));
    file.flush();
    file.close();
  }

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_FALSE(result.was_corrupted); // Stale, not corrupted
  EXPECT_FALSE(result.file_deleted);  // Should not delete stale caches
  EXPECT_TRUE(fs::exists(cache_path));
}

TEST_F(IndexCacheTest, Load_GarbageContent_DeletesFile) {
  std::string source_path = createTempFile("garbage.csv", "a,b\n1,2\n");

  // Create a cache file with correct version but garbage content
  std::string cache_path = temp_dir + "/garbage.vidx";
  {
    std::ofstream file(cache_path, std::ios::binary);

    // Write correct version
    uint8_t version = 3;
    file.write(reinterpret_cast<char*>(&version), 1);

    // Write padding (7 bytes)
    char padding[7] = {0};
    file.write(padding, 7);

    // Get source metadata and write matching mtime/size
    auto [mtime, size] = IndexCache::get_source_metadata(source_path);
    file.write(reinterpret_cast<char*>(&mtime), 8);
    file.write(reinterpret_cast<char*>(&size), 8);

    // Write garbage for rest of header and data
    char garbage[100];
    std::memset(garbage, 0xFF, sizeof(garbage));
    file.write(garbage, sizeof(garbage));
  }

  ASSERT_TRUE(fs::exists(cache_path));

  auto result = IndexCache::load(cache_path, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(result.was_corrupted);
  EXPECT_TRUE(result.file_deleted);
  EXPECT_FALSE(fs::exists(cache_path));
}

// =============================================================================
// Parser Integration with Corruption Detection
// =============================================================================

TEST_F(IndexCacheTest, ParserApi_CorruptedCacheAutomaticallyDeleted) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("corrupt_auto.csv", content);
  std::string cache_path = source_path + ".vidx";

  // Create a corrupted cache file
  {
    std::ofstream file(cache_path, std::ios::binary);
    uint8_t wrong_version = 42;
    file.write(reinterpret_cast<char*>(&wrong_version), 1);
    char padding[50] = {0};
    file.write(padding, sizeof(padding));
  }

  ASSERT_TRUE(fs::exists(cache_path));

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // Parse should succeed (re-parse after detecting corruption)
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);     // Cache was corrupted, so re-parsed
  EXPECT_TRUE(fs::exists(cache_path)); // New cache should be written
}

TEST_F(IndexCacheTest, ParserApi_CorruptedCacheRecreatedOnReparse) {
  std::string content = "name,value\nalice,100\nbob,200\n";
  std::string source_path = createTempFile("recreate.csv", content);
  std::string cache_path = source_path + ".vidx";

  // Create corrupted cache
  {
    std::ofstream file(cache_path, std::ios::binary);
    const char* corrupt_content = "NOT A VALID CACHE FILE";
    file.write(corrupt_content, static_cast<std::streamsize>(strlen(corrupt_content)));
    file.flush();
    file.close();
  }

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - detects corruption, deletes bad cache, re-parses, writes new cache
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result1.success());
  EXPECT_FALSE(result1.used_cache);

  // Second parse - should now hit the valid cache
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache);

  // Verify data is correct from cached index
  EXPECT_EQ(result2.num_rows(), 2);
  auto names = result2.column_string(0);
  EXPECT_EQ(names.size(), 2u);
  EXPECT_EQ(names[0], "alice");
  EXPECT_EQ(names[1], "bob");
}

#ifndef _WIN32
TEST_F(IndexCacheTest, WriteAtomicResult_PermissionDenied) {
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("write_perm.csv", content);
  std::string readonly_dir = createTempDir("readonly_write");
  std::string cache_path = readonly_dir + "/cache.vidx";

  // Make directory read-only
  chmod(readonly_dir.c_str(), 0555);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto parse_result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parse_result.success());

  auto result = IndexCache::write_atomic_result(cache_path, parse_result.idx, source_path);

  EXPECT_FALSE(result.success());
  EXPECT_EQ(result.error, CacheError::PermissionDenied);

  // Restore permissions for cleanup
  chmod(readonly_dir.c_str(), 0755);
}
#endif
