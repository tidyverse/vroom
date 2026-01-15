/**
 * @file mmap_util_test.cpp
 * @brief Unit tests for mmap utility functions and index caching.
 */

#include "libvroom.h"

#include "mmap_util.h"
#include "two_pass.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

// Test fixture for mmap utility tests
class MmapUtilTest : public ::testing::Test {
protected:
  std::string temp_dir;
  std::vector<std::string> temp_files;

  void SetUp() override {
    // Create temp directory for test files
    temp_dir = (fs::temp_directory_path() / ("mmap_test_" + std::to_string(getpid()))).string();
    fs::create_directories(temp_dir);
  }

  void TearDown() override {
    // Clean up temp files
    for (const auto& file : temp_files) {
      fs::remove(file);
    }
    // Remove temp directory if empty
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
    temp_files.push_back(path);
    return path;
  }

  std::string createTempFile(const std::string& filename, const uint8_t* data, size_t size) {
    std::string path = temp_dir + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    file.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    file.flush();
    file.close();
    temp_files.push_back(path);
    return path;
  }
};

// =============================================================================
// MmapBuffer TESTS
// =============================================================================

TEST_F(MmapUtilTest, MmapBuffer_DefaultConstructor) {
  libvroom::MmapBuffer buf;
  EXPECT_FALSE(buf.valid());
  EXPECT_EQ(buf.data(), nullptr);
  EXPECT_EQ(buf.size(), 0);
}

TEST_F(MmapUtilTest, MmapBuffer_OpenNonExistentFile) {
  libvroom::MmapBuffer buf;
  bool result = buf.open("/nonexistent/path/file.txt");
  EXPECT_FALSE(result);
  EXPECT_FALSE(buf.valid());
}

TEST_F(MmapUtilTest, MmapBuffer_OpenEmptyFile) {
  std::string path = createTempFile("empty.txt", "");
  libvroom::MmapBuffer buf;
  bool result = buf.open(path);
  EXPECT_FALSE(result); // Cannot mmap empty files
  EXPECT_FALSE(buf.valid());
}

TEST_F(MmapUtilTest, MmapBuffer_OpenValidFile) {
  std::string content = "Hello, World!";
  std::string path = createTempFile("test.txt", content);

  libvroom::MmapBuffer buf;
  bool result = buf.open(path);

  EXPECT_TRUE(result);
  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), content.size());
  EXPECT_EQ(std::memcmp(buf.data(), content.data(), content.size()), 0);
}

TEST_F(MmapUtilTest, MmapBuffer_MoveConstructor) {
  std::string content = "Test content for move";
  std::string path = createTempFile("move_test.txt", content);

  libvroom::MmapBuffer buf1;
  ASSERT_TRUE(buf1.open(path));

  const uint8_t* original_data = buf1.data();
  size_t original_size = buf1.size();

  libvroom::MmapBuffer buf2(std::move(buf1));

  // buf2 should have the data
  EXPECT_TRUE(buf2.valid());
  EXPECT_EQ(buf2.data(), original_data);
  EXPECT_EQ(buf2.size(), original_size);

  // buf1 should be empty
  EXPECT_FALSE(buf1.valid());
  EXPECT_EQ(buf1.data(), nullptr);
  EXPECT_EQ(buf1.size(), 0);
}

TEST_F(MmapUtilTest, MmapBuffer_MoveAssignment) {
  std::string content1 = "Content for buffer 1";
  std::string content2 = "Content for buffer 2";
  std::string path1 = createTempFile("move_assign1.txt", content1);
  std::string path2 = createTempFile("move_assign2.txt", content2);

  libvroom::MmapBuffer buf1;
  libvroom::MmapBuffer buf2;
  ASSERT_TRUE(buf1.open(path1));
  ASSERT_TRUE(buf2.open(path2));

  const uint8_t* data2 = buf2.data();
  size_t size2 = buf2.size();

  buf1 = std::move(buf2);

  // buf1 should have buf2's data
  EXPECT_TRUE(buf1.valid());
  EXPECT_EQ(buf1.data(), data2);
  EXPECT_EQ(buf1.size(), size2);

  // buf2 should be empty
  EXPECT_FALSE(buf2.valid());
}

TEST_F(MmapUtilTest, MmapBuffer_Reopen) {
  std::string content1 = "First file content";
  std::string content2 = "Second file content";
  std::string path1 = createTempFile("reopen1.txt", content1);
  std::string path2 = createTempFile("reopen2.txt", content2);

  libvroom::MmapBuffer buf;
  ASSERT_TRUE(buf.open(path1));
  EXPECT_EQ(buf.size(), content1.size());

  // Opening another file should unmap the first
  ASSERT_TRUE(buf.open(path2));
  EXPECT_EQ(buf.size(), content2.size());
  EXPECT_EQ(std::memcmp(buf.data(), content2.data(), content2.size()), 0);
}

// =============================================================================
// SourceMetadata TESTS
// =============================================================================

TEST_F(MmapUtilTest, SourceMetadata_NonExistentFile) {
  auto meta = libvroom::SourceMetadata::from_file("/nonexistent/file.csv");
  EXPECT_FALSE(meta.valid);
}

TEST_F(MmapUtilTest, SourceMetadata_ValidFile) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string path = createTempFile("meta_test.csv", content);

  auto meta = libvroom::SourceMetadata::from_file(path);

  EXPECT_TRUE(meta.valid);
  EXPECT_EQ(meta.size, content.size());
  EXPECT_GT(meta.mtime, 0);
}

TEST_F(MmapUtilTest, SourceMetadata_Directory) {
  auto meta = libvroom::SourceMetadata::from_file(temp_dir);
  EXPECT_FALSE(meta.valid); // Directories should not be valid
}

// =============================================================================
// get_cache_path TESTS
// =============================================================================

TEST_F(MmapUtilTest, GetCachePath) {
  EXPECT_EQ(libvroom::get_cache_path("/path/to/file.csv"), "/path/to/file.csv.vidx");
  EXPECT_EQ(libvroom::get_cache_path("data.csv"), "data.csv.vidx");
  EXPECT_EQ(libvroom::get_cache_path(""), ".vidx");
}

// =============================================================================
// ParseIndex v3 format TESTS
// =============================================================================

TEST_F(MmapUtilTest, ParseIndex_WriteV3AndFromMmap) {
  // Create a simple CSV and parse it
  std::string csv_content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string csv_path = createTempFile("test.csv", csv_content);
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  // Parse the CSV
  libvroom::Parser parser(1);
  auto load_result = libvroom::load_file(csv_path);
  auto parse_result = parser.parse(load_result.data(), load_result.size());

  // Get source metadata
  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  ASSERT_TRUE(source_meta.valid);

  // Write v3 format
  EXPECT_NO_THROW(parse_result.idx.write(cache_path, source_meta));

  // Load via mmap
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, source_meta);

  EXPECT_TRUE(loaded_idx.is_valid());
  EXPECT_TRUE(loaded_idx.is_mmap_backed());
  EXPECT_EQ(loaded_idx.columns, parse_result.idx.columns);
  EXPECT_EQ(loaded_idx.n_threads, parse_result.idx.n_threads);

  // Compare n_indexes
  for (uint16_t i = 0; i < loaded_idx.n_threads; ++i) {
    EXPECT_EQ(loaded_idx.n_indexes[i], parse_result.idx.n_indexes[i]);
  }

  // Compare indexes
  size_t total_indexes = 0;
  for (uint16_t i = 0; i < loaded_idx.n_threads; ++i) {
    total_indexes += loaded_idx.n_indexes[i];
  }
  for (size_t i = 0; i < total_indexes; ++i) {
    EXPECT_EQ(loaded_idx.indexes[i], parse_result.idx.indexes[i]);
  }
}

TEST_F(MmapUtilTest, ParseIndex_FromMmapInvalidVersion) {
  std::string csv_path = createTempFile("test_invalid.csv", "a,b\n1,2\n");
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  // Write a file with wrong version
  uint8_t data[] = {99}; // Invalid version
  createTempFile("test_invalid.csv.vidx", data, sizeof(data));

  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, source_meta);

  EXPECT_FALSE(loaded_idx.is_valid());
}

TEST_F(MmapUtilTest, ParseIndex_FromMmapStaleCache) {
  // Create a simple CSV and parse it
  std::string csv_content = "a,b,c\n1,2,3\n";
  std::string csv_path = createTempFile("stale_test.csv", csv_content);
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  // Parse and write cache
  libvroom::Parser parser(1);
  auto load_result = libvroom::load_file(csv_path);
  auto parse_result = parser.parse(load_result.data(), load_result.size());
  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  parse_result.idx.write(cache_path, source_meta);

  // Modify the source file (change mtime and size)
  std::this_thread::sleep_for(std::chrono::milliseconds(1100)); // Ensure mtime changes
  {
    std::ofstream file(csv_path, std::ios::binary);
    const char* content = "a,b,c,d\n1,2,3,4\n5,6,7,8\n"; // Different content
    file.write(content, static_cast<std::streamsize>(strlen(content)));
    file.flush();
    file.close();
  }

  // Try to load with new metadata - should fail due to mtime/size mismatch
  auto new_meta = libvroom::SourceMetadata::from_file(csv_path);
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, new_meta);

  EXPECT_FALSE(loaded_idx.is_valid());
}

TEST_F(MmapUtilTest, ParseIndex_FromMmapTruncatedFile) {
  std::string csv_path = createTempFile("truncated.csv", "a,b\n1,2\n");
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  // Write a truncated cache file (just the version byte)
  uint8_t data[] = {3}; // v3 version, but no data
  createTempFile("truncated.csv.vidx", data, sizeof(data));

  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, source_meta);

  EXPECT_FALSE(loaded_idx.is_valid());
}

TEST_F(MmapUtilTest, ParseIndex_FromMmapNonExistent) {
  std::string csv_path = createTempFile("noexist.csv", "a,b\n1,2\n");
  std::string cache_path = csv_path + ".vidx"; // Don't create this file

  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, source_meta);

  EXPECT_FALSE(loaded_idx.is_valid());
}

TEST_F(MmapUtilTest, ParseIndex_MovePreservesMmap) {
  // Create and cache a CSV
  std::string csv_content = "x,y\n10,20\n30,40\n";
  std::string csv_path = createTempFile("move_mmap.csv", csv_content);
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  libvroom::Parser parser(1);
  auto load_result = libvroom::load_file(csv_path);
  auto parse_result = parser.parse(load_result.data(), load_result.size());
  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  parse_result.idx.write(cache_path, source_meta);

  // Load via mmap
  auto idx1 = libvroom::ParseIndex::from_mmap(cache_path, source_meta);
  ASSERT_TRUE(idx1.is_valid());
  ASSERT_TRUE(idx1.is_mmap_backed());

  // Move to another index
  libvroom::ParseIndex idx2 = std::move(idx1);

  // idx2 should have the data
  EXPECT_TRUE(idx2.is_valid());
  EXPECT_TRUE(idx2.is_mmap_backed());

  // idx1 should be invalid
  EXPECT_FALSE(idx1.is_valid());
  EXPECT_FALSE(idx1.is_mmap_backed());
}

TEST_F(MmapUtilTest, ParseIndex_V2FormatStillWorks) {
  // Ensure existing v2 format write/read still works
  std::string csv_content = "col1,col2\nval1,val2\n";
  std::string csv_path = createTempFile("v2_test.csv", csv_content);
  std::string idx_path = csv_path + ".idx";
  temp_files.push_back(idx_path);

  libvroom::Parser parser(1);
  auto load_result = libvroom::load_file(csv_path);
  auto parse_result = parser.parse(load_result.data(), load_result.size());

  // Write v2 format
  EXPECT_NO_THROW(parse_result.idx.write(idx_path));

  // Read v2 format into a new index
  libvroom::TwoPass tp;
  auto new_idx = tp.init(load_result.size(), 1);
  EXPECT_NO_THROW(new_idx.read(idx_path));

  EXPECT_EQ(new_idx.columns, parse_result.idx.columns);
  EXPECT_EQ(new_idx.n_threads, parse_result.idx.n_threads);
}

// =============================================================================
// Multi-threaded index tests
// =============================================================================

TEST_F(MmapUtilTest, ParseIndex_MultiThreadedWriteAndLoad) {
  // Create a larger CSV that will use multiple threads
  std::string csv_content = "a,b,c,d,e\n";
  for (int i = 0; i < 1000; ++i) {
    csv_content += std::to_string(i) + "," + std::to_string(i * 2) + "," + std::to_string(i * 3) +
                   "," + std::to_string(i * 4) + "," + std::to_string(i * 5) + "\n";
  }
  std::string csv_path = createTempFile("multithread.csv", csv_content);
  std::string cache_path = csv_path + ".vidx";
  temp_files.push_back(cache_path);

  // Parse with multiple threads
  libvroom::Parser parser(4);
  auto load_result = libvroom::load_file(csv_path);
  auto parse_result = parser.parse(load_result.data(), load_result.size());

  auto source_meta = libvroom::SourceMetadata::from_file(csv_path);
  ASSERT_TRUE(source_meta.valid);

  // Write v3 format
  parse_result.idx.write(cache_path, source_meta);

  // Load via mmap
  auto loaded_idx = libvroom::ParseIndex::from_mmap(cache_path, source_meta);

  EXPECT_TRUE(loaded_idx.is_valid());
  EXPECT_EQ(loaded_idx.columns, parse_result.idx.columns);
  // n_threads might be different if single-threaded fallback was used
  // but the data should be equivalent
}
