/**
 * @file mmap_source_test.cpp
 * @brief Tests for MmapSource memory-mapped file I/O.
 *
 * Tests open, close, content integrity, empty files, error handling,
 * and reopen behavior for the platform-specific MmapSource implementation.
 *
 * @see GitHub issue #649
 */

#include "libvroom/vroom.h"

#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

using libvroom::MmapSource;

class MmapSourceTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }
};

// =============================================================================
// Open and basic state tests
// =============================================================================

TEST_F(MmapSourceTest, DefaultStateNotOpen) {
  MmapSource source;
  EXPECT_FALSE(source.is_open());
  EXPECT_EQ(source.size(), 0);
  EXPECT_EQ(source.data(), nullptr);
}

TEST_F(MmapSourceTest, OpenValidFile) {
  std::string path = testDataPath("basic/simple.csv");
  std::ifstream check(path);
  if (!check.good()) {
    GTEST_SKIP() << "Test data not found: " << path;
  }

  MmapSource source;
  auto result = source.open(path);
  ASSERT_TRUE(result.ok);
  EXPECT_TRUE(source.is_open());
  EXPECT_GT(source.size(), 0);
  EXPECT_NE(source.data(), nullptr);
}

TEST_F(MmapSourceTest, OpenNonExistentFile) {
  MmapSource source;
  auto result = source.open("nonexistent_file_that_does_not_exist.csv");
  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(source.is_open());
}

// =============================================================================
// Content integrity tests
// =============================================================================

TEST_F(MmapSourceTest, ContentMatchesFile) {
  std::string path = testDataPath("basic/simple.csv");
  std::ifstream file(path, std::ios::binary);
  if (!file.good()) {
    GTEST_SKIP() << "Test data not found: " << path;
  }
  std::string expected((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

  MmapSource source;
  ASSERT_TRUE(source.open(path).ok);
  EXPECT_EQ(source.size(), expected.size());
  EXPECT_EQ(std::memcmp(source.data(), expected.data(), expected.size()), 0);
}

TEST_F(MmapSourceTest, EmptyFile) {
  std::string temp = "/tmp/libvroom_mmap_test_empty.csv";
  { std::ofstream ofs(temp, std::ios::binary); }

  MmapSource source;
  auto result = source.open(temp);
  ASSERT_TRUE(result.ok);
  EXPECT_TRUE(source.is_open());
  EXPECT_EQ(source.size(), 0);

  std::remove(temp.c_str());
}

// =============================================================================
// Close and lifecycle tests
// =============================================================================

TEST_F(MmapSourceTest, CloseReleasesResources) {
  MmapSource source;
  ASSERT_TRUE(source.open(testDataPath("basic/simple.csv")).ok);
  EXPECT_TRUE(source.is_open());

  source.close();
  EXPECT_FALSE(source.is_open());
  EXPECT_EQ(source.size(), 0);
}

TEST_F(MmapSourceTest, DoubleCloseIsSafe) {
  MmapSource source;
  ASSERT_TRUE(source.open(testDataPath("basic/simple.csv")).ok);
  source.close();
  source.close(); // Should not crash
  EXPECT_FALSE(source.is_open());
}

TEST_F(MmapSourceTest, ReopenDifferentFile) {
  std::string path1 = testDataPath("basic/simple.csv");
  std::string path2 = testDataPath("quoted/quoted_fields.csv");

  std::ifstream check1(path1), check2(path2);
  if (!check1.good() || !check2.good()) {
    GTEST_SKIP() << "Test data files not found";
  }

  MmapSource source;
  ASSERT_TRUE(source.open(path1).ok);
  size_t first_size = source.size();
  EXPECT_GT(first_size, 0);

  // Opening a new file should implicitly close the first
  ASSERT_TRUE(source.open(path2).ok);
  EXPECT_TRUE(source.is_open());
  EXPECT_GT(source.size(), 0);
}

TEST_F(MmapSourceTest, DestructorCleansUp) {
  // Construct and open in a scope, let destructor run
  {
    MmapSource source;
    ASSERT_TRUE(source.open(testDataPath("basic/simple.csv")).ok);
    EXPECT_TRUE(source.is_open());
  }
  // No crash or leak — destructor handled cleanup
}
