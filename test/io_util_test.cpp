/**
 * @file io_util_test.cpp
 * @brief Tests for AlignedBuffer and file I/O utilities.
 *
 * Rewritten from old io_util_test.cpp to use the libvroom2 AlignedBuffer API.
 * Tests memory alignment, padding, move semantics, and file loading.
 *
 * @see GitHub issue #626
 */

#include "libvroom.h"
#include "libvroom/io_util.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <vector>
using libvroom::AlignedBuffer;

// =============================================================================
// Test Fixture
// =============================================================================

class IoUtilTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }
};

// =============================================================================
// AlignedBuffer::allocate TESTS
// =============================================================================

TEST_F(IoUtilTest, Allocate_BasicAllocation) {
  auto buf = AlignedBuffer::allocate(1024);

  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), 1024);
  EXPECT_GE(buf.capacity(), 1024 + LIBVROOM_PADDING);
}

TEST_F(IoUtilTest, Allocate_ZeroLength) {
  auto buf = AlignedBuffer::allocate(0);

  // Zero-length allocation should still be valid (padding is still allocated)
  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), 0);
  EXPECT_TRUE(buf.empty());
  EXPECT_GE(buf.capacity(), LIBVROOM_PADDING);
  EXPECT_NE(buf.data(), nullptr);
}

TEST_F(IoUtilTest, Allocate_SmallAllocation) {
  auto buf = AlignedBuffer::allocate(1);

  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), 1);
  EXPECT_GE(buf.capacity(), 1 + LIBVROOM_PADDING);

  // Should be able to write and read back a single byte
  buf.data()[0] = 0xAB;
  EXPECT_EQ(buf.data()[0], 0xAB);
}

TEST_F(IoUtilTest, Allocate_Alignment) {
  auto buf = AlignedBuffer::allocate(100);

  ASSERT_NE(buf.data(), nullptr);
  uintptr_t addr = reinterpret_cast<uintptr_t>(buf.data());
  EXPECT_EQ(addr % 64, 0) << "Buffer data pointer should be 64-byte aligned";
}

TEST_F(IoUtilTest, Allocate_LargeAllocation) {
  const size_t ten_mb = 10 * 1024 * 1024;
  auto buf = AlignedBuffer::allocate(ten_mb);

  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), ten_mb);
  EXPECT_GE(buf.capacity(), ten_mb + LIBVROOM_PADDING);

  // Write to first and last bytes to verify memory is accessible
  buf.data()[0] = 0xFF;
  buf.data()[ten_mb - 1] = 0xFE;
  EXPECT_EQ(buf.data()[0], 0xFF);
  EXPECT_EQ(buf.data()[ten_mb - 1], 0xFE);
}

TEST_F(IoUtilTest, Allocate_PaddingZeroed) {
  auto buf = AlignedBuffer::allocate(100);

  ASSERT_NE(buf.data(), nullptr);
  // Fill the data region with non-zero values to ensure we are checking padding
  std::memset(buf.data(), 0xAA, 100);

  // Verify the padding bytes [100..100+LIBVROOM_PADDING) are zero
  for (size_t i = 0; i < LIBVROOM_PADDING; ++i) {
    EXPECT_EQ(buf.data()[100 + i], 0)
        << "Padding byte at offset " << (100 + i) << " should be zero";
  }
}

TEST_F(IoUtilTest, Allocate_CapacityIncludesPadding) {
  auto buf = AlignedBuffer::allocate(256);

  EXPECT_EQ(buf.capacity(), 256 + LIBVROOM_PADDING);
}

TEST_F(IoUtilTest, Allocate_DefaultPaddingIsLibvroomPadding) {
  // allocate(size) with no second argument should use LIBVROOM_PADDING (64)
  auto buf = AlignedBuffer::allocate(128);

  EXPECT_EQ(buf.capacity(), 128 + LIBVROOM_PADDING);

  // Explicit padding should produce a different capacity
  auto buf_custom = AlignedBuffer::allocate(128, 128);
  EXPECT_EQ(buf_custom.capacity(), 128 + 128);
}

// =============================================================================
// AlignedBuffer move semantics TESTS
// =============================================================================

TEST_F(IoUtilTest, Move_ConstructorSourceBecomesInvalid) {
  auto source = AlignedBuffer::allocate(512);
  ASSERT_TRUE(source.valid());
  source.data()[0] = 0xCD;

  AlignedBuffer target(std::move(source));

  EXPECT_FALSE(source.valid());
  EXPECT_EQ(source.data(), nullptr);
  EXPECT_EQ(source.size(), 0);
  EXPECT_EQ(source.capacity(), 0);

  EXPECT_TRUE(target.valid());
  EXPECT_EQ(target.size(), 512);
  EXPECT_EQ(target.data()[0], 0xCD);
}

TEST_F(IoUtilTest, Move_AssignmentSourceBecomesInvalid) {
  auto source = AlignedBuffer::allocate(256);
  ASSERT_TRUE(source.valid());
  source.data()[0] = 0xEF;

  AlignedBuffer target;
  target = std::move(source);

  EXPECT_FALSE(source.valid());
  EXPECT_EQ(source.data(), nullptr);

  EXPECT_TRUE(target.valid());
  EXPECT_EQ(target.size(), 256);
  EXPECT_EQ(target.data()[0], 0xEF);
}

TEST_F(IoUtilTest, Move_SelfAssignmentNoCrash) {
  auto buf = AlignedBuffer::allocate(64);
  ASSERT_TRUE(buf.valid());
  buf.data()[0] = 0x42;

  // Self-assignment via move. Use a pointer to avoid compiler warnings.
  AlignedBuffer* ptr = &buf;
  buf = std::move(*ptr);

  // After self-move-assignment, the buffer should remain valid (implementation
  // has a self-check: if (this != &other))
  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.data()[0], 0x42);
}

TEST_F(IoUtilTest, Move_FromDefaultConstructed) {
  AlignedBuffer empty;
  EXPECT_FALSE(empty.valid());

  AlignedBuffer target(std::move(empty));

  EXPECT_FALSE(target.valid());
  EXPECT_EQ(target.data(), nullptr);
  EXPECT_EQ(target.size(), 0);
}

TEST_F(IoUtilTest, Move_ChainOfMoves) {
  auto a = AlignedBuffer::allocate(128);
  ASSERT_TRUE(a.valid());
  a.data()[0] = 0x77;

  AlignedBuffer b(std::move(a));
  AlignedBuffer c;
  c = std::move(b);

  EXPECT_FALSE(a.valid());
  EXPECT_FALSE(b.valid());
  EXPECT_TRUE(c.valid());
  EXPECT_EQ(c.size(), 128);
  EXPECT_EQ(c.data()[0], 0x77);
}

// =============================================================================
// AlignedBuffer default state TESTS
// =============================================================================

TEST_F(IoUtilTest, DefaultState_DefaultConstructed) {
  AlignedBuffer buf;

  EXPECT_FALSE(buf.valid());
  EXPECT_TRUE(buf.empty());
  EXPECT_EQ(buf.data(), nullptr);
  EXPECT_EQ(buf.size(), 0);
  EXPECT_EQ(buf.capacity(), 0);
}

TEST_F(IoUtilTest, DefaultState_AfterMoveFrom) {
  auto buf = AlignedBuffer::allocate(256);
  AlignedBuffer other(std::move(buf));

  EXPECT_FALSE(buf.valid());
  EXPECT_TRUE(buf.empty());
  EXPECT_EQ(buf.data(), nullptr);
}

TEST_F(IoUtilTest, DefaultState_OperatorBoolMatchesValid) {
  AlignedBuffer empty;
  EXPECT_FALSE(static_cast<bool>(empty));
  EXPECT_EQ(static_cast<bool>(empty), empty.valid());

  auto allocated = AlignedBuffer::allocate(64);
  EXPECT_TRUE(static_cast<bool>(allocated));
  EXPECT_EQ(static_cast<bool>(allocated), allocated.valid());
}

// =============================================================================
// AlignedBuffer data integrity TESTS
// =============================================================================

TEST_F(IoUtilTest, DataIntegrity_WriteAndReadBack) {
  auto buf = AlignedBuffer::allocate(256);
  ASSERT_TRUE(buf.valid());

  // Fill with a known pattern
  for (size_t i = 0; i < 256; ++i) {
    buf.data()[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // Read back and verify
  for (size_t i = 0; i < 256; ++i) {
    EXPECT_EQ(buf.data()[i], static_cast<uint8_t>(i & 0xFF)) << "Mismatch at byte " << i;
  }
}

TEST_F(IoUtilTest, DataIntegrity_BinaryData) {
  auto buf = AlignedBuffer::allocate(256);
  ASSERT_TRUE(buf.valid());

  // Write all byte values 0x00-0xFF
  for (int i = 0; i < 256; ++i) {
    buf.data()[i] = static_cast<uint8_t>(i);
  }

  // Verify with memcmp against expected pattern
  uint8_t expected[256];
  for (int i = 0; i < 256; ++i) {
    expected[i] = static_cast<uint8_t>(i);
  }
  EXPECT_EQ(std::memcmp(buf.data(), expected, 256), 0);
}

TEST_F(IoUtilTest, DataIntegrity_LargeData) {
  const size_t one_mb = 1024 * 1024;
  auto buf = AlignedBuffer::allocate(one_mb);
  ASSERT_TRUE(buf.valid());

  // Fill with repeating pattern
  for (size_t i = 0; i < one_mb; ++i) {
    buf.data()[i] = static_cast<uint8_t>(i % 251); // prime modulus to avoid alignment tricks
  }

  // Verify
  for (size_t i = 0; i < one_mb; ++i) {
    EXPECT_EQ(buf.data()[i], static_cast<uint8_t>(i % 251)) << "Mismatch at byte " << i;
  }
}

// =============================================================================
// load_file_to_ptr TESTS
// =============================================================================

TEST_F(IoUtilTest, LoadFile_SimpleCSV) {
  std::string path = testDataPath("basic/simple.csv");
  if (!std::ifstream(path).good()) {
    GTEST_SKIP() << "Test data file not found: " << path;
  }

  auto buf = libvroom::load_file_to_ptr(path);

  EXPECT_TRUE(buf.valid());
  // simple.csv is "A,B,C\n1,2,3\n4,5,6\n7,8,9\n" = 24 bytes
  EXPECT_EQ(buf.size(), 24);
}

TEST_F(IoUtilTest, LoadFile_EmptyFile) {
  // Create a temporary empty file
  std::string temp_path = "/tmp/libvroom_io_util_test_empty.csv";
  { std::ofstream ofs(temp_path, std::ios::binary); }

  auto buf = libvroom::load_file_to_ptr(temp_path);

  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), 0);
  EXPECT_TRUE(buf.empty());

  std::remove(temp_path.c_str());
}

TEST_F(IoUtilTest, LoadFile_NonExistentFileThrows) {
  EXPECT_THROW(libvroom::load_file_to_ptr("nonexistent_file_that_does_not_exist.csv"),
               std::runtime_error);
}

TEST_F(IoUtilTest, LoadFile_ContentIntegrity) {
  // Create a temp file with known content and verify byte-by-byte
  std::string temp_path = "/tmp/libvroom_io_util_test_integrity.csv";
  std::string content = "name,value\nalice,100\nbob,200\n";
  {
    std::ofstream ofs(temp_path, std::ios::binary);
    ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
  }

  auto buf = libvroom::load_file_to_ptr(temp_path);

  EXPECT_EQ(buf.size(), content.size());
  EXPECT_EQ(std::memcmp(buf.data(), content.data(), content.size()), 0);

  std::remove(temp_path.c_str());
}

TEST_F(IoUtilTest, LoadFile_Alignment) {
  std::string path = testDataPath("basic/simple.csv");
  if (!std::ifstream(path).good()) {
    GTEST_SKIP() << "Test data file not found: " << path;
  }

  auto buf = libvroom::load_file_to_ptr(path);

  ASSERT_NE(buf.data(), nullptr);
  uintptr_t addr = reinterpret_cast<uintptr_t>(buf.data());
  EXPECT_EQ(addr % 64, 0) << "Loaded file buffer should be 64-byte aligned";
}

TEST_F(IoUtilTest, LoadFile_PaddingZeroed) {
  std::string temp_path = "/tmp/libvroom_io_util_test_padding.csv";
  std::string content = "x,y\n1,2\n";
  {
    std::ofstream ofs(temp_path, std::ios::binary);
    ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
  }

  auto buf = libvroom::load_file_to_ptr(temp_path);

  ASSERT_EQ(buf.size(), content.size());
  // Verify padding bytes after the data are zeroed
  for (size_t i = 0; i < LIBVROOM_PADDING; ++i) {
    EXPECT_EQ(buf.data()[buf.size() + i], 0)
        << "Padding byte at offset " << (buf.size() + i) << " should be zero";
  }

  std::remove(temp_path.c_str());
}

TEST_F(IoUtilTest, LoadFile_LargeFile) {
  std::string path = testDataPath("large/parallel_chunk_boundary.csv");
  if (!std::ifstream(path).good()) {
    GTEST_SKIP() << "Test data file not found: " << path;
  }

  // Get file size without <filesystem>
  std::ifstream size_stream(path, std::ios::ate | std::ios::binary);
  auto file_size = static_cast<size_t>(size_stream.tellg());
  auto buf = libvroom::load_file_to_ptr(path);

  EXPECT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), file_size);
}

TEST_F(IoUtilTest, LoadFile_QuotedFields) {
  std::string path = testDataPath("quoted/quoted_fields.csv");
  if (!std::ifstream(path).good()) {
    GTEST_SKIP() << "Test data file not found: " << path;
  }

  auto buf = libvroom::load_file_to_ptr(path);

  EXPECT_TRUE(buf.valid());
  EXPECT_GT(buf.size(), 0);

  // Verify content matches the file by reading independently
  std::ifstream file(path, std::ios::binary);
  std::string file_content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
  EXPECT_EQ(buf.size(), file_content.size());
  EXPECT_EQ(std::memcmp(buf.data(), file_content.data(), file_content.size()), 0);
}

// =============================================================================
// Stress Tests
// =============================================================================

TEST_F(IoUtilTest, Stress_RapidAllocateDestroy) {
  // Allocate and destroy 1000 buffers in a tight loop - no crash or leak
  for (int i = 0; i < 1000; ++i) {
    auto buf = AlignedBuffer::allocate(1024);
    ASSERT_TRUE(buf.valid()) << "Allocation failed on iteration " << i;
    buf.data()[0] = static_cast<uint8_t>(i & 0xFF);
  }
}

TEST_F(IoUtilTest, Stress_MultipleSimultaneousBuffers) {
  std::vector<AlignedBuffer> buffers;
  buffers.reserve(100);

  for (int i = 0; i < 100; ++i) {
    buffers.push_back(AlignedBuffer::allocate(4096));
    ASSERT_TRUE(buffers.back().valid()) << "Allocation failed on iteration " << i;
    // Write a distinct marker to each buffer
    buffers.back().data()[0] = static_cast<uint8_t>(i);
  }

  // Verify all buffers are still valid and retain their data
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(buffers[i].valid());
    EXPECT_EQ(buffers[i].data()[0], static_cast<uint8_t>(i))
        << "Buffer " << i << " data was corrupted";
  }
}

TEST_F(IoUtilTest, Stress_ManySmallAllocations) {
  // 10000 small allocations of 64 bytes each
  std::vector<AlignedBuffer> buffers;
  buffers.reserve(10000);

  for (int i = 0; i < 10000; ++i) {
    buffers.push_back(AlignedBuffer::allocate(64));
    ASSERT_TRUE(buffers.back().valid()) << "Allocation failed on iteration " << i;
  }

  // Verify all buffers are valid
  for (int i = 0; i < 10000; ++i) {
    EXPECT_TRUE(buffers[i].valid());
    EXPECT_EQ(buffers[i].size(), 64);
  }
}
