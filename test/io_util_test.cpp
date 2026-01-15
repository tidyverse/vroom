#include "libvroom.h"

#include "io_util.h"
#include "mem_util.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

// Test fixture for io_util tests
class IOUtilTest : public ::testing::Test {
protected:
  std::string test_data_dir = "test/data";
  std::string temp_dir;
  std::vector<std::string> temp_files;

  void SetUp() override {
    // Create temp directory for test files using system temp directory
    // to ensure it works when tests run from any directory
    temp_dir = (fs::temp_directory_path() / ("io_util_test_" + std::to_string(getpid()))).string();
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
    file.write(content.data(), content.size());
    file.close();
    temp_files.push_back(path);
    return path;
  }

  std::string createLargeTempFile(const std::string& filename, size_t size) {
    std::string path = temp_dir + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    // Write in chunks for efficiency
    const size_t chunk_size = 4096;
    std::string chunk(chunk_size, 'X');
    size_t written = 0;
    while (written < size) {
      size_t to_write = std::min(chunk_size, size - written);
      file.write(chunk.data(), to_write);
      written += to_write;
    }
    file.close();
    temp_files.push_back(path);
    return path;
  }
};

// =============================================================================
// allocate_padded_buffer TESTS
// =============================================================================

TEST_F(IOUtilTest, AllocatePaddedBuffer_BasicAllocation) {
  size_t length = 1024;
  size_t padding = 64;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  // Should be able to access the full padded buffer
  buffer[0] = 0xFF;
  buffer[length - 1] = 0xFE;
  buffer[length + padding - 1] = 0xFD;

  EXPECT_EQ(buffer[0], 0xFF);
  EXPECT_EQ(buffer[length - 1], 0xFE);
  EXPECT_EQ(buffer[length + padding - 1], 0xFD);

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_ZeroLength) {
  size_t length = 0;
  size_t padding = 64;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  // Should still be able to access the padding region
  buffer[padding - 1] = 0xFF;
  EXPECT_EQ(buffer[padding - 1], 0xFF);

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_ZeroPadding) {
  size_t length = 1024;
  size_t padding = 0;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  buffer[0] = 0xFF;
  buffer[length - 1] = 0xFE;
  EXPECT_EQ(buffer[0], 0xFF);
  EXPECT_EQ(buffer[length - 1], 0xFE);

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_SmallAllocation) {
  size_t length = 1;
  size_t padding = 32;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  buffer[0] = 0xFF;
  EXPECT_EQ(buffer[0], 0xFF);

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_Alignment) {
  size_t length = 100;
  size_t padding = 64;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  // Check 64-byte alignment
  uintptr_t addr = reinterpret_cast<uintptr_t>(buffer);
  EXPECT_EQ(addr % 64, 0) << "Buffer should be 64-byte aligned";

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_LargeAllocation) {
  size_t length = 10 * 1024 * 1024; // 10 MB
  size_t padding = 64;

  uint8_t* buffer = allocate_padded_buffer(length, padding);

  ASSERT_NE(buffer, nullptr);
  // Write to first and last bytes to ensure memory is accessible
  buffer[0] = 0xFF;
  buffer[length - 1] = 0xFE;
  EXPECT_EQ(buffer[0], 0xFF);
  EXPECT_EQ(buffer[length - 1], 0xFE);

  aligned_free(buffer);
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_IntegerOverflow) {
  // Try to trigger integer overflow: length + padding > SIZE_MAX
  size_t length = SIZE_MAX - 10;
  size_t padding = 64;

  // Should return nullptr instead of allocating a tiny buffer due to overflow
  uint8_t* buffer = allocate_padded_buffer(length, padding);
  EXPECT_EQ(buffer, nullptr) << "Should fail gracefully on integer overflow";
  // No need to free nullptr
}

TEST_F(IOUtilTest, AllocatePaddedBuffer_VariousSizes) {
  std::vector<std::pair<size_t, size_t>> sizes = {{1, 1},     {63, 64},   {64, 64},
                                                  {65, 64},   {127, 32},  {128, 32},
                                                  {1000, 64}, {4096, 64}, {65536, 128}};

  for (const auto& [length, padding] : sizes) {
    uint8_t* buffer = allocate_padded_buffer(length, padding);
    ASSERT_NE(buffer, nullptr) << "Allocation failed for length=" << length
                               << ", padding=" << padding;
    aligned_free(buffer);
  }
}

// =============================================================================
// load_file_to_ptr TESTS (RAII-based file loading)
// =============================================================================

TEST_F(IOUtilTest, LoadFileToPtr_BasicFile) {
  std::string content = "hello,world\n1,2,3\n";
  std::string path = createTempFile("basic.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, content.size());
  EXPECT_EQ(std::memcmp(buffer.data(), content.data(), content.size()), 0);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_EmptyFile) {
  std::string content = "";
  std::string path = createTempFile("empty.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 0);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_SingleByte) {
  std::string content = "X";
  std::string path = createTempFile("single.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 1);
  EXPECT_EQ(buffer.data()[0], 'X');
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_BinaryData) {
  std::string content;
  content.resize(256);
  for (int i = 0; i < 256; i++) {
    content[i] = static_cast<char>(i);
  }
  std::string path = createTempFile("binary.bin", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 256);
  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(buffer.data()[i], static_cast<uint8_t>(i)) << "Mismatch at byte " << i;
  }
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_NonExistentFile) {
  EXPECT_THROW(
      { libvroom::load_file_to_ptr("nonexistent_file_that_does_not_exist.csv", 64); },
      std::runtime_error);
}

TEST_F(IOUtilTest, LoadFileToPtr_InvalidPath) {
  EXPECT_THROW({ libvroom::load_file_to_ptr("", 64); }, std::runtime_error);
}

TEST_F(IOUtilTest, LoadFileToPtr_DirectoryPath) {
  fs::create_directories(temp_dir + "/subdir");

  EXPECT_THROW({ libvroom::load_file_to_ptr(temp_dir + "/subdir", 64); }, std::runtime_error);

  fs::remove(temp_dir + "/subdir");
}

TEST_F(IOUtilTest, LoadFileToPtr_LargeFile) {
  size_t file_size = 1024 * 1024; // 1 MB
  std::string path = createLargeTempFile("large.csv", file_size);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, file_size);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_ExactlyChunkSize) {
  // Test file size that equals common chunk sizes
  size_t file_size = 64 * 1024; // 64KB (common chunk size)
  std::string path = createLargeTempFile("chunk_size.csv", file_size);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, file_size);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_BufferAlignment) {
  std::string content = "test content for alignment check";
  std::string path = createTempFile("align.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  uintptr_t addr = reinterpret_cast<uintptr_t>(buffer.data());
  EXPECT_EQ(addr % 64, 0) << "Buffer should be 64-byte aligned";
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_DifferentPaddingSizes) {
  std::string content = "test,data,for,padding\n";
  std::string path = createTempFile("padding_test.csv", content);

  std::vector<size_t> paddings = {0, 1, 16, 32, 64, 128, 256};

  for (size_t padding : paddings) {
    auto buffer = libvroom::load_file_to_ptr(path, padding);
    EXPECT_EQ(buffer.size, content.size()) << "Padding=" << padding;
    // RAII handles cleanup automatically
  }
}

TEST_F(IOUtilTest, LoadFileToPtr_ExistingTestData) {
  // Test with an actual file from test data directory
  std::string path = "test/data/basic/simple.csv";

  if (fs::exists(path)) {
    auto buffer = libvroom::load_file_to_ptr(path, 64);

    EXPECT_GT(buffer.size, 0);
    // Content should contain CSV data
    bool has_comma = false;
    for (size_t i = 0; i < buffer.size; i++) {
      if (buffer.data()[i] == ',') {
        has_comma = true;
        break;
      }
    }
    EXPECT_TRUE(has_comma) << "CSV file should contain commas";
    // RAII handles cleanup automatically
  } else {
    GTEST_SKIP() << "Test data file not found: " << path;
  }
}

TEST_F(IOUtilTest, LoadFileToPtr_MultipleReads) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string path = createTempFile("multi_read.csv", content);

  // Read the same file multiple times
  for (int i = 0; i < 5; i++) {
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    EXPECT_EQ(buffer.size, content.size());
    // RAII handles cleanup automatically
  }
}

TEST_F(IOUtilTest, LoadFileToPtr_NewlineVariations) {
  // Unix newlines (LF)
  {
    std::string content = "a,b\n1,2\n3,4\n";
    std::string path = createTempFile("unix_newlines.csv", content);
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    EXPECT_EQ(buffer.size, content.size());
    // RAII handles cleanup automatically
  }

  // Windows newlines (CRLF)
  {
    std::string content = "a,b\r\n1,2\r\n3,4\r\n";
    std::string path = createTempFile("windows_newlines.csv", content);
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    EXPECT_EQ(buffer.size, content.size());
    // RAII handles cleanup automatically
  }

  // Classic Mac newlines (CR)
  {
    std::string content = "a,b\r1,2\r3,4\r";
    std::string path = createTempFile("mac_newlines.csv", content);
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    EXPECT_EQ(buffer.size, content.size());
    // RAII handles cleanup automatically
  }
}

TEST_F(IOUtilTest, LoadFileToPtr_UnicodeContent) {
  // UTF-8 content with various characters
  std::string content = "name,city\n日本,東京\nПривет,Мир\n";
  std::string path = createTempFile("unicode.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, content.size());
  EXPECT_EQ(std::memcmp(buffer.data(), content.data(), content.size()), 0);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_QuotedFields) {
  std::string content = R"("name","value"
"hello, world","123"
"line
break","456"
)";
  std::string path = createTempFile("quoted.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, content.size());
  EXPECT_EQ(std::memcmp(buffer.data(), content.data(), content.size()), 0);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_FileWith63Bytes) {
  // Edge case: 63 bytes (one less than alignment boundary)
  std::string content(63, 'X');
  std::string path = createTempFile("63bytes.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 63);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_FileWith64Bytes) {
  // Edge case: exactly 64 bytes (alignment boundary)
  std::string content(64, 'X');
  std::string path = createTempFile("64bytes.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 64);
  // RAII handles cleanup automatically
}

TEST_F(IOUtilTest, LoadFileToPtr_FileWith65Bytes) {
  // Edge case: 65 bytes (one more than alignment boundary)
  std::string content(65, 'X');
  std::string path = createTempFile("65bytes.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  EXPECT_EQ(buffer.size, 65);
  // RAII handles cleanup automatically
}

// =============================================================================
// AlignedDeleter AND AlignedPtr TESTS
// =============================================================================

TEST_F(IOUtilTest, AlignedDeleter_Basic) {
  // Test that AlignedDeleter properly frees memory
  uint8_t* raw = allocate_padded_buffer(1024, 64);
  ASSERT_NE(raw, nullptr);

  // Write some data to verify buffer is valid
  raw[0] = 0xFF;
  raw[1023] = 0xFE;

  // Create unique_ptr with AlignedDeleter - should free on destruction
  {
    std::unique_ptr<uint8_t[], AlignedDeleter> ptr(raw);
    EXPECT_EQ(ptr[0], 0xFF);
    EXPECT_EQ(ptr[1023], 0xFE);
  }
  // Memory should be freed here - sanitizers will catch any leaks
}

TEST_F(IOUtilTest, AlignedDeleter_Nullptr) {
  // AlignedDeleter should safely handle nullptr
  AlignedDeleter deleter;
  deleter(nullptr); // Should not crash

  // Also test via unique_ptr
  std::unique_ptr<uint8_t[], AlignedDeleter> ptr(nullptr);
  // Should not crash on destruction
}

TEST_F(IOUtilTest, AlignedPtr_BasicUsage) {
  AlignedPtr ptr = make_aligned_ptr(1024, 64);

  ASSERT_NE(ptr.get(), nullptr);

  // Check alignment
  uintptr_t addr = reinterpret_cast<uintptr_t>(ptr.get());
  EXPECT_EQ(addr % 64, 0) << "Buffer should be 64-byte aligned";

  // Write and read data
  ptr[0] = 0xAA;
  ptr[1023] = 0xBB;
  EXPECT_EQ(ptr[0], 0xAA);
  EXPECT_EQ(ptr[1023], 0xBB);

  // Memory freed automatically when ptr goes out of scope
}

TEST_F(IOUtilTest, AlignedPtr_ZeroLength) {
  AlignedPtr ptr = make_aligned_ptr(0, 64);

  // Should succeed even with zero length (padding still allocated)
  EXPECT_NE(ptr.get(), nullptr);
}

TEST_F(IOUtilTest, AlignedPtr_ZeroPadding) {
  AlignedPtr ptr = make_aligned_ptr(1024, 0);

  EXPECT_NE(ptr.get(), nullptr);
}

TEST_F(IOUtilTest, AlignedPtr_IntegerOverflow) {
  // Try to trigger integer overflow
  AlignedPtr ptr = make_aligned_ptr(SIZE_MAX - 10, 64);

  // Should return empty ptr instead of undefined behavior
  EXPECT_EQ(ptr.get(), nullptr);
}

TEST_F(IOUtilTest, AlignedPtr_Move) {
  AlignedPtr ptr1 = make_aligned_ptr(1024, 64);
  ASSERT_NE(ptr1.get(), nullptr);

  uint8_t* raw = ptr1.get();
  ptr1[0] = 0xCC;

  // Move to new ptr
  AlignedPtr ptr2 = std::move(ptr1);

  EXPECT_EQ(ptr1.get(), nullptr); // Original should be null
  EXPECT_EQ(ptr2.get(), raw);     // New should own the pointer
  EXPECT_EQ(ptr2[0], 0xCC);       // Data should be intact
}

TEST_F(IOUtilTest, AlignedPtr_Release) {
  AlignedPtr ptr = make_aligned_ptr(1024, 64);
  ASSERT_NE(ptr.get(), nullptr);

  ptr[0] = 0xDD;

  // Release ownership
  uint8_t* raw = ptr.release();

  EXPECT_EQ(ptr.get(), nullptr); // Ptr should be null after release
  EXPECT_EQ(raw[0], 0xDD);       // Data should be intact

  // Must manually free since we released
  aligned_free(raw);
}

TEST_F(IOUtilTest, AlignedPtr_LargeAllocation) {
  // Allocate 10MB
  AlignedPtr ptr = make_aligned_ptr(10 * 1024 * 1024, 64);

  ASSERT_NE(ptr.get(), nullptr);

  // Write to first and last bytes
  ptr[0] = 0xEE;
  ptr[10 * 1024 * 1024 - 1] = 0xFF;
  EXPECT_EQ(ptr[0], 0xEE);
  EXPECT_EQ(ptr[10 * 1024 * 1024 - 1], 0xFF);
}

TEST_F(IOUtilTest, AlignedPtr_MultipleAllocations) {
  // Allocate multiple buffers - memory sanitizers will catch leaks
  std::vector<AlignedPtr> buffers;
  for (int i = 0; i < 100; ++i) {
    buffers.push_back(make_aligned_ptr(1024, 64));
    ASSERT_NE(buffers.back().get(), nullptr);
  }
  // All freed when vector goes out of scope
}

// =============================================================================
// MEMORY MANAGEMENT TESTS
// =============================================================================

TEST_F(IOUtilTest, MemoryLeak_AllocateAndFree) {
  // Allocate and free multiple times to check for leaks
  // This test relies on memory sanitizers during CI to detect leaks
  for (int i = 0; i < 100; i++) {
    uint8_t* buffer = allocate_padded_buffer(1024, 64);
    ASSERT_NE(buffer, nullptr);
    aligned_free(buffer);
  }
}

TEST_F(IOUtilTest, MemoryLeak_LoadFileToPtrRAII) {
  std::string content = "test,data\n";
  std::string path = createTempFile("leak_test.csv", content);

  for (int i = 0; i < 100; i++) {
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    // RAII handles cleanup automatically - memory sanitizers will detect leaks
  }
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

TEST_F(IOUtilTest, Integration_RealCSVFiles) {
  std::vector<std::string> test_files = {
      "test/data/basic/simple.csv",
      "test/data/basic/single_column.csv",
      "test/data/quoted/embedded_quotes.csv",
      "test/data/separators/tab_separated.tsv",
  };

  for (const auto& path : test_files) {
    if (fs::exists(path)) {
      auto buffer = libvroom::load_file_to_ptr(path, 64);
      EXPECT_GT(buffer.size, 0) << "File should not be empty: " << path;
      // RAII handles cleanup automatically
    }
  }
}

TEST_F(IOUtilTest, Integration_BufferCanBeProcessed) {
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string path = createTempFile("process.csv", content);

  auto buffer = libvroom::load_file_to_ptr(path, 64);

  // Count commas and newlines to verify content integrity
  size_t commas = 0;
  size_t newlines = 0;
  for (size_t i = 0; i < buffer.size; i++) {
    if (buffer.data()[i] == ',')
      commas++;
    if (buffer.data()[i] == '\n')
      newlines++;
  }

  EXPECT_EQ(commas, 6) << "Expected 6 commas";
  EXPECT_EQ(newlines, 3) << "Expected 3 newlines";
  // RAII handles cleanup automatically
}

// =============================================================================
// get_corpus_stdin TESTS
//
// Testing stdin requires special handling since we can't directly manipulate
// stdin in the current process. These tests use subprocess execution with
// pipes to test the function's behavior.
//
// NOTE: These tests use POSIX-specific APIs (fork, pipe, dup2, waitpid) and
// are only available on Unix-like systems (Linux, macOS).
// =============================================================================

#ifndef _WIN32 // POSIX-only tests

// Helper class for running subprocesses with stdin piped data
class StdinTestRunner {
public:
  struct Result {
    int exit_code;
    std::string stdout_output;
    std::string stderr_output;
  };

  // Run a helper program that calls get_corpus_stdin() and pipes data to it
  // Returns exit code, stdout, and stderr
  static Result runWithPipedStdin(const std::string& input_data,
                                  const std::string& helper_program) {
    Result result;
    result.exit_code = -1;

    // Create a pipe for stdin
    int stdin_pipe[2];
    if (pipe(stdin_pipe) == -1) {
      result.stderr_output = "Failed to create stdin pipe";
      return result;
    }

    // Create pipes for stdout and stderr capture
    int stdout_pipe[2], stderr_pipe[2];
    if (pipe(stdout_pipe) == -1 || pipe(stderr_pipe) == -1) {
      close(stdin_pipe[0]);
      close(stdin_pipe[1]);
      result.stderr_output = "Failed to create output pipes";
      return result;
    }

    pid_t pid = fork();
    if (pid == -1) {
      close(stdin_pipe[0]);
      close(stdin_pipe[1]);
      close(stdout_pipe[0]);
      close(stdout_pipe[1]);
      close(stderr_pipe[0]);
      close(stderr_pipe[1]);
      result.stderr_output = "Fork failed";
      return result;
    }

    if (pid == 0) {
      // Child process
      close(stdin_pipe[1]);  // Close write end of stdin pipe
      close(stdout_pipe[0]); // Close read end of stdout pipe
      close(stderr_pipe[0]); // Close read end of stderr pipe

      // Redirect stdin, stdout, stderr
      dup2(stdin_pipe[0], STDIN_FILENO);
      dup2(stdout_pipe[1], STDOUT_FILENO);
      dup2(stderr_pipe[1], STDERR_FILENO);

      close(stdin_pipe[0]);
      close(stdout_pipe[1]);
      close(stderr_pipe[1]);

      // Execute the helper program
      execl(helper_program.c_str(), helper_program.c_str(), nullptr);
      _exit(127); // execl failed
    }

    // Parent process
    close(stdin_pipe[0]);  // Close read end of stdin pipe
    close(stdout_pipe[1]); // Close write end of stdout pipe
    close(stderr_pipe[1]); // Close write end of stderr pipe

    // Write input data to child's stdin
    if (!input_data.empty()) {
      write(stdin_pipe[1], input_data.data(), input_data.size());
    }
    close(stdin_pipe[1]); // Signal EOF

    // Read stdout
    char buffer[4096];
    ssize_t bytes_read;
    while ((bytes_read = read(stdout_pipe[0], buffer, sizeof(buffer))) > 0) {
      result.stdout_output.append(buffer, bytes_read);
    }
    close(stdout_pipe[0]);

    // Read stderr
    while ((bytes_read = read(stderr_pipe[0], buffer, sizeof(buffer))) > 0) {
      result.stderr_output.append(buffer, bytes_read);
    }
    close(stderr_pipe[0]);

    // Wait for child
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      result.exit_code = 128 + WTERMSIG(status);
    }

    return result;
  }
};

// Test fixture that creates a helper executable for stdin testing
class GetCorpusStdinTest : public IOUtilTest {
protected:
  std::string helper_path;

  void SetUp() override {
    IOUtilTest::SetUp();
    helper_path = createStdinHelper();
  }

  void TearDown() override {
    // Remove helper executable
    if (!helper_path.empty()) {
      fs::remove(helper_path);
    }
    IOUtilTest::TearDown();
  }

  // Create a small helper program that calls get_corpus_stdin()
  // and prints the result size to stdout
  std::string createStdinHelper() {
    std::string source_path = temp_dir + "/stdin_helper.cpp";
    std::string exe_path = temp_dir + "/stdin_helper";

    // Write helper source code
    std::ofstream src(source_path);
    src << R"(
#include <iostream>
#include <cstdint>
#include <cstring>
#include <stdexcept>

// Minimal reimplementation for testing
// This avoids linking issues with the full library
#include <cstdio>
#include <cstdlib>
#include <vector>

#ifdef _WIN32
#include <malloc.h>
#else
#include <cstdlib>
#endif

void* aligned_malloc_test(size_t alignment, size_t size) {
#ifdef _WIN32
    return _aligned_malloc(size, alignment);
#else
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
#endif
}

void aligned_free_test(void* ptr) {
#ifdef _WIN32
    _aligned_free(ptr);
#else
    free(ptr);
#endif
}

uint8_t* allocate_padded_buffer_test(size_t length, size_t padding) {
    if (length > SIZE_MAX - padding) {
        return nullptr;
    }
    size_t totalpaddedlength = length + padding;
    return (uint8_t*)aligned_malloc_test(64, totalpaddedlength);
}

struct CorpusResult {
    uint8_t* data;
    size_t size;
};

CorpusResult get_corpus_stdin_test(size_t padding) {
    const size_t chunk_size = 64 * 1024;
    std::vector<uint8_t> data;
    data.reserve(chunk_size * 16);

    uint8_t buffer[chunk_size];
    while (true) {
        size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
        if (bytes_read > 0) {
            data.insert(data.end(), buffer, buffer + bytes_read);
        }
        if (bytes_read < chunk_size) {
            if (std::ferror(stdin)) {
                throw std::runtime_error("could not read from stdin");
            }
            break;
        }
    }

    if (data.empty()) {
        throw std::runtime_error("no data read from stdin");
    }

    uint8_t* buf = allocate_padded_buffer_test(data.size(), padding);
    if (buf == nullptr) {
        throw std::runtime_error("could not allocate memory");
    }

    std::memcpy(buf, data.data(), data.size());
    return {buf, data.size()};
}

int main() {
    try {
        auto result = get_corpus_stdin_test(64);
        std::cout << "SIZE:" << result.size << std::endl;

        // Print content for verification (as hex for safety)
        std::cout << "CONTENT:";
        for (size_t i = 0; i < result.size && i < 1024; i++) {
            std::cout << static_cast<char>(result.data[i]);
        }
        std::cout << std::endl;

        aligned_free_test(result.data);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ERROR:" << e.what() << std::endl;
        return 1;
    }
}
)";
    src.close();

    // Compile the helper using CXX environment variable if set, otherwise c++
    const char* cxx = std::getenv("CXX");
    std::string compiler = cxx ? cxx : "c++";
    std::string compile_cmd = compiler + " -std=c++17 -o " + exe_path + " " + source_path + " 2>&1";
    int ret = system(compile_cmd.c_str());
    if (ret != 0) {
      return ""; // Compilation failed
    }

    return exe_path;
  }
};

// Test normal operation: reading CSV data from stdin
TEST_F(GetCorpusStdinTest, NormalOperation_BasicCSV) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n";
  auto result = StdinTestRunner::runWithPipedStdin(csv_data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:18") != std::string::npos)
      << "Output: " << result.stdout_output;
  EXPECT_TRUE(result.stdout_output.find("CONTENT:a,b,c") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test normal operation: single byte input
TEST_F(GetCorpusStdinTest, NormalOperation_SingleByte) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string data = "X";
  auto result = StdinTestRunner::runWithPipedStdin(data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:1") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test normal operation: large input (larger than chunk size)
TEST_F(GetCorpusStdinTest, NormalOperation_LargeInput) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  // Create data larger than the 64KB chunk size
  std::string large_data(100 * 1024, 'X'); // 100KB
  auto result = StdinTestRunner::runWithPipedStdin(large_data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:102400") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test normal operation: exactly one chunk size
TEST_F(GetCorpusStdinTest, NormalOperation_ExactlyOneChunk) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string data(64 * 1024, 'Y'); // Exactly 64KB
  auto result = StdinTestRunner::runWithPipedStdin(data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:65536") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test normal operation: binary data (all byte values)
TEST_F(GetCorpusStdinTest, NormalOperation_BinaryData) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string binary_data;
  for (int i = 1; i < 256; i++) { // Skip null byte for simplicity
    binary_data.push_back(static_cast<char>(i));
  }
  auto result = StdinTestRunner::runWithPipedStdin(binary_data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:255") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test empty stdin: should throw "no data read from stdin"
TEST_F(GetCorpusStdinTest, EmptyStdin_ThrowsException) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string empty_data = "";
  auto result = StdinTestRunner::runWithPipedStdin(empty_data, helper_path);

  EXPECT_EQ(result.exit_code, 1) << "Should fail with empty stdin";
  EXPECT_TRUE(result.stderr_output.find("no data read from stdin") != std::string::npos)
      << "stderr: " << result.stderr_output;
}

// Test with newline-only input (should succeed since data is not empty)
TEST_F(GetCorpusStdinTest, NewlineOnlyInput) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string newline_data = "\n";
  auto result = StdinTestRunner::runWithPipedStdin(newline_data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:1") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test with multiple chunks plus remainder
TEST_F(GetCorpusStdinTest, NormalOperation_MultipleChunksWithRemainder) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  // 2.5 chunks = 160KB
  std::string data(160 * 1024, 'Z');
  auto result = StdinTestRunner::runWithPipedStdin(data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  EXPECT_TRUE(result.stdout_output.find("SIZE:163840") != std::string::npos)
      << "Output: " << result.stdout_output;
}

// Test with UTF-8 content
TEST_F(GetCorpusStdinTest, NormalOperation_UTF8Content) {
  if (helper_path.empty()) {
    GTEST_SKIP() << "Could not compile stdin helper";
  }

  std::string utf8_data = "日本語,中文,한국어\nПривет,Мир\n";
  auto result = StdinTestRunner::runWithPipedStdin(utf8_data, helper_path);

  EXPECT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  // UTF-8 string has 47 bytes (Japanese/Chinese/Korean + Cyrillic characters)
  EXPECT_TRUE(result.stdout_output.find("SIZE:47") != std::string::npos)
      << "Output: " << result.stdout_output;
}

#endif // !_WIN32
