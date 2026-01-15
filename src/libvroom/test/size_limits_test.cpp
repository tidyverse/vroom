/**
 * @file size_limits_test.cpp
 * @brief Tests for input size limits security feature (issue #270).
 *
 * These tests verify that the library properly enforces size limits to prevent
 * denial-of-service attacks through excessive memory allocation.
 */

#include "libvroom.h"

#include "streaming.h"

#include <cstring>
#include <gtest/gtest.h>
#include <limits>
#include <string>

using namespace libvroom;

// ============================================================================
// SizeLimits STRUCTURE TESTS
// ============================================================================

TEST(SizeLimitsTest, DefaultValues) {
  SizeLimits limits;
  EXPECT_EQ(limits.max_file_size, 10ULL * 1024 * 1024 * 1024); // 10GB
  EXPECT_EQ(limits.max_field_size, 16ULL * 1024 * 1024);       // 16MB
}

TEST(SizeLimitsTest, DefaultsFactory) {
  auto limits = SizeLimits::defaults();
  EXPECT_EQ(limits.max_file_size, 10ULL * 1024 * 1024 * 1024);
  EXPECT_EQ(limits.max_field_size, 16ULL * 1024 * 1024);
}

TEST(SizeLimitsTest, UnlimitedFactory) {
  auto limits = SizeLimits::unlimited();
  EXPECT_EQ(limits.max_file_size, 0);
  EXPECT_EQ(limits.max_field_size, 0);
}

TEST(SizeLimitsTest, StrictFactory) {
  auto limits = SizeLimits::strict();
  EXPECT_EQ(limits.max_file_size, 100ULL * 1024 * 1024); // 100MB
  EXPECT_EQ(limits.max_field_size, 1ULL * 1024 * 1024);  // 1MB
}

TEST(SizeLimitsTest, StrictFactoryCustomValues) {
  auto limits = SizeLimits::strict(50ULL * 1024 * 1024, 512 * 1024);
  EXPECT_EQ(limits.max_file_size, 50ULL * 1024 * 1024); // 50MB
  EXPECT_EQ(limits.max_field_size, 512 * 1024);         // 512KB
}

// ============================================================================
// OVERFLOW DETECTION TESTS
// ============================================================================

TEST(OverflowTest, MultiplyNoOverflow) {
  EXPECT_FALSE(would_overflow_multiply(0, 100));
  EXPECT_FALSE(would_overflow_multiply(100, 0));
  EXPECT_FALSE(would_overflow_multiply(1000, 1000));
  EXPECT_FALSE(would_overflow_multiply(1, std::numeric_limits<size_t>::max()));
}

TEST(OverflowTest, MultiplyOverflow) {
  size_t max = std::numeric_limits<size_t>::max();
  EXPECT_TRUE(would_overflow_multiply(max, 2));
  EXPECT_TRUE(would_overflow_multiply(max / 2 + 1, 2));
  EXPECT_TRUE(would_overflow_multiply(1ULL << 32, 1ULL << 32)); // On 64-bit systems
}

TEST(OverflowTest, AddNoOverflow) {
  EXPECT_FALSE(would_overflow_add(0, 100));
  EXPECT_FALSE(would_overflow_add(100, 0));
  EXPECT_FALSE(would_overflow_add(1000, 1000));
}

TEST(OverflowTest, AddOverflow) {
  size_t max = std::numeric_limits<size_t>::max();
  EXPECT_TRUE(would_overflow_add(max, 1));
  EXPECT_TRUE(would_overflow_add(max - 10, 20));
  EXPECT_TRUE(would_overflow_add(max / 2 + 1, max / 2 + 1));
}

// ============================================================================
// FILE SIZE LIMIT TESTS
// ============================================================================

class FileSizeLimitTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a simple CSV for testing
    small_csv = "a,b,c\n1,2,3\n4,5,6\n";
    small_csv_len = small_csv.size();

    // Create a buffer with padding for SIMD operations
    small_csv_buffer.resize(small_csv_len + 64, 0);
    std::memcpy(small_csv_buffer.data(), small_csv.data(), small_csv_len);
  }

  std::string small_csv;
  size_t small_csv_len;
  std::vector<uint8_t> small_csv_buffer;
};

TEST_F(FileSizeLimitTest, AcceptsFileWithinLimit) {
  Parser parser;
  SizeLimits limits;
  limits.max_file_size = 1000; // 1KB limit

  auto result = parser.parse(small_csv_buffer.data(), small_csv_len, {.limits = limits});

  EXPECT_TRUE(result.success());
}

TEST_F(FileSizeLimitTest, RejectsFileTooLarge) {
  Parser parser;
  SizeLimits limits;
  limits.max_file_size = 10; // Very small limit

  // Parser::parse() no longer throws for parse errors (Issue #281)
  // Instead, errors are returned in result.errors()
  auto result = parser.parse(small_csv_buffer.data(), small_csv_len, {.limits = limits});

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(result.has_fatal_errors());
  EXPECT_EQ(result.errors()[0].code, ErrorCode::FILE_TOO_LARGE);
}

TEST_F(FileSizeLimitTest, RejectsFileTooLargeWithErrorCollector) {
  Parser parser;
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  SizeLimits limits;
  limits.max_file_size = 10; // Very small limit

  auto result =
      parser.parse(small_csv_buffer.data(), small_csv_len, {.errors = &errors, .limits = limits});

  EXPECT_FALSE(result.success());
  EXPECT_TRUE(errors.has_fatal_errors());
  EXPECT_EQ(errors.errors()[0].code, ErrorCode::FILE_TOO_LARGE);
}

TEST_F(FileSizeLimitTest, AllowsWithUnlimitedSize) {
  Parser parser;
  auto limits = SizeLimits::unlimited();

  // Should not throw even with unlimited settings
  auto result = parser.parse(small_csv_buffer.data(), small_csv_len, {.limits = limits});

  EXPECT_TRUE(result.success());
}

// ============================================================================
// INDEX ALLOCATION OVERFLOW TESTS
// ============================================================================

TEST(IndexAllocationTest, ThrowsOnOverflow) {
  TwoPass parser;

  // Attempt to allocate with extreme size that would overflow
  // SIZE_MAX / 8 would overflow when multiplied by sizeof(uint64_t)
  size_t huge_len = std::numeric_limits<size_t>::max() - 10;

  EXPECT_THROW({ parser.init_safe(huge_len, 1, nullptr); }, std::runtime_error);
}

TEST(IndexAllocationTest, ReportsOverflowWithErrorCollector) {
  TwoPass parser;
  ErrorCollector errors(ErrorMode::PERMISSIVE);

  size_t huge_len = std::numeric_limits<size_t>::max() - 10;

  auto idx = parser.init_safe(huge_len, 1, &errors);

  EXPECT_EQ(idx.indexes, nullptr);
  EXPECT_TRUE(errors.has_fatal_errors());
  EXPECT_EQ(errors.errors()[0].code, ErrorCode::INDEX_ALLOCATION_OVERFLOW);
}

TEST(IndexAllocationTest, MultiThreadOverflow) {
  TwoPass parser;
  ErrorCollector errors(ErrorMode::PERMISSIVE);

  // A size that's fine for single thread but overflows with many threads
  // (len + 8) * n_threads would overflow
  size_t len = std::numeric_limits<size_t>::max() / 4;
  size_t n_threads = 8;

  auto idx = parser.init_safe(len, n_threads, &errors);

  EXPECT_EQ(idx.indexes, nullptr);
  EXPECT_TRUE(errors.has_fatal_errors());
}

TEST(IndexAllocationTest, AcceptsNormalSize) {
  TwoPass parser;

  // Normal allocation should succeed
  auto idx = parser.init_safe(1000, 4, nullptr);

  EXPECT_NE(idx.indexes, nullptr);
  EXPECT_NE(idx.n_indexes, nullptr);
}

// ============================================================================
// STREAMING PARSER FIELD SIZE TESTS
// ============================================================================

TEST(StreamingFieldSizeTest, RejectsOversizeField) {
  StreamConfig config;
  config.max_field_size = 10; // Very small limit
  config.parse_header = false;

  StreamParser parser(config);

  // Create a CSV with a field larger than the limit
  std::string csv = "short,thisfieldiswaytoolongandwillberejected,ok\n";

  parser.parse_chunk(csv.data(), csv.size());
  parser.finish();

  const auto& errors = parser.error_collector();
  EXPECT_TRUE(errors.has_errors());

  // Find the FIELD_TOO_LARGE error
  bool found_field_too_large = false;
  for (const auto& err : errors.errors()) {
    if (err.code == ErrorCode::FIELD_TOO_LARGE) {
      found_field_too_large = true;
      break;
    }
  }
  EXPECT_TRUE(found_field_too_large) << "Expected FIELD_TOO_LARGE error";
}

TEST(StreamingFieldSizeTest, AcceptsFieldWithinLimit) {
  StreamConfig config;
  config.max_field_size = 100; // Reasonable limit
  config.parse_header = false;

  StreamParser parser(config);

  std::string csv = "short,medium,ok\n";

  parser.parse_chunk(csv.data(), csv.size());
  parser.finish();

  const auto& errors = parser.error_collector();
  // Should not have any FIELD_TOO_LARGE errors
  for (const auto& err : errors.errors()) {
    EXPECT_NE(err.code, ErrorCode::FIELD_TOO_LARGE);
  }
}

TEST(StreamingFieldSizeTest, DisabledWithZeroLimit) {
  StreamConfig config;
  config.max_field_size = 0; // Disabled
  config.parse_header = false;

  StreamParser parser(config);

  // Large field should be accepted when limit is disabled
  std::string large_field(1000, 'x');
  std::string csv = large_field + ",ok\n";

  parser.parse_chunk(csv.data(), csv.size());
  parser.finish();

  const auto& errors = parser.error_collector();
  for (const auto& err : errors.errors()) {
    EXPECT_NE(err.code, ErrorCode::FIELD_TOO_LARGE);
  }
}

// ============================================================================
// ERROR CODE STRING TESTS
// ============================================================================

TEST(ErrorCodeTest, FileTooLargeString) {
  EXPECT_STREQ(error_code_to_string(ErrorCode::FILE_TOO_LARGE), "FILE_TOO_LARGE");
}

TEST(ErrorCodeTest, IndexAllocationOverflowString) {
  EXPECT_STREQ(error_code_to_string(ErrorCode::INDEX_ALLOCATION_OVERFLOW),
               "INDEX_ALLOCATION_OVERFLOW");
}

TEST(ErrorCodeTest, FieldTooLargeString) {
  EXPECT_STREQ(error_code_to_string(ErrorCode::FIELD_TOO_LARGE), "FIELD_TOO_LARGE");
}

// ============================================================================
// PARSE OPTIONS LIMITS INTEGRATION
// ============================================================================

TEST(ParseOptionsTest, DefaultLimits) {
  ParseOptions opts;
  EXPECT_EQ(opts.limits.max_file_size, SizeLimits::defaults().max_file_size);
  EXPECT_EQ(opts.limits.max_field_size, SizeLimits::defaults().max_field_size);
}

TEST(ParseOptionsTest, CustomLimits) {
  ParseOptions opts;
  opts.limits.max_file_size = 1024;
  opts.limits.max_field_size = 512;

  EXPECT_EQ(opts.limits.max_file_size, 1024);
  EXPECT_EQ(opts.limits.max_field_size, 512);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
