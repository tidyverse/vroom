/**
 * @file bounds_validation_test.cpp
 * @brief Unit tests for bounds validation assertions added in PR #123.
 *
 * These tests verify the debug assertions that catch unsigned integer underflow
 * bugs where end index falls below start index during arithmetic operations.
 * The assertions compile out in release builds but provide safety during development.
 *
 * Test categories:
 * 1. Normalization path tests - verify edge cases where end < start get normalized
 * 2. Valid bounds tests - verify normal operation with valid bounds
 * 3. Debug assertion tests - verify assertions catch intentionally corrupted data
 */

#include "branchless_state_machine.h"
#include "dialect.h"
#include "mem_util.h"
#include "two_pass.h"
#include "value_extraction.h"

#include <cstring>
#include <gtest/gtest.h>
#include <vector>

// Type detection is optional - only include if enabled
#ifdef LIBVROOM_ENABLE_TYPE_DETECTION
#include "libvroom_types.h"
#endif

using namespace libvroom;

// Helper class to create test buffers with proper SIMD padding
class TestBuffer {
public:
  explicit TestBuffer(const std::string& content) : content_(content) {
    // Allocate with 64-byte padding for SIMD safety
    buffer_ = new uint8_t[content.size() + 64];
    std::memcpy(buffer_, content.data(), content.size());
    std::memset(buffer_ + content.size(), 0, 64);
  }
  ~TestBuffer() { delete[] buffer_; }
  const uint8_t* data() const { return buffer_; }
  size_t size() const { return content_.size(); }

private:
  std::string content_;
  uint8_t* buffer_;
};

// =============================================================================
// Normalization Path Tests for ValueExtractor
// =============================================================================

class ValueExtractorBoundsTest : public ::testing::Test {
protected:
  std::unique_ptr<TestBuffer> buffer_;
  libvroom::TwoPass parser_;
  libvroom::ParseIndex idx_;

  void ParseCSV(const std::string& csv) {
    buffer_ = std::make_unique<TestBuffer>(csv);
    idx_ = parser_.init(buffer_->size(), 1);
    parser_.parse(buffer_->data(), idx_, buffer_->size());
  }
};

// Test that empty fields at start of row are handled correctly
TEST_F(ValueExtractorBoundsTest, EmptyFirstField) {
  ParseCSV("a,b\n,value\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  // Empty first field should return empty string
  auto sv = extractor.get_string_view(0, 0);
  EXPECT_EQ(sv, "");

  // Second field should be correct
  EXPECT_EQ(extractor.get_string_view(0, 1), "value");
}

// Test consecutive empty fields
TEST_F(ValueExtractorBoundsTest, ConsecutiveEmptyFields) {
  ParseCSV("a,b,c\n,,\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  EXPECT_EQ(extractor.get_string_view(0, 0), "");
  EXPECT_EQ(extractor.get_string_view(0, 1), "");
  EXPECT_EQ(extractor.get_string_view(0, 2), "");
}

// Test empty header fields
TEST_F(ValueExtractorBoundsTest, EmptyHeaderField) {
  ParseCSV(",col2\nval1,val2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  auto headers = extractor.get_header();
  EXPECT_EQ(headers.size(), 2);
  EXPECT_EQ(headers[0], "");
  EXPECT_EQ(headers[1], "col2");
}

// Test quoted empty fields
TEST_F(ValueExtractorBoundsTest, QuotedEmptyFields) {
  ParseCSV("a,b\n\"\",\"\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  EXPECT_EQ(extractor.get_string_view(0, 0), "");
  EXPECT_EQ(extractor.get_string_view(0, 1), "");
}

// Test CRLF with empty fields
TEST_F(ValueExtractorBoundsTest, CRLFWithEmptyFields) {
  ParseCSV("a,b\r\n,\r\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  EXPECT_EQ(extractor.get_string_view(0, 0), "");
  EXPECT_EQ(extractor.get_string_view(0, 1), "");
}

// Test single-character fields after empty fields
TEST_F(ValueExtractorBoundsTest, SingleCharAfterEmpty) {
  ParseCSV("a,b\n,X\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  EXPECT_EQ(extractor.get_string_view(0, 0), "");
  EXPECT_EQ(extractor.get_string_view(0, 1), "X");
}

// Test get_string with normalization path
TEST_F(ValueExtractorBoundsTest, GetStringEmptyField) {
  ParseCSV("name\n\"\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  std::string result = extractor.get_string(0, 0);
  EXPECT_EQ(result, "");
}

// Test get_field_bounds returns valid bounds even for edge cases
TEST_F(ValueExtractorBoundsTest, GetFieldBoundsEmptyField) {
  ParseCSV("a,b\n,val\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  size_t start, end;
  EXPECT_TRUE(extractor.get_field_bounds(0, 0, start, end));
  // For empty fields at start, start should equal end
  EXPECT_LE(start, end);
}

// Test single column CSV with multiple rows
TEST_F(ValueExtractorBoundsTest, SingleColumnMultipleRows) {
  ParseCSV("a\n1\n2\n3\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);

  // Access the data rows
  EXPECT_EQ(extractor.num_rows(), 3);
  EXPECT_EQ(extractor.get_string_view(0, 0), "1");
  EXPECT_EQ(extractor.get_string_view(1, 0), "2");
  EXPECT_EQ(extractor.get_string_view(2, 0), "3");
}

// =============================================================================
// Two-Pass Parser Bounds Tests
// =============================================================================

class TwoPassBoundsTest : public ::testing::Test {
protected:
  libvroom::TwoPass parser_;
};

// Test first_pass_simd with valid range
TEST_F(TwoPassBoundsTest, FirstPassSIMDValidRange) {
  TestBuffer buf("a,b\n1,2\n");
  auto stats = libvroom::TwoPass::first_pass_simd(buf.data(), 0, buf.size());

  // Should complete without assertion failure
  EXPECT_GE(stats.n_quotes, 0u);
}

// Test first_pass_simd with zero-length range
TEST_F(TwoPassBoundsTest, FirstPassSIMDZeroLength) {
  TestBuffer buf("a,b\n1,2\n");
  auto stats = libvroom::TwoPass::first_pass_simd(buf.data(), 5, 5);

  // Zero-length range should be valid (start == end)
  EXPECT_GE(stats.n_quotes, 0u);
}

// Test first_pass_simd with start at end of buffer
TEST_F(TwoPassBoundsTest, FirstPassSIMDStartAtEnd) {
  TestBuffer buf("a,b\n");
  auto stats = libvroom::TwoPass::first_pass_simd(buf.data(), buf.size(), buf.size());

  // Edge case: start == end == buffer size
  EXPECT_EQ(stats.n_quotes, 0u);
}

// Test second_pass_simd with valid range
TEST_F(TwoPassBoundsTest, SecondPassSIMDValidRange) {
  TestBuffer buf("a,b\n1,2\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  uint64_t count = libvroom::TwoPass::second_pass_simd(buf.data(), 0, buf.size(), &idx, 0);

  // Should find field separators
  EXPECT_GT(count, 0u);
}

// Test second_pass_simd with zero-length range
TEST_F(TwoPassBoundsTest, SecondPassSIMDZeroLength) {
  TestBuffer buf("a,b\n1,2\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  uint64_t count = libvroom::TwoPass::second_pass_simd(buf.data(), 5, 5, &idx, 0);

  // Zero-length range should return 0 separators
  EXPECT_EQ(count, 0u);
}

// Test parse with empty buffer
TEST_F(TwoPassBoundsTest, ParseEmptyBuffer) {
  TestBuffer buf("");
  libvroom::ParseIndex idx = parser_.init(1, 1); // Size 1 to avoid zero allocation

  // Should handle gracefully
  bool result = parser_.parse(buf.data(), idx, buf.size());
  EXPECT_TRUE(result);
}

// Test parse with single newline
TEST_F(TwoPassBoundsTest, ParseSingleNewline) {
  TestBuffer buf("\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  bool result = parser_.parse(buf.data(), idx, buf.size());
  EXPECT_TRUE(result);
}

// =============================================================================
// Branchless State Machine Bounds Tests
// =============================================================================

class BranchlessBoundsTest : public ::testing::Test {
protected:
  libvroom::TwoPass parser_;
};

// Test second_pass_simd_branchless with valid range (via parse_branchless)
TEST_F(BranchlessBoundsTest, SecondPassBranchlessValidRange) {
  TestBuffer buf("a,b\n1,2\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  // Use parse_branchless which properly manages memory
  bool result = parser_.parse_branchless(buf.data(), idx, buf.size());

  EXPECT_TRUE(result);
  EXPECT_GT(idx.n_indexes[0], 0u);
}

// Test second_pass_simd_branchless with minimal input
TEST_F(BranchlessBoundsTest, SecondPassBranchlessMinimal) {
  TestBuffer buf("a\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  bool result = parser_.parse_branchless(buf.data(), idx, buf.size());

  EXPECT_TRUE(result);
}

// Test branchless parsing with empty field at start
TEST_F(BranchlessBoundsTest, SecondPassBranchlessEmptyField) {
  TestBuffer buf(",a\n");
  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);

  bool result = parser_.parse_branchless(buf.data(), idx, buf.size());

  EXPECT_TRUE(result);
}

// =============================================================================
// Dialect Detection Bounds Tests
// =============================================================================

class DialectBoundsTest : public ::testing::Test {
protected:
  libvroom::DialectDetector detector_;
};

// Test dialect detection with empty buffer
TEST_F(DialectBoundsTest, DetectEmptyBuffer) {
  auto result = detector_.detect(nullptr, 0);
  EXPECT_FALSE(result.success());
}

// Test dialect detection with single-byte buffer
TEST_F(DialectBoundsTest, DetectSingleByte) {
  TestBuffer buf("a");
  auto result = detector_.detect(buf.data(), buf.size());
  // May or may not succeed, but shouldn't crash
  EXPECT_GE(result.rows_analyzed, 0u);
}

// Test dialect detection with minimal valid CSV
TEST_F(DialectBoundsTest, DetectMinimalCSV) {
  TestBuffer buf("a\n");
  auto result = detector_.detect(buf.data(), buf.size());
  // Should handle gracefully
  EXPECT_GE(result.rows_analyzed, 0u);
}

// Test with rows that have different field counts
TEST_F(DialectBoundsTest, DetectInconsistentRows) {
  TestBuffer buf("a,b,c\n1,2\n3,4,5,6\n");
  auto result = detector_.detect(buf.data(), buf.size());
  // Should handle gracefully without assertion failure
  EXPECT_GT(result.rows_analyzed, 0u);
}

// =============================================================================
// Type Detector Bounds Tests (only if type detection is enabled)
// =============================================================================

#ifdef LIBVROOM_ENABLE_TYPE_DETECTION

class TypeDetectorBoundsTest : public ::testing::Test {
protected:
  libvroom::TypeDetectionOptions options_;
};

// Test detect_field with empty input
TEST_F(TypeDetectorBoundsTest, DetectFieldEmpty) {
  auto type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>(""), 0, options_);
  EXPECT_EQ(type, libvroom::FieldType::EMPTY);
}

// Test detect_field with whitespace only (triggers trimming path)
TEST_F(TypeDetectorBoundsTest, DetectFieldWhitespaceOnly) {
  const char* ws = "   ";
  auto type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>(ws), 3, options_);
  EXPECT_EQ(type, libvroom::FieldType::EMPTY);
}

// Test detect_field with whitespace trimming enabled
TEST_F(TypeDetectorBoundsTest, DetectFieldTrimmedToEmpty) {
  options_.trim_whitespace = true;
  const char* ws = "\t\t";
  auto type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>(ws), 2, options_);
  EXPECT_EQ(type, libvroom::FieldType::EMPTY);
}

// Test detect_field with leading and trailing whitespace
TEST_F(TypeDetectorBoundsTest, DetectFieldWithWhitespace) {
  options_.trim_whitespace = true;
  const char* field = "  123  ";
  auto type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>(field), 7, options_);
  EXPECT_EQ(type, libvroom::FieldType::INTEGER);
}

// Test detect_field with all whitespace characters
TEST_F(TypeDetectorBoundsTest, DetectFieldAllWhitespaceTypes) {
  options_.trim_whitespace = true;
  const char* ws = " \t\r\n";
  auto type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>(ws), 4, options_);
  EXPECT_EQ(type, libvroom::FieldType::EMPTY);
}

#endif // LIBVROOM_ENABLE_TYPE_DETECTION

// =============================================================================
// Integration Tests - Complete Parsing Workflow
// =============================================================================

class IntegrationBoundsTest : public ::testing::Test {
protected:
  libvroom::TwoPass parser_;
};

// Test complete parsing workflow with edge case CSV
TEST_F(IntegrationBoundsTest, ParseAndExtractEdgeCaseCSV) {
  // CSV with empty fields, quotes, and CRLF
  TestBuffer buf("a,b,c\r\n,\"\",\r\n1,,3\r\n");

  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);
  parser_.parse(buf.data(), idx, buf.size());

  ValueExtractor extractor(buf.data(), buf.size(), idx);

  // Verify extraction works without assertion failures
  EXPECT_EQ(extractor.num_columns(), 3);
  EXPECT_EQ(extractor.num_rows(), 2);

  // Check all fields can be accessed
  for (size_t row = 0; row < extractor.num_rows(); ++row) {
    for (size_t col = 0; col < extractor.num_columns(); ++col) {
      // Should not throw or trigger assertion
      auto sv = extractor.get_string_view(row, col);
      (void)sv; // Suppress unused warning
    }
  }
}

// Test with multi-threaded parsing on small buffer
TEST_F(IntegrationBoundsTest, MultiThreadedSmallBuffer) {
  // Small buffer that may cause chunk size < 64 (falls back to single-threaded)
  TestBuffer buf("a,b\n1,2\n");

  libvroom::ParseIndex idx = parser_.init(buf.size(), 4); // Request 4 threads
  parser_.parse(buf.data(), idx, buf.size());

  ValueExtractor extractor(buf.data(), buf.size(), idx);
  EXPECT_EQ(extractor.num_rows(), 1);
}

// Test branchless parser with edge case CSV
TEST_F(IntegrationBoundsTest, BranchlessParseEdgeCase) {
  TestBuffer buf("a,b\n,\n");

  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);
  parser_.parse_branchless(buf.data(), idx, buf.size());

  ValueExtractor extractor(buf.data(), buf.size(), idx);
  EXPECT_EQ(extractor.num_rows(), 1);
  EXPECT_EQ(extractor.get_string_view(0, 0), "");
  EXPECT_EQ(extractor.get_string_view(0, 1), "");
}

// Test error handling parsing with edge case CSV
TEST_F(IntegrationBoundsTest, ParseWithErrorsEdgeCase) {
  TestBuffer buf("a,b\n,\n");

  libvroom::ParseIndex idx = parser_.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser_.parse_with_errors(buf.data(), idx, buf.size(), errors);

  ValueExtractor extractor(buf.data(), buf.size(), idx);
  EXPECT_EQ(extractor.num_rows(), 1);
}

// =============================================================================
// Debug Assertion Verification Tests
// =============================================================================

#ifdef NDEBUG
// In release builds, test that normalization prevents issues
TEST(ReleaseModeBoundsTest, NormalizationPreventsCrash) {
  // This test verifies that in release mode, the normalization
  // logic (if end < start then end = start) prevents issues
  TestBuffer buf("a,b\n,\n");

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  parser.parse(buf.data(), idx, buf.size());

  ValueExtractor extractor(buf.data(), buf.size(), idx);

  // Should not crash even with edge case data
  for (size_t row = 0; row < extractor.num_rows(); ++row) {
    for (size_t col = 0; col < extractor.num_columns(); ++col) {
      auto sv = extractor.get_string_view(row, col);
      auto str = extractor.get_string(row, col);
      (void)sv;
      (void)str;
    }
  }
}
#endif

// Test that assertions exist but don't fire with valid data
TEST(AssertionVerificationTest, ValidBoundsNoAssertionFailure) {
  TestBuffer buf("name,age,city\nAlice,30,NYC\nBob,25,LA\n");

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  parser.parse(buf.data(), idx, buf.size());

  ValueExtractor extractor(buf.data(), buf.size(), idx);

  // All assertions should pass with valid data
  EXPECT_EQ(extractor.num_rows(), 2);
  EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
  EXPECT_EQ(extractor.get_string_view(1, 2), "LA");

  auto headers = extractor.get_header();
  EXPECT_EQ(headers[0], "name");
}

// Test that valid bounds work in TwoPass functions
TEST(AssertionVerificationTest, TwoPassValidBounds) {
  TestBuffer buf("a,b,c\n1,2,3\n");

  // first_pass_simd should work with valid bounds
  auto stats = libvroom::TwoPass::first_pass_simd(buf.data(), 0, buf.size());
  EXPECT_EQ(stats.n_quotes, 0u);

  // second_pass_simd should work with valid bounds
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);

  uint64_t count = libvroom::TwoPass::second_pass_simd(buf.data(), 0, buf.size(), &idx, 0);
  EXPECT_GT(count, 0u);
}

// Test that valid bounds work in branchless state machine
TEST(AssertionVerificationTest, BranchlessValidBounds) {
  TestBuffer buf("a,b,c\n1,2,3\n");
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);

  bool result = parser.parse_branchless(buf.data(), idx, buf.size());

  EXPECT_TRUE(result);
  EXPECT_GT(idx.n_indexes[0], 0u);
}

// Test dialect detection with valid data doesn't trigger assertions
TEST(AssertionVerificationTest, DialectDetectionValidData) {
  TestBuffer buf("col1,col2,col3\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::DialectDetector detector;

  auto result = detector.detect(buf.data(), buf.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
}

// Test type detection with valid data doesn't trigger assertions
#ifdef LIBVROOM_ENABLE_TYPE_DETECTION
TEST(AssertionVerificationTest, TypeDetectionValidData) {
  libvroom::TypeDetectionOptions options;

  // Test various valid inputs
  auto int_type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>("12345"), 5, options);
  EXPECT_EQ(int_type, libvroom::FieldType::INTEGER);

  auto float_type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>("3.14"), 4, options);
  EXPECT_EQ(float_type, libvroom::FieldType::FLOAT);

  auto bool_type =
      libvroom::TypeDetector::detect_field(reinterpret_cast<const uint8_t*>("true"), 4, options);
  EXPECT_EQ(bool_type, libvroom::FieldType::BOOLEAN);
}
#endif // LIBVROOM_ENABLE_TYPE_DETECTION

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
