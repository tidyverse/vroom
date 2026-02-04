/**
 * @file simd_parsing_test.cpp
 * @brief Tests for SIMD-optimized error detection and boundary edge cases.
 *
 * Ported from simd_error_detection_test.cpp and SIMD-specific tests from
 * csv_parsing_test.cpp to use the libvroom2 CsvReader API.
 *
 * Tests cover:
 * - Cross-block error detection (errors spanning 64-byte boundaries)
 * - Quote-in-unquoted-field logic
 * - Null byte handling in partial blocks
 * - Multi-threaded error merging
 * - SIMD vs scalar consistency
 * - SIMD alignment boundary tests (63, 64, 65, 128 bytes)
 */

#include "libvroom.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

using namespace libvroom;

// Counter to ensure unique file names across all tests
static std::atomic<uint64_t> g_simd_temp_counter{0};

// RAII helper for temporary CSV files
class TempCsv {
public:
  explicit TempCsv(const std::string& content) {
    uint64_t id = g_simd_temp_counter.fetch_add(1);
    path_ = "/tmp/simd_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + ".csv";
    std::ofstream f(path_, std::ios::binary);
    f.write(content.data(), static_cast<std::streamsize>(content.size()));
    f.close();
  }

  ~TempCsv() { std::remove(path_.c_str()); }

  const std::string& path() const { return path_; }

private:
  std::string path_;
};

// ============================================================================
// TEST FIXTURE
// ============================================================================

class SIMDParsingTest : public ::testing::Test {
protected:
  // Helper to parse CSV content with error collection via CsvReader
  struct ParseResult {
    bool opened = false;
    bool read_ok = false;
    std::vector<ParseError> errors;
    size_t row_count = 0;
  };

  ParseResult parseWithErrors(const std::string& content, size_t num_threads = 1) {
    ParseResult result;
    TempCsv csv(content);

    CsvOptions opts;
    opts.error_mode = ErrorMode::PERMISSIVE;
    opts.num_threads = num_threads;

    CsvReader reader(opts);
    auto open_result = reader.open(csv.path());
    result.opened = open_result.ok;
    if (!result.opened)
      return result;

    auto read_result = reader.read_all();
    result.read_ok = read_result.ok;
    result.errors = reader.errors();
    result.row_count = reader.row_count();
    return result;
  }

  // Helper to parse from buffer directly
  ParseResult parseBufferWithErrors(const std::string& content, size_t num_threads = 1) {
    ParseResult result;

    // Create aligned buffer with SIMD padding
    auto buffer = AlignedBuffer::allocate(content.size(), LIBVROOM_PADDING);
    std::memcpy(buffer.data(), content.data(), content.size());

    CsvOptions opts;
    opts.error_mode = ErrorMode::PERMISSIVE;
    opts.num_threads = num_threads;

    CsvReader reader(opts);
    auto open_result = reader.open_from_buffer(std::move(buffer));
    result.opened = open_result.ok;
    if (!result.opened)
      return result;

    auto read_result = reader.read_all();
    result.read_ok = read_result.ok;
    result.errors = reader.errors();
    result.row_count = reader.row_count();
    return result;
  }

  // Helper to check if specific error code is present
  bool hasErrorCode(const std::vector<ParseError>& errors, ErrorCode code) {
    for (const auto& err : errors) {
      if (err.code == code)
        return true;
    }
    return false;
  }

  // Helper to count errors of a specific type
  size_t countErrorCode(const std::vector<ParseError>& errors, ErrorCode code) {
    size_t count = 0;
    for (const auto& err : errors) {
      if (err.code == code)
        ++count;
    }
    return count;
  }
};

// ============================================================================
// CROSS-BLOCK ERROR DETECTION TESTS (64-BYTE BOUNDARIES)
// ============================================================================

TEST_F(SIMDParsingTest, QuoteErrorAtExact64ByteBoundary) {

  // Place a quote-in-unquoted-field error exactly at byte 64 (start of second block)
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += "1,2,3\n";            // 6 bytes (total: 12)
  content += std::string(51, 'x'); // 51 bytes of padding (total: 63)
  content += "\"";                 // quote at byte 63 (inside unquoted field)
  content += ",4,5\n";             // continue
  content += "6,7,8\n";            // more data

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error at 64-byte boundary";
}

TEST_F(SIMDParsingTest, QuoteErrorSpanningBlockBoundary) {

  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'y'); // 59 bytes (total: 63)
  content += "a";                  // byte 63: start of unquoted field
  content += "\"";                 // byte 64: quote in unquoted field (second block)
  content += ",value\n";           // continue

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error spanning block boundary";
}

TEST_F(SIMDParsingTest, NullByteAtBlockBoundary) {

  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(58, 'x'); // 58 bytes (total: 64)
  content += '\0';                 // null at byte 64
  content += ",value\n";           // continue

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte at block boundary";
}

TEST_F(SIMDParsingTest, MultipleErrorsAcrossBlocks) {

  std::string content;
  content += "A,B,C\n"; // 6 bytes

  // Error in block 0: null byte at position ~30
  content += std::string(24, 'a'); // 24 bytes (total: 30)
  content += '\0';                 // null at ~30
  content += std::string(33, 'b'); // 33 bytes (total: 64)

  // Error in block 1: quote in unquoted at ~70
  content += std::string(5, 'c'); // 5 bytes (total: 69)
  content += "x\"y";              // quote in unquoted at ~70
  content += std::string(56, 'd');

  // Error in block 2: another null
  content += '\0';
  content += "\nend\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  size_t null_count = countErrorCode(result.errors, ErrorCode::NULL_BYTE);
  size_t quote_count = countErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);

  EXPECT_GE(null_count, 2) << "Should detect null bytes in multiple blocks";
  EXPECT_GE(quote_count, 1) << "Should detect quote errors";
}

TEST_F(SIMDParsingTest, ErrorAtLastByteOfBlock) {

  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(58, 'x'); // 58 bytes (total: 62)
  content += "a\"";                // 'a' at 62, quote at 63 (in unquoted field)
  content += ",B\n";               // continue in block 1

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect error at last byte of block";
}

TEST_F(SIMDParsingTest, ErrorAtFirstByteOfSecondBlock) {

  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "a";                  // byte 63
  content += '\0';                 // null at byte 64 (start of block 1)
  content += ",B\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect error at first byte of second block";
}

// ============================================================================
// QUOTE-IN-UNQUOTED-FIELD EDGE CASES
// ============================================================================

TEST_F(SIMDParsingTest, QuoteAfterFieldSeparator) {
  std::string content = "A,B,C\n1,\"quoted\",3\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote after separator should be valid";
}

TEST_F(SIMDParsingTest, QuoteInMiddleOfUnquotedField) {

  std::string content = "A,B,C\n1,val\"ue,3\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote in middle of unquoted field should be detected";
}

TEST_F(SIMDParsingTest, QuoteAtEndOfUnquotedField) {

  std::string content = "A,B,C\n1,value\",3\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at end of unquoted field should be detected";
}

TEST_F(SIMDParsingTest, MultipleQuotesInUnquotedField) {

  std::string content = "A,B\n1,a\"b\"c\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Multiple quotes in unquoted field should be detected";
}

TEST_F(SIMDParsingTest, QuoteAfterQuotedFieldClosed) {

  // "value"x - character after closing quote
  std::string content = "A,B\n\"value\"x,2\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::INVALID_QUOTE_ESCAPE))
      << "Character after closing quote should be INVALID_QUOTE_ESCAPE";
}

TEST_F(SIMDParsingTest, QuoteAtRecordStart) {
  std::string content = "A,B\n\"quoted\",2\n";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at record start should be valid";
}

TEST_F(SIMDParsingTest, QuoteInUnquotedCrossingBlockBoundary) {

  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",longunquoted";      // starts at 59
  content.resize(63);
  content += "ab"; // bytes 63-64
  content += "\""; // byte 65: quote in unquoted field
  content += ",end\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote in unquoted field crossing block boundary";
}

// ============================================================================
// NULL BYTE HANDLING IN PARTIAL BLOCKS
// ============================================================================

TEST_F(SIMDParsingTest, NullByteInPartialFinalBlock) {

  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(60, 'x'); // 60 bytes (total: 66)
  content += "\na,b";
  content.push_back('\0');
  content += "c\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte in partial final block";
}

TEST_F(SIMDParsingTest, NullByteAtEndOfPartialBlock) {

  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(64, 'x'); // 64 bytes (total: 70)
  content += "ab";
  content.push_back('\0');
  content += "c\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect null near end of partial block";
}

TEST_F(SIMDParsingTest, MultipleNullBytesInPartialBlock) {

  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(64, 'x'); // 64 bytes (total: 70)
  content += "\na";
  content.push_back('\0');
  content += ",b";
  content.push_back('\0');
  content += ",c\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_GE(countErrorCode(result.errors, ErrorCode::NULL_BYTE), 2)
      << "Should detect multiple null bytes in partial block";
}

TEST_F(SIMDParsingTest, NullByteInVerySmallPartialBlock) {

  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(60, 'x'); // 60 bytes (total: 66)
  content += "\n";                 // newline at 66
  content.push_back('\0');         // null at 67

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect null in very small partial block";
}

TEST_F(SIMDParsingTest, ValidityMaskCorrectForPartialBlock) {
  // Padding zeros should not be detected as null bytes
  std::string content = "A,B\n1,2\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Padding zeros should not be detected as null bytes";
}

// ============================================================================
// MULTI-THREADED ERROR MERGING SCENARIOS
// ============================================================================

TEST_F(SIMDParsingTest, ErrorsFromMultipleThreadsAreMerged) {

  std::string content;
  content += "A,B,C\n";

  for (int i = 0; i < 2000; ++i) {
    content += "1,2,3\n";
  }

  content += "a\"b,2,3\n"; // quote in unquoted

  for (int i = 0; i < 2000; ++i) {
    content += "4,5,6\n";
  }

  content += "7,8\n"; // missing field

  for (int i = 0; i < 500; ++i) {
    content += "a,b,c\n";
  }

  auto result = parseWithErrors(content, 4);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error from one thread";
  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect field count error from another thread";
}

TEST_F(SIMDParsingTest, ErrorsFromDifferentPositionsAreCaptured) {

  std::string content;
  content += "A,B,C\n";
  content += "1,2,3\n";
  content += "error1\n"; // field count error
  content += "4,5,6\n";
  content += "a\"b,5,6\n"; // quote in unquoted field
  content += "7,8,9\n";
  content += "late\n"; // another field count error
  content += "x,y,z\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_GE(countErrorCode(result.errors, ErrorCode::INCONSISTENT_FIELD_COUNT), 2)
      << "Should detect both field count errors";
  EXPECT_GE(countErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD), 1)
      << "Should detect quote error";
}

TEST_F(SIMDParsingTest, ThreadBoundaryErrorDetection) {

  std::string content;
  content += "A,B,C\n";

  size_t target_size = 40000;
  while (content.size() < target_size / 4) {
    content += "1,2,3\n";
  }
  content += "err\"or,2,3\n";

  while (content.size() < target_size / 2) {
    content += "4,5,6\n";
  }
  content += "x\"y,5,6\n";

  while (content.size() < 3 * target_size / 4) {
    content += "7,8,9\n";
  }
  content += "bad\"val,8,9\n";

  while (content.size() < target_size) {
    content += "a,b,c\n";
  }

  auto result = parseWithErrors(content, 4);
  ASSERT_TRUE(result.opened);

  size_t quote_errors = countErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
  EXPECT_GE(quote_errors, 2) << "Should detect at least 2 quote errors from different regions";
}

TEST_F(SIMDParsingTest, SingleThreadVsMultiThreadConsistency) {

  std::string content;
  content += "A,B,C\n";
  content += "1,val\"ue,3\n"; // quote error
  content += "2,short\n";     // field count error
  content += "5,6,7\n";

  auto result1 = parseWithErrors(content, 1);
  auto result2 = parseWithErrors(content, 2);

  ASSERT_TRUE(result1.opened);
  ASSERT_TRUE(result2.opened);

  EXPECT_EQ(countErrorCode(result1.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD),
            countErrorCode(result2.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Single and multi-threaded should find same quote errors";

  EXPECT_EQ(countErrorCode(result1.errors, ErrorCode::INCONSISTENT_FIELD_COUNT),
            countErrorCode(result2.errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Single and multi-threaded should find same field count errors";
}

// ============================================================================
// PARSING CONSISTENCY TESTS
// ============================================================================

TEST_F(SIMDParsingTest, ParsingWithQuotedFields) {
  std::string content = "A,B,C\n"
                        "\"contains,comma\",\"has\"\"quote\",plain\n"
                        "\"newline\nfield\",value,123\n"
                        "simple,\"quoted\",data\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(result.read_ok) << "Parsing should succeed";
  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "No errors expected for valid CSV";
}

TEST_F(SIMDParsingTest, ParsingWithErrorsStillCompletes) {

  std::string content = "A,B,C\n"
                        "1,2,3\n"
                        "a,b\"c,d\n" // quote error
                        "4,5,6\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error";

  // Parsing should still complete in permissive mode
  EXPECT_TRUE(result.read_ok) << "Parsing should complete in permissive mode";
}

TEST_F(SIMDParsingTest, ParsingAcrossMultipleBlocks) {
  std::string content;
  content += "A,B,C,D\n";

  for (int i = 0; i < 50; ++i) {
    content += std::to_string(i) + ",";
    content += "\"value" + std::to_string(i) + "\",";
    content += "plain" + std::to_string(i) + ",";
    content += std::to_string(i * 2) + "\n";
  }

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(result.read_ok) << "Multi-block parsing should succeed";
  EXPECT_EQ(result.errors.size(), 0u) << "No errors expected for valid multi-block CSV";
}

// ============================================================================
// EDGE CASES FOR INSIDE_BEFORE BITWISE LOGIC
// ============================================================================

TEST_F(SIMDParsingTest, QuoteAtPosition0OfBlockAfterNewline) {
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "\n";                 // newline at byte 63
  content += "\"quoted\",value\n"; // quote at position 64

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at block start after newline should be valid";
}

TEST_F(SIMDParsingTest, QuoteAtPosition0ContinuingFromPrevBlock) {

  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "y";                  // unquoted field at byte 63
  content += "\"";                 // quote at byte 64 in unquoted field
  content += ",z\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at block start continuing unquoted field should be error";
}

TEST_F(SIMDParsingTest, QuotedFieldCrossingBlockBoundary) {
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",\"hello";           // quote at ~60, quoted field spans boundary
  content += " world\",next\n";    // closing quote after boundary

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quoted field crossing boundary should be valid";
  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Quoted field that closes should be valid";
}

TEST_F(SIMDParsingTest, EscapedQuoteCrossingBlockBoundary) {
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",\"abc";             // start quoted field at ~60, 'c' at 63
  content += "\"\"";               // escaped quote at 64-65
  content += "def\",z\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Escaped quote crossing boundary should be valid";
}

// ============================================================================
// UNCLOSED QUOTE DETECTION
// ============================================================================

TEST_F(SIMDParsingTest, UnclosedQuoteAtEOF) {

  std::string content = "A,B\n\"unclosed";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote at EOF";
}

TEST_F(SIMDParsingTest, UnclosedQuoteInPartialBlock) {

  std::string content;
  content += "A,B,C\n";
  content += std::string(65, 'x');
  content += "\n\"never closed";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote in partial block";
}

TEST_F(SIMDParsingTest, UnclosedQuoteSpanningMultipleBlocks) {

  std::string content;
  content += "A,B\n";
  content += "\"this quoted field";
  content += std::string(100, ' ');
  content += "never ends";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote spanning blocks";
}

TEST_F(SIMDParsingTest, ClosedQuoteNoTrailingNewline) {
  // A properly closed quoted field with no trailing newline should NOT
  // be reported as an unclosed quote (regression test for false positive)
  std::string content = "A,B\n\"val\",2";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Properly closed quoted field without trailing newline should not be UNCLOSED_QUOTE";
}

TEST_F(SIMDParsingTest, ClosedQuoteSingleColumnNoTrailingNewline) {
  // Single-column CSV where the last field is a properly closed quoted field
  // with no trailing newline. Regression test for false positive in finish().
  std::string content = "A\n\"val\"";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_FALSE(hasErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE))
      << "Single-column closed quoted field without trailing newline should not be UNCLOSED_QUOTE";
}

TEST_F(SIMDParsingTest, UnclosedQuoteReportedExactlyOnce) {
  // Verify no double-reporting of UNCLOSED_QUOTE
  std::string content = "A,B\n\"unclosed";
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_EQ(countErrorCode(result.errors, ErrorCode::UNCLOSED_QUOTE), 1u)
      << "UNCLOSED_QUOTE should be reported exactly once";
}

// ============================================================================
// SPECIAL CHARACTERS AND EDGE CASES
// ============================================================================

TEST_F(SIMDParsingTest, ConsecutiveNullBytes) {

  std::string content = "A,B\n1,";
  content.push_back('\0');
  content.push_back('\0');
  content.push_back('\0');
  content += ",2\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_GE(countErrorCode(result.errors, ErrorCode::NULL_BYTE), 3)
      << "Should detect all consecutive null bytes";
}

TEST_F(SIMDParsingTest, NullByteInQuotedField) {

  std::string content = "A,B\n\"has";
  content.push_back('\0');
  content += "null\",2\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte even in quoted field";
}

TEST_F(SIMDParsingTest, MixedErrorTypes) {

  std::string content = "A,B,C\n"
                        "1,bad\"quote,3\n"
                        "4,has";
  content.push_back('\0');
  content += "null,6\n"
             "7,8\n"; // missing field

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);

  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error";
  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::NULL_BYTE)) << "Should detect null byte error";
  EXPECT_TRUE(hasErrorCode(result.errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect field count error";
}

// ============================================================================
// SIMD ALIGNMENT AND BOUNDARY TESTS
// (Ported from csv_parsing_test.cpp)
// ============================================================================

TEST_F(SIMDParsingTest, ParseData63Bytes) {
  // Data size just under 64 bytes (one SIMD block)
  std::string content;
  for (int i = 0; i < 20; i++) {
    content += "x,";
  }
  content.resize(63);

  auto result = parseBufferWithErrors(content);
  // Just verify no crash - parsing may succeed or fail depending on content validity
  EXPECT_TRUE(result.opened) << "Should handle 63-byte data without crash";
}

TEST_F(SIMDParsingTest, ParseDataAligned64) {
  // Data size exactly 64 bytes (one SIMD block)
  std::string content = "A,B\n";
  while (content.size() < 64) {
    content += "1,2\n";
  }
  content.resize(64);

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle 64-byte aligned data";
}

TEST_F(SIMDParsingTest, ParseData65Bytes) {
  // Data size just over 64 bytes (crosses into second block)
  std::string content;
  for (int i = 0; i < 21; i++) {
    content += "xy,";
  }
  content.resize(65);

  auto result = parseBufferWithErrors(content);
  EXPECT_TRUE(result.opened) << "Should handle 65-byte data without crash";
}

TEST_F(SIMDParsingTest, ParseData128Bytes) {
  // Data size at 128 bytes (2 SIMD blocks)
  std::string content;
  for (int i = 0; i < 42; i++) {
    content += "ab,";
  }
  content.resize(128);

  auto result = parseBufferWithErrors(content);
  EXPECT_TRUE(result.opened) << "Should handle 128-byte data without crash";
}

TEST_F(SIMDParsingTest, ParseDataUnaligned) {
  std::string content = "A,B,C\n1,2,3\n4,5,6\n7,8,9\n"; // 30 bytes, not aligned
  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle unaligned data";
}

// ============================================================================
// QUOTE STATE TRANSITION TESTS AT BOUNDARIES
// (Ported from csv_parsing_test.cpp)
// ============================================================================

TEST_F(SIMDParsingTest, QuotedFieldCrossingMultipleSIMDBlocks) {
  // Quoted field spanning >128 bytes (multiple SIMD blocks)
  std::string content = "A,B\n\"";
  content += std::string(200, 'x'); // 200-byte quoted field
  content += "\",2\n";

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle quoted field spanning multiple SIMD blocks";
}

TEST_F(SIMDParsingTest, ManyRowsWithQuotesStressSIMD) {
  std::string content = "A,B,C\n";
  for (int i = 0; i < 10000; i++) {
    content += "\"" + std::to_string(i) + "\",";
    content += "\"value" + std::to_string(i) + "\",";
    content += "\"data" + std::to_string(i) + "\"\n";
  }

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle many rows with quotes";
}

TEST_F(SIMDParsingTest, AlternatingQuotedUnquotedFields) {
  std::string content;
  for (int i = 0; i < 100; i++) {
    if (i % 2 == 0) {
      content += "\"quoted\",unquoted,\"quoted\"\n";
    } else {
      content += "unquoted,\"quoted\",unquoted\n";
    }
  }

  auto result = parseBufferWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle alternating patterns";
}

TEST_F(SIMDParsingTest, EscapedQuotesStressSIMD) {
  std::string content = "A\n";
  for (int i = 0; i < 100; i++) {
    content += "\"a\"\"b\"\"c\"\"d\"\"e\"\n"; // deeply nested escaped quotes
  }

  auto result = parseWithErrors(content);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle deeply nested quotes";
}

TEST_F(SIMDParsingTest, LargeMultiThreadedMixedQuotePatterns) {
  std::string content;
  for (int i = 0; i < 50000; i++) {
    if (i % 5 == 0) {
      content += "\"q1\",\"q2\",\"q3\"\n";
    } else if (i % 5 == 1) {
      content += "u1,u2,u3\n";
    } else if (i % 5 == 2) {
      content += "\"q1\",u2,\"q3\"\n";
    } else if (i % 5 == 3) {
      content += "u1,\"q2\",u3\n";
    } else {
      content += "\"a\"\"b\",\"c\"\"d\",\"e\"\"f\"\n";
    }
  }

  auto result = parseWithErrors(content, 4);
  ASSERT_TRUE(result.opened);
  EXPECT_TRUE(result.read_ok) << "Parser should handle mixed quote patterns multi-threaded";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
