/**
 * @file simd_error_detection_test.cpp
 * @brief Unit tests for SIMD-optimized error detection edge cases.
 *
 * These tests exercise the error detection paths in the SIMD second-pass
 * implementation, specifically targeting:
 * - Cross-block error detection (errors spanning 64-byte boundaries)
 * - Quote-in-unquoted-field logic using bitwise operations
 * - Null byte handling in final partial blocks
 * - Multi-threaded error merging scenarios
 * - SIMD vs scalar implementation consistency
 *
 * @see branchless_state_machine.h for process_block_simd_branchless_with_errors
 * @see GitHub issue #383
 */

#include "libvroom.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

using namespace libvroom;

// ============================================================================
// TEST FIXTURE
// ============================================================================

class SIMDErrorDetectionTest : public ::testing::Test {
protected:
  // Helper to parse with error collection
  // Note: Creates a padded copy of content for SIMD safety (LIBVROOM_PADDING)
  bool parseWithErrors(const std::string& content, ErrorCollector& errors, int n_threads = 1) {
    std::string padded = content;
    padded.resize(content.size() + 64, '\0'); // Add SIMD padding
    Parser parser(n_threads);
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());
    auto result = parser.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors});
    return result.successful;
  }

  // Helper to check if specific error code is present
  bool hasErrorCode(const ErrorCollector& errors, ErrorCode code) {
    for (const auto& err : errors.errors()) {
      if (err.code == code)
        return true;
    }
    return false;
  }

  // Helper to count errors of a specific type
  size_t countErrorCode(const ErrorCollector& errors, ErrorCode code) {
    size_t count = 0;
    for (const auto& err : errors.errors()) {
      if (err.code == code)
        ++count;
    }
    return count;
  }

  // Helper to get error at specific byte offset (approximate)
  const ParseError* getErrorNear(const ErrorCollector& errors, size_t offset,
                                 size_t tolerance = 5) {
    for (const auto& err : errors.errors()) {
      if (err.byte_offset >= offset - tolerance && err.byte_offset <= offset + tolerance) {
        return &err;
      }
    }
    return nullptr;
  }
};

// ============================================================================
// CROSS-BLOCK ERROR DETECTION TESTS (64-BYTE BOUNDARIES)
// ============================================================================

TEST_F(SIMDErrorDetectionTest, QuoteErrorAtExact64ByteBoundary) {
  // Place a quote-in-unquoted-field error exactly at byte 64 (start of second block)
  // First 63 bytes: valid CSV, then quote at position 63 (0-indexed)
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += "1,2,3\n";            // 6 bytes (total: 12)
  content += std::string(51, 'x'); // 51 bytes of padding (total: 63)
  content += "\"";                 // quote at byte 63 (inside unquoted field)
  content += ",4,5\n";             // continue
  content += "6,7,8\n";            // more data

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error at 64-byte boundary";
}

TEST_F(SIMDErrorDetectionTest, QuoteErrorSpanningBlockBoundary) {
  // Create a scenario where unquoted field starts in block 0 and has quote in block 1
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'y'); // 59 bytes (total: 63)
  content += "a";                  // byte 63: start of unquoted field
  content += "\"";                 // byte 64: quote in unquoted field (second block)
  content += ",value\n";           // continue

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error spanning block boundary";

  // Verify the error is near the expected position
  const ParseError* err = getErrorNear(errors, 64);
  EXPECT_NE(err, nullptr) << "Error should be near byte 64";
}

TEST_F(SIMDErrorDetectionTest, NullByteAtBlockBoundary) {
  // Place null byte exactly at byte 64
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(58, 'x'); // 58 bytes (total: 64)
  content += '\0';                 // null at byte 64
  content += ",value\n";           // continue

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte at block boundary";

  const ParseError* err = getErrorNear(errors, 64);
  EXPECT_NE(err, nullptr) << "Null byte error should be near byte 64";
}

TEST_F(SIMDErrorDetectionTest, MultipleErrorsAcrossBlocks) {
  // Create errors in multiple 64-byte blocks
  std::string content;
  content += "A,B,C\n"; // 6 bytes

  // Error in block 0: null byte at position ~30
  content += std::string(24, 'a'); // 24 bytes (total: 30)
  content += '\0';                 // null at ~30
  content += std::string(33, 'b'); // 33 bytes (total: 64)

  // Error in block 1: quote in unquoted at ~70
  content += std::string(5, 'c');  // 5 bytes (total: 69)
  content += "x\"y";               // quote in unquoted at ~70
  content += std::string(56, 'd'); // padding to block 2

  // Error in block 2: another null
  content += '\0';
  content += "\nend\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // Should detect errors in each block
  size_t null_count = countErrorCode(errors, ErrorCode::NULL_BYTE);
  size_t quote_count = countErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);

  EXPECT_GE(null_count, 2) << "Should detect null bytes in multiple blocks";
  EXPECT_GE(quote_count, 1) << "Should detect quote errors";
}

TEST_F(SIMDErrorDetectionTest, ErrorAtLastByteOfBlock) {
  // Error at byte 63 (last byte of first block, 0-indexed)
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(58, 'x'); // 58 bytes (total: 62)
  content += "a\"";                // 'a' at 62, quote at 63 (in unquoted field)
  content += ",B\n";               // continue in block 1

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect error at last byte of block";
}

TEST_F(SIMDErrorDetectionTest, ErrorAtFirstByteOfSecondBlock) {
  // Error at byte 64 (first byte of second block)
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "a";                  // byte 63
  content += '\0';                 // null at byte 64 (start of block 1)
  content += ",B\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect error at first byte of second block";
}

// ============================================================================
// QUOTE-IN-UNQUOTED-FIELD EDGE CASES
// ============================================================================

TEST_F(SIMDErrorDetectionTest, QuoteAfterFieldSeparator) {
  // Quote immediately after separator is valid (starts quoted field)
  std::string content = "A,B,C\n1,\"quoted\",3\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote after separator should be valid";
}

TEST_F(SIMDErrorDetectionTest, QuoteInMiddleOfUnquotedField) {
  // Quote in middle of unquoted field is an error
  std::string content = "A,B,C\n1,val\"ue,3\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote in middle of unquoted field should be detected";
}

TEST_F(SIMDErrorDetectionTest, QuoteAtEndOfUnquotedField) {
  // Quote at end of unquoted field (before separator) is an error
  std::string content = "A,B,C\n1,value\",3\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at end of unquoted field should be detected";
}

TEST_F(SIMDErrorDetectionTest, MultipleQuotesInUnquotedField) {
  // Multiple quotes in unquoted field
  std::string content = "A,B\n1,a\"b\"c\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Multiple quotes in unquoted field should be detected";
  EXPECT_GE(countErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD), 1)
      << "Should detect at least one quote error";
}

TEST_F(SIMDErrorDetectionTest, QuoteAfterQuotedFieldClosed) {
  // Test: "value"x - character after closing quote
  std::string content = "A,B\n\"value\"x,2\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INVALID_QUOTE_ESCAPE))
      << "Character after closing quote should be INVALID_QUOTE_ESCAPE";
}

TEST_F(SIMDErrorDetectionTest, QuoteAtRecordStart) {
  // Quote at start of record is valid
  std::string content = "A,B\n\"quoted\",2\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at record start should be valid";
}

TEST_F(SIMDErrorDetectionTest, QuoteInUnquotedCrossingBlockBoundary) {
  // Unquoted field starts near end of block 0, has quote in block 1
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",longunquoted";      // starts at 59, quote at ~64
  // Adjust to have quote exactly at boundary
  content.resize(63);
  content += "ab"; // bytes 63-64: unquoted content
  content += "\""; // byte 65: quote in unquoted field
  content += ",end\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote in unquoted field crossing block boundary";
}

// ============================================================================
// NULL BYTE HANDLING IN PARTIAL BLOCKS
// ============================================================================

TEST_F(SIMDErrorDetectionTest, NullByteInPartialFinalBlock) {
  // Create a file where the final block is < 64 bytes with null byte
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(60, 'x'); // 60 bytes (total: 66, partial block of 2 bytes)
  content += "\na,b";
  content.push_back('\0'); // null in partial final block
  content += "c\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte in partial final block";
}

TEST_F(SIMDErrorDetectionTest, NullByteAtEndOfPartialBlock) {
  // Null byte near the end of a partial block
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(64, 'x'); // 64 bytes (total: 70)
  content += "ab";
  content.push_back('\0'); // null
  content += "c\n";        // proper line ending

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect null near end of partial block";
}

TEST_F(SIMDErrorDetectionTest, MultipleNullBytesInPartialBlock) {
  // Multiple null bytes in a partial final block
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(64, 'x'); // 64 bytes (total: 70)
  content += "\na";
  content.push_back('\0'); // first null
  content += ",b";
  content.push_back('\0'); // second null
  content += ",c\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_GE(countErrorCode(errors, ErrorCode::NULL_BYTE), 2)
      << "Should detect multiple null bytes in partial block";
}

TEST_F(SIMDErrorDetectionTest, NullByteInVerySmallPartialBlock) {
  // Final block is just a few bytes
  std::string content;
  content += "A,B,C\n";            // 6 bytes
  content += std::string(60, 'x'); // 60 bytes (total: 66)
  content += "\n";                 // newline at 66 (total: 67)
  content.push_back('\0');         // null at 67, partial block size = 3

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect null in very small partial block";
}

TEST_F(SIMDErrorDetectionTest, ValidityMaskCorrectForPartialBlock) {
  // Verify that bytes beyond the partial block length are not flagged
  // by ensuring no false positives when buffer padding contains nulls
  std::string content = "A,B\n1,2\n"; // 8 bytes, partial block

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Padding zeros should not be detected as null bytes";
}

// ============================================================================
// MULTI-THREADED ERROR MERGING SCENARIOS
// ============================================================================

TEST_F(SIMDErrorDetectionTest, ErrorsFromMultipleThreadsAreMerged) {
  // Create content large enough to span multiple thread chunks
  std::string content;
  content += "A,B,C\n";

  // Add enough valid rows to ensure multi-threading
  for (int i = 0; i < 2000; ++i) {
    content += "1,2,3\n";
  }

  // Error in middle section
  content += "a\"b,2,3\n"; // quote in unquoted

  // More valid rows
  for (int i = 0; i < 2000; ++i) {
    content += "4,5,6\n";
  }

  // Error toward end - use inconsistent field count instead of null byte
  // (null byte in multi-threaded context can cause issues with our test setup)
  content += "7,8\n"; // missing field

  // More valid rows
  for (int i = 0; i < 500; ++i) {
    content += "a,b,c\n";
  }

  Parser parser(4); // 4 threads
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parser.parse(buf, content.size(), {.errors = &errors});

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error from one thread";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect field count error from another thread";
}

TEST_F(SIMDErrorDetectionTest, ErrorsFromDifferentPositionsAreCaptured) {
  // Verify that errors at different positions are all captured
  std::string content;
  content += "A,B,C\n";
  content += "1,2,3\n";
  content += "error1\n"; // field count error
  content += "4,5,6\n";
  content += "a\"b,5,6\n"; // quote in unquoted field
  content += "7,8,9\n";
  content += "late\n"; // another field count error
  content += "x,y,z\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // Verify all expected errors are captured
  EXPECT_GE(countErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT), 2)
      << "Should detect both field count errors";
  EXPECT_GE(countErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD), 1)
      << "Should detect quote error";
}

TEST_F(SIMDErrorDetectionTest, ThreadBoundaryErrorDetection) {
  // Create content where errors might land near chunk boundaries
  // Each thread gets approximately (size / n_threads) bytes
  std::string content;
  content += "A,B,C\n";

  // Fill with data to create predictable chunk boundaries
  size_t target_size = 40000; // ~10KB per thread with 4 threads
  while (content.size() < target_size / 4) {
    content += "1,2,3\n";
  }
  content += "err\"or,2,3\n"; // Error near 1/4 point

  while (content.size() < target_size / 2) {
    content += "4,5,6\n";
  }
  content += "x\"y,5,6\n"; // Another quote error near 1/2 point

  while (content.size() < 3 * target_size / 4) {
    content += "7,8,9\n";
  }
  content += "bad\"val,8,9\n"; // Error near 3/4 point

  while (content.size() < target_size) {
    content += "a,b,c\n";
  }

  Parser parser(4); // 4 threads
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parser.parse(buf, content.size(), {.errors = &errors});

  // Should detect multiple quote errors across thread regions
  size_t quote_errors = countErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
  EXPECT_GE(quote_errors, 2) << "Should detect at least 2 quote errors from different regions";
}

TEST_F(SIMDErrorDetectionTest, SingleThreadVsMultiThreadConsistency) {
  // Verify single-threaded and multi-threaded produce same errors
  std::string content;
  content += "A,B,C\n";
  content += "1,val\"ue,3\n"; // quote error
  content += "2,short\n";     // field count error
  content += "5,6,7\n";

  // Add SIMD padding
  std::string padded = content;
  padded.resize(content.size() + 64, '\0');

  const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());

  // Single-threaded
  Parser parser1(1);
  ErrorCollector errors1(ErrorMode::PERMISSIVE);
  parser1.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors1});

  // Multi-threaded
  Parser parser2(2);
  ErrorCollector errors2(ErrorMode::PERMISSIVE);
  parser2.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors2});

  // Should find same errors
  EXPECT_EQ(countErrorCode(errors1, ErrorCode::QUOTE_IN_UNQUOTED_FIELD),
            countErrorCode(errors2, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Single and multi-threaded should find same quote errors";

  EXPECT_EQ(countErrorCode(errors1, ErrorCode::INCONSISTENT_FIELD_COUNT),
            countErrorCode(errors2, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Single and multi-threaded should find same field count errors";
}

// ============================================================================
// PARSING CONSISTENCY TESTS
// ============================================================================

TEST_F(SIMDErrorDetectionTest, ParsingWithQuotedFields) {
  // Test parsing with complex quoted fields produces correct results
  std::string content = "A,B,C\n"
                        "\"contains,comma\",\"has\"\"quote\",plain\n"
                        "\"newline\nfield\",value,123\n"
                        "simple,\"quoted\",data\n";

  // Add SIMD padding
  std::string padded = content;
  padded.resize(content.size() + 64, '\0');

  const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());

  Parser parser(1);
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  auto result = parser.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors});

  EXPECT_TRUE(result.successful) << "Parsing should succeed";
  EXPECT_FALSE(errors.has_errors()) << "No errors expected for valid CSV";
}

TEST_F(SIMDErrorDetectionTest, ParsingWithErrorsStillCompletes) {
  // Verify error detection doesn't prevent parsing completion
  std::string content = "A,B,C\n"
                        "1,2,3\n"
                        "a,b\"c,d\n" // quote error
                        "4,5,6\n";

  // Add SIMD padding
  std::string padded = content;
  padded.resize(content.size() + 64, '\0');

  const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());

  Parser parser(1);
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  auto result = parser.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors});

  // Should have detected the error
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error";

  // Parsing should still complete (permissive mode)
  EXPECT_TRUE(result.successful) << "Parsing should complete in permissive mode";
}

TEST_F(SIMDErrorDetectionTest, ParsingAcrossMultipleBlocks) {
  // Test parsing content spanning multiple 64-byte blocks
  std::string content;
  content += "A,B,C,D\n"; // 4-field header

  // Generate content that spans ~5 blocks (320 bytes)
  // Each row has 4 fields matching the header
  for (int i = 0; i < 50; ++i) {
    content += std::to_string(i) + ",";
    content += "\"value" + std::to_string(i) + "\",";
    content += "plain" + std::to_string(i) + ",";
    content += std::to_string(i * 2) + "\n";
  }

  // Add SIMD padding
  std::string padded = content;
  padded.resize(content.size() + 64, '\0');

  const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());

  Parser parser(1);
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  auto result = parser.parse(buf, content.size(), {.dialect = Dialect::csv(), .errors = &errors});

  EXPECT_TRUE(result.successful) << "Multi-block parsing should succeed";
  EXPECT_FALSE(errors.has_errors()) << "No errors expected for valid multi-block CSV";
}

// ============================================================================
// EDGE CASES FOR INSIDE_BEFORE BITWISE LOGIC
// ============================================================================

TEST_F(SIMDErrorDetectionTest, QuoteAtPosition0OfBlockAfterNewline) {
  // Quote at position 0 of a block that follows a newline
  // This tests proper state tracking across block boundaries
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "\n";                 // newline at byte 63
  content += "\"quoted\",value\n"; // quote at position 64 (after newline)

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // Quote after newline starts a new field, so it should be valid
  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at block start after newline should be valid";
}

TEST_F(SIMDErrorDetectionTest, QuoteAtPosition0ContinuingFromPrevBlock) {
  // Quote at position 0 when we're inside an unquoted field from previous block
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(59, 'x'); // 59 bytes (total: 63)
  content += "y";                  // unquoted field content at byte 63
  content += "\"";                 // quote at byte 64 (pos 0 of block 1) in unquoted field
  content += ",z\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quote at block start continuing unquoted field should be error";
}

TEST_F(SIMDErrorDetectionTest, QuotedFieldCrossingBlockBoundary) {
  // Quoted field that starts before block boundary and ends after
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",\"hello";           // quote at ~60, quoted field spans boundary
  // Now at position 66, still in quoted field
  content += " world\",next\n"; // closing quote after boundary

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Quoted field crossing boundary should be valid";
  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Quoted field that closes should be valid";
}

TEST_F(SIMDErrorDetectionTest, EscapedQuoteCrossingBlockBoundary) {
  // Escaped quote ("") that spans block boundary
  std::string content;
  content += "A,B\n";              // 4 bytes
  content += std::string(55, 'x'); // 55 bytes (total: 59)
  content += ",\"abc";             // start quoted field at ~60, 'c' at 63
  content += "\"\"";               // escaped quote at 64-65 (crosses boundary)
  content += "def\",z\n";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_FALSE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Escaped quote crossing boundary should be valid";
}

// ============================================================================
// UNCLOSED QUOTE DETECTION
// ============================================================================

TEST_F(SIMDErrorDetectionTest, UnclosedQuoteAtEOF) {
  std::string content = "A,B\n\"unclosed";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  bool success = parseWithErrors(content, errors);

  EXPECT_FALSE(success) << "Parsing should fail with unclosed quote";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote at EOF";
}

TEST_F(SIMDErrorDetectionTest, UnclosedQuoteInPartialBlock) {
  // Unclosed quote where EOF is in a partial block
  std::string content;
  content += "A,B,C\n";
  content += std::string(65, 'x'); // Force into second block
  content += "\n\"never closed";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  bool success = parseWithErrors(content, errors);

  EXPECT_FALSE(success) << "Should fail with unclosed quote";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote in partial block";
}

TEST_F(SIMDErrorDetectionTest, UnclosedQuoteSpanningMultipleBlocks) {
  // Quoted field that never closes, spanning multiple blocks
  std::string content;
  content += "A,B\n";
  content += "\"this quoted field";
  content += std::string(100, ' '); // padding to span blocks
  content += "never ends";

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  bool success = parseWithErrors(content, errors);

  EXPECT_FALSE(success) << "Should fail with unclosed quote";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote spanning blocks";
}

// ============================================================================
// SPECIAL CHARACTERS AND EDGE CASES
// ============================================================================

TEST_F(SIMDErrorDetectionTest, ConsecutiveNullBytes) {
  std::string content = "A,B\n1,";
  content.push_back('\0');
  content.push_back('\0');
  content.push_back('\0');
  content += ",2\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_GE(countErrorCode(errors, ErrorCode::NULL_BYTE), 3)
      << "Should detect all consecutive null bytes";
}

TEST_F(SIMDErrorDetectionTest, NullByteInQuotedField) {
  std::string content = "A,B\n\"has";
  content.push_back('\0');
  content += "null\",2\n";
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
      << "Should detect null byte even in quoted field";
}

TEST_F(SIMDErrorDetectionTest, MixedErrorTypes) {
  // Multiple different error types in same file
  std::string content = "A,B,C\n"
                        "1,bad\"quote,3\n"
                        "4,has";
  content.push_back('\0');
  content += "null,6\n"
             "7,8\n"; // missing field (different error type)

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote error";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE)) << "Should detect null byte error";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect field count error";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
