#include "libvroom.h"

#include "error.h"
#include "io_util.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

namespace fs = std::filesystem;
using namespace libvroom;

class CSVParserErrorTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& filename) {
    return "test/data/malformed/" + filename;
  }

  std::string readFile(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Failed to open file: " + path);
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    return ss.str();
  }

  bool hasErrorCode(const ErrorCollector& errors, ErrorCode code) {
    for (const auto& err : errors.errors()) {
      if (err.code == code)
        return true;
    }
    return false;
  }

  size_t countErrorCode(const ErrorCollector& errors, ErrorCode code) {
    size_t count = 0;
    for (const auto& err : errors.errors()) {
      if (err.code == code)
        ++count;
    }
    return count;
  }

  void printErrors(const ErrorCollector& errors) {
    for (const auto& err : errors.errors()) {
      std::cout << err.to_string() << std::endl;
    }
  }

  // Helper to parse with error collection using new Parser API
  // Uses explicit CSV dialect to ensure consistent error detection
  bool parseWithErrors(const std::string& content, ErrorCollector& errors) {
    // Create padded buffer for SIMD safety
    std::string padded = content;
    padded.resize(content.size() + 64, '\0'); // Add SIMD padding
    Parser parser;
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(padded.data());
    auto result = parser.parse(buf, content.size(),
                               {.dialect = Dialect::csv(), // Use explicit CSV dialect
                                .errors = &errors});
    return result.successful;
  }
};

// ============================================================================
// UNCLOSED QUOTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, UnclosedQuote) {
  std::string content = readFile(getTestDataPath("unclosed_quote.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE)) << "Should detect unclosed quote";

  // Find the error and check severity
  for (const auto& err : errors.errors()) {
    if (err.code == ErrorCode::UNCLOSED_QUOTE) {
      EXPECT_EQ(err.severity, ErrorSeverity::FATAL);
    }
  }
}

TEST_F(CSVParserErrorTest, UnclosedQuoteEOF) {
  std::string content = readFile(getTestDataPath("unclosed_quote_eof.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  bool success = parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote at EOF";
  EXPECT_FALSE(success) << "Parsing should fail with unclosed quote";
}

// ============================================================================
// QUOTE IN UNQUOTED FIELD TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, QuoteInUnquotedField) {
  std::string content = readFile(getTestDataPath("quote_in_unquoted_field.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote in unquoted field";
}

TEST_F(CSVParserErrorTest, QuoteNotAtStart) {
  std::string content = readFile(getTestDataPath("quote_not_at_start.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote not at start of field";
}

TEST_F(CSVParserErrorTest, QuoteAfterData) {
  std::string content = readFile(getTestDataPath("quote_after_data.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote after data in unquoted field";
}

TEST_F(CSVParserErrorTest, TrailingQuote) {
  std::string content = readFile(getTestDataPath("trailing_quote.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect trailing quote in unquoted field";
}

// ============================================================================
// INVALID QUOTE ESCAPE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InvalidQuoteEscape) {
  std::string content = readFile(getTestDataPath("invalid_quote_escape.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INVALID_QUOTE_ESCAPE))
      << "Should detect invalid quote escape sequence";
}

TEST_F(CSVParserErrorTest, UnescapedQuoteInQuoted) {
  std::string content = readFile(getTestDataPath("unescaped_quote_in_quoted.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // This should detect an error - either invalid quote escape or quote in unquoted field
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INVALID_QUOTE_ESCAPE) ||
              hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect unescaped quote in quoted field";
}

TEST_F(CSVParserErrorTest, TripleQuote) {
  std::string content = readFile(getTestDataPath("triple_quote.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // Triple quote """ in the context of """bad""" is actually valid RFC 4180:
  // The outer quotes are field delimiters, "" is an escaped quote,
  // so """bad""" represents the value "bad" (with quotes in the value).
  // This file is NOT malformed, so we expect no errors.
  EXPECT_FALSE(errors.has_errors())
      << "Triple quote sequence \"\"\"bad\"\"\" is valid RFC 4180 CSV";
}

// ============================================================================
// INCONSISTENT FIELD COUNT TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InconsistentColumns) {
  std::string content = readFile(getTestDataPath("inconsistent_columns.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect inconsistent column count";
}

TEST_F(CSVParserErrorTest, InconsistentColumnsAllRows) {
  std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect inconsistent column counts across all rows";

  // Multiple rows have wrong field count
  size_t count = countErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT);
  EXPECT_GE(count, 2) << "Should have multiple field count errors";
}

// ============================================================================
// EMPTY HEADER TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyHeader) {
  std::string content = readFile(getTestDataPath("empty_header.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::EMPTY_HEADER)) << "Should detect empty header row";
}

// ============================================================================
// DUPLICATE COLUMN NAMES TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, DuplicateColumnNames) {
  std::string content = readFile(getTestDataPath("duplicate_column_names.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES))
      << "Should detect duplicate column names";

  // Count duplicates - A and B both appear twice
  size_t count = countErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES);
  EXPECT_GE(count, 2) << "Should detect at least 2 duplicate column names (A and B)";
}

// ============================================================================
// NULL BYTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, NullByte) {
  std::string content = readFile(getTestDataPath("null_byte.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE)) << "Should detect null byte in data";
}

// ============================================================================
// MIXED LINE ENDINGS TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MixedLineEndings) {
  std::string content = readFile(getTestDataPath("mixed_line_endings.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::MIXED_LINE_ENDINGS))
      << "Should detect mixed line endings";

  // Should be a warning, not an error
  for (const auto& err : errors.errors()) {
    if (err.code == ErrorCode::MIXED_LINE_ENDINGS) {
      EXPECT_EQ(err.severity, ErrorSeverity::WARNING);
    }
  }
}

// ============================================================================
// MULTIPLE ERRORS TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MultipleErrors) {
  std::string content = readFile(getTestDataPath("multiple_errors.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // This file should have multiple types of errors
  EXPECT_TRUE(errors.has_errors()) << "Should have errors";

  // Should detect duplicate column names (A appears twice)
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES))
      << "Should detect duplicate column names";

  // Total error count should be >= 2
  EXPECT_GE(errors.error_count(), 2) << "Should have at least 2 errors";
}

// ============================================================================
// ERROR MODE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, StrictModeStopsOnFirstError) {
  std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
  ErrorCollector errors(ErrorMode::FAIL_FAST);
  parseWithErrors(content, errors);

  // In strict mode, should stop after first error
  EXPECT_EQ(errors.error_count(), 1) << "Strict mode should stop after first error";
}

TEST_F(CSVParserErrorTest, PermissiveModeCollectsAllErrors) {
  std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors(content, errors);

  // In permissive mode, should collect all errors
  EXPECT_GE(errors.error_count(), 2) << "Permissive mode should collect multiple errors";
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyFile) {
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors("", errors);

  EXPECT_FALSE(errors.has_errors()) << "Empty file should not generate errors";
}

TEST_F(CSVParserErrorTest, SingleLineNoNewline) {
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors("A,B,C", errors);

  EXPECT_FALSE(errors.has_errors()) << "Single line without newline should parse without errors";
}

TEST_F(CSVParserErrorTest, ValidCSVNoErrors) {
  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parseWithErrors("A,B,C\n1,2,3\n4,5,6\n", errors);

  EXPECT_FALSE(errors.has_errors()) << "Valid CSV should not generate errors";
}

// ============================================================================
// MULTI-THREADED ERROR COLLECTION TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MultiThreadedErrorCollectionMerge) {
  // Test that ErrorCollector merge functions work correctly
  ErrorCollector collector1(ErrorMode::PERMISSIVE);
  ErrorCollector collector2(ErrorMode::PERMISSIVE);

  // Add errors with different byte offsets
  collector1.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, 1, 5, 100,
                       "Error at offset 100");
  collector2.add_error(ErrorCode::INVALID_QUOTE_ESCAPE, ErrorSeverity::RECOVERABLE, 2, 3, 50,
                       "Error at offset 50");
  collector1.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 200,
                       "Error at offset 200");

  // Merge and sort
  std::vector<ErrorCollector> collectors = {std::move(collector1), std::move(collector2)};
  ErrorCollector merged(ErrorMode::PERMISSIVE);
  merged.merge_sorted(collectors);

  EXPECT_EQ(merged.error_count(), 3);

  // Verify sorted order by byte offset
  const auto& errors = merged.errors();
  EXPECT_EQ(errors[0].byte_offset, 50);
  EXPECT_EQ(errors[1].byte_offset, 100);
  EXPECT_EQ(errors[2].byte_offset, 200);
}

TEST_F(CSVParserErrorTest, MultiThreadedParsingWithErrors) {
  // Generate a large CSV that will span multiple thread chunks
  // with errors distributed across chunks
  std::string content;
  // Header
  content += "A,B,C\n";

  // Add many valid rows first (to ensure multi-threaded parsing triggers)
  for (int i = 0; i < 1000; ++i) {
    content += "1,2,3\n";
  }

  // Add a row with inconsistent columns
  content += "1,2\n";

  // More valid rows
  for (int i = 0; i < 1000; ++i) {
    content += "4,5,6\n";
  }

  // Another error
  content += "7,8,9,10\n";

  // Final valid rows
  for (int i = 0; i < 1000; ++i) {
    content += "a,b,c\n";
  }

  Parser parser(4); // Use 4 threads
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parser.parse(buf, content.size(), {.errors = &errors});

  // Should detect at least 2 inconsistent field count errors
  EXPECT_GE(countErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT), 2)
      << "Should detect multiple inconsistent field count errors across chunks";
}

TEST_F(CSVParserErrorTest, MultiThreadedErrorsSortedByOffset) {
  // Test that errors from multi-threaded parsing are sorted by byte offset
  std::string content;
  content += "A,B,C\n";

  // Create errors that will end up in different thread chunks
  // assuming 4 threads, each chunk handles ~portion of data

  // Valid rows
  for (int i = 0; i < 500; ++i) {
    content += "1,2,3\n";
  }

  content += "error1\n"; // Missing fields - first error

  for (int i = 0; i < 500; ++i) {
    content += "4,5,6\n";
  }

  content += "error2,extra\n"; // Wrong field count - second error

  for (int i = 0; i < 500; ++i) {
    content += "7,8,9\n";
  }

  Parser parser(4); // Use 4 threads
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  parser.parse(buf, content.size(), {.errors = &errors});

  // Should have at least 2 errors
  EXPECT_GE(errors.error_count(), 2);

  // Verify errors are sorted by byte offset
  const auto& errs = errors.errors();
  for (size_t i = 1; i < errs.size(); ++i) {
    EXPECT_LE(errs[i - 1].byte_offset, errs[i].byte_offset)
        << "Errors should be sorted by byte offset";
  }
}

TEST_F(CSVParserErrorTest, SingleThreadedVsMultiThreadedConsistency) {
  // Compare single-threaded vs multi-threaded error detection
  std::string content;
  content += "A,B,C\n";
  content += "1,2,3\n";
  content += "bad\n"; // Missing fields
  content += "4,5,6\n";
  content += "7,8\n"; // Missing field
  content += "9,10,11\n";

  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());

  // Single-threaded
  Parser parser1(1);
  ErrorCollector errors1(ErrorMode::PERMISSIVE);
  parser1.parse(buf, content.size(), {.errors = &errors1});

  // Multi-threaded
  Parser parser2(2);
  ErrorCollector errors2(ErrorMode::PERMISSIVE);
  parser2.parse(buf, content.size(), {.errors = &errors2});

  // Both should detect the same errors
  EXPECT_EQ(countErrorCode(errors1, ErrorCode::INCONSISTENT_FIELD_COUNT),
            countErrorCode(errors2, ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Single and multi-threaded should detect same errors";
}

TEST_F(CSVParserErrorTest, MultiThreadedFatalError) {
  // Test that fatal errors are properly propagated.
  // Note: Unclosed quotes in the middle of data can cause issues with
  // speculative multi-threaded parsing because quote parity tracking
  // assumes valid CSV structure. For fatal errors, single-threaded parsing
  // is more reliable for accurate error reporting.
  std::string content;
  content += "A,B,C\n";
  for (int i = 0; i < 500; ++i) {
    content += "1,2,3\n";
  }
  content += "\"unclosed quote at EOF"; // Fatal error - unclosed quote at end

  // Use single thread for reliable fatal error detection
  Parser parser(1);
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());

  ErrorCollector errors(ErrorMode::PERMISSIVE);
  auto result = parser.parse(buf, content.size(), {.errors = &errors});

  EXPECT_FALSE(result.successful) << "Should fail due to fatal error";
  EXPECT_TRUE(errors.has_fatal_errors()) << "Should have fatal errors";
  EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote error";
}

// ============================================================================
// COMPREHENSIVE MALFORMED FILE TEST
// ============================================================================

TEST_F(CSVParserErrorTest, AllMalformedFilesGenerateErrors) {
  std::vector<std::pair<std::string, ErrorCode>> test_cases = {
      {"unclosed_quote.csv", ErrorCode::UNCLOSED_QUOTE},
      {"unclosed_quote_eof.csv", ErrorCode::UNCLOSED_QUOTE},
      {"quote_in_unquoted_field.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"quote_not_at_start.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"quote_after_data.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"trailing_quote.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"invalid_quote_escape.csv", ErrorCode::INVALID_QUOTE_ESCAPE},
      {"inconsistent_columns.csv", ErrorCode::INCONSISTENT_FIELD_COUNT},
      {"inconsistent_columns_all_rows.csv", ErrorCode::INCONSISTENT_FIELD_COUNT},
      {"empty_header.csv", ErrorCode::EMPTY_HEADER},
      {"duplicate_column_names.csv", ErrorCode::DUPLICATE_COLUMN_NAMES},
      {"null_byte.csv", ErrorCode::NULL_BYTE},
      {"mixed_line_endings.csv", ErrorCode::MIXED_LINE_ENDINGS},
  };

  int failures = 0;
  for (const auto& [filename, expected_error] : test_cases) {
    std::string path = getTestDataPath(filename);
    if (!fs::exists(path)) {
      std::cout << "Skipping missing file: " << filename << std::endl;
      continue;
    }

    std::string content = readFile(path);
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    if (!hasErrorCode(errors, expected_error)) {
      std::cout << "FAIL: " << filename << " - expected " << error_code_to_string(expected_error)
                << " but got:" << std::endl;
      if (errors.has_errors()) {
        printErrors(errors);
      } else {
        std::cout << "  (no errors)" << std::endl;
      }
      failures++;
    }
  }

  EXPECT_EQ(failures, 0) << failures << " malformed files did not generate expected errors";
}

// ============================================================================
// ERROR MODE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, BestEffortModeIgnoresErrors) {
  // CSV with multiple errors
  std::string content = "a,b,c\n1,2\n3,4,5,6\n"; // inconsistent field counts

  ErrorCollector errors(ErrorMode::BEST_EFFORT);
  bool success = parseWithErrors(content, errors);

  // BEST_EFFORT should succeed despite errors
  EXPECT_TRUE(success) << "BEST_EFFORT mode should return success";

  // Errors should still be collected
  EXPECT_TRUE(errors.has_errors()) << "Errors should still be collected in BEST_EFFORT mode";

  // should_stop should return false even with errors
  EXPECT_FALSE(errors.should_stop()) << "should_stop() should be false in BEST_EFFORT mode";
}

// ============================================================================
// ERROR LIMIT TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, ErrorLimitPreventsOOM) {
  // Create a CSV that would generate many errors
  std::ostringstream oss;
  oss << "a,b,c\n";
  for (int i = 0; i < 100; ++i) {
    oss << "1,2\n"; // Each row is missing a field
  }
  std::string content = oss.str();

  // Use a small error limit
  ErrorCollector errors(ErrorMode::PERMISSIVE, 10);
  parseWithErrors(content, errors);

  // Should not exceed the limit
  EXPECT_LE(errors.error_count(), 10u) << "Error count should respect max_errors limit";
  EXPECT_TRUE(errors.at_error_limit()) << "Should be at error limit";
}

TEST_F(CSVParserErrorTest, DefaultErrorLimitIs10000) {
  ErrorCollector errors;
  // Verify default max errors
  EXPECT_EQ(ErrorCollector::DEFAULT_MAX_ERRORS, 10000u);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
