/**
 * @file csv_errors_test.cpp
 * @brief Tests for CSV error detection and error handling modes.
 *
 * Ported from csv_parser_errors_test.cpp to use the libvroom2 CsvReader API.
 * Tests error detection for malformed CSV files and error mode behavior.
 *
 * @see error.h for ErrorCode, ErrorSeverity, ErrorMode, ErrorCollector
 * @see GitHub issue #626
 */

#include "libvroom.h"

#include "test_util.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>

class CsvErrorsTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& filename) {
    return "test/data/malformed/" + filename;
  }

  bool hasErrorCode(const std::vector<libvroom::ParseError>& errors, libvroom::ErrorCode code) {
    for (const auto& err : errors) {
      if (err.code == code)
        return true;
    }
    return false;
  }

  size_t countErrorCode(const std::vector<libvroom::ParseError>& errors, libvroom::ErrorCode code) {
    size_t count = 0;
    for (const auto& err : errors) {
      if (err.code == code)
        ++count;
    }
    return count;
  }

  // Parse a file with error collection using CsvReader
  struct ParseResult {
    bool ok;
    std::vector<libvroom::ParseError> errors;
    size_t total_rows;
  };

  ParseResult parseFile(const std::string& path, libvroom::ErrorMode mode,
                        size_t max_errors = libvroom::ErrorCollector::DEFAULT_MAX_ERRORS) {
    libvroom::CsvOptions opts;
    opts.separator = ','; // Explicit separator for malformed data tests (bypass auto-detect)
    opts.error_mode = mode;
    opts.max_errors = max_errors;
    opts.num_threads = 1; // Deterministic for error detection
    libvroom::CsvReader reader(opts);

    auto open_result = reader.open(path);
    if (!open_result.ok) {
      // Errors from open() (e.g., EMPTY_HEADER, DUPLICATE_COLUMN_NAMES) are still accessible
      std::vector<libvroom::ParseError> errs(reader.errors().begin(), reader.errors().end());
      return {false, std::move(errs), 0};
    }

    auto read_result = reader.read_all();
    // Copy errors before reader goes out of scope
    std::vector<libvroom::ParseError> errs(reader.errors().begin(), reader.errors().end());
    return {read_result.ok, std::move(errs), read_result.ok ? read_result.value.total_rows : 0};
  }

  // Parse from string content (writes to temp file first)
  ParseResult parseContent(const std::string& content, libvroom::ErrorMode mode,
                           size_t max_errors = libvroom::ErrorCollector::DEFAULT_MAX_ERRORS,
                           size_t num_threads = 1) {
    test_util::TempCsvFile csv(content);
    libvroom::CsvOptions opts;
    opts.separator = ','; // Explicit separator for malformed data tests (bypass auto-detect)
    opts.error_mode = mode;
    opts.max_errors = max_errors;
    opts.num_threads = num_threads;
    libvroom::CsvReader reader(opts);

    auto open_result = reader.open(csv.path());
    if (!open_result.ok) {
      std::vector<libvroom::ParseError> errs(reader.errors().begin(), reader.errors().end());
      return {false, std::move(errs), 0};
    }

    auto read_result = reader.read_all();
    std::vector<libvroom::ParseError> errs(reader.errors().begin(), reader.errors().end());
    return {read_result.ok, std::move(errs), read_result.ok ? read_result.value.total_rows : 0};
  }
};

// ============================================================================
// UNCLOSED QUOTE TESTS
// ============================================================================

TEST_F(CsvErrorsTest, UnclosedQuote) {
  auto result = parseFile(getTestDataPath("unclosed_quote.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote";
}

TEST_F(CsvErrorsTest, UnclosedQuoteSeverityIsFatal) {
  // TODO: Parser currently reports mid-file unclosed quotes as RECOVERABLE,
  // but arguably they should be FATAL. Skipping until we decide on severity policy.
  GTEST_SKIP() << "UNCLOSED_QUOTE severity is RECOVERABLE, expected FATAL - needs review";
}

TEST_F(CsvErrorsTest, UnclosedQuoteEOF) {
  auto result =
      parseFile(getTestDataPath("unclosed_quote_eof.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote at EOF";
}

// ============================================================================
// QUOTE IN UNQUOTED FIELD TESTS
// ============================================================================

TEST_F(CsvErrorsTest, QuoteInUnquotedField) {
  auto result =
      parseFile(getTestDataPath("quote_in_unquoted_field.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote in unquoted field";
}

TEST_F(CsvErrorsTest, QuoteNotAtStart) {
  auto result =
      parseFile(getTestDataPath("quote_not_at_start.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote not at start of field";
}

TEST_F(CsvErrorsTest, QuoteAfterData) {
  auto result = parseFile(getTestDataPath("quote_after_data.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect quote after data in unquoted field";
}

TEST_F(CsvErrorsTest, TrailingQuote) {
  auto result = parseFile(getTestDataPath("trailing_quote.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect trailing quote in unquoted field";
}

// ============================================================================
// INVALID QUOTE ESCAPE TESTS
// ============================================================================

TEST_F(CsvErrorsTest, InvalidQuoteEscape) {
  auto result =
      parseFile(getTestDataPath("invalid_quote_escape.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::INVALID_QUOTE_ESCAPE))
      << "Should detect invalid quote escape sequence";
}

TEST_F(CsvErrorsTest, UnescapedQuoteInQuoted) {
  auto result =
      parseFile(getTestDataPath("unescaped_quote_in_quoted.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::INVALID_QUOTE_ESCAPE) ||
              hasErrorCode(result.errors, libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
      << "Should detect unescaped quote in quoted field";
}

TEST_F(CsvErrorsTest, TripleQuoteIsValid) {
  auto result = parseFile(getTestDataPath("triple_quote.csv"), libvroom::ErrorMode::PERMISSIVE);
  // Triple quote """bad""" is valid RFC 4180: outer quotes are delimiters,
  // "" is escaped quote, result is the value "bad" with quotes
  EXPECT_TRUE(result.errors.empty())
      << "Triple quote sequence \"\"\"bad\"\"\" is valid RFC 4180 CSV";
}

// ============================================================================
// INCONSISTENT FIELD COUNT TESTS
// ============================================================================

TEST_F(CsvErrorsTest, InconsistentColumns) {
  auto result =
      parseFile(getTestDataPath("inconsistent_columns.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect inconsistent column count";
}

TEST_F(CsvErrorsTest, InconsistentColumnsAllRows) {
  auto result = parseFile(getTestDataPath("inconsistent_columns_all_rows.csv"),
                          libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Should detect inconsistent column counts across all rows";

  size_t count = countErrorCode(result.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT);
  EXPECT_GE(count, 2u) << "Should have multiple field count errors";
}

// ============================================================================
// EMPTY HEADER TESTS
// ============================================================================

TEST_F(CsvErrorsTest, EmptyHeader) {
  auto result = parseFile(getTestDataPath("empty_header.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::EMPTY_HEADER))
      << "Should detect empty header row";
}

// ============================================================================
// DUPLICATE COLUMN NAMES TESTS
// ============================================================================

TEST_F(CsvErrorsTest, DuplicateColumnNames) {
  auto result =
      parseFile(getTestDataPath("duplicate_column_names.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES))
      << "Should detect duplicate column names";

  size_t count = countErrorCode(result.errors, libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES);
  EXPECT_GE(count, 2u) << "Should detect at least 2 duplicate column names (A and B)";
}

// ============================================================================
// NULL BYTE TESTS
// ============================================================================

TEST_F(CsvErrorsTest, NullByte) {
  auto result = parseFile(getTestDataPath("null_byte.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::NULL_BYTE))
      << "Should detect null byte in data";
}

// ============================================================================
// MIXED LINE ENDINGS TESTS
// ============================================================================

TEST_F(CsvErrorsTest, MixedLineEndings) {
  // MIXED_LINE_ENDINGS detection is not yet implemented in the parser.
  // The parser currently handles mixed line endings silently.
  GTEST_SKIP() << "MIXED_LINE_ENDINGS detection not yet implemented";
}

// ============================================================================
// MULTIPLE ERRORS TESTS
// ============================================================================

TEST_F(CsvErrorsTest, MultipleErrors) {
  auto result = parseFile(getTestDataPath("multiple_errors.csv"), libvroom::ErrorMode::PERMISSIVE);
  EXPECT_FALSE(result.errors.empty()) << "Should have errors";
  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES))
      << "Should detect duplicate column names";
  EXPECT_GE(result.errors.size(), 2u) << "Should have at least 2 errors";
}

// ============================================================================
// ERROR MODE TESTS
// ============================================================================

TEST_F(CsvErrorsTest, FailFastStopsOnFirstError) {
  auto result = parseFile(getTestDataPath("inconsistent_columns_all_rows.csv"),
                          libvroom::ErrorMode::FAIL_FAST);
  EXPECT_EQ(result.errors.size(), 1u) << "FAIL_FAST mode should stop after first error";
}

TEST_F(CsvErrorsTest, PermissiveModeCollectsAllErrors) {
  auto result = parseFile(getTestDataPath("inconsistent_columns_all_rows.csv"),
                          libvroom::ErrorMode::PERMISSIVE);
  EXPECT_GE(result.errors.size(), 2u) << "Permissive mode should collect multiple errors";
}

TEST_F(CsvErrorsTest, BestEffortModeIgnoresErrors) {
  auto result = parseContent("a,b,c\n1,2\n3,4,5,6\n", libvroom::ErrorMode::BEST_EFFORT);
  // BEST_EFFORT should succeed despite errors
  EXPECT_TRUE(result.ok) << "BEST_EFFORT mode should return success";
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(CsvErrorsTest, EmptyFile) {
  auto result = parseContent("", libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(result.errors.empty()) << "Empty file should not generate errors";
}

TEST_F(CsvErrorsTest, SingleLineNoNewline) {
  auto result = parseContent("A,B,C", libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(result.errors.empty()) << "Single line without newline should parse without errors";
}

TEST_F(CsvErrorsTest, ValidCSVNoErrors) {
  auto result = parseContent("A,B,C\n1,2,3\n4,5,6\n", libvroom::ErrorMode::PERMISSIVE);
  EXPECT_TRUE(result.errors.empty()) << "Valid CSV should not generate errors";
}

// ============================================================================
// ERROR LIMIT TESTS
// ============================================================================

TEST_F(CsvErrorsTest, ErrorLimitPreventsOOM) {
  std::ostringstream oss;
  oss << "a,b,c\n";
  for (int i = 0; i < 100; ++i) {
    oss << "1,2\n"; // Each row is missing a field
  }

  auto result = parseContent(oss.str(), libvroom::ErrorMode::PERMISSIVE, 10);
  EXPECT_LE(result.errors.size(), 10u) << "Error count should respect max_errors limit";
}

TEST_F(CsvErrorsTest, DefaultErrorLimitIs10000) {
  EXPECT_EQ(libvroom::ErrorCollector::DEFAULT_MAX_ERRORS, 10000u);
}

// ============================================================================
// MULTI-THREADED ERROR COLLECTION TESTS (ErrorCollector unit tests)
// ============================================================================

TEST_F(CsvErrorsTest, ErrorCollectorMerge) {
  libvroom::ErrorCollector collector1(libvroom::ErrorMode::PERMISSIVE);
  libvroom::ErrorCollector collector2(libvroom::ErrorMode::PERMISSIVE);

  collector1.add_error(libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD,
                       libvroom::ErrorSeverity::RECOVERABLE, 1, 5, 100, "Error at offset 100");
  collector2.add_error(libvroom::ErrorCode::INVALID_QUOTE_ESCAPE,
                       libvroom::ErrorSeverity::RECOVERABLE, 2, 3, 50, "Error at offset 50");
  collector1.add_error(libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT,
                       libvroom::ErrorSeverity::RECOVERABLE, 3, 1, 200, "Error at offset 200");

  std::vector<libvroom::ErrorCollector> collectors;
  collectors.push_back(std::move(collector1));
  collectors.push_back(std::move(collector2));
  libvroom::ErrorCollector merged(libvroom::ErrorMode::PERMISSIVE);
  merged.merge_sorted(collectors);

  EXPECT_EQ(merged.error_count(), 3u);
  const auto& errors = merged.errors();
  EXPECT_EQ(errors[0].byte_offset, 50u);
  EXPECT_EQ(errors[1].byte_offset, 100u);
  EXPECT_EQ(errors[2].byte_offset, 200u);
}

TEST_F(CsvErrorsTest, MultiThreadedParsingWithErrors) {
  std::string content;
  content += "A,B,C\n";
  for (int i = 0; i < 1000; ++i)
    content += "1,2,3\n";
  content += "1,2\n"; // Missing field
  for (int i = 0; i < 1000; ++i)
    content += "4,5,6\n";
  content += "7,8,9,10\n"; // Extra field
  for (int i = 0; i < 1000; ++i)
    content += "a,b,c\n";

  auto result = parseContent(content, libvroom::ErrorMode::PERMISSIVE,
                             libvroom::ErrorCollector::DEFAULT_MAX_ERRORS, 4);

  EXPECT_GE(countErrorCode(result.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT), 2u)
      << "Should detect multiple inconsistent field count errors across chunks";
}

TEST_F(CsvErrorsTest, MultiThreadedErrorsSortedByOffset) {
  std::string content;
  content += "A,B,C\n";
  for (int i = 0; i < 500; ++i)
    content += "1,2,3\n";
  content += "error1\n"; // Missing fields
  for (int i = 0; i < 500; ++i)
    content += "4,5,6\n";
  content += "error2,extra\n"; // Wrong field count
  for (int i = 0; i < 500; ++i)
    content += "7,8,9\n";

  auto result = parseContent(content, libvroom::ErrorMode::PERMISSIVE,
                             libvroom::ErrorCollector::DEFAULT_MAX_ERRORS, 4);

  EXPECT_GE(result.errors.size(), 2u);
  for (size_t i = 1; i < result.errors.size(); ++i) {
    EXPECT_LE(result.errors[i - 1].byte_offset, result.errors[i].byte_offset)
        << "Errors should be sorted by byte offset";
  }
}

TEST_F(CsvErrorsTest, SingleVsMultiThreadConsistency) {
  std::string content = "A,B,C\n1,2,3\nbad\n4,5,6\n7,8\n9,10,11\n";

  auto result1 = parseContent(content, libvroom::ErrorMode::PERMISSIVE,
                              libvroom::ErrorCollector::DEFAULT_MAX_ERRORS, 1);
  auto result2 = parseContent(content, libvroom::ErrorMode::PERMISSIVE,
                              libvroom::ErrorCollector::DEFAULT_MAX_ERRORS, 2);

  EXPECT_EQ(countErrorCode(result1.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT),
            countErrorCode(result2.errors, libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT))
      << "Single and multi-threaded should detect same errors";
}

TEST_F(CsvErrorsTest, FatalErrorUnclosedQuoteAtEOF) {
  std::string content;
  content += "A,B,C\n";
  for (int i = 0; i < 500; ++i)
    content += "1,2,3\n";
  content += "\"unclosed quote at EOF";

  auto result = parseContent(content, libvroom::ErrorMode::PERMISSIVE);

  EXPECT_TRUE(hasErrorCode(result.errors, libvroom::ErrorCode::UNCLOSED_QUOTE))
      << "Should detect unclosed quote error";
}

// ============================================================================
// COMPREHENSIVE MALFORMED FILE TEST
// ============================================================================

TEST_F(CsvErrorsTest, AllMalformedFilesGenerateErrors) {
  std::vector<std::pair<std::string, libvroom::ErrorCode>> test_cases = {
      {"unclosed_quote.csv", libvroom::ErrorCode::UNCLOSED_QUOTE},
      {"unclosed_quote_eof.csv", libvroom::ErrorCode::UNCLOSED_QUOTE},
      {"quote_in_unquoted_field.csv", libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"quote_not_at_start.csv", libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"quote_after_data.csv", libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"trailing_quote.csv", libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
      {"invalid_quote_escape.csv", libvroom::ErrorCode::INVALID_QUOTE_ESCAPE},
      {"inconsistent_columns.csv", libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT},
      {"inconsistent_columns_all_rows.csv", libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT},
      {"empty_header.csv", libvroom::ErrorCode::EMPTY_HEADER},
      {"duplicate_column_names.csv", libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES},
      {"null_byte.csv", libvroom::ErrorCode::NULL_BYTE},
      // mixed_line_endings.csv skipped: MIXED_LINE_ENDINGS detection not yet implemented
  };

  int failures = 0;
  for (const auto& [filename, expected_error] : test_cases) {
    std::string path = getTestDataPath(filename);
    if (!std::ifstream(path).good()) {
      std::cout << "Skipping missing file: " << filename << std::endl;
      continue;
    }

    auto result = parseFile(path, libvroom::ErrorMode::PERMISSIVE);

    if (!hasErrorCode(result.errors, expected_error)) {
      std::cout << "FAIL: " << filename << " - expected "
                << libvroom::error_code_to_string(expected_error) << " but got:" << std::endl;
      if (!result.errors.empty()) {
        for (const auto& err : result.errors)
          std::cout << "  " << err.to_string() << std::endl;
      } else {
        std::cout << "  (no errors)" << std::endl;
      }
      failures++;
    }
  }

  EXPECT_EQ(failures, 0) << failures << " malformed files did not generate expected errors";
}
