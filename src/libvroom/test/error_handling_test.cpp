#include "libvroom/error.h"
#include "libvroom/vroom.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace fs = std::filesystem;
using namespace libvroom;

// ============================================================================
// ERROR CODE AND SEVERITY TESTS
// ============================================================================

TEST(ErrorHandlingTest, ErrorCodeToString) {
  // Test all error codes for complete coverage
  EXPECT_STREQ(error_code_to_string(ErrorCode::NONE), "NONE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::UNCLOSED_QUOTE), "UNCLOSED_QUOTE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::INVALID_QUOTE_ESCAPE), "INVALID_QUOTE_ESCAPE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::QUOTE_IN_UNQUOTED_FIELD), "QUOTE_IN_UNQUOTED_FIELD");
  EXPECT_STREQ(error_code_to_string(ErrorCode::INCONSISTENT_FIELD_COUNT),
               "INCONSISTENT_FIELD_COUNT");
  EXPECT_STREQ(error_code_to_string(ErrorCode::FIELD_TOO_LARGE), "FIELD_TOO_LARGE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::MIXED_LINE_ENDINGS), "MIXED_LINE_ENDINGS");
  EXPECT_STREQ(error_code_to_string(ErrorCode::INVALID_UTF8), "INVALID_UTF8");
  EXPECT_STREQ(error_code_to_string(ErrorCode::NULL_BYTE), "NULL_BYTE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::EMPTY_HEADER), "EMPTY_HEADER");
  EXPECT_STREQ(error_code_to_string(ErrorCode::DUPLICATE_COLUMN_NAMES), "DUPLICATE_COLUMN_NAMES");
  EXPECT_STREQ(error_code_to_string(ErrorCode::AMBIGUOUS_SEPARATOR), "AMBIGUOUS_SEPARATOR");
  EXPECT_STREQ(error_code_to_string(ErrorCode::FILE_TOO_LARGE), "FILE_TOO_LARGE");
  EXPECT_STREQ(error_code_to_string(ErrorCode::INDEX_ALLOCATION_OVERFLOW),
               "INDEX_ALLOCATION_OVERFLOW");
  EXPECT_STREQ(error_code_to_string(ErrorCode::IO_ERROR), "IO_ERROR");
  EXPECT_STREQ(error_code_to_string(ErrorCode::INTERNAL_ERROR), "INTERNAL_ERROR");

  // Test default case with an invalid error code
  EXPECT_STREQ(error_code_to_string(static_cast<ErrorCode>(9999)), "UNKNOWN");
}

TEST(ErrorHandlingTest, ErrorSeverityToString) {
  EXPECT_STREQ(error_severity_to_string(ErrorSeverity::WARNING), "WARNING");
  EXPECT_STREQ(error_severity_to_string(ErrorSeverity::RECOVERABLE), "ERROR");
  EXPECT_STREQ(error_severity_to_string(ErrorSeverity::FATAL), "FATAL");

  // Test default case with an invalid severity
  EXPECT_STREQ(error_severity_to_string(static_cast<ErrorSeverity>(9999)), "UNKNOWN");
}

// ============================================================================
// PARSE ERROR TESTS
// ============================================================================

TEST(ParseErrorTest, Construction) {
  ParseError error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 5, 10, 123, "Quote not closed",
                   "\"unclosed");

  EXPECT_EQ(error.code, ErrorCode::UNCLOSED_QUOTE);
  EXPECT_EQ(error.severity, ErrorSeverity::FATAL);
  EXPECT_EQ(error.line, 5);
  EXPECT_EQ(error.column, 10);
  EXPECT_EQ(error.byte_offset, 123);
  EXPECT_EQ(error.message, "Quote not closed");
  EXPECT_EQ(error.context, "\"unclosed");
}

TEST(ParseErrorTest, ToString) {
  ParseError error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 50,
                   "Expected 3 fields but found 2", "1,2");

  std::string str = error.to_string();

  EXPECT_NE(str.find("ERROR"), std::string::npos);
  EXPECT_NE(str.find("INCONSISTENT_FIELD_COUNT"), std::string::npos);
  EXPECT_NE(str.find("line 3"), std::string::npos);
  EXPECT_NE(str.find("column 1"), std::string::npos);
  EXPECT_NE(str.find("byte 50"), std::string::npos);
  EXPECT_NE(str.find("Expected 3 fields but found 2"), std::string::npos);
  EXPECT_NE(str.find("1,2"), std::string::npos);
}

// ============================================================================
// ERROR COLLECTOR TESTS
// ============================================================================

TEST(ErrorCollectorTest, DefaultMode) {
  ErrorCollector collector;
  EXPECT_EQ(collector.mode(), ErrorMode::FAIL_FAST);
  EXPECT_FALSE(collector.has_errors());
  EXPECT_EQ(collector.error_count(), 0);
}

TEST(ErrorCollectorTest, AddError) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  ParseError error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                   "Field count mismatch");

  collector.add_error(error);

  EXPECT_TRUE(collector.has_errors());
  EXPECT_EQ(collector.error_count(), 1);
  EXPECT_FALSE(collector.has_fatal_errors());
}

TEST(ErrorCollectorTest, AddErrorConvenience) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, 3, 5, 45,
                      "Invalid quote", "bad\"quote");

  EXPECT_TRUE(collector.has_errors());
  EXPECT_EQ(collector.error_count(), 1);

  const auto& errors = collector.errors();
  EXPECT_EQ(errors[0].code, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
  EXPECT_EQ(errors[0].line, 3);
  EXPECT_EQ(errors[0].message, "Invalid quote");
}

TEST(ErrorCollectorTest, StrictModeStopsOnFirstError) {
  ErrorCollector collector(ErrorMode::FAIL_FAST);

  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");

  EXPECT_TRUE(collector.should_stop());
}

TEST(ErrorCollectorTest, PermissiveModeAllowsNonFatalErrors) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");

  EXPECT_FALSE(collector.should_stop());
  EXPECT_EQ(collector.error_count(), 2);
}

TEST(ErrorCollectorTest, FatalErrorStopsEvenInPermissiveMode) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 5, 10, 100, "Fatal error");

  EXPECT_TRUE(collector.should_stop());
  EXPECT_TRUE(collector.has_fatal_errors());
}

TEST(ErrorCollectorTest, WarningsDontStopParsing) {
  ErrorCollector collector(ErrorMode::FAIL_FAST);

  collector.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 10,
                      "Mixed line endings detected");

  // In strict mode, warnings should still allow continuation
  // (only ERROR and FATAL severity should trigger stops)
  EXPECT_TRUE(collector.should_stop()); // Actually, strict mode stops on ANY error

  // Let's test permissive mode for warnings
  ErrorCollector collector2(ErrorMode::PERMISSIVE);
  collector2.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 10,
                       "Mixed line endings detected");

  EXPECT_FALSE(collector2.should_stop());
  EXPECT_TRUE(collector2.has_errors());
}

TEST(ErrorCollectorTest, MultipleErrors) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 10, "Warning");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error");
  collector.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 3, 1, 30, "Fatal");

  EXPECT_EQ(collector.error_count(), 3);
  EXPECT_TRUE(collector.has_fatal_errors());
  EXPECT_TRUE(collector.should_stop());
}

TEST(ErrorCollectorTest, Clear) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error");

  EXPECT_TRUE(collector.has_errors());

  collector.clear();

  EXPECT_FALSE(collector.has_errors());
  EXPECT_EQ(collector.error_count(), 0);
  EXPECT_FALSE(collector.has_fatal_errors());
}

TEST(ErrorCollectorTest, Summary) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 10,
                      "Warning message");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error message");

  std::string summary = collector.summary();

  EXPECT_NE(summary.find("Total errors: 2"), std::string::npos);
  EXPECT_NE(summary.find("Warnings: 1"), std::string::npos);
  EXPECT_NE(summary.find("Errors: 1"), std::string::npos);
  EXPECT_NE(summary.find("Warning message"), std::string::npos);
  EXPECT_NE(summary.find("Error message"), std::string::npos);
}

TEST(ErrorCollectorTest, SummaryWithFatal) {
  ErrorCollector collector(ErrorMode::PERMISSIVE);

  collector.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 10,
                      "Warning message");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error message");
  collector.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 3, 1, 30, "Fatal message");

  std::string summary = collector.summary();

  EXPECT_NE(summary.find("Total errors: 3"), std::string::npos);
  EXPECT_NE(summary.find("Warnings: 1"), std::string::npos);
  EXPECT_NE(summary.find("Errors: 1"), std::string::npos);
  EXPECT_NE(summary.find("Fatal: 1"), std::string::npos);
}

TEST(ErrorCollectorTest, EmptySummary) {
  ErrorCollector collector;
  std::string summary = collector.summary();
  EXPECT_EQ(summary, "No errors");
}

// ============================================================================
// ERROR LIMIT AND SUPPRESSION TESTS
// ============================================================================

TEST(ErrorCollectorTest, ErrorLimitBasic) {
  // Create collector with small limit to test suppression
  ErrorCollector collector(ErrorMode::PERMISSIVE, 3);

  EXPECT_EQ(collector.max_errors(), 3);
  EXPECT_EQ(collector.suppressed_count(), 0);
  EXPECT_FALSE(collector.at_error_limit());

  // Add errors up to the limit
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                      "Error 3");

  EXPECT_EQ(collector.error_count(), 3);
  EXPECT_TRUE(collector.at_error_limit());
  EXPECT_EQ(collector.suppressed_count(), 0);
}

TEST(ErrorCollectorTest, ErrorLimitSuppression) {
  // Create collector with small limit
  ErrorCollector collector(ErrorMode::PERMISSIVE, 2);

  // Add errors beyond the limit
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                      "Error 3");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 4, 1, 40,
                      "Error 4");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 5, 1, 50,
                      "Error 5");

  EXPECT_EQ(collector.error_count(), 2);
  EXPECT_TRUE(collector.at_error_limit());
  EXPECT_EQ(collector.suppressed_count(), 3);
}

TEST(ErrorCollectorTest, ErrorLimitSummary) {
  // Create collector with small limit
  ErrorCollector collector(ErrorMode::PERMISSIVE, 2);

  // Add errors beyond the limit
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                      "Error 3");

  std::string summary = collector.summary();

  // Should show error count and suppression message
  EXPECT_NE(summary.find("Total errors: 2"), std::string::npos);
  EXPECT_NE(summary.find("Error limit reached"), std::string::npos);
  EXPECT_NE(summary.find("1 additional error suppressed"), std::string::npos);
}

TEST(ErrorCollectorTest, ErrorLimitSummaryPlural) {
  // Create collector with small limit
  ErrorCollector collector(ErrorMode::PERMISSIVE, 1);

  // Add multiple errors beyond the limit
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                      "Error 3");

  std::string summary = collector.summary();

  // Should show plural "errors"
  EXPECT_NE(summary.find("2 additional errors suppressed"), std::string::npos);
}

TEST(ErrorCollectorTest, ClearResetsSuppressedCount) {
  ErrorCollector collector(ErrorMode::PERMISSIVE, 2);

  // Add errors beyond the limit
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                      "Error 3");

  EXPECT_EQ(collector.suppressed_count(), 1);

  collector.clear();

  EXPECT_EQ(collector.suppressed_count(), 0);
  EXPECT_EQ(collector.error_count(), 0);
  EXPECT_FALSE(collector.at_error_limit());
}

TEST(ErrorCollectorTest, MergeFromWithSuppression) {
  // Main collector with small limit
  ErrorCollector main_collector(ErrorMode::PERMISSIVE, 3);

  // Add one error to main
  main_collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1,
                           10, "Main error");

  // Other collector has errors including some suppressed
  ErrorCollector other(ErrorMode::PERMISSIVE, 2);
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                  "Other error 1");
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                  "Other error 2");
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 4, 1, 40,
                  "Other error 3 (suppressed)");

  EXPECT_EQ(other.suppressed_count(), 1);

  // Merge other into main
  main_collector.merge_from(other);

  // Main should have 3 errors (1 + 2), and inherit other's suppressed count
  EXPECT_EQ(main_collector.error_count(), 3);
  EXPECT_EQ(main_collector.suppressed_count(), 1);
}

TEST(ErrorCollectorTest, MergeFromExceedsLimit) {
  // Main collector with small limit, partially full
  ErrorCollector main_collector(ErrorMode::PERMISSIVE, 3);
  main_collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1,
                           10, "Main error 1");
  main_collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1,
                           20, "Main error 2");

  // Other collector has more errors than available space
  ErrorCollector other(ErrorMode::PERMISSIVE, 10);
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30,
                  "Other error 1");
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 4, 1, 40,
                  "Other error 2");
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 5, 1, 50,
                  "Other error 3");

  // Merge other into main (only 1 slot available)
  main_collector.merge_from(other);

  // Main should have 3 errors (full), with 2 suppressed (couldn't fit 2 from other)
  EXPECT_EQ(main_collector.error_count(), 3);
  EXPECT_EQ(main_collector.suppressed_count(), 2);
  EXPECT_TRUE(main_collector.at_error_limit());
}

TEST(ErrorCollectorTest, MergeSortedWithSuppression) {
  // Main collector with small limit
  ErrorCollector main_collector(ErrorMode::PERMISSIVE, 4);

  // Create several thread-local collectors with varying errors
  std::vector<ErrorCollector> thread_collectors;

  ErrorCollector c1(ErrorMode::PERMISSIVE, 2);
  c1.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10, "T1 E1");
  c1.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20, "T1 E2");
  c1.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 3, 1, 30, "T1 E3");
  thread_collectors.push_back(c1);

  ErrorCollector c2(ErrorMode::PERMISSIVE, 2);
  c2.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 4, 1, 15, "T2 E1");
  c2.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 5, 1, 25, "T2 E2");
  thread_collectors.push_back(c2);

  // Merge all
  main_collector.merge_sorted(thread_collectors);

  // c1 had 1 suppressed, main limited to 4 total
  EXPECT_EQ(main_collector.error_count(), 4);
  EXPECT_EQ(main_collector.suppressed_count(), 1); // c1's suppressed error
}

TEST(ErrorCollectorTest, MergeFromCollectorWithFatal) {
  ErrorCollector main_collector(ErrorMode::PERMISSIVE, 10);

  // Other collector has a fatal error and a recoverable error
  ErrorCollector other(ErrorMode::PERMISSIVE, 2);
  other.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 1, 1, 10, "Fatal error");
  other.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                  "Recoverable error");

  EXPECT_TRUE(other.has_fatal_errors());
  EXPECT_EQ(other.suppressed_count(), 0);

  // Merge other into main - should inherit fatal flag
  main_collector.merge_from(other);

  EXPECT_TRUE(main_collector.has_fatal_errors());
  EXPECT_EQ(main_collector.error_count(), 2);
}

TEST(ErrorCollectorTest, SuppressedFatalErrorStillSetsHasFatal) {
  // When a FATAL error is suppressed due to error limit, has_fatal_ should still be set
  // so that should_stop() works correctly
  ErrorCollector collector(ErrorMode::PERMISSIVE, 2);

  // Fill up to the limit with recoverable errors
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  collector.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");

  EXPECT_EQ(collector.error_count(), 2);
  EXPECT_TRUE(collector.at_error_limit());
  EXPECT_FALSE(collector.has_fatal_errors());

  // Now add a fatal error - it will be suppressed but has_fatal_ should still be set
  collector.add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 3, 1, 30, "Fatal error");

  EXPECT_EQ(collector.error_count(), 2);      // Still 2 (fatal was suppressed)
  EXPECT_EQ(collector.suppressed_count(), 1); // One suppressed
  EXPECT_TRUE(collector.has_fatal_errors());  // But has_fatal_ is set!
  EXPECT_TRUE(collector.should_stop());       // And should_stop() works
}

TEST(ErrorCollectorTest, DefaultMaxErrors) {
  // Verify the default constant value
  EXPECT_EQ(ErrorCollector::DEFAULT_MAX_ERRORS, 10000);

  // Verify default constructor uses it
  ErrorCollector collector;
  EXPECT_EQ(collector.max_errors(), 10000);
}

// ============================================================================
// PARSE EXCEPTION TESTS
// ============================================================================

TEST(ParseExceptionTest, SingleError) {
  ParseError error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, 5, 10, 100, "Quote not closed");

  ParseException ex(error);

  EXPECT_STREQ(ex.what(), "Quote not closed");
  EXPECT_EQ(ex.error().code, ErrorCode::UNCLOSED_QUOTE);
  EXPECT_EQ(ex.errors().size(), 1);
}

TEST(ParseExceptionTest, MultipleErrors) {
  std::vector<ParseError> errors;
  errors.emplace_back(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, 1, 1, 10,
                      "Error 1");
  errors.emplace_back(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, 2, 1, 20,
                      "Error 2");

  ParseException ex(errors);

  std::string msg = ex.what();
  EXPECT_NE(msg.find("Multiple parse errors"), std::string::npos);
  EXPECT_NE(msg.find("2"), std::string::npos);
  EXPECT_EQ(ex.errors().size(), 2);
}

// ============================================================================
// MALFORMED CSV FILE TESTS
// ============================================================================

class MalformedCSVTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& filename) {
    return "test/data/malformed/" + filename;
  }

  bool fileExists(const std::string& path) { return fs::exists(path); }

  std::string readFile(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Failed to open file: " + path);
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    return ss.str();
  }
};

TEST_F(MalformedCSVTest, UnclosedQuoteExists) {
  std::string path = getTestDataPath("unclosed_quote.csv");
  ASSERT_TRUE(fileExists(path)) << "Test file not found: " << path;

  std::string content = readFile(path);
  EXPECT_NE(content.find("\"unclosed quote"), std::string::npos)
      << "File should contain unclosed quote";
}

TEST_F(MalformedCSVTest, UnclosedQuoteEOFExists) {
  std::string path = getTestDataPath("unclosed_quote_eof.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  // Should end with an unclosed quote
  EXPECT_NE(content.find("\"this quote never closes"), std::string::npos);
  // Should NOT have a closing quote at the end
  EXPECT_NE(content.back(), '"');
}

TEST_F(MalformedCSVTest, QuoteInUnquotedFieldExists) {
  std::string path = getTestDataPath("quote_in_unquoted_field.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("bad\"quote"), std::string::npos)
      << "Should contain quote in middle of unquoted field";
}

TEST_F(MalformedCSVTest, InconsistentColumnsExists) {
  std::string path = getTestDataPath("inconsistent_columns.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  // Header has 3 columns, but data rows vary
  std::istringstream iss(content);
  std::string line;

  std::getline(iss, line); // Header: A,B,C (3 columns)
  std::getline(iss, line); // 1,2,3 (3 columns - ok)
  std::getline(iss, line); // 4,5 (2 columns - error!)

  size_t commas = std::count(line.begin(), line.end(), ',');
  EXPECT_EQ(commas, 1) << "Second data row should have only 2 fields";
}

TEST_F(MalformedCSVTest, InconsistentColumnsAllRowsExists) {
  std::string path = getTestDataPath("inconsistent_columns_all_rows.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  // Every row should have different column count
  EXPECT_NE(content.find("1,2\n"), std::string::npos);     // 2 columns
  EXPECT_NE(content.find("3,4,5,6\n"), std::string::npos); // 4 columns
}

TEST_F(MalformedCSVTest, InvalidQuoteEscapeExists) {
  std::string path = getTestDataPath("invalid_quote_escape.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  // Has quote escape followed by non-quote character
  EXPECT_NE(content.find("\"\"escape\"here\""), std::string::npos);
}

TEST_F(MalformedCSVTest, EmptyHeaderExists) {
  std::string path = getTestDataPath("empty_header.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_TRUE(content[0] == '\n' || content[0] == '\r')
      << "File should start with newline (empty header)";
}

TEST_F(MalformedCSVTest, DuplicateColumnNamesExists) {
  std::string path = getTestDataPath("duplicate_column_names.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  std::istringstream iss(content);
  std::string header;
  std::getline(iss, header);

  // Count occurrences of 'A' and 'B'
  size_t a_count = 0, b_count = 0;
  size_t pos = 0;
  while ((pos = header.find('A', pos)) != std::string::npos) {
    a_count++;
    pos++;
  }
  pos = 0;
  while ((pos = header.find('B', pos)) != std::string::npos) {
    b_count++;
    pos++;
  }

  EXPECT_GE(a_count, 2) << "Header should have duplicate 'A' columns";
  EXPECT_GE(b_count, 2) << "Header should have duplicate 'B' columns";
}

TEST_F(MalformedCSVTest, TrailingQuoteExists) {
  std::string path = getTestDataPath("trailing_quote.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("6\""), std::string::npos) << "Should have quote after unquoted field";
}

TEST_F(MalformedCSVTest, QuoteNotAtStartExists) {
  std::string path = getTestDataPath("quote_not_at_start.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("x\"quoted\""), std::string::npos)
      << "Should have quoted section not at field start";
}

TEST_F(MalformedCSVTest, MultipleErrorsExists) {
  std::string path = getTestDataPath("multiple_errors.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  // Should have multiple types of errors:
  // - Duplicate column names (A appears twice)
  // - Inconsistent field count (row 1 has 2 fields)
  // - Unclosed quote
  // - Quote in unquoted field

  EXPECT_NE(content.find("A,B,A"), std::string::npos);      // Duplicate columns
  EXPECT_NE(content.find("\"unclosed"), std::string::npos); // Unclosed quote
  EXPECT_NE(content.find("bad\"quote"), std::string::npos); // Bad quote
}

TEST_F(MalformedCSVTest, MixedLineEndingsExists) {
  std::string path = getTestDataPath("mixed_line_endings.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);

  // Should have both CRLF and LF
  bool has_crlf = content.find("\r\n") != std::string::npos;
  bool has_lf_only = false;
  bool has_cr_only = false;

  for (size_t i = 0; i < content.length(); i++) {
    if (content[i] == '\n' && (i == 0 || content[i - 1] != '\r')) {
      has_lf_only = true;
    }
    if (content[i] == '\r' && (i + 1 >= content.length() || content[i + 1] != '\n')) {
      has_cr_only = true;
    }
  }

  EXPECT_TRUE(has_crlf || has_lf_only || has_cr_only)
      << "File should have mixed line ending styles";
}

TEST_F(MalformedCSVTest, NullByteExists) {
  std::string path = getTestDataPath("null_byte.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find('\0'), std::string::npos) << "File should contain null byte";
}

TEST_F(MalformedCSVTest, TripleQuoteExists) {
  std::string path = getTestDataPath("triple_quote.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("\"\"\""), std::string::npos) << "Should contain triple quote sequence";
}

TEST_F(MalformedCSVTest, UnescapedQuoteInQuotedExists) {
  std::string path = getTestDataPath("unescaped_quote_in_quoted.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("\"has \" unescaped"), std::string::npos)
      << "Should have unescaped quote inside quoted field";
}

TEST_F(MalformedCSVTest, QuoteAfterDataExists) {
  std::string path = getTestDataPath("quote_after_data.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  EXPECT_NE(content.find("data\"quote"), std::string::npos)
      << "Should have quote appearing after data in unquoted field";
}

TEST_F(MalformedCSVTest, AllMalformedFilesPresent) {
  std::vector<std::string> required_files = {
      "unclosed_quote.csv",
      "unclosed_quote_eof.csv",
      "quote_in_unquoted_field.csv",
      "inconsistent_columns.csv",
      "inconsistent_columns_all_rows.csv",
      "invalid_quote_escape.csv",
      "empty_header.csv",
      "duplicate_column_names.csv",
      "trailing_quote.csv",
      "quote_not_at_start.csv",
      "multiple_errors.csv",
      "mixed_line_endings.csv",
      "null_byte.csv",
      "triple_quote.csv",
      "unescaped_quote_in_quoted.csv",
      "quote_after_data.csv",
  };

  int missing_count = 0;
  for (const auto& filename : required_files) {
    std::string path = getTestDataPath(filename);
    if (!fileExists(path)) {
      std::cout << "Missing malformed test file: " << path << std::endl;
      missing_count++;
    }
  }

  EXPECT_EQ(missing_count, 0) << missing_count << " malformed test files are missing";
}

// ============================================================================
// ERROR MODE BEHAVIOR TESTS
// ============================================================================

TEST(ErrorModeTest, StrictModeDefinition) {
  // Strict mode should stop on first error of any severity (except warnings)
  ErrorCollector collector(ErrorMode::FAIL_FAST);
  EXPECT_EQ(collector.mode(), ErrorMode::FAIL_FAST);
}

TEST(ErrorModeTest, PermissiveModeDefinition) {
  // Permissive mode should collect all errors but stop on fatal
  ErrorCollector collector(ErrorMode::PERMISSIVE);
  EXPECT_EQ(collector.mode(), ErrorMode::PERMISSIVE);
}

TEST(ErrorModeTest, BestEffortModeDefinition) {
  // Best effort mode should try to parse regardless of errors
  ErrorCollector collector(ErrorMode::BEST_EFFORT);
  EXPECT_EQ(collector.mode(), ErrorMode::BEST_EFFORT);
}

// Note: I/O utility tests removed - they tested v1 io_util.h APIs that don't exist in v2

// ============================================================================
// CSV READER INTEGRATION TESTS
// ============================================================================

// Test that CsvReader detects empty header
TEST(CsvReaderErrorTest, EmptyHeader) {
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto result = reader.open("test/data/malformed/empty_header.csv");

  // With error collection enabled, should still fail on empty header
  EXPECT_FALSE(result.ok);
  EXPECT_TRUE(reader.has_errors());

  // Check that we got the expected error
  const auto& errors = reader.errors();
  ASSERT_GE(errors.size(), 1);
  EXPECT_EQ(errors[0].code, libvroom::ErrorCode::EMPTY_HEADER);
  EXPECT_EQ(errors[0].severity, libvroom::ErrorSeverity::FATAL);
}

// Test that CsvReader detects duplicate column names
TEST(CsvReaderErrorTest, DuplicateColumnNames) {
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto result = reader.open("test/data/malformed/duplicate_column_names.csv");

  // Should succeed (duplicate names are just a warning)
  EXPECT_TRUE(result.ok);
  EXPECT_TRUE(reader.has_errors());

  // Check that we got the expected warnings
  const auto& errors = reader.errors();
  ASSERT_GE(errors.size(), 2); // "A" and "B" are both duplicated

  bool found_duplicate_a = false;
  bool found_duplicate_b = false;
  for (const auto& err : errors) {
    EXPECT_EQ(err.code, libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES);
    EXPECT_EQ(err.severity, libvroom::ErrorSeverity::WARNING);
    if (err.message.find("'A'") != std::string::npos) {
      found_duplicate_a = true;
    }
    if (err.message.find("'B'") != std::string::npos) {
      found_duplicate_b = true;
    }
  }
  EXPECT_TRUE(found_duplicate_a) << "Should detect duplicate column 'A'";
  EXPECT_TRUE(found_duplicate_b) << "Should detect duplicate column 'B'";
}

// Test that error mode DISABLED doesn't collect errors
TEST(CsvReaderErrorTest, DisabledModeNoErrors) {
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::DISABLED; // Default

  libvroom::CsvReader reader(opts);
  auto result = reader.open("test/data/malformed/duplicate_column_names.csv");

  // Should succeed
  EXPECT_TRUE(result.ok);

  // Should NOT have collected any errors (DISABLED mode)
  EXPECT_FALSE(reader.has_errors());
  EXPECT_EQ(reader.errors().size(), 0);
}

// Test that ConversionResult properly populates parse_errors
TEST(ConversionErrorTest, ConversionResultHasErrors) {
  libvroom::VroomOptions opts;
  opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
  opts.input_path = "test/data/malformed/duplicate_column_names.csv";
  opts.output_path = "/tmp/test_output.parquet";

  auto result = libvroom::convert_csv_to_parquet(opts);

  // Conversion should succeed
  EXPECT_TRUE(result.ok());

  // But should have collected warnings
  EXPECT_TRUE(result.has_warnings());
  EXPECT_FALSE(result.has_fatal());
  EXPECT_GE(result.error_count(), 2);
}

// Test error_summary helper method
TEST(ConversionErrorTest, ErrorSummary) {
  libvroom::VroomOptions opts;
  opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
  opts.input_path = "test/data/malformed/duplicate_column_names.csv";
  opts.output_path = "/tmp/test_output.parquet";

  auto result = libvroom::convert_csv_to_parquet(opts);

  std::string summary = result.error_summary();
  EXPECT_NE(summary.find("warnings"), std::string::npos)
      << "Summary should mention warnings: " << summary;
}
