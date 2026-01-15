/**
 * @file comment_line_test.cpp
 * @brief Comprehensive tests for comment line skipping during CSV parsing.
 *
 * Tests the comment_char field in Dialect and related functionality:
 * - Basic comment line skipping with '#' character
 * - Comments with various delimiters (comma, tab, semicolon, pipe)
 * - Comments within quoted fields (should NOT be treated as comments)
 * - Mid-file comments
 * - Edge cases: empty files with comments, comments at EOF
 * - csv_with_comments() dialect factory
 * - Comment detection in dialect detection
 * - Multi-threaded parsing with comments
 */

#include "libvroom.h"

#include "common_defs.h"
#include "dialect.h"
#include "error.h"
#include "io_util.h"
#include "two_pass.h"
#include "value_extraction.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

class CommentLineTest : public ::testing::Test {
protected:
  // Create a padded buffer from a string for SIMD-safe parsing
  std::vector<uint8_t> makeBuffer(const std::string& content) {
    std::vector<uint8_t> buf(content.size() + LIBVROOM_PADDING);
    std::memcpy(buf.data(), content.data(), content.size());
    std::memset(buf.data() + content.size(), 0, LIBVROOM_PADDING);
    return buf;
  }

  // Parse CSV with comment support and return ValueExtractor
  std::pair<std::vector<uint8_t>, libvroom::ParseIndex>
  parseWithComments(const std::string& content, char comment_char = '#', bool has_header = true) {
    auto buf = makeBuffer(content);
    libvroom::Dialect dialect = libvroom::Dialect::csv();
    dialect.comment_char = comment_char;

    libvroom::TwoPass parser;
    libvroom::ParseIndex idx = parser.init(buf.size(), 1);
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

    parser.parse_with_errors(buf.data(), idx, content.size(), errors, dialect);

    return {std::move(buf), std::move(idx)};
  }
};

// ============================================================================
// Dialect Factory Tests
// ============================================================================

TEST_F(CommentLineTest, CsvWithCommentsFactory) {
  auto dialect = libvroom::Dialect::csv_with_comments();

  EXPECT_EQ(dialect.delimiter, ',');
  EXPECT_EQ(dialect.quote_char, '"');
  EXPECT_EQ(dialect.comment_char, '#');
  EXPECT_TRUE(dialect.double_quote);
}

TEST_F(CommentLineTest, CsvWithCommentsFactoryCustomChar) {
  auto dialect = libvroom::Dialect::csv_with_comments(';');

  EXPECT_EQ(dialect.delimiter, ',');
  EXPECT_EQ(dialect.quote_char, '"');
  EXPECT_EQ(dialect.comment_char, ';');
}

TEST_F(CommentLineTest, DefaultDialectNoCommentChar) {
  auto dialect = libvroom::Dialect::csv();

  EXPECT_EQ(dialect.comment_char, '\0');
}

TEST_F(CommentLineTest, DialectEqualityIncludesCommentChar) {
  auto d1 = libvroom::Dialect::csv();
  auto d2 = libvroom::Dialect::csv_with_comments();

  EXPECT_NE(d1, d2) << "Dialects with different comment_char should not be equal";

  d1.comment_char = '#';
  EXPECT_EQ(d1, d2) << "Dialects with same comment_char should be equal";
}

TEST_F(CommentLineTest, DialectToStringIncludesCommentChar) {
  auto dialect = libvroom::Dialect::csv_with_comments();
  std::string str = dialect.to_string();

  EXPECT_NE(str.find("comment"), std::string::npos)
      << "to_string() should include comment char info";
  EXPECT_NE(str.find("#"), std::string::npos) << "to_string() should show '#' character";
}

// ============================================================================
// Basic Comment Line Skipping Tests
// ============================================================================

TEST_F(CommentLineTest, SkipCommentAtStartOfFile) {
  std::string csv = "# This is a comment\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1); // Only data row, not comment
  EXPECT_EQ(ve.get_string_view(0, 0), "1");
}

TEST_F(CommentLineTest, SkipMultipleCommentsAtStart) {
  std::string csv = "# Comment 1\n# Comment 2\n# Comment 3\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, SkipCommentInMiddleOfFile) {
  std::string csv = "a,b,c\n1,2,3\n# mid-file comment\n4,5,6\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 2); // Two data rows, comment skipped

  EXPECT_EQ(ve.get_string_view(0, 0), "1");
  EXPECT_EQ(ve.get_string_view(1, 0), "4");
}

TEST_F(CommentLineTest, SkipCommentAtEndOfFile) {
  std::string csv = "a,b,c\n1,2,3\n# trailing comment\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, SkipCommentAtEndNoTrailingNewline) {
  std::string csv = "a,b,c\n1,2,3\n# trailing comment";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, MultipleCommentsScatteredThroughFile) {
  std::string csv = "# header comment\na,b,c\n# row 1 comment\n1,2,3\n# middle comment\n4,5,6\n# "
                    "end comment\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 2);
}

// ============================================================================
// Comment Character in Quoted Fields (should NOT be treated as comment)
// ============================================================================

TEST_F(CommentLineTest, HashInQuotedFieldNotComment) {
  std::string csv = "a,b,c\n\"# not a comment\",2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
  EXPECT_EQ(ve.get_string_view(0, 0), "# not a comment");
}

TEST_F(CommentLineTest, HashInMiddleOfQuotedFieldNotComment) {
  std::string csv = "a,b,c\n\"value # with hash\",2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
  EXPECT_EQ(ve.get_string_view(0, 0), "value # with hash");
}

TEST_F(CommentLineTest, MultilineQuotedFieldWithHash) {
  std::string csv = "a,b,c\n\"line1\n# not a comment\nline3\",2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
  // The quoted field spans multiple lines and contains #
  std::string_view field = ve.get_string_view(0, 0);
  EXPECT_NE(field.find("# not a comment"), std::string::npos);
}

// ============================================================================
// Comments with Leading Whitespace
// ============================================================================

TEST_F(CommentLineTest, CommentWithLeadingSpaces) {
  std::string csv = "   # Comment with leading spaces\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithLeadingTabs) {
  std::string csv = "\t\t# Comment with leading tabs\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithMixedWhitespace) {
  std::string csv = " \t # Comment with mixed whitespace\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

// ============================================================================
// Different Delimiter Tests
// ============================================================================

TEST_F(CommentLineTest, CommentWithTabDelimiter) {
  std::string csv = "# Tab-separated comment\na\tb\tc\n1\t2\t3\n";
  auto buf = makeBuffer(csv);

  libvroom::Dialect dialect = libvroom::Dialect::tsv();
  dialect.comment_char = '#';

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithSemicolonDelimiter) {
  std::string csv = "# Semicolon-separated comment\na;b;c\n1;2;3\n";
  auto buf = makeBuffer(csv);

  libvroom::Dialect dialect = libvroom::Dialect::semicolon();
  dialect.comment_char = '#';

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithPipeDelimiter) {
  std::string csv = "# Pipe-separated comment\na|b|c\n1|2|3\n";
  auto buf = makeBuffer(csv);

  libvroom::Dialect dialect = libvroom::Dialect::pipe();
  dialect.comment_char = '#';

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

// ============================================================================
// Different Comment Characters
// ============================================================================

TEST_F(CommentLineTest, SemicolonAsCommentChar) {
  std::string csv = "; This is a semicolon comment\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv, ';');
  libvroom::Dialect dialect = libvroom::Dialect::csv();
  dialect.comment_char = ';';

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, PercentAsCommentChar) {
  std::string csv = "% This is a percent comment\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv, '%');
  libvroom::Dialect dialect = libvroom::Dialect::csv();
  dialect.comment_char = '%';

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, SlashAsCommentChar) {
  std::string csv = "/ This is a slash comment\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv, '/');
  libvroom::Dialect dialect = libvroom::Dialect::csv();
  dialect.comment_char = '/';

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(CommentLineTest, OnlyCommentLines) {
  std::string csv = "# Comment 1\n# Comment 2\n# Comment 3\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  // Should report empty header error since all lines are comments
  EXPECT_TRUE(errors.has_errors());
}

TEST_F(CommentLineTest, EmptyCommentLine) {
  std::string csv = "#\na,b,c\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, HashNotAtLineStart) {
  std::string csv = "a,b,c\nvalue#notcomment,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
  EXPECT_EQ(ve.get_string_view(0, 0), "value#notcomment");
}

TEST_F(CommentLineTest, NoCommentCharDisablesSkipping) {
  std::string csv = "# This should be parsed as data\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::Dialect dialect = libvroom::Dialect::csv(); // No comment_char
  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  // When no comment char is set, # line is parsed as data (causing field count error)
  // The parser will see different field counts
  EXPECT_TRUE(errors.has_errors());
}

// ============================================================================
// Line Ending Variations with Comments
// ============================================================================

TEST_F(CommentLineTest, CommentWithCRLFLineEnding) {
  std::string csv = "# Comment\r\na,b,c\r\n1,2,3\r\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithCROnlyLineEnding) {
  std::string csv = "# Comment\ra,b,c\r1,2,3\r";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

TEST_F(CommentLineTest, CommentWithMixedLineEndings) {
  std::string csv = "# Comment LF\na,b,c\r\n# Comment CRLF\r\n1,2,3\n";

  auto [buf, idx] = parseWithComments(csv);
  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, libvroom::Dialect::csv_with_comments());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 1);
}

// ============================================================================
// Validation Function Tests (TwoPass helper functions)
// ============================================================================

TEST_F(CommentLineTest, CheckEmptyHeaderSkipsComments) {
  std::string csv = "# Comment\n# Another\na,b,c\n";

  auto buf = makeBuffer(csv);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::TwoPass::check_empty_header(buf.data(), csv.size(), errors, '#');

  EXPECT_FALSE(errors.has_errors()) << "Should not report empty header when comments are skipped";
}

TEST_F(CommentLineTest, CheckDuplicateColumnsSkipsComments) {
  std::string csv = "# Comment with a,a,a\na,b,c\n";

  auto buf = makeBuffer(csv);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::TwoPass::check_duplicate_columns(buf.data(), csv.size(), errors, ',', '"', '#');

  EXPECT_FALSE(errors.has_errors())
      << "Should not detect duplicates in comment line, header is a,b,c";
}

TEST_F(CommentLineTest, CheckFieldCountsSkipsComments) {
  std::string csv = "# Comment,with,extra,fields,here\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::TwoPass::check_field_counts(buf.data(), csv.size(), errors, ',', '"', '#');

  EXPECT_FALSE(errors.has_errors())
      << "Should not count fields in comment line, data rows have 3 fields each";
}

TEST_F(CommentLineTest, CheckFieldCountsSkipsCommentsCROnly) {
  // Test CR-only line endings with comments
  std::string csv = "# Comment\ra,b,c\r1,2,3\r";

  auto buf = makeBuffer(csv);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::TwoPass::check_field_counts(buf.data(), csv.size(), errors, ',', '"', '#');

  EXPECT_FALSE(errors.has_errors()) << "Should handle CR-only line endings with comments";
}

// ============================================================================
// Dialect Detection with Comments
// ============================================================================

TEST_F(CommentLineTest, DialectDetectionSkipsCommentLines) {
  std::string csv = "# This is a comment\n# Another comment\na,b,c\n1,2,3\n4,5,6\n";

  auto buf = makeBuffer(csv);
  libvroom::DialectDetector detector;
  auto result = detector.detect(buf.data(), csv.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 3);
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 2);
}

TEST_F(CommentLineTest, DialectDetectionNoComments) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

  auto buf = makeBuffer(csv);
  libvroom::DialectDetector detector;
  auto result = detector.detect(buf.data(), csv.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, '\0');
  EXPECT_EQ(result.comment_lines_skipped, 0);
}

TEST_F(CommentLineTest, DetectionResultIncludesCommentChar) {
  std::string csv = "# Header comment\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::DialectDetector detector;
  auto result = detector.detect(buf.data(), csv.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.dialect.comment_char, '#')
      << "Detected comment char should be propagated to dialect";
}

// ============================================================================
// is_comment_line and skip_to_line_end Helper Functions
// ============================================================================

TEST_F(CommentLineTest, IsCommentLineBasic) {
  std::string line = "# comment";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(line.data());

  EXPECT_TRUE(libvroom::TwoPass::is_comment_line(buf, 0, line.size(), '#'));
  EXPECT_FALSE(libvroom::TwoPass::is_comment_line(buf, 0, line.size(), '\0'));
  EXPECT_FALSE(libvroom::TwoPass::is_comment_line(buf, 0, line.size(), ';'));
}

TEST_F(CommentLineTest, IsCommentLineWithWhitespace) {
  std::string line = "   # comment";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(line.data());

  EXPECT_TRUE(libvroom::TwoPass::is_comment_line(buf, 0, line.size(), '#'));
}

TEST_F(CommentLineTest, IsCommentLineNotAtStart) {
  std::string line = "data # not comment";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(line.data());

  EXPECT_FALSE(libvroom::TwoPass::is_comment_line(buf, 0, line.size(), '#'));
}

TEST_F(CommentLineTest, IsCommentLineEmptyBuffer) {
  EXPECT_FALSE(libvroom::TwoPass::is_comment_line(nullptr, 0, 0, '#'));
}

TEST_F(CommentLineTest, SkipToLineEndLF) {
  std::string data = "line1\nline2";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(data.data());

  size_t result = libvroom::TwoPass::skip_to_line_end(buf, 0, data.size());
  EXPECT_EQ(result, 6); // Position after \n
}

TEST_F(CommentLineTest, SkipToLineEndCRLF) {
  std::string data = "line1\r\nline2";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(data.data());

  size_t result = libvroom::TwoPass::skip_to_line_end(buf, 0, data.size());
  EXPECT_EQ(result, 7); // Position after \r\n
}

TEST_F(CommentLineTest, SkipToLineEndCR) {
  std::string data = "line1\rline2";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(data.data());

  size_t result = libvroom::TwoPass::skip_to_line_end(buf, 0, data.size());
  EXPECT_EQ(result, 6); // Position after \r
}

TEST_F(CommentLineTest, SkipToLineEndNoNewline) {
  std::string data = "line1";
  const uint8_t* buf = reinterpret_cast<const uint8_t*>(data.data());

  size_t result = libvroom::TwoPass::skip_to_line_end(buf, 0, data.size());
  EXPECT_EQ(result, 5); // End of buffer
}

// ============================================================================
// Error Handling with Comments
// ============================================================================

TEST_F(CommentLineTest, StrictModeWithComments) {
  std::string csv = "# Comment\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::FAIL_FAST);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  bool success = parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  EXPECT_TRUE(success);
  EXPECT_FALSE(errors.has_fatal_errors());
}

TEST_F(CommentLineTest, PermissiveModeWithMalformedAfterComment) {
  std::string csv = "# Comment\na,b,c\n1,2\n"; // Missing field in second row

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  // Should report inconsistent field count
  EXPECT_TRUE(errors.has_errors());
}

// ============================================================================
// Throwing Parser with Comments
// ============================================================================

TEST_F(CommentLineTest, ThrowingParserSkipsComments) {
  std::string csv = "# Comment\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();

  // This should not throw
  EXPECT_NO_THROW({
    idx.n_indexes[0] = libvroom::TwoPass::second_pass_chunk_throwing(
        buf.data(), 0, csv.size(), &idx, 0, dialect.delimiter, dialect.quote_char,
        dialect.comment_char);
  });
}

// ============================================================================
// Multi-threaded Parsing with Comments
// ============================================================================

TEST_F(CommentLineTest, TwoPassParsingWithComments) {
  // Create a larger file with comments scattered throughout
  std::string csv = "# File header comment\na,b,c\n";
  for (int i = 0; i < 100; ++i) {
    if (i % 10 == 0) {
      csv += "# Comment at row " + std::to_string(i) + "\n";
    }
    csv += std::to_string(i) + "," + std::to_string(i * 2) + "," + std::to_string(i * 3) + "\n";
  }

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  bool success = parser.parse_two_pass_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  EXPECT_TRUE(success);
  EXPECT_FALSE(errors.has_fatal_errors());

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3);
  EXPECT_EQ(ve.num_rows(), 100); // Should have 100 data rows (comments skipped)
}

// ============================================================================
// parse_with_errors and parse_two_pass_with_errors Integration
// ============================================================================

TEST_F(CommentLineTest, ParseWithErrorsCommentSupport) {
  std::string csv = "# Comment\na,b,c\n1,2,3\n4,5,6\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  bool success = parser.parse_with_errors(buf.data(), idx, csv.size(), errors, dialect);

  EXPECT_TRUE(success);

  libvroom::ValueExtractor ve(buf.data(), csv.size(), idx, dialect);
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_rows(), 2);
}

TEST_F(CommentLineTest, ParseValidateWithComments) {
  std::string csv = "# Validation test\na,b,c\n1,2,3\n";

  auto buf = makeBuffer(csv);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buf.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::FAIL_FAST);

  libvroom::Dialect dialect = libvroom::Dialect::csv_with_comments();
  bool success = parser.parse_validate(buf.data(), idx, csv.size(), errors, dialect);

  EXPECT_TRUE(success);
  EXPECT_FALSE(errors.has_fatal_errors());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
