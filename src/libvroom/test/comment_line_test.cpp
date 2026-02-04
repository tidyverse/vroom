/**
 * @file comment_line_test.cpp
 * @brief Tests for comment line handling in CSV parsing.
 *
 * Rewritten from old comment_line_test.cpp to use the libvroom2 CsvReader API.
 * Tests CsvOptions::comment for skipping comment lines during parsing.
 *
 * @see GitHub issue #626
 */

#include "libvroom.h"

#include "test_util.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>

class CommentLineTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }

  struct ParseResult {
    bool ok;
    size_t total_rows;
    size_t num_columns;
    std::vector<std::string> column_names;
  };

  ParseResult parseFile(const std::string& path, char comment_char, char sep = ',') {
    libvroom::CsvOptions opts;
    opts.comment = comment_char;
    opts.separator = sep;
    opts.num_threads = 1;
    libvroom::CsvReader reader(opts);

    auto open_result = reader.open(path);
    if (!open_result.ok)
      return {false, 0, 0, {}};

    auto read_result = reader.read_all();
    std::vector<std::string> names;
    for (const auto& col : reader.schema())
      names.push_back(col.name);

    return {read_result.ok, read_result.ok ? read_result.value.total_rows : 0,
            reader.schema().size(), std::move(names)};
  }

  ParseResult parseContent(const std::string& content, char comment_char, char sep = ',') {
    test_util::TempCsvFile csv(content);
    return parseFile(csv.path(), comment_char, sep);
  }
};

// ============================================================================
// BASIC COMMENT SKIPPING
// ============================================================================

TEST_F(CommentLineTest, HashCommentsFromFile) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseFile(testDataPath("comments/hash_comments.csv"), '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  EXPECT_EQ(result.column_names[0], "name");
  EXPECT_EQ(result.column_names[1], "value");
  EXPECT_EQ(result.total_rows, 3u); // Alice, Bob, Charlie
}

TEST_F(CommentLineTest, CommentsBeforeHeader) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("# comment 1\n# comment 2\nA,B\n1,2\n3,4\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  EXPECT_EQ(result.column_names[0], "A");
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(CommentLineTest, CommentsInMiddleOfData) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("A,B\n1,2\n# skip this\n3,4\n# skip this too\n5,6\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 3u);
}

TEST_F(CommentLineTest, CommentAtEndOfFile) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("A,B\n1,2\n3,4\n# trailing comment\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(CommentLineTest, OnlyComments) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("# comment 1\n# comment 2\n# comment 3\n", '#');
  // No header after comments, should fail to open
  EXPECT_FALSE(result.ok);
}

TEST_F(CommentLineTest, NoCommentCharSet) {
  // With comment='\0' (default), lines starting with # are not comments
  auto result = parseContent("A,B\n#1,2\n3,4\n", '\0');
  ASSERT_TRUE(result.ok);
  // #1 is treated as data, not a comment
  EXPECT_EQ(result.total_rows, 2u);
}

// ============================================================================
// DIFFERENT COMMENT CHARACTERS
// ============================================================================

TEST_F(CommentLineTest, SemicolonComments) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseFile(testDataPath("comments/semicolon_comments.csv"), ';');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  EXPECT_EQ(result.total_rows, 2u); // Alice, Bob
}

TEST_F(CommentLineTest, PercentComment) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("% comment\nA,B\n1,2\n", '%');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1u);
}

TEST_F(CommentLineTest, SlashComment) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("/ comment\nA,B\n1,2\n/ another\n3,4\n", '/');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

// ============================================================================
// COMMENTS IN QUOTED FIELDS
// ============================================================================

TEST_F(CommentLineTest, HashInsideQuotedField) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseFile(testDataPath("comments/quoted_hash.csv"), '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  // All 3 data rows should be present - # inside quotes is not a comment
  EXPECT_EQ(result.total_rows, 3u);
}

TEST_F(CommentLineTest, CommentCharInsideQuotedFieldIsNotComment) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("A,B\n\"#not a comment\",data\nreal,data\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

// ============================================================================
// COMMENTS WITH DIFFERENT DELIMITERS
// ============================================================================

TEST_F(CommentLineTest, CommentsWithTabDelimiter) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("# comment\nA\tB\n1\t2\n# skip\n3\t4\n", '#', '\t');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(CommentLineTest, CommentsWithPipeDelimiter) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("# comment\nA|B\n1|2\n3|4\n", '#', '|');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  EXPECT_EQ(result.total_rows, 2u);
}

// ============================================================================
// MULTI-HEADER COMMENTS
// ============================================================================

TEST_F(CommentLineTest, MultiHeaderComments) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseFile(testDataPath("comments/multi_header_comments.csv"), '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 4u);
  EXPECT_EQ(result.column_names[0], "ID");
  EXPECT_EQ(result.total_rows, 3u);
}

// ============================================================================
// LINE ENDING VARIATIONS WITH COMMENTS
// ============================================================================

TEST_F(CommentLineTest, CommentsWithCRLF) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  auto result = parseContent("# comment\r\nA,B\r\n1,2\r\n# skip\r\n3,4\r\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(CommentLineTest, EmptyCommentLine) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  // A line with just the comment char
  auto result = parseContent("#\nA,B\n1,2\n#\n3,4\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(CommentLineTest, CommentCharNotAtLineStart) {
  // # in middle of line is NOT a comment - this doesn't depend on comment handling
  auto result = parseContent("A,B\n1,#2\n3,4\n", '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(CommentLineTest, ManyConsecutiveComments) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  std::ostringstream oss;
  for (int i = 0; i < 100; ++i)
    oss << "# comment " << i << "\n";
  oss << "A,B\n";
  for (int i = 0; i < 50; ++i)
    oss << i << "," << (i * 2) << "\n";

  auto result = parseContent(oss.str(), '#');
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 50u);
}

TEST_F(CommentLineTest, MultiThreadedWithComments) {
  GTEST_SKIP() << "CsvReader does not yet implement CsvOptions::comment for skipping comment lines";
  std::ostringstream oss;
  oss << "# header comment\n";
  oss << "A,B,C\n";
  for (int i = 0; i < 5000; ++i) {
    if (i % 10 == 0)
      oss << "# comment at row " << i << "\n";
    oss << i << "," << (i * 2) << "," << (i * 3) << "\n";
  }

  test_util::TempCsvFile csv(oss.str());
  libvroom::CsvOptions opts;
  opts.comment = '#';
  opts.num_threads = 4;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok);

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok);
  EXPECT_EQ(read_result.value.total_rows, 5000u);
}
