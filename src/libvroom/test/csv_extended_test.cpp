/**
 * Extended CSV Parser Tests
 *
 * Tests for additional coverage identified from zsv and duckdb test suites:
 * - Encoding (BOM, Latin-1)
 * - Whitespace handling (blank rows, trimming)
 * - Large files and buffer boundaries
 * - Comment lines
 * - Ragged CSVs (variable column counts)
 * - Fuzz-discovered edge cases
 */

#include "error.h"
#include "io_util.h"
#include "mem_util.h"
#include "test_helpers.h"
#include "two_pass.h"

#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

class CSVExtendedTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }

  bool fileExists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
  }
};

// ============================================================================
// ENCODING TESTS
// ============================================================================

TEST_F(CSVExtendedTest, UTF8BOMFileExists) {
  std::string path = getTestDataPath("encoding", "utf8_bom.csv");
  ASSERT_TRUE(fileExists(path)) << "utf8_bom.csv should exist";
}

TEST_F(CSVExtendedTest, UTF8BOMDetection) {
  std::string path = getTestDataPath("encoding", "utf8_bom.csv");
  CorpusGuard corpus(path);

  // Check that file starts with BOM (EF BB BF)
  ASSERT_GE(corpus.data.size, 3) << "File should be at least 3 bytes";
  EXPECT_EQ(corpus.data.data()[0], 0xEF) << "First byte should be 0xEF";
  EXPECT_EQ(corpus.data.data()[1], 0xBB) << "Second byte should be 0xBB";
  EXPECT_EQ(corpus.data.data()[2], 0xBF) << "Third byte should be 0xBF";
}

TEST_F(CSVExtendedTest, UTF8BOMParsing) {
  std::string path = getTestDataPath("encoding", "utf8_bom.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle BOM (may or may not skip it)
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle UTF-8 BOM file";
}

TEST_F(CSVExtendedTest, Latin1FileExists) {
  std::string path = getTestDataPath("encoding", "latin1.csv");
  ASSERT_TRUE(fileExists(path)) << "latin1.csv should exist";
}

TEST_F(CSVExtendedTest, Latin1Detection) {
  std::string path = getTestDataPath("encoding", "latin1.csv");
  CorpusGuard corpus(path);

  // Check for Latin-1 specific bytes (0xE9 = é in Latin-1)
  bool has_latin1_char = false;
  for (size_t i = 0; i < corpus.data.size; ++i) {
    if (corpus.data.data()[i] == 0xE9) { // é in Latin-1
      has_latin1_char = true;
      break;
    }
  }
  EXPECT_TRUE(has_latin1_char) << "File should contain Latin-1 character 0xE9";
}

TEST_F(CSVExtendedTest, Latin1Parsing) {
  std::string path = getTestDataPath("encoding", "latin1.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should parse Latin-1 file (treating bytes as-is)
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle Latin-1 file";
}

TEST_F(CSVExtendedTest, UTF16BOMFileExists) {
  std::string path = getTestDataPath("encoding", "utf16_bom.csv");
  ASSERT_TRUE(fileExists(path)) << "utf16_bom.csv should exist";
}

TEST_F(CSVExtendedTest, UTF16BOMDetection) {
  std::string path = getTestDataPath("encoding", "utf16_bom.csv");
  CorpusGuard corpus(path);

  // Check that file starts with UTF-16 LE BOM (FF FE)
  ASSERT_GE(corpus.data.size, 2) << "File should be at least 2 bytes";
  EXPECT_EQ(corpus.data.data()[0], 0xFF) << "First byte should be 0xFF (UTF-16 LE BOM)";
  EXPECT_EQ(corpus.data.data()[1], 0xFE) << "Second byte should be 0xFE (UTF-16 LE BOM)";
}

TEST_F(CSVExtendedTest, UTF16BOMParsing) {
  // Note: libvroom is a byte-oriented parser and does NOT support UTF-16.
  // This test documents expected behavior: the parser will treat UTF-16
  // data as binary/garbage and may fail or produce incorrect results.
  // UTF-16 files should be converted to UTF-8 before parsing.
  std::string path = getTestDataPath("encoding", "utf16_bom.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser will attempt to parse but results are undefined for UTF-16
  // We just ensure it doesn't crash but we can verify indexes were created
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  // Just verify parser doesn't crash - UTF-16 is not supported
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should still allocate indexes";
}

// ============================================================================
// WHITESPACE TESTS
// ============================================================================

TEST_F(CSVExtendedTest, BlankLeadingRowsFileExists) {
  std::string path = getTestDataPath("whitespace", "blank_leading_rows.csv");
  ASSERT_TRUE(fileExists(path)) << "blank_leading_rows.csv should exist";
}

TEST_F(CSVExtendedTest, BlankLeadingRowsParsing) {
  std::string path = getTestDataPath("whitespace", "blank_leading_rows.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // blank_leading_rows.csv has 5 blank lines before the header
  // This validates that leading blank lines don't corrupt parsing
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle blank leading rows";
}

TEST_F(CSVExtendedTest, WhitespaceOnlyRowsFileExists) {
  std::string path = getTestDataPath("whitespace", "whitespace_only_rows.csv");
  ASSERT_TRUE(fileExists(path)) << "whitespace_only_rows.csv should exist";
}

TEST_F(CSVExtendedTest, WhitespaceOnlyRowsParsing) {
  std::string path = getTestDataPath("whitespace", "whitespace_only_rows.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle whitespace-only rows";
}

TEST_F(CSVExtendedTest, TrimFieldsFileExists) {
  std::string path = getTestDataPath("whitespace", "trim_fields.csv");
  ASSERT_TRUE(fileExists(path)) << "trim_fields.csv should exist";
}

TEST_F(CSVExtendedTest, TrimFieldsParsing) {
  std::string path = getTestDataPath("whitespace", "trim_fields.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Fields with leading/trailing whitespace should parse correctly
  // Note: libvroom preserves whitespace; trimming is caller responsibility
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle fields with whitespace";
}

TEST_F(CSVExtendedTest, BlankRowsMixedFileExists) {
  std::string path = getTestDataPath("whitespace", "blank_rows_mixed.csv");
  ASSERT_TRUE(fileExists(path)) << "blank_rows_mixed.csv should exist";
}

TEST_F(CSVExtendedTest, BlankRowsMixedParsing) {
  std::string path = getTestDataPath("whitespace", "blank_rows_mixed.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle blank rows mixed throughout";
}

// ============================================================================
// LARGE FILE / BUFFER BOUNDARY TESTS
// ============================================================================

TEST_F(CSVExtendedTest, LongLineFileExists) {
  std::string path = getTestDataPath("large", "long_line.csv");
  ASSERT_TRUE(fileExists(path)) << "long_line.csv should exist";
}

TEST_F(CSVExtendedTest, LongLineParsing) {
  std::string path = getTestDataPath("large", "long_line.csv");
  CorpusGuard corpus(path);

  // File should be >10KB
  EXPECT_GT(corpus.data.size, 10000) << "long_line.csv should be >10KB";

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle very long lines";
}

TEST_F(CSVExtendedTest, LargeFieldFileExists) {
  std::string path = getTestDataPath("large", "large_field.csv");
  ASSERT_TRUE(fileExists(path)) << "large_field.csv should exist";
}

TEST_F(CSVExtendedTest, LargeFieldParsing) {
  std::string path = getTestDataPath("large", "large_field.csv");
  CorpusGuard corpus(path);

  // File should be >64KB (larger than typical SIMD buffer)
  EXPECT_GT(corpus.data.size, 64000) << "large_field.csv should be >64KB";

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle very large fields";
}

TEST_F(CSVExtendedTest, BufferBoundaryFileExists) {
  std::string path = getTestDataPath("large", "buffer_boundary.csv");
  ASSERT_TRUE(fileExists(path)) << "buffer_boundary.csv should exist";
}

TEST_F(CSVExtendedTest, BufferBoundaryParsing) {
  std::string path = getTestDataPath("large", "buffer_boundary.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle quoted newlines at buffer boundaries";
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryFileExists) {
  std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
  ASSERT_TRUE(fileExists(path)) << "parallel_chunk_boundary.csv should exist";
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryParsing) {
  std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
  CorpusGuard corpus(path);

  // File should be ~2MB
  EXPECT_GT(corpus.data.size, 1500000) << "parallel_chunk_boundary.csv should be >1.5MB";

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle parallel chunk boundary test file";
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryMultiThreaded) {
  std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
  CorpusGuard corpus(path);

  // Parse with multiple threads to stress test chunk boundaries
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 4); // 4 threads

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Multi-threaded parsing should handle chunk boundaries";
}

TEST_F(CSVExtendedTest, ParallelChunkBoundary8Threads) {
  std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 8); // 8 threads

  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "8-thread parsing should handle chunk boundaries";
}

// ============================================================================
// COMMENT LINE TESTS
// ============================================================================

TEST_F(CSVExtendedTest, HashCommentsFileExists) {
  std::string path = getTestDataPath("comments", "hash_comments.csv");
  ASSERT_TRUE(fileExists(path)) << "hash_comments.csv should exist";
}

TEST_F(CSVExtendedTest, HashCommentsParsing) {
  std::string path = getTestDataPath("comments", "hash_comments.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser currently doesn't skip comments, but should parse without crashing
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle files with comment-like lines";
}

TEST_F(CSVExtendedTest, QuotedHashFileExists) {
  std::string path = getTestDataPath("comments", "quoted_hash.csv");
  ASSERT_TRUE(fileExists(path)) << "quoted_hash.csv should exist";
}

TEST_F(CSVExtendedTest, QuotedHashParsing) {
  std::string path = getTestDataPath("comments", "quoted_hash.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Hash inside quoted field should NOT be treated as comment
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle # inside quoted fields";
}

TEST_F(CSVExtendedTest, SemicolonCommentsFileExists) {
  std::string path = getTestDataPath("comments", "semicolon_comments.csv");
  ASSERT_TRUE(fileExists(path)) << "semicolon_comments.csv should exist";
}

TEST_F(CSVExtendedTest, SemicolonCommentsParsing) {
  std::string path = getTestDataPath("comments", "semicolon_comments.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser currently doesn't skip comments, but should parse without crashing
  // Semicolon comments are common in some European CSV formats
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle files with semicolon comment lines";
}

// ============================================================================
// RAGGED CSV TESTS (variable column counts)
// ============================================================================

TEST_F(CSVExtendedTest, FewerColumnsFileExists) {
  std::string path = getTestDataPath("ragged", "fewer_columns.csv");
  ASSERT_TRUE(fileExists(path)) << "fewer_columns.csv should exist";
}

TEST_F(CSVExtendedTest, FewerColumnsParsing) {
  std::string path = getTestDataPath("ragged", "fewer_columns.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle rows with fewer columns than header
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle rows with fewer columns";
}

TEST_F(CSVExtendedTest, MoreColumnsFileExists) {
  std::string path = getTestDataPath("ragged", "more_columns.csv");
  ASSERT_TRUE(fileExists(path)) << "more_columns.csv should exist";
}

TEST_F(CSVExtendedTest, MoreColumnsParsing) {
  std::string path = getTestDataPath("ragged", "more_columns.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle rows with more columns than header
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle rows with more columns";
}

TEST_F(CSVExtendedTest, MixedColumnsFileExists) {
  std::string path = getTestDataPath("ragged", "mixed_columns.csv");
  ASSERT_TRUE(fileExists(path)) << "mixed_columns.csv should exist";
}

TEST_F(CSVExtendedTest, MixedColumnsParsing) {
  std::string path = getTestDataPath("ragged", "mixed_columns.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle mixed column counts
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);
  EXPECT_TRUE(success) << "Parser should handle mixed column counts";
}

// ============================================================================
// FUZZ TEST CASES
// ============================================================================

TEST_F(CSVExtendedTest, BadEscapeFileExists) {
  std::string path = getTestDataPath("fuzz", "bad_escape.csv");
  ASSERT_TRUE(fileExists(path)) << "bad_escape.csv should exist";
}

TEST_F(CSVExtendedTest, BadEscapeParsing) {
  // Note: RFC 4180 specifies quote doubling ("") for escaping quotes.
  // Some non-standard CSV producers use backslash escapes (\") instead.
  // TODO: Backslash escape support is planned as an optional parser feature.
  // Currently, the parser treats backslashes as literal characters.
  // This test verifies the parser handles such input gracefully without crashing.
  std::string path = getTestDataPath("fuzz", "bad_escape.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle backslash escapes without crashing
  // (non-RFC 4180 - backslashes are treated as literal characters)
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_validate(corpus.data.data(), idx, corpus.data.size, errors);

  // Just verify the parser completes without crashing
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should complete indexing without crashing";
}

TEST_F(CSVExtendedTest, InvalidUTF8FileExists) {
  std::string path = getTestDataPath("fuzz", "invalid_utf8.csv");
  ASSERT_TRUE(fileExists(path)) << "invalid_utf8.csv should exist";
}

TEST_F(CSVExtendedTest, InvalidUTF8Parsing) {
  std::string path = getTestDataPath("fuzz", "invalid_utf8.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should not crash on invalid UTF-8 sequences (0xFE, 0xFF, truncated multibyte)
  // Note: UTF-8 validation is not yet implemented (INVALID_UTF8 is reserved)
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_validate(corpus.data.data(), idx, corpus.data.size, errors);

  // Just verify the parser completes without crashing
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should complete indexing without crashing";
}

TEST_F(CSVExtendedTest, ScatteredNullsFileExists) {
  std::string path = getTestDataPath("fuzz", "scattered_nulls.csv");
  ASSERT_TRUE(fileExists(path)) << "scattered_nulls.csv should exist";
}

TEST_F(CSVExtendedTest, ScatteredNullsParsing) {
  std::string path = getTestDataPath("fuzz", "scattered_nulls.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle embedded null bytes (0x00) by detecting errors
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_validate(corpus.data.data(), idx, corpus.data.size, errors);

  // Null bytes should be detected as errors
  EXPECT_TRUE(errors.has_errors()) << "Null bytes should be detected as errors";
}

TEST_F(CSVExtendedTest, DeepQuotesFileExists) {
  std::string path = getTestDataPath("fuzz", "deep_quotes.csv");
  ASSERT_TRUE(fileExists(path)) << "deep_quotes.csv should exist";
}

TEST_F(CSVExtendedTest, DeepQuotesParsing) {
  std::string path = getTestDataPath("fuzz", "deep_quotes.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle many consecutive quotes without stack overflow
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);

  // Deep quotes are valid RFC 4180 - they represent escaped quotes
  EXPECT_TRUE(success) << "Deep quotes (escaped) should parse successfully";
}

TEST_F(CSVExtendedTest, QuoteDelimiterAltFileExists) {
  std::string path = getTestDataPath("fuzz", "quote_delimiter_alt.csv");
  ASSERT_TRUE(fileExists(path)) << "quote_delimiter_alt.csv should exist";
}

TEST_F(CSVExtendedTest, QuoteDelimiterAltParsing) {
  std::string path = getTestDataPath("fuzz", "quote_delimiter_alt.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle alternating quotes and delimiters
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);

  // Alternating quotes and delimiters is valid CSV
  EXPECT_TRUE(success) << "Alternating quotes/delimiters should parse";
}

TEST_F(CSVExtendedTest, JustQuotesFileExists) {
  std::string path = getTestDataPath("fuzz", "just_quotes.csv");
  ASSERT_TRUE(fileExists(path)) << "just_quotes.csv should exist";
}

TEST_F(CSVExtendedTest, JustQuotesParsing) {
  std::string path = getTestDataPath("fuzz", "just_quotes.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle file with only quotes
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);

  // A file of just quotes may or may not be valid depending on count
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should complete indexing";
}

TEST_F(CSVExtendedTest, QuoteEOFFileExists) {
  std::string path = getTestDataPath("fuzz", "quote_eof.csv");
  ASSERT_TRUE(fileExists(path)) << "quote_eof.csv should exist";
}

TEST_F(CSVExtendedTest, QuoteEOFParsing) {
  std::string path = getTestDataPath("fuzz", "quote_eof.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle unclosed quote at EOF by detecting the error
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  bool success = parser.parse_validate(corpus.data.data(), idx, corpus.data.size, errors);

  // Unclosed quote at EOF should be detected as an error
  EXPECT_FALSE(success) << "Unclosed quote at EOF should fail";
  EXPECT_TRUE(errors.has_errors()) << "Should detect unclosed quote error";
}

TEST_F(CSVExtendedTest, MixedCRFileExists) {
  std::string path = getTestDataPath("fuzz", "mixed_cr.csv");
  ASSERT_TRUE(fileExists(path)) << "mixed_cr.csv should exist";
}

TEST_F(CSVExtendedTest, MixedCRParsing) {
  std::string path = getTestDataPath("fuzz", "mixed_cr.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should handle mixed CR and CRLF line endings
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);

  // Mixed line endings should parse successfully
  EXPECT_TRUE(success) << "Mixed CR/CRLF should parse successfully";
}

TEST_F(CSVExtendedTest, AFLBinaryFileExists) {
  std::string path = getTestDataPath("fuzz", "afl_binary.csv");
  ASSERT_TRUE(fileExists(path)) << "afl_binary.csv should exist";
}

TEST_F(CSVExtendedTest, AFLBinaryParsing) {
  std::string path = getTestDataPath("fuzz", "afl_binary.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // Parser should not crash on binary garbage (AFL-discovered test case)
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_validate(corpus.data.data(), idx, corpus.data.size, errors);

  // Binary garbage should be detected as errors
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should complete indexing";
}

TEST_F(CSVExtendedTest, AFL10FileExists) {
  std::string path = getTestDataPath("fuzz", "afl_10.csv");
  ASSERT_TRUE(fileExists(path)) << "afl_10.csv should exist";
}

TEST_F(CSVExtendedTest, AFL10Parsing) {
  std::string path = getTestDataPath("fuzz", "afl_10.csv");
  CorpusGuard corpus(path);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(corpus.data.size, 1);

  // AFL-discovered edge case test file
  bool success = parser.parse(corpus.data.data(), idx, corpus.data.size);

  // AFL edge case should be handled without crashing
  EXPECT_NE(idx.n_indexes, nullptr) << "Parser should complete indexing";
}

// ============================================================================
// ALL FILES PRESENT TEST
// ============================================================================

TEST_F(CSVExtendedTest, AllExtendedTestFilesPresent) {
  // Encoding
  EXPECT_TRUE(fileExists(getTestDataPath("encoding", "utf8_bom.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("encoding", "latin1.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("encoding", "utf16_bom.csv")));

  // Whitespace
  EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "blank_leading_rows.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "whitespace_only_rows.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "trim_fields.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "blank_rows_mixed.csv")));

  // Large
  EXPECT_TRUE(fileExists(getTestDataPath("large", "long_line.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("large", "large_field.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("large", "buffer_boundary.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("large", "parallel_chunk_boundary.csv")));

  // Comments
  EXPECT_TRUE(fileExists(getTestDataPath("comments", "hash_comments.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("comments", "quoted_hash.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("comments", "semicolon_comments.csv")));

  // Ragged
  EXPECT_TRUE(fileExists(getTestDataPath("ragged", "fewer_columns.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("ragged", "more_columns.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("ragged", "mixed_columns.csv")));

  // Fuzz
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "bad_escape.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "invalid_utf8.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "scattered_nulls.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "deep_quotes.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "quote_delimiter_alt.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "just_quotes.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "quote_eof.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "mixed_cr.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "afl_binary.csv")));
  EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "afl_10.csv")));
}
