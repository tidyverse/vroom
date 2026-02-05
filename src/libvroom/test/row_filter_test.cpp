/**
 * @file row_filter_test.cpp
 * @brief Tests for row filtering options (skip_empty_rows, skip, n_max).
 *
 * Rewritten from old row_filter_test.cpp to use the libvroom2 CsvReader API.
 * CsvOptions currently only supports skip_empty_rows. Tests for skip and n_max
 * are skipped pending implementation (see original issue #559).
 *
 * @see GitHub issue #626
 */

#include "libvroom.h"

#include "test_util.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

class RowFilterTest : public ::testing::Test {
protected:
  struct ParseResult {
    bool ok;
    size_t total_rows;
    size_t num_columns;
  };

  ParseResult parseContent(const std::string& content, bool skip_empty = true) {
    test_util::TempCsvFile csv(content);

    libvroom::CsvOptions opts;
    opts.skip_empty_rows = skip_empty;
    opts.num_threads = 1;
    libvroom::CsvReader reader(opts);

    auto open_result = reader.open(csv.path());
    if (!open_result.ok)
      return {false, 0, 0};

    auto read_result = reader.read_all();
    return {read_result.ok, read_result.ok ? read_result.value.total_rows : 0,
            reader.schema().size()};
  }
};

// =============================================================================
// skip_empty_rows TESTS (implemented)
// =============================================================================

TEST_F(RowFilterTest, SkipEmptyRowsDefault) {
  // Default: skip_empty_rows=true should skip blank lines
  auto result = parseContent("A,B\n1,2\n\n3,4\n\n5,6\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.num_columns, 2u);
  // Empty lines should be skipped
  EXPECT_EQ(result.total_rows, 3u);
}

TEST_F(RowFilterTest, SkipEmptyRowsMultipleConsecutive) {
  auto result = parseContent("A,B\n1,2\n\n\n\n3,4\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(RowFilterTest, SkipEmptyRowsAtEnd) {
  auto result = parseContent("A,B\n1,2\n3,4\n\n\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2u);
}

TEST_F(RowFilterTest, NoEmptyRows) {
  auto result = parseContent("A,B\n1,2\n3,4\n5,6\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 3u);
}

TEST_F(RowFilterTest, OnlyHeaderNoData) {
  auto result = parseContent("A,B\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 0u);
}

TEST_F(RowFilterTest, SingleDataRow) {
  auto result = parseContent("A,B\n1,2\n", true);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1u);
  EXPECT_EQ(result.num_columns, 2u);
}

// =============================================================================
// skip option TESTS (not implemented)
// =============================================================================

TEST_F(RowFilterTest, SkipZeroRows) {
  GTEST_SKIP() << "CsvOptions does not yet support skip (skip N initial data rows)";
}

TEST_F(RowFilterTest, SkipOneRow) {
  GTEST_SKIP() << "CsvOptions does not yet support skip (skip N initial data rows)";
}

TEST_F(RowFilterTest, SkipAllRows) {
  GTEST_SKIP() << "CsvOptions does not yet support skip (skip N initial data rows)";
}

TEST_F(RowFilterTest, SkipMoreThanAvailable) {
  GTEST_SKIP() << "CsvOptions does not yet support skip (skip N initial data rows)";
}

// =============================================================================
// n_max option TESTS (not implemented)
// =============================================================================

TEST_F(RowFilterTest, NMaxZero) {
  GTEST_SKIP() << "CsvOptions does not yet support n_max (limit number of rows read)";
}

TEST_F(RowFilterTest, NMaxOne) {
  GTEST_SKIP() << "CsvOptions does not yet support n_max (limit number of rows read)";
}

TEST_F(RowFilterTest, NMaxLargerThanFile) {
  GTEST_SKIP() << "CsvOptions does not yet support n_max (limit number of rows read)";
}

TEST_F(RowFilterTest, NMaxExactlyFileSize) {
  GTEST_SKIP() << "CsvOptions does not yet support n_max (limit number of rows read)";
}

// =============================================================================
// Combined skip + n_max TESTS (not implemented)
// =============================================================================

TEST_F(RowFilterTest, SkipAndNMaxCombined) {
  GTEST_SKIP() << "CsvOptions does not yet support skip or n_max";
}
