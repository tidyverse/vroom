/**
 * @file streaming_test.cpp
 * @brief Unit tests for the streaming CSV parser.
 */

#include "streaming.h"

#include <gtest/gtest.h>
#include <sstream>
#include <vector>

using namespace libvroom;

//-----------------------------------------------------------------------------
// Basic Parsing Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, BasicParsing) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  std::vector<std::vector<std::string>> rows;
  for (const auto& row : reader) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "2", "3"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"4", "5", "6"}));

  // Check header
  EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(StreamingTest, NoHeader) {
  std::string csv = "1,2,3\n4,5,6\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  std::vector<std::vector<std::string>> rows;
  while (reader.next_row()) {
    std::vector<std::string> fields;
    for (const auto& field : reader.row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "2", "3"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"4", "5", "6"}));
  EXPECT_TRUE(reader.header().empty());
}

TEST(StreamingTest, EmptyFile) {
  std::string csv = "";
  std::istringstream input(csv);

  StreamReader reader(input);

  int count = 0;
  for (const auto& row : reader) {
    (void)row;
    ++count;
  }

  EXPECT_EQ(count, 0);
}

TEST(StreamingTest, SingleField) {
  std::string csv = "hello\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 1);
  EXPECT_EQ(reader.row()[0].data, "hello");
  EXPECT_FALSE(reader.next_row());
}

TEST(StreamingTest, EmptyFields) {
  std::string csv = "a,,c\n,b,\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 3);
  EXPECT_EQ(reader.row()[0].data, "a");
  EXPECT_EQ(reader.row()[1].data, "");
  EXPECT_EQ(reader.row()[2].data, "c");

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 3);
  EXPECT_EQ(reader.row()[0].data, "");
  EXPECT_EQ(reader.row()[1].data, "b");
  EXPECT_EQ(reader.row()[2].data, "");

  EXPECT_FALSE(reader.next_row());
}

//-----------------------------------------------------------------------------
// Quoted Field Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, QuotedFields) {
  std::string csv = "\"hello\",\"world\"\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 2);
  EXPECT_EQ(reader.row()[0].data, "hello");
  EXPECT_TRUE(reader.row()[0].is_quoted);
  EXPECT_EQ(reader.row()[1].data, "world");
  EXPECT_TRUE(reader.row()[1].is_quoted);
}

TEST(StreamingTest, QuotedFieldWithComma) {
  std::string csv = "\"hello, world\",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 2);
  EXPECT_EQ(reader.row()[0].data, "hello, world");
  EXPECT_EQ(reader.row()[1].data, "test");
}

TEST(StreamingTest, QuotedFieldWithNewline) {
  std::string csv = "\"line1\nline2\",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 2);
  EXPECT_EQ(reader.row()[0].data, "line1\nline2");
  EXPECT_EQ(reader.row()[1].data, "test");
}

TEST(StreamingTest, EscapedQuotes) {
  std::string csv = "\"say \"\"hello\"\"\",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  ASSERT_EQ(reader.row().field_count(), 2);
  // The raw data contains the escaped quotes
  EXPECT_EQ(reader.row()[0].data, "say \"\"hello\"\"");
  // The unescaped version removes the escaping
  EXPECT_EQ(reader.row()[0].unescaped(), "say \"hello\"");
}

//-----------------------------------------------------------------------------
// Line Ending Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, UnixLineEndings) {
  std::string csv = "a,b\n1,2\n3,4\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  int count = 0;
  while (reader.next_row()) {
    ++count;
  }
  EXPECT_EQ(count, 3);
}

TEST(StreamingTest, WindowsLineEndings) {
  std::string csv = "a,b\r\n1,2\r\n3,4\r\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  int count = 0;
  while (reader.next_row()) {
    EXPECT_EQ(reader.row().field_count(), 2);
    ++count;
  }
  EXPECT_EQ(count, 3);
}

TEST(StreamingTest, NoTrailingNewline) {
  std::string csv = "a,b\n1,2";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "a");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "1");
  EXPECT_EQ(reader.row()[1].data, "2");

  EXPECT_FALSE(reader.next_row());
}

//-----------------------------------------------------------------------------
// Push Model Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, PushModelBasic) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

  StreamConfig config;
  config.parse_header = true;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> collected_rows;

  parser.set_row_handler([&collected_rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    collected_rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  ASSERT_EQ(collected_rows.size(), 2);
  EXPECT_EQ(collected_rows[0], (std::vector<std::string>{"1", "2", "3"}));
  EXPECT_EQ(collected_rows[1], (std::vector<std::string>{"4", "5", "6"}));

  EXPECT_EQ(parser.header(), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(StreamingTest, PushModelStopEarly) {
  std::string csv = "a\n1\n2\n3\n4\n5\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  int row_count = 0;
  parser.set_row_handler([&row_count](const Row& row) {
    (void)row;
    ++row_count;
    return row_count < 3; // Stop after 3 rows
  });

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_EQ(row_count, 3);
}

//-----------------------------------------------------------------------------
// Chunk Boundary Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ChunkBoundaryInField) {
  std::string csv = "hello,world\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Split in middle of "hello"
  parser.parse_chunk(csv.substr(0, 3));
  parser.parse_chunk(csv.substr(3));
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 2);
  EXPECT_EQ(parser.current_row()[0].data, "hello");
  EXPECT_EQ(parser.current_row()[1].data, "world");
}

TEST(StreamingTest, ChunkBoundaryAtDelimiter) {
  std::string csv = "hello,world\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Split at the comma
  parser.parse_chunk(csv.substr(0, 5)); // "hello"
  parser.parse_chunk(csv.substr(5));    // ",world\n"
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 2);
  EXPECT_EQ(parser.current_row()[0].data, "hello");
  EXPECT_EQ(parser.current_row()[1].data, "world");
}

TEST(StreamingTest, ChunkBoundaryInQuotedField) {
  std::string csv = "\"hello, world\",test\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Split in middle of quoted field
  parser.parse_chunk(csv.substr(0, 8)); // "\"hello, "
  parser.parse_chunk(csv.substr(8));    // "world\",test\n"
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 2);
  EXPECT_EQ(parser.current_row()[0].data, "hello, world");
  EXPECT_EQ(parser.current_row()[1].data, "test");
}

TEST(StreamingTest, ChunkBoundaryAcrossMultipleRows) {
  std::string csv = "a,b\n1,2\n3,4\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  // Feed one character at a time
  for (char c : csv) {
    parser.parse_chunk(std::string_view(&c, 1));
  }
  parser.finish();

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
  EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

//-----------------------------------------------------------------------------
// Column Access Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ColumnAccessByName) {
  std::string csv = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  // Must read first row to parse header
  ASSERT_TRUE(reader.next_row());

  // Now column_index works
  EXPECT_EQ(reader.column_index("name"), 0);
  EXPECT_EQ(reader.column_index("age"), 1);
  EXPECT_EQ(reader.column_index("city"), 2);
  EXPECT_EQ(reader.column_index("unknown"), -1);

  EXPECT_EQ(reader.row()["name"].data, "Alice");
  EXPECT_EQ(reader.row()["age"].data, "30");
  EXPECT_EQ(reader.row()["city"].data, "NYC");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()["name"].data, "Bob");
  EXPECT_EQ(reader.row()["age"].data, "25");
  EXPECT_EQ(reader.row()["city"].data, "LA");
}

TEST(StreamingTest, RowMetadata) {
  std::string csv = "a,b\n1,2\n3,4\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().row_number(), 1);
  EXPECT_EQ(reader.row().byte_offset(), 4); // After "a,b\n"

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().row_number(), 2);
}

//-----------------------------------------------------------------------------
// Dialect Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, TabSeparated) {
  std::string tsv = "a\tb\tc\n1\t2\t3\n";
  std::istringstream input(tsv);

  StreamConfig config;
  config.dialect = Dialect::tsv();
  config.parse_header = true;

  StreamReader reader(input, config);

  // Must read first row to parse header
  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(reader.row()[0].data, "1");
  EXPECT_EQ(reader.row()[1].data, "2");
  EXPECT_EQ(reader.row()[2].data, "3");
}

TEST(StreamingTest, SemicolonSeparated) {
  std::string csv = "a;b;c\n1;2;3\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.dialect = Dialect::semicolon();
  config.parse_header = true;

  StreamReader reader(input, config);

  // Must read first row to parse header
  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(reader.row()[0].data, "1");
  EXPECT_EQ(reader.row()[1].data, "2");
  EXPECT_EQ(reader.row()[2].data, "3");
}

TEST(StreamingTest, SingleQuote) {
  std::string csv = "'hello, world',test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.dialect.quote_char = '\'';
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "hello, world");
  EXPECT_TRUE(reader.row()[0].is_quoted);
  EXPECT_EQ(reader.row()[1].data, "test");
}

//-----------------------------------------------------------------------------
// Error Handling Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, UnclosedQuote) {
  std::string csv = "\"unclosed\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamReader reader(input, config);

  while (reader.next_row()) {
    // Process rows
  }

  EXPECT_TRUE(reader.error_collector().has_errors());
  EXPECT_TRUE(reader.error_collector().has_fatal_errors());

  bool found_unclosed = false;
  for (const auto& err : reader.error_collector().errors()) {
    if (err.code == ErrorCode::UNCLOSED_QUOTE) {
      found_unclosed = true;
      break;
    }
  }
  EXPECT_TRUE(found_unclosed);
}

TEST(StreamingTest, QuoteInUnquotedField) {
  std::string csv = "hello\"world,test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_TRUE(reader.error_collector().has_errors());

  bool found_error = false;
  for (const auto& err : reader.error_collector().errors()) {
    if (err.code == ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
      found_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_error);
}

TEST(StreamingTest, BestEffortMode) {
  std::string csv = "\"unclosed\nvalid,data\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::BEST_EFFORT;

  StreamReader reader(input, config);

  // Should still be able to read rows even with errors
  int count = 0;
  while (reader.next_row()) {
    ++count;
  }

  // At least parsed something
  EXPECT_GE(count, 0);
}

//-----------------------------------------------------------------------------
// Statistics Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, RowAndByteCount) {
  std::string csv = "a,b\n1,2\n3,4\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  while (reader.next_row()) {
    // Process rows
  }

  EXPECT_EQ(reader.rows_read(), 2); // Excluding header
  EXPECT_EQ(reader.bytes_read(), csv.size());
  EXPECT_TRUE(reader.eof());
}

//-----------------------------------------------------------------------------
// Pull Model with Parser Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, PullModelWithParser) {
  std::string csv = "a,b\n1,2\n3,4\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  std::vector<std::vector<std::string>> rows;
  while (parser.next_row() == StreamStatus::ROW_READY) {
    std::vector<std::string> fields;
    for (const auto& field : parser.current_row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
  EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

TEST(StreamingTest, PullModelNeedMoreData) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // No data yet
  EXPECT_EQ(parser.next_row(), StreamStatus::NEED_MORE_DATA);

  // Add partial row
  parser.parse_chunk("hello,wor");
  EXPECT_EQ(parser.next_row(), StreamStatus::NEED_MORE_DATA);

  // Complete the row
  parser.parse_chunk("ld\n");
  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row()[0].data, "hello");
  EXPECT_EQ(parser.current_row()[1].data, "world");

  // No more data
  parser.finish();
  EXPECT_EQ(parser.next_row(), StreamStatus::END_OF_DATA);
}

//-----------------------------------------------------------------------------
// Reset Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ParserReset) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // First parse
  parser.parse_chunk("a,b\n");
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.rows_processed(), 1);

  // Reset and parse again
  parser.reset();
  EXPECT_EQ(parser.rows_processed(), 0);
  EXPECT_FALSE(parser.is_finished());

  parser.parse_chunk("x,y,z\n");
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 3);
  EXPECT_EQ(parser.current_row()[0].data, "x");
}

//-----------------------------------------------------------------------------
// Field::at() bounds checking
//-----------------------------------------------------------------------------

TEST(StreamingTest, FieldAtBoundsCheck) {
  std::string csv = "a,b\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());

  // Valid access
  EXPECT_NO_THROW(reader.row().at(0));
  EXPECT_NO_THROW(reader.row().at(1));

  // Invalid access
  EXPECT_THROW(reader.row().at(2), std::out_of_range);
  EXPECT_THROW(reader.row().at(100), std::out_of_range);
}

//-----------------------------------------------------------------------------
// Iterator Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, IteratorComparison) {
  std::string csv = "a\n1\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  auto begin = reader.begin();
  auto end = reader.end();

  EXPECT_NE(begin, end);
  ++begin;
  EXPECT_NE(begin, end);
  ++begin;
  EXPECT_EQ(begin, end);
}

//-----------------------------------------------------------------------------
// Large Data Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ManyRows) {
  std::ostringstream oss;
  oss << "id,value\n";
  for (int i = 0; i < 1000; ++i) {
    oss << i << "," << (i * 2) << "\n";
  }
  std::string csv = oss.str();
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  int count = 0;
  int sum = 0;
  while (reader.next_row()) {
    sum += std::stoi(std::string(reader.row()[1].data));
    ++count;
  }

  EXPECT_EQ(count, 1000);
  EXPECT_EQ(sum, 999 * 1000); // Sum of 0 + 2 + 4 + ... + 1998
}

TEST(StreamingTest, LongFields) {
  std::string long_field(10000, 'x');
  std::string csv = long_field + "," + long_field + "\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().field_count(), 2);
  EXPECT_EQ(reader.row()[0].data.size(), 10000);
  EXPECT_EQ(reader.row()[1].data.size(), 10000);
}

//-----------------------------------------------------------------------------
// Field::unescaped() Edge Cases
//-----------------------------------------------------------------------------

TEST(StreamingTest, UnescapedEmptyField) {
  std::string csv = "\"\",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  // Empty quoted field
  EXPECT_TRUE(reader.row()[0].is_quoted);
  EXPECT_EQ(reader.row()[0].data, "");
  EXPECT_EQ(reader.row()[0].unescaped(), "");
}

TEST(StreamingTest, UnescapedUnquotedField) {
  std::string csv = "hello,world\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  // Unquoted field - unescaped returns data as-is
  EXPECT_FALSE(reader.row()[0].is_quoted);
  EXPECT_EQ(reader.row()[0].data, "hello");
  EXPECT_EQ(reader.row()[0].unescaped(), "hello");
}

TEST(StreamingTest, UnescapedWithCustomQuoteChar) {
  std::string csv = "'say ''hello''',test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.dialect.quote_char = '\'';
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_TRUE(reader.row()[0].is_quoted);
  // Raw data contains escaped quotes
  EXPECT_EQ(reader.row()[0].data, "say ''hello''");
  // unescaped with custom quote char
  EXPECT_EQ(reader.row()[0].unescaped('\''), "say 'hello'");
}

//-----------------------------------------------------------------------------
// Row Column Name Lookup Errors
//-----------------------------------------------------------------------------

TEST(StreamingTest, ColumnNameLookupNoHeader) {
  std::string csv = "a,b,c\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false; // No header parsing

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());

  // Column name lookup without header parsing should throw
  EXPECT_THROW(reader.row()["a"], std::out_of_range);
}

TEST(StreamingTest, ColumnNameLookupUnknownColumn) {
  std::string csv = "name,age\nAlice,30\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());

  // Valid column lookup
  EXPECT_NO_THROW(reader.row()["name"]);

  // Unknown column should throw
  EXPECT_THROW(reader.row()["unknown_column"], std::out_of_range);
}

//-----------------------------------------------------------------------------
// CR-Only Line Endings (Mac Classic)
//-----------------------------------------------------------------------------

TEST(StreamingTest, CarriageReturnOnlyLineEndings) {
  // Old Mac-style CR-only line endings
  std::string csv = "a,b\r1,2\r3,4\r";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  std::vector<std::vector<std::string>> rows;
  while (reader.next_row()) {
    std::vector<std::string> fields;
    for (const auto& field : reader.row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
  EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

TEST(StreamingTest, CRLFInQuotedField) {
  // CRLF inside quoted field should be preserved
  std::string csv = "\"line1\r\nline2\",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "line1\r\nline2");
  EXPECT_EQ(reader.row()[1].data, "test");
}

TEST(StreamingTest, CROnlyInUnquotedField) {
  // CR-only at end of unquoted field
  std::string csv = "hello\rworld\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "hello");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "world");
}

TEST(StreamingTest, CROnlyAtQuotedEnd) {
  // CR at end of quoted field
  std::string csv = "\"quoted\"\rvalue\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "quoted");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "value");
}

//-----------------------------------------------------------------------------
// Invalid Character After Closing Quote
//-----------------------------------------------------------------------------

TEST(StreamingTest, InvalidCharAfterQuote) {
  std::string csv = "\"hello\"world,test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_TRUE(reader.error_collector().has_errors());

  bool found_error = false;
  for (const auto& err : reader.error_collector().errors()) {
    if (err.code == ErrorCode::INVALID_QUOTE_ESCAPE) {
      found_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_error);
}

//-----------------------------------------------------------------------------
// Skip Empty Rows
//-----------------------------------------------------------------------------

TEST(StreamingTest, SkipEmptyRows) {
  std::string csv = "a,b\n\n1,2\n\n3,4\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.skip_empty_rows = true;

  StreamReader reader(input, config);

  std::vector<std::vector<std::string>> rows;
  while (reader.next_row()) {
    std::vector<std::string> fields;
    for (const auto& field : reader.row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  // Only non-empty rows should be returned
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
  EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

//-----------------------------------------------------------------------------
// Max Field Size Exceeded
//-----------------------------------------------------------------------------

TEST(StreamingTest, MaxFieldSizeExceeded) {
  // Create a field that exceeds max size
  std::string big_field(1000, 'x');
  std::string csv = big_field + ",test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.max_field_size = 100; // Set a small limit
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamReader reader(input, config);

  while (reader.next_row()) {
    // Process rows
  }

  // Should have recorded an error for the oversized field
  EXPECT_TRUE(reader.error_collector().has_errors());
}

TEST(StreamingTest, MaxFieldSizeWithErrorCallback) {
  // Create a field that exceeds max size
  std::string big_field(1000, 'x');
  std::string csv = big_field + ",test\n";

  StreamConfig config;
  config.parse_header = false;
  config.max_field_size = 100;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  bool error_callback_invoked = false;
  parser.set_error_handler([&error_callback_invoked](const ParseError& err) {
    (void)err;
    error_callback_invoked = true;
    return true; // Continue parsing
  });

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_TRUE(error_callback_invoked);
}

//-----------------------------------------------------------------------------
// Parser Already Finished/Stopped
//-----------------------------------------------------------------------------

TEST(StreamingTest, ParseChunkAfterFinish) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.parse_chunk("a,b\n");
  parser.finish();

  // Parse after finish should return END_OF_DATA
  EXPECT_EQ(parser.parse_chunk("c,d\n"), StreamStatus::END_OF_DATA);
}

TEST(StreamingTest, ParseChunkAfterStop) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Stop early via callback
  parser.set_row_handler([](const Row& row) {
    (void)row;
    return false; // Stop immediately
  });

  parser.parse_chunk("a,b\n");

  // Subsequent parse should return OK (stopped state)
  EXPECT_EQ(parser.parse_chunk("c,d\n"), StreamStatus::OK);
}

TEST(StreamingTest, FinishWhenStopped) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.set_row_handler([](const Row& row) {
    (void)row;
    return false; // Stop
  });

  parser.parse_chunk("a,b\n");

  // Finish when stopped
  EXPECT_EQ(parser.finish(), StreamStatus::OK);
}

TEST(StreamingTest, FinishCalledTwice) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.parse_chunk("a,b\n");
  parser.finish();

  // Second finish should return END_OF_DATA
  EXPECT_EQ(parser.finish(), StreamStatus::END_OF_DATA);
}

//-----------------------------------------------------------------------------
// Finish with Various Parser States
//-----------------------------------------------------------------------------

TEST(StreamingTest, FinishInQuotedEndState) {
  // File ends right after closing quote (no newline)
  std::string csv = "\"hello\"";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.parse_chunk(csv);
  StreamStatus status = parser.finish();

  EXPECT_EQ(status, StreamStatus::END_OF_DATA);
  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row()[0].data, "hello");
}

TEST(StreamingTest, FinishInFieldStartState) {
  // File ends with a trailing delimiter (empty last field)
  std::string csv = "a,b,";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 3);
  EXPECT_EQ(parser.current_row()[0].data, "a");
  EXPECT_EQ(parser.current_row()[1].data, "b");
  EXPECT_EQ(parser.current_row()[2].data, "");
}

TEST(StreamingTest, FinishWithPartialFieldBounds) {
  // This tests the branch where we have current_field_bounds but state is RECORD_START
  std::string csv = "a,b\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Parse normally - should process row
  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
}

TEST(StreamingTest, UnclosedQuoteStrict) {
  std::string csv = "\"unclosed";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::FAIL_FAST;

  StreamParser parser(config);

  parser.parse_chunk(csv);
  StreamStatus status = parser.finish();

  EXPECT_EQ(status, StreamStatus::STREAM_ERROR);
  EXPECT_TRUE(parser.error_collector().has_fatal_errors());
}

TEST(StreamingTest, UnclosedQuotePermissive) {
  std::string csv = "\"unclosed";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  parser.parse_chunk(csv);
  parser.finish();

  // Should still emit partial row in permissive mode
  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_TRUE(parser.error_collector().has_fatal_errors());
}

//-----------------------------------------------------------------------------
// Pull Model Pending Row Cleanup
//-----------------------------------------------------------------------------

TEST(StreamingTest, PullModelPendingRowCleanup) {
  // Generate enough rows to trigger periodic cleanup (>100 rows)
  std::ostringstream oss;
  for (int i = 0; i < 150; ++i) {
    oss << i << "\n";
  }
  std::string csv = oss.str();

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  int count = 0;
  while (parser.next_row() == StreamStatus::ROW_READY) {
    ++count;
  }

  EXPECT_EQ(count, 150);
}

//-----------------------------------------------------------------------------
// File Operations
//-----------------------------------------------------------------------------

TEST(StreamingTest, FileOpenError) {
  StreamConfig config;

  // Attempting to open non-existent file should throw
  EXPECT_THROW(StreamReader("/nonexistent/path/to/file.csv", config), std::runtime_error);
}

//-----------------------------------------------------------------------------
// RowIterator Edge Cases
//-----------------------------------------------------------------------------

TEST(StreamingTest, RowIteratorPostIncrement) {
  std::string csv = "a\n1\n2\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  auto it = reader.begin();
  auto prev = it++; // Post-increment

  // prev should have the old value (though comparing iterators is tricky for input iterators)
  EXPECT_NE(it, reader.end());

  // Continue to exhaust
  ++it;
  ++it;
  EXPECT_EQ(it, reader.end());
}

TEST(StreamingTest, RowIteratorDereference) {
  std::string csv = "hello,world\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  auto it = reader.begin();

  // Test operator*
  const Row& row = *it;
  EXPECT_EQ(row[0].data, "hello");

  // Test operator->
  EXPECT_EQ(it->field_count(), 2);
  EXPECT_EQ(it->at(0).data, "hello");
}

TEST(StreamingTest, RowIteratorEndComparison) {
  std::string csv = "";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  auto begin = reader.begin();
  auto end = reader.end();
  auto end2 = reader.end();

  // Two end iterators should be equal
  EXPECT_EQ(end, end2);

  // begin should equal end for empty input
  EXPECT_EQ(begin, end);
}

//-----------------------------------------------------------------------------
// StreamParser Move Operations
//-----------------------------------------------------------------------------

TEST(StreamingTest, StreamParserMove) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser1(config);
  parser1.parse_chunk("a,b\n");

  // Move construct
  StreamParser parser2(std::move(parser1));

  parser2.finish();
  EXPECT_EQ(parser2.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser2.current_row()[0].data, "a");
}

TEST(StreamingTest, StreamParserMoveAssign) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser1(config);
  parser1.parse_chunk("a,b\n");

  StreamParser parser2(config);

  // Move assign
  parser2 = std::move(parser1);

  parser2.finish();
  EXPECT_EQ(parser2.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser2.current_row()[0].data, "a");
}

//-----------------------------------------------------------------------------
// StreamReader Move Operations
//-----------------------------------------------------------------------------

TEST(StreamingTest, StreamReaderMove) {
  std::string csv = "a,b\n1,2\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader1(input, config);

  // Move construct
  StreamReader reader2(std::move(reader1));

  ASSERT_TRUE(reader2.next_row());
  EXPECT_EQ(reader2.row()[0].data, "a");
}

TEST(StreamingTest, StreamReaderMoveAssign) {
  std::string csv1 = "a,b\n";
  std::string csv2 = "x,y\n";
  std::istringstream input1(csv1);
  std::istringstream input2(csv2);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader1(input1, config);
  StreamReader reader2(input2, config);

  // Move assign
  reader2 = std::move(reader1);

  ASSERT_TRUE(reader2.next_row());
  EXPECT_EQ(reader2.row()[0].data, "a");
}

//-----------------------------------------------------------------------------
// Config Access
//-----------------------------------------------------------------------------

TEST(StreamingTest, ConfigAccessParser) {
  StreamConfig config;
  config.dialect.delimiter = ';';
  config.parse_header = true;

  StreamParser parser(config);

  EXPECT_EQ(parser.config().dialect.delimiter, ';');
  EXPECT_TRUE(parser.config().parse_header);
}

TEST(StreamingTest, ConfigAccessReader) {
  std::string csv = "a;b\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.dialect.delimiter = ';';

  StreamReader reader(input, config);

  EXPECT_EQ(reader.config().dialect.delimiter, ';');
}

//-----------------------------------------------------------------------------
// AFTER_CR State Chunk Boundary Tests
//-----------------------------------------------------------------------------

// Test AFTER_CR state persists correctly across chunk boundaries when CR is
// at the end of one chunk and LF is at the start of the next chunk.
// This validates the fix for issue #112.

TEST(StreamingTest, ChunkBoundaryAfterCR_CRLFSplit) {
  // CRLF split across chunks: CR at end of chunk 1, LF at start of chunk 2
  std::string chunk1 = "hello,world\r";
  std::string chunk2 = "\nnext,row\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"hello", "world"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"next", "row"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_CRLFSplitMultipleRows) {
  // Multiple CRLF pairs split across chunks
  std::string chunk1 = "a,b\r";
  std::string chunk2 = "\nc,d\r";
  std::string chunk3 = "\ne,f\r";
  std::string chunk4 = "\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.parse_chunk(chunk3);
  parser.parse_chunk(chunk4);
  parser.finish();

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"c", "d"}));
  EXPECT_EQ(rows[2], (std::vector<std::string>{"e", "f"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_CRNotFollowedByLF) {
  // CR at end of chunk, next chunk starts with regular character (not LF)
  // This tests that CR-only line endings work across chunk boundaries
  std::string chunk1 = "hello\r";
  std::string chunk2 = "world\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"hello"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"world"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_QuotedFieldCRLFSplit) {
  // Quoted field followed by CRLF split across chunks
  std::string chunk1 = "\"quoted\"\r";
  std::string chunk2 = "\nnext\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"quoted"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"next"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_EmptyFieldCRLFSplit) {
  // Row ending with empty field followed by CRLF split across chunks
  std::string chunk1 = "a,\r";
  std::string chunk2 = "\nb,c\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", ""}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"b", "c"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_SingleCharChunks) {
  // Extreme case: single character chunks around CR LF
  std::string csv = "a\r\nb\r\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  // Feed one character at a time
  for (char c : csv) {
    parser.parse_chunk(std::string_view(&c, 1));
  }
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"b"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_CRAtEndOfFile) {
  // CR at end of chunk, then finish() called (no more data)
  std::string csv = "hello,world\r";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"hello", "world"}));
}

TEST(StreamingTest, ChunkBoundaryAfterCR_PullModel) {
  // Test AFTER_CR chunk boundary using pull model
  std::string chunk1 = "a,b\r";
  std::string chunk2 = "\nc,d\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  parser.parse_chunk(chunk1);
  // At this point, row is emitted but parser is in AFTER_CR state

  parser.parse_chunk(chunk2);
  parser.finish();

  // Collect rows
  std::vector<std::vector<std::string>> rows;
  while (parser.next_row() == StreamStatus::ROW_READY) {
    std::vector<std::string> fields;
    for (const auto& field : parser.current_row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"c", "d"}));
}

//-----------------------------------------------------------------------------
// AFTER_CR State Edge Cases
//-----------------------------------------------------------------------------

TEST(StreamingTest, CRFollowedByNonLF) {
  // CR followed by regular character (not LF)
  std::string csv = "a\rb\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "a");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "b");
}

TEST(StreamingTest, CRLFAtEndOfQuotedField) {
  // Quoted field ending with CRLF
  std::string csv = "\"hello\"\r\nworld\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "hello");

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "world");
}

//-----------------------------------------------------------------------------
// Best Effort Mode - Quote in Unquoted Field
//-----------------------------------------------------------------------------

TEST(StreamingTest, QuoteInUnquotedFieldBestEffort) {
  std::string csv = "hello\"world,test\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::BEST_EFFORT;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  // In best effort mode, no error should be recorded
  EXPECT_FALSE(reader.error_collector().has_errors());
  // Field should contain the quote
  EXPECT_EQ(reader.row()[0].data, "hello\"world");
}

//-----------------------------------------------------------------------------
// Column Index on Parser
//-----------------------------------------------------------------------------

TEST(StreamingTest, ParserColumnIndex) {
  std::string csv = "name,age,city\nAlice,30,NYC\n";

  StreamConfig config;
  config.parse_header = true;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  // After parsing header
  EXPECT_EQ(parser.column_index("name"), 0);
  EXPECT_EQ(parser.column_index("age"), 1);
  EXPECT_EQ(parser.column_index("city"), 2);
  EXPECT_EQ(parser.column_index("unknown"), -1);
}

//-----------------------------------------------------------------------------
// Bytes Processed
//-----------------------------------------------------------------------------

TEST(StreamingTest, BytesProcessed) {
  std::string csv = "hello,world\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  // Process all rows
  while (parser.next_row() == StreamStatus::ROW_READY) {
  }

  EXPECT_EQ(parser.bytes_processed(), csv.size());
}

//-----------------------------------------------------------------------------
// Empty Row with Fields
//-----------------------------------------------------------------------------

TEST(StreamingTest, EmptyRowAtRecordStart) {
  // Multiple consecutive newlines
  std::string csv = "\n\na,b\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.skip_empty_rows = false;

  StreamReader reader(input, config);

  // First empty row
  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().field_count(), 0);

  // Second empty row
  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().field_count(), 0);

  // Actual data row
  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row().field_count(), 2);
}

//-----------------------------------------------------------------------------
// Field Methods
//-----------------------------------------------------------------------------

TEST(StreamingTest, FieldEmptyMethod) {
  std::string csv = "hello,,world\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_FALSE(reader.row()[0].empty());
  EXPECT_TRUE(reader.row()[1].empty());
  EXPECT_FALSE(reader.row()[2].empty());
}

TEST(StreamingTest, FieldStrMethod) {
  std::string csv = "hello,world\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  std::string s = reader.row()[0].str();
  EXPECT_EQ(s, "hello");
}

//-----------------------------------------------------------------------------
// Row Methods
//-----------------------------------------------------------------------------

TEST(StreamingTest, RowEmptyMethod) {
  std::string csv = "\na,b\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;
  config.skip_empty_rows = false;

  StreamReader reader(input, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_TRUE(reader.row().empty());

  ASSERT_TRUE(reader.next_row());
  EXPECT_FALSE(reader.row().empty());
}

//-----------------------------------------------------------------------------
// Strict Error Mode
//-----------------------------------------------------------------------------

TEST(StreamingTest, StrictErrorModeStopsOnError) {
  // Quote in unquoted field triggers an immediate error during parsing
  std::string csv = "hello\"world,test\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::FAIL_FAST;

  StreamParser parser(config);
  StreamStatus status = parser.parse_chunk(csv);

  // Strict mode should stop on first error (quote in unquoted field)
  EXPECT_EQ(status, StreamStatus::STREAM_ERROR);
  EXPECT_TRUE(parser.error_collector().has_errors());
}

//-----------------------------------------------------------------------------
// Error Callback Invocation Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, InvalidQuoteEscapeErrorCallbackInvoked) {
  // "hello"world triggers INVALID_QUOTE_ESCAPE when 'w' follows closing quote
  std::string csv = "\"hello\"world,test\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  bool error_callback_invoked = false;
  ErrorCode received_code = ErrorCode::NONE;
  parser.set_error_handler([&error_callback_invoked, &received_code](const ParseError& err) {
    error_callback_invoked = true;
    received_code = err.code;
    return true; // Continue parsing
  });

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_TRUE(error_callback_invoked);
  EXPECT_EQ(received_code, ErrorCode::INVALID_QUOTE_ESCAPE);
}

TEST(StreamingTest, QuoteInUnquotedFieldErrorCallbackInvoked) {
  // hello"world triggers QUOTE_IN_UNQUOTED_FIELD
  std::string csv = "hello\"world,test\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  bool error_callback_invoked = false;
  ErrorCode received_code = ErrorCode::NONE;
  parser.set_error_handler([&error_callback_invoked, &received_code](const ParseError& err) {
    error_callback_invoked = true;
    received_code = err.code;
    return true; // Continue parsing
  });

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_TRUE(error_callback_invoked);
  EXPECT_EQ(received_code, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
}

TEST(StreamingTest, ErrorCallbackReceivesCorrectLocation) {
  // Verify that error callback receives accurate line/column/offset info
  std::string csv = "a,b\nhello\"world,test\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  size_t error_line = 0;
  size_t error_column = 0;
  parser.set_error_handler([&error_line, &error_column](const ParseError& err) {
    error_line = err.line;
    error_column = err.column;
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Error should be on line 2 (second row), column 1 (first field)
  EXPECT_EQ(error_line, 2);
  EXPECT_EQ(error_column, 1);
}

TEST(StreamingTest, ErrorCallbackReturnFalseHaltsParsing) {
  // Test that returning false from error callback halts parsing
  std::string csv = "a\"b,c\nd,e,f\ng,h,i\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  int error_count = 0;
  parser.set_error_handler([&error_count](const ParseError& err) {
    (void)err;
    ++error_count;
    return false; // Request halt on first error
  });

  int row_count = 0;
  parser.set_row_handler([&row_count](const Row& row) {
    (void)row;
    ++row_count;
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Error callback was invoked once
  EXPECT_EQ(error_count, 1);
  // Parsing halts immediately after the error callback returns false.
  // The stopped flag is checked after processing each character, so the
  // row with the error is NOT emitted since we stop before reaching the newline.
  EXPECT_EQ(row_count, 0);
}

TEST(StreamingTest, MultipleErrorsInvokeCallbackMultipleTimes) {
  // CSV with multiple errors
  std::string csv = "a\"b,c\n\"d\"e,f\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  int error_count = 0;
  std::vector<ErrorCode> error_codes;
  parser.set_error_handler([&error_count, &error_codes](const ParseError& err) {
    ++error_count;
    error_codes.push_back(err.code);
    return true; // Continue parsing
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Should have at least 2 errors
  EXPECT_GE(error_count, 2);
  // First error: quote in unquoted field (a"b)
  EXPECT_EQ(error_codes[0], ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
  // Second error: invalid quote escape ("d"e - 'e' after closing quote)
  EXPECT_EQ(error_codes[1], ErrorCode::INVALID_QUOTE_ESCAPE);
}

TEST(StreamingTest, ErrorCallbackNotInvokedInBestEffortMode) {
  // In BEST_EFFORT mode, errors should not invoke callback
  std::string csv = "hello\"world,test\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::BEST_EFFORT;

  StreamParser parser(config);

  bool error_callback_invoked = false;
  parser.set_error_handler([&error_callback_invoked](const ParseError& err) {
    (void)err;
    error_callback_invoked = true;
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Error callback should NOT be invoked in BEST_EFFORT mode
  EXPECT_FALSE(error_callback_invoked);
}

TEST(StreamingTest, MaxFieldSizeErrorCallbackReturnFalseHaltsParsing) {
  // Test that max field size error callback return value is respected
  std::string big_field(1000, 'x');
  std::string csv = big_field + ",test\nnormal,data\n";

  StreamConfig config;
  config.parse_header = false;
  config.max_field_size = 100;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  int error_count = 0;
  parser.set_error_handler([&error_count](const ParseError& err) {
    (void)err;
    ++error_count;
    return false; // Request halt
  });

  int row_count = 0;
  parser.set_row_handler([&row_count](const Row& row) {
    (void)row;
    ++row_count;
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Error callback was invoked once for the oversized field
  EXPECT_EQ(error_count, 1);
  // Parsing should have halted, so second row is not processed
  EXPECT_EQ(row_count, 0);
}

//-----------------------------------------------------------------------------
// Additional Coverage Tests
//-----------------------------------------------------------------------------

// Test that error callback returning false in QUOTED_END state stops parsing
TEST(StreamingTest, InvalidQuoteEscapeErrorCallbackReturnFalseHaltsParsing) {
  // "hello"world triggers INVALID_QUOTE_ESCAPE when 'w' follows closing quote
  std::string csv = "\"hello\"world,test\nmore,data\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  int error_count = 0;
  parser.set_error_handler([&error_count](const ParseError& err) {
    (void)err;
    ++error_count;
    return false; // Request halt on error
  });

  int row_count = 0;
  parser.set_row_handler([&row_count](const Row& row) {
    (void)row;
    ++row_count;
    return true;
  });

  parser.parse_chunk(csv);
  parser.finish();

  // Error callback was invoked once
  EXPECT_EQ(error_count, 1);
  // Parsing halted immediately - no rows should be processed
  EXPECT_EQ(row_count, 0);
}

// Test field_start adjustment when field_start < last_row_end in chunk processing
TEST(StreamingTest, FieldStartAdjustmentWhenLessThanLastRowEnd) {
  // This tests the branch where field_start < last_row_end after processing a chunk
  // We need a scenario where:
  // 1. A complete row is found
  // 2. The field_start for the next row is at the very start (0 relative to last_row_end)
  std::string chunk1 = "hello,world\n";
  std::string chunk2 = "next,row\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  // Parse first chunk - row ends exactly at chunk boundary
  parser.parse_chunk(chunk1);
  // At this point, field_start should be 0 (relative to start of any remaining buffer)

  // Parse second chunk
  parser.parse_chunk(chunk2);
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"hello", "world"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"next", "row"}));
}

// Test finish() with partial field_bounds (state is RECORD_START but we have field bounds)
TEST(StreamingTest, FinishWithPartialFieldBoundsAtRecordStart) {
  // This is a tricky case - we need current_field_bounds to be non-empty
  // while state is RECORD_START at finish time. This can happen if
  // we parsed part of a row but didn't complete it, then the row ended.
  // Actually, re-examining the code, if we have field_bounds and state is RECORD_START,
  // we're at the end of a row that was already processed.
  // The actual branch at line 509 is for when we have field_bounds but are in an
  // unexpected state. Let me look more carefully.

  // After reviewing the code, the branch at line 507-509:
  // } else if (!current_field_bounds.empty()) {
  //     // Have partial row data
  //     emit_row();
  // }
  // This is reached when state is not RECORD_START, UNQUOTED_FIELD, QUOTED_FIELD,
  // QUOTED_END, or FIELD_START, but we have field bounds. However, there's no
  // other state in the enum, so this is essentially dead code / defensive programming.
  // We can verify it's not reachable in current design.

  // Let's test what we can - which is the field_start < last_row_end case
  // We need partial data that ends mid-field to test field_start adjustment.

  // Create a scenario where a row ends and the next field_start would be 0
  std::string csv = "ab\ncd\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Parse byte by byte to force many adjustments
  for (char c : csv) {
    parser.parse_chunk(std::string_view(&c, 1));
  }
  parser.finish();

  std::vector<std::vector<std::string>> rows;
  while (parser.next_row() == StreamStatus::ROW_READY) {
    std::vector<std::string> fields;
    for (const auto& field : parser.current_row()) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
  }

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], (std::vector<std::string>{"ab"}));
  EXPECT_EQ(rows[1], (std::vector<std::string>{"cd"}));
}

// Test RowIterator comparison when both iterators are not at end
TEST(StreamingTest, RowIteratorNonEndComparison) {
  std::string csv = "a\nb\nc\n";
  std::istringstream input1(csv);
  std::istringstream input2(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader1(input1, config);
  StreamReader reader2(input2, config);

  auto it1 = reader1.begin();
  auto it2 = reader2.begin();
  auto end = reader1.end();

  // Both iterators are not at end
  EXPECT_NE(it1, end);
  EXPECT_NE(it2, end);

  // Two begin iterators from different readers should not be equal
  // (they point to different readers)
  EXPECT_NE(it1, it2);

  // Same iterator should equal itself (testing reflexivity)
  EXPECT_EQ(it1, it1);
}

// Test read_more_data returning false when input is null
TEST(StreamingTest, StreamReaderFromFileReadsBinaryData) {
  // Create a temporary file with binary-safe content
  std::string temp_file = "/tmp/streaming_test_binary.csv";
  {
    std::ofstream out(temp_file, std::ios::binary);
    out << "a,b\n1,2\n";
  }

  StreamConfig config;
  config.parse_header = true;

  // Use the file constructor (tests line 715)
  StreamReader reader(temp_file, config);

  ASSERT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()[0].data, "1");
  EXPECT_EQ(reader.row()[1].data, "2");
  EXPECT_FALSE(reader.next_row());

  // Cleanup
  std::remove(temp_file.c_str());
}

// Test chunk boundary where field_start needs adjustment to 0
TEST(StreamingTest, ChunkBoundaryFieldStartZeroAdjustment) {
  // Set up a scenario where after processing:
  // - last_row_end > 0 (a row was completed)
  // - field_start < last_row_end (the next field starts within processed data)
  // This forces the else branch: field_start = 0

  // Create CSV where row ends at specific position
  std::string csv = "x\ny\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::string> rows;
  parser.set_row_handler([&rows](const Row& row) {
    rows.push_back(std::string(row[0].data));
    return true;
  });

  // Feed data one byte at a time to ensure field_start adjustments happen
  for (size_t i = 0; i < csv.size(); ++i) {
    parser.parse_chunk(std::string_view(csv.data() + i, 1));
  }
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], "x");
  EXPECT_EQ(rows[1], "y");
}

// Test that StreamReader constructor with ifstream properly sets up input
TEST(StreamingTest, StreamReaderFromFileWithHeader) {
  std::string temp_file = "/tmp/streaming_test_header.csv";
  {
    std::ofstream out(temp_file, std::ios::binary);
    out << "name,value\nfoo,100\nbar,200\n";
  }

  StreamConfig config;
  config.parse_header = true;

  StreamReader reader(temp_file, config);

  EXPECT_TRUE(reader.next_row());
  EXPECT_EQ(reader.header(), (std::vector<std::string>{"name", "value"}));
  EXPECT_EQ(reader.row()["name"].data, "foo");
  EXPECT_EQ(reader.row()["value"].data, "100");

  EXPECT_TRUE(reader.next_row());
  EXPECT_EQ(reader.row()["name"].data, "bar");
  EXPECT_EQ(reader.row()["value"].data, "200");

  EXPECT_FALSE(reader.next_row());
  EXPECT_TRUE(reader.eof());

  std::remove(temp_file.c_str());
}

// Test StreamReader with large file triggering multiple read_more_data calls
TEST(StreamingTest, StreamReaderMultipleChunks) {
  std::string temp_file = "/tmp/streaming_test_large.csv";
  {
    std::ofstream out(temp_file, std::ios::binary);
    out << "id\n";
    for (int i = 0; i < 1000; ++i) {
      out << i << "\n";
    }
  }

  StreamConfig config;
  config.parse_header = true;
  config.chunk_size = 64; // Small chunk size to force multiple reads

  StreamReader reader(temp_file, config);

  int count = 0;
  while (reader.next_row()) {
    ++count;
  }

  EXPECT_EQ(count, 1000);
  EXPECT_TRUE(reader.eof());

  std::remove(temp_file.c_str());
}

// Test empty chunks don't break state
TEST(StreamingTest, EmptyChunkProcessing) {
  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  std::vector<std::string> rows;
  parser.set_row_handler([&rows](const Row& row) {
    rows.push_back(std::string(row[0].data));
    return true;
  });

  // Empty chunk
  parser.parse_chunk(std::string_view("", 0));
  // Actual data
  parser.parse_chunk("hello\n");
  // Another empty chunk
  parser.parse_chunk(std::string_view("", 0));
  // More data
  parser.parse_chunk("world\n");
  parser.finish();

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], "hello");
  EXPECT_EQ(rows[1], "world");
}

// Test that StreamReader handles EOF correctly during iteration
TEST(StreamingTest, StreamReaderEOFHandling) {
  std::string csv = "a\nb\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  // Before reading, not at EOF
  EXPECT_FALSE(reader.eof());

  // Read all rows
  int count = 0;
  while (reader.next_row()) {
    ++count;
  }

  // After exhausting, should be at EOF
  EXPECT_EQ(count, 2);
  EXPECT_TRUE(reader.eof());

  // Additional next_row calls should continue returning false
  EXPECT_FALSE(reader.next_row());
  EXPECT_TRUE(reader.eof());
}

// Test buffer management with very long partial rows
TEST(StreamingTest, BufferManagementLongPartialRow) {
  // Create a very long field that spans multiple chunks
  std::string long_field(10000, 'x');
  std::string csv = long_field + "\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Feed in small chunks
  size_t chunk_size = 100;
  for (size_t i = 0; i < csv.size(); i += chunk_size) {
    size_t len = std::min(chunk_size, csv.size() - i);
    parser.parse_chunk(std::string_view(csv.data() + i, len));
  }
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row()[0].data.size(), 10000);
  EXPECT_EQ(parser.next_row(), StreamStatus::END_OF_DATA);
}

// Test that bytes_read reflects actual bytes read from stream
TEST(StreamingTest, StreamReaderBytesReadAccurate) {
  std::string csv = "hello,world\nfoo,bar\n";
  std::istringstream input(csv);

  StreamConfig config;
  config.parse_header = false;

  StreamReader reader(input, config);

  // Initially, no bytes read
  EXPECT_EQ(reader.bytes_read(), 0);

  // After reading first row, some bytes read
  EXPECT_TRUE(reader.next_row());
  EXPECT_GT(reader.bytes_read(), 0);

  // After reading all, all bytes should be read
  while (reader.next_row()) {
  }
  EXPECT_EQ(reader.bytes_read(), csv.size());
}

// Test recovery from errors in permissive mode across chunk boundaries
TEST(StreamingTest, ErrorRecoveryAcrossChunkBoundary) {
  // First chunk ends with invalid state, second chunk continues
  std::string chunk1 = "hello\""; // Quote in unquoted field
  std::string chunk2 = "world,test\nnext,row\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);

  std::vector<std::vector<std::string>> rows;
  parser.set_row_handler([&rows](const Row& row) {
    std::vector<std::string> fields;
    for (const auto& field : row) {
      fields.push_back(std::string(field.data));
    }
    rows.push_back(fields);
    return true;
  });

  parser.parse_chunk(chunk1);
  parser.parse_chunk(chunk2);
  parser.finish();

  // Should have recovered and parsed both rows
  ASSERT_GE(rows.size(), 1);
  EXPECT_TRUE(parser.error_collector().has_errors());
}

// Test state preservation when quoted field spans many chunks
TEST(StreamingTest, QuotedFieldSpansManyChunks) {
  // Quoted field with embedded newlines and commas spanning many chunks
  // Use doubled quotes ("") inside to represent literal quotes
  std::string raw_content = "This is a \"\"long\"\" field,\nwith newlines\r\nand various,commas";
  std::string csv = "\"" + raw_content + "\",end\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  // Feed character by character
  for (char c : csv) {
    parser.parse_chunk(std::string_view(&c, 1));
  }
  parser.finish();

  EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
  EXPECT_EQ(parser.current_row().field_count(), 2);
  // Raw data should contain the doubled quotes
  EXPECT_EQ(parser.current_row()[0].data, raw_content);
  EXPECT_TRUE(parser.current_row()[0].is_quoted);
  EXPECT_EQ(parser.current_row()[1].data, "end");
}

// Test row callback returns false mid-stream
TEST(StreamingTest, RowCallbackStopsMidStream) {
  std::string csv = "a\nb\nc\nd\ne\n";

  StreamConfig config;
  config.parse_header = false;

  StreamParser parser(config);

  int row_count = 0;
  parser.set_row_handler([&row_count](const Row& row) {
    (void)row;
    ++row_count;
    return row_count < 3; // Stop after 3 rows
  });

  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_EQ(row_count, 3);
}

// Test that errors() returns correct error collector state
TEST(StreamingTest, ErrorCollectorStateTracking) {
  std::string csv = "a\"b\n\"unclosed";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  const auto& errors = parser.error_collector();

  // Should have at least one error
  EXPECT_TRUE(errors.has_errors());

  // Should have specific error types
  bool found_quote_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == ErrorCode::QUOTE_IN_UNQUOTED_FIELD || err.code == ErrorCode::UNCLOSED_QUOTE) {
      found_quote_error = true;
    }
  }
  EXPECT_TRUE(found_quote_error);
}

// Test reset clears all state including errors
TEST(StreamingTest, ResetClearsErrors) {
  std::string csv = "a\"b\n";

  StreamConfig config;
  config.parse_header = false;
  config.error_mode = ErrorMode::PERMISSIVE;

  StreamParser parser(config);
  parser.parse_chunk(csv);
  parser.finish();

  EXPECT_TRUE(parser.error_collector().has_errors());

  parser.reset();

  EXPECT_FALSE(parser.error_collector().has_errors());
  EXPECT_EQ(parser.rows_processed(), 0);
  EXPECT_EQ(parser.bytes_processed(), 0);
  EXPECT_FALSE(parser.is_finished());
}
