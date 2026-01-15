/**
 * @file branchless_test.cpp
 * @brief Unit tests for the branchless CSV state machine implementation.
 */

#include "libvroom.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

using namespace libvroom;

// ============================================================================
// BRANCHLESS STATE MACHINE UNIT TESTS
// ============================================================================

class BranchlessStateMachineTest : public ::testing::Test {
protected:
  BranchlessStateMachine sm;

  BranchlessStateMachineTest() : sm(',', '"') {}
};

TEST_F(BranchlessStateMachineTest, CharacterClassification) {
  // Test character classification
  EXPECT_EQ(sm.classify(','), CHAR_DELIMITER);
  EXPECT_EQ(sm.classify('"'), CHAR_QUOTE);
  EXPECT_EQ(sm.classify('\n'), CHAR_NEWLINE);
  EXPECT_EQ(sm.classify('a'), CHAR_OTHER);
  EXPECT_EQ(sm.classify('1'), CHAR_OTHER);
  EXPECT_EQ(sm.classify(' '), CHAR_OTHER);
  EXPECT_EQ(sm.classify('\t'), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, CustomDelimiter) {
  BranchlessStateMachine sm_tab('\t', '"');
  EXPECT_EQ(sm_tab.classify('\t'), CHAR_DELIMITER);
  EXPECT_EQ(sm_tab.classify(','), CHAR_OTHER);

  BranchlessStateMachine sm_semicolon(';', '"');
  EXPECT_EQ(sm_semicolon.classify(';'), CHAR_DELIMITER);
  EXPECT_EQ(sm_semicolon.classify(','), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, CustomQuote) {
  BranchlessStateMachine sm_single(',', '\'');
  EXPECT_EQ(sm_single.classify('\''), CHAR_QUOTE);
  EXPECT_EQ(sm_single.classify('"'), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_RecordStart) {
  PackedResult r;

  // RECORD_START + DELIMITER -> FIELD_START (separator)
  r = sm.transition(STATE_RECORD_START, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // RECORD_START + QUOTE -> QUOTED_FIELD
  r = sm.transition(STATE_RECORD_START, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // RECORD_START + NEWLINE -> RECORD_START (separator)
  r = sm.transition(STATE_RECORD_START, CHAR_NEWLINE);
  EXPECT_EQ(r.state(), STATE_RECORD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // RECORD_START + OTHER -> UNQUOTED_FIELD
  r = sm.transition(STATE_RECORD_START, CHAR_OTHER);
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_FieldStart) {
  PackedResult r;

  // FIELD_START + DELIMITER -> FIELD_START (empty field, separator)
  r = sm.transition(STATE_FIELD_START, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // FIELD_START + QUOTE -> QUOTED_FIELD
  r = sm.transition(STATE_FIELD_START, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // FIELD_START + NEWLINE -> RECORD_START (separator)
  r = sm.transition(STATE_FIELD_START, CHAR_NEWLINE);
  EXPECT_EQ(r.state(), STATE_RECORD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // FIELD_START + OTHER -> UNQUOTED_FIELD
  r = sm.transition(STATE_FIELD_START, CHAR_OTHER);
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_UnquotedField) {
  PackedResult r;

  // UNQUOTED_FIELD + DELIMITER -> FIELD_START (separator)
  r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // UNQUOTED_FIELD + QUOTE -> error
  r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_QUOTE_IN_UNQUOTED);

  // UNQUOTED_FIELD + NEWLINE -> RECORD_START (separator)
  r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_NEWLINE);
  EXPECT_EQ(r.state(), STATE_RECORD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // UNQUOTED_FIELD + OTHER -> UNQUOTED_FIELD
  r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_OTHER);
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_QuotedField) {
  PackedResult r;

  // QUOTED_FIELD + DELIMITER -> QUOTED_FIELD (literal comma)
  r = sm.transition(STATE_QUOTED_FIELD, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_FIELD + QUOTE -> QUOTED_END
  r = sm.transition(STATE_QUOTED_FIELD, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_QUOTED_END);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_FIELD + NEWLINE -> QUOTED_FIELD (literal newline)
  r = sm.transition(STATE_QUOTED_FIELD, CHAR_NEWLINE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_FIELD + OTHER -> QUOTED_FIELD
  r = sm.transition(STATE_QUOTED_FIELD, CHAR_OTHER);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_QuotedEnd) {
  PackedResult r;

  // QUOTED_END + DELIMITER -> FIELD_START (separator)
  r = sm.transition(STATE_QUOTED_END, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_END + QUOTE -> QUOTED_FIELD (escaped quote)
  r = sm.transition(STATE_QUOTED_END, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_END + NEWLINE -> RECORD_START (separator)
  r = sm.transition(STATE_QUOTED_END, CHAR_NEWLINE);
  EXPECT_EQ(r.state(), STATE_RECORD_START);
  EXPECT_TRUE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_NONE);

  // QUOTED_END + OTHER -> error
  r = sm.transition(STATE_QUOTED_END, CHAR_OTHER);
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());
  EXPECT_EQ(r.error(), ERR_INVALID_AFTER_QUOTE);
}

TEST_F(BranchlessStateMachineTest, ProcessCharacter) {
  BranchlessState state = STATE_RECORD_START;
  PackedResult r;

  // Process "ab,cd\n"
  r = sm.process(state, 'a');
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  state = r.state();

  r = sm.process(state, 'b');
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  state = r.state();

  r = sm.process(state, ',');
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator());
  state = r.state();

  r = sm.process(state, 'c');
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  state = r.state();

  r = sm.process(state, 'd');
  EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
  state = r.state();

  r = sm.process(state, '\n');
  EXPECT_EQ(r.state(), STATE_RECORD_START);
  EXPECT_TRUE(r.is_separator());
}

TEST_F(BranchlessStateMachineTest, ProcessQuotedField) {
  BranchlessState state = STATE_RECORD_START;
  PackedResult r;

  // Process "\"a,b\""
  r = sm.process(state, '"');
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  state = r.state();

  r = sm.process(state, 'a');
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  state = r.state();

  r = sm.process(state, ',');
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD); // Comma inside quotes
  EXPECT_FALSE(r.is_separator());
  state = r.state();

  r = sm.process(state, 'b');
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  state = r.state();

  r = sm.process(state, '"');
  EXPECT_EQ(r.state(), STATE_QUOTED_END);
  state = r.state();

  r = sm.process(state, ',');
  EXPECT_EQ(r.state(), STATE_FIELD_START);
  EXPECT_TRUE(r.is_separator()); // Comma after quote ends field
}

TEST_F(BranchlessStateMachineTest, ProcessEscapedQuote) {
  BranchlessState state = STATE_RECORD_START;
  PackedResult r;

  // Process "\"a\"\"b\""
  r = sm.process(state, '"');
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_FIELD);

  r = sm.process(state, 'a');
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_FIELD);

  r = sm.process(state, '"');
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_END);

  r = sm.process(state, '"'); // Escaped quote
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_FIELD);
  EXPECT_FALSE(r.is_separator());

  r = sm.process(state, 'b');
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_FIELD);

  r = sm.process(state, '"');
  state = r.state();
  EXPECT_EQ(state, STATE_QUOTED_END);
}

// ============================================================================
// BRANCHLESS PARSING INTEGRATION TESTS
// ============================================================================

class BranchlessParsingTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }
};

TEST_F(BranchlessParsingTest, ParseSimpleCSV) {
  std::string path = getTestDataPath("basic", "simple.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should successfully parse simple.csv";
}

TEST_F(BranchlessParsingTest, ParseQuotedFields) {
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle quoted fields";
}

TEST_F(BranchlessParsingTest, ParseEscapedQuotes) {
  std::string path = getTestDataPath("quoted", "escaped_quotes.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle escaped quotes";
}

TEST_F(BranchlessParsingTest, ParseNewlinesInQuotes) {
  std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle newlines in quoted fields";
}

TEST_F(BranchlessParsingTest, ParseManyRows) {
  std::string path = getTestDataPath("basic", "many_rows.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle many rows";
}

TEST_F(BranchlessParsingTest, ParseWideColumns) {
  std::string path = getTestDataPath("basic", "wide_columns.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle wide CSV";
}

TEST_F(BranchlessParsingTest, ParseEmptyFields) {
  std::string path = getTestDataPath("edge_cases", "empty_fields.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle empty fields";
}

TEST_F(BranchlessParsingTest, ParseCustomDelimiter) {
  // Test with semicolon delimiter
  std::vector<uint8_t> data;
  std::string content = "A;B;C\n1;2;3\n";
  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success =
      parser.parse_branchless(data.data(), idx, content.size(), libvroom::Dialect::semicolon());

  EXPECT_TRUE(success) << "Branchless parser should handle semicolon delimiter";
}

TEST_F(BranchlessParsingTest, ParseCustomQuote) {
  // Test with single quote
  std::vector<uint8_t> data;
  std::string content = "A,B,C\n'a,b',2,3\n";
  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  // Create dialect with single quote as quote character
  libvroom::Dialect dialect{',', '\'', '\'', true, libvroom::Dialect::LineEnding::UNKNOWN};
  bool success = parser.parse_branchless(data.data(), idx, content.size(), dialect);

  EXPECT_TRUE(success) << "Branchless parser should handle single quote character";
}

TEST_F(BranchlessParsingTest, MultiThreadedParsing) {
  std::string path = getTestDataPath("basic", "many_rows.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 2);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size);

  EXPECT_TRUE(success) << "Branchless parser should handle multi-threaded parsing";
}

TEST_F(BranchlessParsingTest, ConsistencyWithStandardParser) {
  // Verify branchless parser produces same results as standard parser
  std::string path = getTestDataPath("basic", "simple.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;

  // Parse with standard parser
  libvroom::ParseIndex idx1 = parser.init(buffer.size, 1);
  parser.parse(buffer.data(), idx1, buffer.size);

  // Parse with branchless parser
  libvroom::ParseIndex idx2 = parser.init(buffer.size, 1);
  parser.parse_branchless(buffer.data(), idx2, buffer.size);

  // Compare results
  EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
      << "Branchless parser should find same number of field separators";

  for (size_t i = 0; i < idx1.n_indexes[0]; ++i) {
    EXPECT_EQ(idx1.indexes[i], idx2.indexes[i])
        << "Field separator positions should match at index " << i;
  }
}

TEST_F(BranchlessParsingTest, ConsistencyWithQuotedFields) {
  // Verify branchless parser produces same results with quoted fields
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");

  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;

  // Parse with standard parser
  libvroom::ParseIndex idx1 = parser.init(buffer.size, 1);
  parser.parse(buffer.data(), idx1, buffer.size);

  // Parse with branchless parser
  libvroom::ParseIndex idx2 = parser.init(buffer.size, 1);
  parser.parse_branchless(buffer.data(), idx2, buffer.size);

  // Compare results
  EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
      << "Branchless parser should find same number of field separators";

  for (size_t i = 0; i < idx1.n_indexes[0]; ++i) {
    EXPECT_EQ(idx1.indexes[i], idx2.indexes[i])
        << "Field separator positions should match at index " << i;
  }
}

TEST_F(BranchlessParsingTest, LargeDataMultithreaded) {
  // Test with large generated data
  std::vector<uint8_t> data;
  std::string content;

  // Generate large CSV
  content = "A,B,C\n";
  for (int i = 0; i < 10000; i++) {
    content += std::to_string(i) + ",";
    content += "\"value" + std::to_string(i) + "\",";
    content += "data" + std::to_string(i) + "\n";
  }

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 4);

  bool success = parser.parse_branchless(data.data(), idx, content.size());

  EXPECT_TRUE(success) << "Branchless parser should handle large multithreaded data";
}

TEST_F(BranchlessParsingTest, CustomDelimiterMultithreaded) {
  // Test multi-threaded parsing with semicolon delimiter
  std::vector<uint8_t> data;
  std::string content;

  // Generate large semicolon-delimited data
  content = "A;B;C\n";
  for (int i = 0; i < 10000; i++) {
    content += std::to_string(i) + ";";
    content += "\"value" + std::to_string(i) + "\";";
    content += "data" + std::to_string(i) + "\n";
  }

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 4);

  bool success =
      parser.parse_branchless(data.data(), idx, content.size(), libvroom::Dialect::semicolon());

  EXPECT_TRUE(success) << "Branchless parser should handle multi-threaded semicolon delimiter";

  // Verify we found the expected number of separators
  uint64_t total_seps = 0;
  for (int i = 0; i < 4; i++) {
    total_seps += idx.n_indexes[i];
  }
  // Should have ~30000 separators (3 per row * 10001 rows including header)
  EXPECT_GT(total_seps, 30000) << "Should find separators with semicolon delimiter";
}

// Test specifically designed to trigger the data race condition fixed in issue #343.
// The race occurred when multiple threads wrote to the same index positions due to
// incorrect offset calculation in the write() function. This test uses many threads
// and repeated iterations to maximize the chance of detecting any race conditions
// under ThreadSanitizer.
TEST_F(BranchlessParsingTest, ThreadSafetyStressTest) {
  // Generate CSV with many separators per block to stress the interleaved write pattern
  std::vector<uint8_t> data;
  std::string content;

  // Create dense CSV: many short fields = many separators = high write contention
  content = "A,B,C,D,E,F,G,H\n";
  for (int i = 0; i < 50000; i++) {
    content += "1,2,3,4,5,6,7,8\n";
  }

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  // Run multiple iterations to increase chance of exposing race conditions
  for (int iteration = 0; iteration < 5; iteration++) {
    // Test with maximum threads to increase contention
    int n_threads = 8;

    libvroom::TwoPass parser;
    libvroom::ParseIndex idx = parser.init(data.size(), n_threads);

    bool success = parser.parse_branchless(data.data(), idx, content.size());
    EXPECT_TRUE(success) << "Iteration " << iteration << ": parse should succeed";

    // Verify separator counts
    uint64_t total_seps = 0;
    for (int t = 0; t < n_threads; t++) {
      total_seps += idx.n_indexes[t];
    }

    // 8 separators per row (7 commas + 1 newline) * 50001 rows (header + data)
    // Expected: 400008 separators. Allow small variation due to chunk boundary handling.
    EXPECT_GE(total_seps, 400008ULL)
        << "Iteration " << iteration << ": should find at least expected separator count";
    EXPECT_LE(total_seps, 400020ULL)
        << "Iteration " << iteration << ": should not find too many separators";

    // Verify no duplicate or out-of-order indices within each thread's slice
    // This catches races that cause threads to overwrite each other's positions
    for (int t = 0; t < n_threads; t++) {
      uint64_t prev = 0;
      for (uint64_t i = 0; i < idx.n_indexes[t]; i++) {
        uint64_t pos = idx.indexes[t + i * n_threads];
        EXPECT_GT(pos, prev) << "Thread " << t << " index " << i
                             << ": positions should be strictly increasing";
        prev = pos;
      }
    }
  }
}

// ============================================================================
// BRANCHLESS ERROR COLLECTION TESTS
// ============================================================================

class BranchlessErrorCollectionTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }
};

TEST_F(BranchlessErrorCollectionTest, BranchlessWithErrorsBasic) {
  // Test that branchless parser with error collection works for valid CSV
  std::string path = getTestDataPath("basic", "simple.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  bool success = parser.parse_branchless_with_errors(buffer.data(), idx, buffer.size, errors);

  EXPECT_TRUE(success) << "Branchless with errors should parse valid CSV successfully";
  EXPECT_EQ(errors.error_count(), 0) << "No errors expected for valid CSV";
}

TEST_F(BranchlessErrorCollectionTest, BranchlessWithErrorsUnclosedQuote) {
  // Test detection of unclosed quote
  std::string path = getTestDataPath("malformed", "unclosed_quote.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  bool success = parser.parse_branchless_with_errors(buffer.data(), idx, buffer.size, errors);

  EXPECT_TRUE(errors.has_errors()) << "Should detect unclosed quote error";
}

TEST_F(BranchlessErrorCollectionTest, BranchlessWithErrorsQuoteInUnquoted) {
  // Test detection of quote in unquoted field
  std::string path = getTestDataPath("malformed", "quote_in_unquoted_field.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_branchless_with_errors(buffer.data(), idx, buffer.size, errors);

  EXPECT_TRUE(errors.has_errors()) << "Should detect quote in unquoted field";
  bool found_quote_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
      found_quote_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_quote_error) << "Should have QUOTE_IN_UNQUOTED_FIELD error";
}

TEST_F(BranchlessErrorCollectionTest, BranchlessWithErrorsNullByte) {
  // Test detection of null byte
  std::string path = getTestDataPath("malformed", "null_byte.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_branchless_with_errors(buffer.data(), idx, buffer.size, errors);

  bool found_null_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::NULL_BYTE) {
      found_null_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_null_error) << "Should detect NULL_BYTE error";
}

TEST_F(BranchlessErrorCollectionTest, ErrorInSIMDBlock) {
  // Test detection of errors within a 64-byte SIMD block.
  // This specifically tests the error handling code path at line 112 in
  // branchless_state_machine.cpp which only executes when errors occur
  // within a full 64-byte block (not in the partial block at the end).
  // The content must be >= 64 bytes with an error in the first 64 bytes.
  std::vector<uint8_t> data;
  std::string content;

  // Build content: header + padding + null byte + more content
  // Total needs to be >= 64 bytes with error in first 64 bytes
  content = "A,B,C\n";             // 6 bytes
  content += std::string(20, 'x'); // 20 bytes padding (total: 26)
  content.push_back('\0');         // null byte at position 26
  content += std::string(40, 'y'); // 40 more bytes (total: 67)
  content += "\n";                 // newline (total: 68)

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_branchless_with_errors(data.data(), idx, content.size(), errors);

  // Should find the null byte error in the SIMD block
  bool found_null_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::NULL_BYTE) {
      found_null_error = true;
      // Verify error position is within first 64 bytes (SIMD block)
      EXPECT_LT(err.byte_offset, 64u) << "Error should be detected within first 64-byte SIMD block";
      break;
    }
  }
  EXPECT_TRUE(found_null_error) << "Should detect NULL_BYTE error in SIMD block";
}

TEST_F(BranchlessErrorCollectionTest, QuoteErrorInSIMDBlock) {
  // Test detection of quote-in-unquoted-field within a 64-byte SIMD block.
  // Similar to ErrorInSIMDBlock but tests the quote error detection path.
  std::vector<uint8_t> data;
  std::string content;

  // Build content: header + unquoted field with embedded quote
  // Total needs to be >= 64 bytes with error in first 64 bytes
  content = "A,B,C\n";             // 6 bytes
  content += "value";              // 5 bytes (total: 11)
  content += "\"";                 // quote in unquoted field at position 11
  content += "more";               // 4 bytes (total: 16)
  content += ",2,3\n";             // 5 bytes (total: 21)
  content += std::string(50, 'x'); // padding to exceed 64 bytes (total: 71)
  content += "\n";

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  parser.parse_branchless_with_errors(data.data(), idx, content.size(), errors);

  // Should find the quote error in the SIMD block
  bool found_quote_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
      found_quote_error = true;
      // Verify error position is within first 64 bytes (SIMD block)
      EXPECT_LT(err.byte_offset, 64u) << "Error should be detected within first 64-byte SIMD block";
      break;
    }
  }
  EXPECT_TRUE(found_quote_error) << "Should detect QUOTE_IN_UNQUOTED_FIELD error in SIMD block";
}

TEST_F(BranchlessErrorCollectionTest, BranchlessWithErrorsMultiThreaded) {
  // Test multi-threaded branchless parsing with error collection
  std::vector<uint8_t> data;
  std::string content;

  // Generate large CSV with some errors
  content = "A,B,C\n";
  for (int i = 0; i < 5000; i++) {
    content += std::to_string(i) + ",";
    content += "\"value" + std::to_string(i) + "\",";
    content += "data" + std::to_string(i) + "\n";
  }

  data.resize(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 4);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  bool success = parser.parse_branchless_with_errors(data.data(), idx, content.size(), errors);

  EXPECT_TRUE(success) << "Should successfully parse large valid CSV";
  EXPECT_EQ(errors.error_count(), 0) << "No errors expected for valid large CSV";
}

TEST_F(BranchlessErrorCollectionTest, ConsistencyBranchlessWithErrorsVsSwitch) {
  // Verify branchless with errors produces same field positions as switch-based
  std::string path = getTestDataPath("basic", "simple.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;

  // Parse with switch-based parser
  libvroom::ParseIndex idx1 = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors1(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_with_errors(buffer.data(), idx1, buffer.size, errors1);

  // Parse with branchless parser with errors
  libvroom::ParseIndex idx2 = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors2(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_branchless_with_errors(buffer.data(), idx2, buffer.size, errors2);

  // Compare results
  EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
      << "Branchless with errors should find same number of field separators as switch-based";

  for (size_t i = 0; i < idx1.n_indexes[0]; ++i) {
    EXPECT_EQ(idx1.indexes[i], idx2.indexes[i])
        << "Field separator positions should match at index " << i;
  }
}

TEST_F(BranchlessErrorCollectionTest, ConsistencyBranchlessWithErrorsQuotedFields) {
  // Verify consistency with quoted fields
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;

  // Parse with switch-based parser
  libvroom::ParseIndex idx1 = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors1(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_with_errors(buffer.data(), idx1, buffer.size, errors1);

  // Parse with branchless parser with errors
  libvroom::ParseIndex idx2 = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors2(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_branchless_with_errors(buffer.data(), idx2, buffer.size, errors2);

  // Compare results
  EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
      << "Should find same number of separators for quoted fields";
}

TEST_F(BranchlessErrorCollectionTest, ParserAPIUsesUnified) {
  // Test that Parser::parse() with errors uses the branchless implementation
  std::string path = getTestDataPath("basic", "simple.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::Parser parser(1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size, {.errors = &errors});

  EXPECT_TRUE(result.success()) << "Parser should succeed with error collection";
  EXPECT_EQ(errors.error_count(), 0) << "No errors expected for valid CSV";
}

TEST_F(BranchlessErrorCollectionTest, ParserAPIWithErrorsDetectsProblems) {
  // Test that Parser::parse() with BRANCHLESS algorithm and errors detects malformed CSV
  // Use explicit dialect because auto-detection may pick wrong quote char for malformed files
  std::string path = getTestDataPath("malformed", "quote_in_unquoted_field.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::Parser parser(1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  // Use BRANCHLESS algorithm with explicit CSV dialect
  libvroom::ParseOptions opts;
  opts.dialect = Dialect::csv();
  opts.errors = &errors;
  opts.algorithm = ParseAlgorithm::BRANCHLESS;

  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(errors.has_errors()) << "Parser should detect errors in malformed CSV";

  // Verify we found the expected quote error
  bool found_quote_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
      found_quote_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_quote_error) << "Should find QUOTE_IN_UNQUOTED_FIELD error";
}

// ============================================================================
// ESCAPE CHARACTER SUPPORT TESTS
// ============================================================================

class EscapeCharacterTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }

  // Helper to create a dialect with backslash escaping
  Dialect backslashDialect() {
    Dialect d;
    d.delimiter = ',';
    d.quote_char = '"';
    d.escape_char = '\\';
    d.double_quote = false;
    return d;
  }
};

TEST_F(EscapeCharacterTest, BackslashEscapedQuote) {
  // Test backslash-escaped quotes: \"
  // CSV content: Name,Value<newline>"Hello \"World\"",100<newline>
  // After C++ escaping: \\\" -> \" (backslash + quote)
  std::string content = "Name,Value\n\"Hello \\\"World\\\"\",100\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse backslash-escaped quotes";
  // Should find 4 separators: comma after Name, newline after Value, comma after quoted, newline at
  // end
  EXPECT_EQ(idx.n_indexes[0], 4) << "Should find 4 field separators";
}

TEST_F(EscapeCharacterTest, BackslashEscapedBackslash) {
  // Test escaped backslash: \\
    // Content: Path,Value<newline>"C:\\Users\\test",100<newline>
  // Separators: comma(4), newline(10), comma(28), newline(32) = 4 separators
  std::string content2 = std::string("Path,Value\n") + "\"C:\\\\Users\\\\test\",100\n";
  std::vector<uint8_t> data(content2.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content2.data(), content2.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content2.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse escaped backslashes";
  EXPECT_EQ(idx.n_indexes[0], 4) << "Should find 4 field separators";
}

TEST_F(EscapeCharacterTest, BackslashEscapedDelimiter) {
  // Test escaped delimiter within quoted field: \,
  // Content: Text,Value<newline>"Hello\, World",100<newline>
  // Separators: comma(4), newline(10), comma(26), newline(30) = 4 separators
  std::string content = "Text,Value\n\"Hello\\, World\",100\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse escaped delimiters";
  EXPECT_EQ(idx.n_indexes[0], 4) << "Should find 4 field separators (comma in quotes is escaped)";
}

TEST_F(EscapeCharacterTest, BackslashEscapedNewline) {
  // Test escaped newline: \n (literal backslash-n in the string, which becomes embedded newline)
  // Note: In C++, \n in string literal becomes actual newline char (0x0A)
  // Content: Text,Value<newline>"Line1<newline>Line2",100<newline>
  // Separators: comma(4), newline(10), comma(25), newline(29) = 4 separators
  std::string content = "Text,Value\n\"Line1\\nLine2\",100\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse escaped newlines";
  EXPECT_EQ(idx.n_indexes[0], 4) << "Should find 4 field separators";
}

TEST_F(EscapeCharacterTest, MixedEscapeSequences) {
  // Test multiple escape sequences in one field
  std::string content = "Data\n\"\\\"test\\\\path\\,value\\\"\"\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse mixed escape sequences";
}

TEST_F(EscapeCharacterTest, ConsecutiveEscapes) {
  // Test consecutive escapes: \\\\ (two escaped backslashes)
  std::string content = "Path\n\"C:\\\\\\\\\"\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should parse consecutive escape sequences";
}

TEST_F(EscapeCharacterTest, BackslashAtEndOfQuotedField) {
  // Test backslash before closing quote: "value\" should close at "
  // In escape mode, \" is an escaped quote, so the field continues
  std::string content = "A,B\n\"val\",\"test\\\"\"\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should handle backslash before quote correctly";
}

TEST_F(EscapeCharacterTest, ParseBackslashEscapeTestFile) {
  // Parse the test file with backslash escapes
  std::string path = getTestDataPath("escape", "backslash_escape.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);

  bool success = parser.parse_branchless(buffer.data(), idx, buffer.size, backslashDialect());

  EXPECT_TRUE(success) << "Should parse backslash_escape.csv successfully";
}

TEST_F(EscapeCharacterTest, BranchlessStateMachineEscapeTransitions) {
  // Test state machine transitions with escape character
  BranchlessStateMachine sm(',', '"', '\\', false);

  // Verify escape char is classified correctly
  EXPECT_EQ(sm.classify('\\'), CHAR_ESCAPE);
  EXPECT_EQ(sm.classify(','), CHAR_DELIMITER);
  EXPECT_EQ(sm.classify('"'), CHAR_QUOTE);

  // Test escape transition from QUOTED_FIELD
  PackedResult r = sm.transition(STATE_QUOTED_FIELD, CHAR_ESCAPE);
  EXPECT_EQ(r.state(), STATE_ESCAPED);
  EXPECT_EQ(r.error(), ERR_NONE);

  // Test that any char after escape returns to QUOTED_FIELD
  r = sm.transition(STATE_ESCAPED, CHAR_QUOTE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_EQ(r.error(), ERR_NONE);

  r = sm.transition(STATE_ESCAPED, CHAR_DELIMITER);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_EQ(r.error(), ERR_NONE);

  r = sm.transition(STATE_ESCAPED, CHAR_ESCAPE);
  EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
  EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(EscapeCharacterTest, RFC4180ModeIgnoresEscape) {
  // In double_quote=true mode, backslash should be treated as OTHER
  BranchlessStateMachine sm(',', '"', '\\', true);

  // Backslash should be classified as OTHER in RFC 4180 mode
  EXPECT_EQ(sm.classify('\\'), CHAR_OTHER);
}

TEST_F(EscapeCharacterTest, ComputeEscapedMaskBasic) {
  // Test the compute_escaped_mask function directly
  // escape_mask with bit 10 set (backslash at position 10)
  uint64_t escape_mask = 1ULL << 10;
  uint64_t carry = 0;

  uint64_t escaped = compute_escaped_mask(escape_mask, carry);

  // Position 11 should be escaped (right after position 10)
  EXPECT_TRUE((escaped & (1ULL << 11)) != 0) << "Position 11 should be escaped";
  EXPECT_FALSE((escaped & (1ULL << 10)) != 0)
      << "Position 10 should NOT be escaped (it's the escape char)";
  EXPECT_FALSE((escaped & (1ULL << 12)) != 0) << "Position 12 should NOT be escaped";
}

TEST_F(EscapeCharacterTest, ComputeEscapedMaskConsecutive) {
  // Test consecutive escapes: \\ should escape the second backslash
  // Bits 10 and 11 set (backslashes at positions 10 and 11)
  uint64_t escape_mask = (1ULL << 10) | (1ULL << 11);
  uint64_t carry = 0;

  uint64_t escaped = compute_escaped_mask(escape_mask, carry);

  // Position 11 should be escaped (the second backslash is escaped by the first)
  EXPECT_TRUE((escaped & (1ULL << 11)) != 0) << "Position 11 should be escaped";
  // Position 12 should NOT be escaped (no escape before it)
  EXPECT_FALSE((escaped & (1ULL << 12)) != 0) << "Position 12 should NOT be escaped";
}

TEST_F(EscapeCharacterTest, ComputeEscapedMaskQuadBackslash) {
  // Test \\\\ (four backslashes) - should result in two literal backslashes
  // Bits 10, 11, 12, 13 set
  uint64_t escape_mask = (1ULL << 10) | (1ULL << 11) | (1ULL << 12) | (1ULL << 13);
  uint64_t carry = 0;

  uint64_t escaped = compute_escaped_mask(escape_mask, carry);

  // Positions 11 and 13 should be escaped (escaped by 10 and 12)
  EXPECT_TRUE((escaped & (1ULL << 11)) != 0) << "Position 11 should be escaped";
  EXPECT_TRUE((escaped & (1ULL << 13)) != 0) << "Position 13 should be escaped";
  // Positions 10, 12 should NOT be escaped (they are the escape chars)
  EXPECT_FALSE((escaped & (1ULL << 10)) != 0) << "Position 10 should NOT be escaped";
  EXPECT_FALSE((escaped & (1ULL << 12)) != 0) << "Position 12 should NOT be escaped";
}

TEST_F(EscapeCharacterTest, ConsistencyWithScalarParsing) {
  // Verify SIMD branchless produces same results as scalar for escape sequences
  // Content: A,B,C<newline>"val\"1","val\\2",3<newline>"x","y\,z",4<newline>
  // Expected separators for escape mode:
  //   comma after A (pos 1), comma after B (pos 3), newline after C (pos 5)
  //   comma after "val\"1" (pos 14), comma after "val\\2" (pos 24), newline after 3 (pos 26)
  //   comma after "x" (pos 30), comma after "y\,z" (pos 38), newline after 4 (pos 40)
  // Total = 9 separators
  std::string content = "A,B,C\n\"val\\\"1\",\"val\\\\2\",3\n\"x\",\"y\\,z\",4\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  Dialect dialect = backslashDialect();

  // Parse with branchless (SIMD)
  libvroom::ParseIndex idx1 = parser.init(data.size(), 1);
  parser.parse_branchless(data.data(), idx1, content.size(), dialect);

  // Parse with error collection (uses scalar path)
  libvroom::ParseIndex idx2 = parser.init(data.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_branchless_with_errors(data.data(), idx2, content.size(), errors, dialect);

  // Results should match
  EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
      << "SIMD and scalar should find same number of separators";

  for (size_t i = 0; i < std::min(idx1.n_indexes[0], idx2.n_indexes[0]); ++i) {
    EXPECT_EQ(idx1.indexes[i], idx2.indexes[i]) << "Separator position mismatch at index " << i;
  }
}

TEST_F(EscapeCharacterTest, MultiThreadedEscapeParsing) {
  // Test multi-threaded parsing with escape characters
  std::string content;
  content = "Name,Value,Path\n";
  for (int i = 0; i < 5000; i++) {
    content += "\"Name" + std::to_string(i) + "\",";
    content += "\"val\\\"" + std::to_string(i) + "\",";
    content += "\"C:\\\\path\\\\" + std::to_string(i) + "\"\n";
  }

  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 4);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Multi-threaded escape parsing should succeed";
}

TEST_F(EscapeCharacterTest, CrossBlockEscapeSequence) {
  // Create content where escape sequence crosses 64-byte block boundary
  std::string padding(62, 'a');                         // 62 bytes of 'a'
  std::string content = "X\n\"" + padding + "\\\"\"\n"; // Escape at position 63-64
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(data.size(), 1);

  bool success = parser.parse_branchless(data.data(), idx, content.size(), backslashDialect());

  EXPECT_TRUE(success) << "Should handle escape sequences crossing block boundaries";
}

TEST_F(EscapeCharacterTest, ParserAPIWithEscapeDialect) {
  // Test the high-level Parser API with escape dialect
  std::string content = "Name,Value\n\"Hello \\\"World\\\"\",100\n";
  std::vector<uint8_t> data(content.size() + LIBVROOM_PADDING);
  std::memcpy(data.data(), content.data(), content.size());

  Parser parser(1);
  auto result = parser.parse(data.data(), content.size(), {.dialect = backslashDialect()});

  EXPECT_TRUE(result.success()) << "Parser API should work with escape dialect";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
