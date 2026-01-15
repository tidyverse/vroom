#include "libvroom.h"

#include "dialect.h"
#include "error.h"
#include "io_util.h"
#include "two_pass.h"
#include "value_extraction.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Helper function to read entire file
std::string readFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Failed to open file: " + path);
  }
  std::ostringstream ss;
  ss << file.rdbuf();
  return ss.str();
}

// Helper function to count lines in a file
size_t countLines(const std::string& content) {
  size_t count = 0;
  for (char c : content) {
    if (c == '\n')
      count++;
  }
  return count;
}

// Helper function to count fields in a line
size_t countFields(const std::string& line, char separator = ',') {
  if (line.empty())
    return 0;

  size_t count = 1;
  bool in_quotes = false;

  for (size_t i = 0; i < line.length(); i++) {
    if (line[i] == '"') {
      in_quotes = !in_quotes;
    } else if (line[i] == separator && !in_quotes) {
      count++;
    }
  }

  return count;
}

// Test fixture for CSV file tests
class CSVFileTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }

  bool fileExists(const std::string& path) { return fs::exists(path); }
};

// ============================================================================
// BASIC CSV TESTS
// ============================================================================

TEST_F(CSVFileTest, SimpleCSVExists) {
  std::string path = getTestDataPath("basic", "simple.csv");
  ASSERT_TRUE(fileExists(path)) << "Test file not found: " << path;
}

TEST_F(CSVFileTest, SimpleCSVStructure) {
  std::string path = getTestDataPath("basic", "simple.csv");
  std::string content = readFile(path);

  EXPECT_FALSE(content.empty());

  // Should have 4 lines (1 header + 3 data rows)
  size_t lines = countLines(content);
  EXPECT_EQ(lines, 4) << "Expected 4 lines in simple.csv";
}

TEST_F(CSVFileTest, SimpleCSVFieldCount) {
  std::string path = getTestDataPath("basic", "simple.csv");
  std::ifstream file(path);

  std::string line;
  std::getline(file, line); // Read header

  size_t fields = countFields(line);
  EXPECT_EQ(fields, 3) << "Expected 3 fields in header";
}

TEST_F(CSVFileTest, SimpleNoHeaderExists) {
  std::string path = getTestDataPath("basic", "simple_no_header.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, SingleColumnExists) {
  std::string path = getTestDataPath("basic", "single_column.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  std::ifstream file(path);
  std::string line;
  std::getline(file, line);

  EXPECT_EQ(countFields(line), 1) << "Expected 1 field in single column CSV";
}

TEST_F(CSVFileTest, WideColumnsExists) {
  std::string path = getTestDataPath("basic", "wide_columns.csv");
  ASSERT_TRUE(fileExists(path));

  std::ifstream file(path);
  std::string line;
  std::getline(file, line);

  EXPECT_EQ(countFields(line), 20) << "Expected 20 fields in wide CSV";
}

TEST_F(CSVFileTest, ManyRowsExists) {
  std::string path = getTestDataPath("basic", "many_rows.csv");
  ASSERT_TRUE(fileExists(path));

  std::string content = readFile(path);
  size_t lines = countLines(content);
  EXPECT_GE(lines, 20) << "Expected at least 20 lines in many_rows.csv";
}

// ============================================================================
// QUOTED FIELD TESTS
// ============================================================================

TEST_F(CSVFileTest, QuotedFieldsExists) {
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, QuotedFieldsContainsQuotes) {
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find('"'), std::string::npos) << "quoted_fields.csv should contain quotes";
}

TEST_F(CSVFileTest, EscapedQuotesExists) {
  std::string path = getTestDataPath("quoted", "escaped_quotes.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, EscapedQuotesContainsDoubledQuotes) {
  std::string path = getTestDataPath("quoted", "escaped_quotes.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find("\"\""), std::string::npos)
      << "escaped_quotes.csv should contain doubled quotes (\"\")";
}

TEST_F(CSVFileTest, MixedQuotedExists) {
  std::string path = getTestDataPath("quoted", "mixed_quoted.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, EmbeddedSeparatorsExists) {
  std::string path = getTestDataPath("quoted", "embedded_separators.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, NewlinesInQuotesExists) {
  std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, NewlinesInQuotesContainsEmbeddedNewlines) {
  std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");
  std::string content = readFile(path);

  // File should contain quoted fields with newlines inside
  EXPECT_NE(content.find("\"Line 1\n"), std::string::npos)
      << "Should contain multiline quoted fields";
}

// ============================================================================
// SEPARATOR TESTS
// ============================================================================

TEST_F(CSVFileTest, SemicolonSeparatorExists) {
  std::string path = getTestDataPath("separators", "semicolon.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, SemicolonSeparatorHasSemicolons) {
  std::string path = getTestDataPath("separators", "semicolon.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find(';'), std::string::npos) << "semicolon.csv should contain semicolons";
}

TEST_F(CSVFileTest, TabSeparatorExists) {
  std::string path = getTestDataPath("separators", "tab.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, TabSeparatorHasTabs) {
  std::string path = getTestDataPath("separators", "tab.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find('\t'), std::string::npos) << "tab.csv should contain tab characters";
}

TEST_F(CSVFileTest, PipeSeparatorExists) {
  std::string path = getTestDataPath("separators", "pipe.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, PipeSeparatorHasPipes) {
  std::string path = getTestDataPath("separators", "pipe.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find('|'), std::string::npos) << "pipe.csv should contain pipe characters";
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

TEST_F(CSVFileTest, EmptyFieldsExists) {
  std::string path = getTestDataPath("edge_cases", "empty_fields.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, EmptyFieldsContainsConsecutiveCommas) {
  std::string path = getTestDataPath("edge_cases", "empty_fields.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find(",,"), std::string::npos)
      << "empty_fields.csv should contain consecutive commas";
}

TEST_F(CSVFileTest, SingleCellExists) {
  std::string path = getTestDataPath("edge_cases", "single_cell.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, SingleRowHeaderOnlyExists) {
  std::string path = getTestDataPath("edge_cases", "single_row_header_only.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, EmptyFileExists) {
  std::string path = getTestDataPath("edge_cases", "empty_file.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, EmptyFileIsEmpty) {
  std::string path = getTestDataPath("edge_cases", "empty_file.csv");
  std::string content = readFile(path);

  EXPECT_TRUE(content.empty()) << "empty_file.csv should be empty";
}

TEST_F(CSVFileTest, WhitespaceFieldsExists) {
  std::string path = getTestDataPath("edge_cases", "whitespace_fields.csv");
  ASSERT_TRUE(fileExists(path));
}

// ============================================================================
// LINE ENDING TESTS
// ============================================================================

TEST_F(CSVFileTest, CRLFLineEndingsExists) {
  std::string path = getTestDataPath("line_endings", "crlf.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, CRLFLineEndingsHasCRLF) {
  std::string path = getTestDataPath("line_endings", "crlf.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find("\r\n"), std::string::npos) << "crlf.csv should contain CRLF line endings";
}

TEST_F(CSVFileTest, LFLineEndingsExists) {
  std::string path = getTestDataPath("line_endings", "lf.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, CRLineEndingsExists) {
  std::string path = getTestDataPath("line_endings", "cr.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, CRLineEndingsHasCR) {
  std::string path = getTestDataPath("line_endings", "cr.csv");
  std::string content = readFile(path);

  EXPECT_NE(content.find('\r'), std::string::npos) << "cr.csv should contain CR characters";
  EXPECT_EQ(content.find("\r\n"), std::string::npos) << "cr.csv should NOT contain CRLF sequences";
}

TEST_F(CSVFileTest, NoFinalNewlineExists) {
  std::string path = getTestDataPath("line_endings", "no_final_newline.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, NoFinalNewlineEndsWithoutNewline) {
  std::string path = getTestDataPath("line_endings", "no_final_newline.csv");
  std::string content = readFile(path);

  ASSERT_FALSE(content.empty());
  EXPECT_NE(content.back(), '\n') << "no_final_newline.csv should not end with newline";
}

// Test that CR-only line endings parse correctly to 3 columns and 3 rows
TEST_F(CSVFileTest, CRLineEndingsParseCorrectly) {
  std::string path = getTestDataPath("line_endings", "cr.csv");
  auto buffer = libvroom::load_file_to_ptr(path, 64);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_with_errors(buffer.data(), idx, buffer.size, errors);

  libvroom::ValueExtractor ve(buffer.data(), buffer.size, idx, libvroom::Dialect::csv());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3) << "CR-only file should have 3 columns";
  EXPECT_EQ(ve.num_rows(), 2) << "CR-only file should have 2 data rows (excluding header)";

  // Verify header values
  auto headers = ve.get_header();
  ASSERT_EQ(headers.size(), 3);
  EXPECT_EQ(headers[0], "A");
  EXPECT_EQ(headers[1], "B");
  EXPECT_EQ(headers[2], "C");
}

// Test that CRLF line endings parse correctly to 3 columns and 3 rows
TEST_F(CSVFileTest, CRLFLineEndingsParseCorrectly) {
  std::string path = getTestDataPath("line_endings", "crlf.csv");
  auto buffer = libvroom::load_file_to_ptr(path, 64);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_with_errors(buffer.data(), idx, buffer.size, errors);

  libvroom::ValueExtractor ve(buffer.data(), buffer.size, idx, libvroom::Dialect::csv());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3) << "CRLF file should have 3 columns";
  EXPECT_EQ(ve.num_rows(), 2) << "CRLF file should have 2 data rows (excluding header)";

  // Verify header values - should NOT include \r
  auto headers = ve.get_header();
  ASSERT_EQ(headers.size(), 3);
  EXPECT_EQ(headers[0], "A");
  EXPECT_EQ(headers[1], "B");
  EXPECT_EQ(headers[2], "C"); // Should be "C", not "C\r"
}

// Test that LF line endings parse correctly to 3 columns and 3 rows
TEST_F(CSVFileTest, LFLineEndingsParseCorrectly) {
  std::string path = getTestDataPath("line_endings", "lf.csv");
  auto buffer = libvroom::load_file_to_ptr(path, 64);
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  parser.parse_with_errors(buffer.data(), idx, buffer.size, errors);

  libvroom::ValueExtractor ve(buffer.data(), buffer.size, idx, libvroom::Dialect::csv());
  ve.set_has_header(true);

  EXPECT_EQ(ve.num_columns(), 3) << "LF file should have 3 columns";
  EXPECT_EQ(ve.num_rows(), 2) << "LF file should have 2 data rows (excluding header)";

  // Verify header values
  auto headers = ve.get_header();
  ASSERT_EQ(headers.size(), 3);
  EXPECT_EQ(headers[0], "A");
  EXPECT_EQ(headers[1], "B");
  EXPECT_EQ(headers[2], "C");
}

// Test that all line ending types produce equivalent results
TEST_F(CSVFileTest, AllLineEndingsProduceEquivalentResults) {
  std::vector<std::string> files = {"cr.csv", "crlf.csv", "lf.csv"};
  std::vector<std::vector<std::vector<std::string>>> all_data;

  for (const auto& file : files) {
    std::string path = getTestDataPath("line_endings", file);
    auto buffer = libvroom::load_file_to_ptr(path, 64);
    libvroom::TwoPass parser;
    libvroom::ParseIndex idx = parser.init(buffer.size, 1);
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    parser.parse_with_errors(buffer.data(), idx, buffer.size, errors);

    libvroom::ValueExtractor ve(buffer.data(), buffer.size, idx, libvroom::Dialect::csv());
    ve.set_has_header(true);

    std::vector<std::vector<std::string>> data;
    // Get header
    data.push_back(ve.get_header());
    // Get data rows
    for (size_t row = 0; row < ve.num_rows(); ++row) {
      std::vector<std::string> row_data;
      for (size_t col = 0; col < ve.num_columns(); ++col) {
        row_data.push_back(std::string(ve.get_string_view(row, col)));
      }
      data.push_back(row_data);
    }
    all_data.push_back(data);
  }

  // All files should produce the same data
  ASSERT_EQ(all_data.size(), 3);
  for (size_t i = 1; i < all_data.size(); ++i) {
    EXPECT_EQ(all_data[0], all_data[i])
        << "File " << files[i] << " should produce same data as " << files[0];
  }
}

// ============================================================================
// REAL WORLD DATA TESTS
// ============================================================================

TEST_F(CSVFileTest, FinancialDataExists) {
  std::string path = getTestDataPath("real_world", "financial.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, FinancialDataHasExpectedColumns) {
  std::string path = getTestDataPath("real_world", "financial.csv");
  std::ifstream file(path);
  std::string header;
  std::getline(file, header);

  EXPECT_NE(header.find("Date"), std::string::npos);
  EXPECT_NE(header.find("Open"), std::string::npos);
  EXPECT_NE(header.find("Close"), std::string::npos);
}

TEST_F(CSVFileTest, ContactsDataExists) {
  std::string path = getTestDataPath("real_world", "contacts.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, UnicodeDataExists) {
  std::string path = getTestDataPath("real_world", "unicode.csv");
  ASSERT_TRUE(fileExists(path));
}

TEST_F(CSVFileTest, UnicodeDataContainsUTF8) {
  std::string path = getTestDataPath("real_world", "unicode.csv");
  std::string content = readFile(path);

  // Check for some UTF-8 characters (Japanese, Greek, Cyrillic, etc.)
  EXPECT_GT(content.length(), 100) << "Unicode file should have content";

  // Simple check: UTF-8 multibyte characters will have bytes > 127
  bool has_utf8 = false;
  for (unsigned char c : content) {
    if (c > 127) {
      has_utf8 = true;
      break;
    }
  }
  EXPECT_TRUE(has_utf8) << "Unicode file should contain UTF-8 multibyte characters";
}

TEST_F(CSVFileTest, ProductCatalogExists) {
  std::string path = getTestDataPath("real_world", "product_catalog.csv");
  ASSERT_TRUE(fileExists(path));
}

// ============================================================================
// SUMMARY TEST
// ============================================================================

TEST_F(CSVFileTest, AllTestFilesPresent) {
  std::vector<std::pair<std::string, std::string>> required_files = {
      {"basic", "simple.csv"},
      {"basic", "simple_no_header.csv"},
      {"basic", "single_column.csv"},
      {"basic", "wide_columns.csv"},
      {"basic", "many_rows.csv"},
      {"quoted", "quoted_fields.csv"},
      {"quoted", "escaped_quotes.csv"},
      {"quoted", "mixed_quoted.csv"},
      {"quoted", "embedded_separators.csv"},
      {"quoted", "newlines_in_quotes.csv"},
      {"separators", "semicolon.csv"},
      {"separators", "tab.csv"},
      {"separators", "pipe.csv"},
      {"edge_cases", "empty_fields.csv"},
      {"edge_cases", "single_cell.csv"},
      {"edge_cases", "single_row_header_only.csv"},
      {"edge_cases", "empty_file.csv"},
      {"edge_cases", "whitespace_fields.csv"},
      {"line_endings", "crlf.csv"},
      {"line_endings", "lf.csv"},
      {"line_endings", "cr.csv"},
      {"line_endings", "no_final_newline.csv"},
      {"real_world", "financial.csv"},
      {"real_world", "contacts.csv"},
      {"real_world", "unicode.csv"},
      {"real_world", "product_catalog.csv"},
  };

  int missing_count = 0;
  for (const auto& [category, filename] : required_files) {
    std::string path = getTestDataPath(category, filename);
    if (!fileExists(path)) {
      std::cout << "Missing test file: " << path << std::endl;
      missing_count++;
    }
  }

  EXPECT_EQ(missing_count, 0) << missing_count << " test files are missing";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
