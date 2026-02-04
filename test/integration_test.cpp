/**
 * @file integration_test.cpp
 * @brief End-to-end integration tests for the CsvReader pipeline.
 *
 * Rewritten from old integration_test.cpp to use the libvroom2 CsvReader API.
 * Tests the full pipeline: file load -> parse -> verify schema + data + errors.
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
#include <vector>

// =============================================================================
// Test Fixture
// =============================================================================

class IntegrationTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }

  // Parse a file and return the result; asserts open+read succeed
  struct ParsedFile {
    libvroom::ParsedChunks chunks;
    std::vector<libvroom::ColumnSchema> schema;
  };

  ParsedFile parseFile(const std::string& path, libvroom::CsvOptions opts = {}) {
    libvroom::CsvReader reader(opts);
    auto open_result = reader.open(path);
    EXPECT_TRUE(open_result.ok) << "Failed to open: " << path;
    auto read_result = reader.read_all();
    EXPECT_TRUE(read_result.ok) << "Failed to read: " << path;
    std::vector<libvroom::ColumnSchema> schema(reader.schema().begin(), reader.schema().end());
    return {std::move(read_result.value), std::move(schema)};
  }

  ParsedFile parseContent(const std::string& content, libvroom::CsvOptions opts = {}) {
    test_util::TempCsvFile csv(content);
    return parseFile(csv.path(), opts);
  }

  std::string getStringValue(const libvroom::ParsedChunks& chunks, size_t col, size_t row) {
    return test_util::getStringValue(chunks, col, row);
  }
};

// =============================================================================
// 1. Basic End-to-End Tests
// =============================================================================

TEST_F(IntegrationTest, BasicE2E_SimpleCSV_SchemaAndRowCount) {
  auto [chunks, schema] = parseFile(testDataPath("basic/simple.csv"));

  // Verify schema
  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(schema[0].name, "A");
  EXPECT_EQ(schema[1].name, "B");
  EXPECT_EQ(schema[2].name, "C");

  // Verify row count (3 data rows: 1,2,3 / 4,5,6 / 7,8,9)
  EXPECT_EQ(chunks.total_rows, 3u);

  // Spot-check values
  EXPECT_EQ(getStringValue(chunks, 0, 0), "1");
  EXPECT_EQ(getStringValue(chunks, 1, 1), "5");
  EXPECT_EQ(getStringValue(chunks, 2, 2), "9");
}

TEST_F(IntegrationTest, BasicE2E_ContactsCSV_RowCountAndColumns) {
  auto [chunks, schema] = parseFile(testDataPath("real_world/contacts.csv"));

  // contacts.csv: Name, Email, Phone, Address with 4 rows
  ASSERT_EQ(schema.size(), 4u);
  EXPECT_EQ(schema[0].name, "Name");
  EXPECT_EQ(schema[1].name, "Email");
  EXPECT_EQ(schema[2].name, "Phone");
  EXPECT_EQ(schema[3].name, "Address");

  EXPECT_EQ(chunks.total_rows, 4u);

  // Verify a quoted field with embedded comma
  EXPECT_EQ(getStringValue(chunks, 0, 0), "Smith, John");
  EXPECT_EQ(getStringValue(chunks, 0, 3), "Williams, Alice");
}

TEST_F(IntegrationTest, BasicE2E_SemicolonDelimiter) {
  libvroom::CsvOptions opts;
  opts.separator = ';';
  auto [chunks, schema] = parseFile(testDataPath("separators/semicolon.csv"), opts);

  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(schema[0].name, "A");
  EXPECT_EQ(schema[1].name, "B");
  EXPECT_EQ(schema[2].name, "C");
  EXPECT_EQ(chunks.total_rows, 3u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "1");
  EXPECT_EQ(getStringValue(chunks, 2, 2), "9");
}

TEST_F(IntegrationTest, BasicE2E_TabDelimiter) {
  libvroom::CsvOptions opts;
  opts.separator = '\t';
  auto [chunks, schema] = parseFile(testDataPath("separators/tab.csv"), opts);

  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(schema[0].name, "A");
  EXPECT_EQ(schema[1].name, "B");
  EXPECT_EQ(schema[2].name, "C");
  EXPECT_EQ(chunks.total_rows, 3u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "1");
  EXPECT_EQ(getStringValue(chunks, 1, 2), "8");
}

// =============================================================================
// 2. Multi-threaded Consistency Tests
// =============================================================================

TEST_F(IntegrationTest, MultiThread_SimpleCSV_1vs2vs4) {
  // Parse simple.csv with 1, 2, and 4 threads and verify identical results
  std::string path = testDataPath("basic/simple.csv");

  for (size_t threads : {1u, 2u, 4u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto [chunks, schema] = parseFile(path, opts);

    EXPECT_EQ(chunks.total_rows, 3u) << "Row count mismatch with " << threads << " threads";
    EXPECT_EQ(schema.size(), 3u) << "Schema size mismatch with " << threads << " threads";
    EXPECT_EQ(getStringValue(chunks, 0, 0), "1") << "Value mismatch with " << threads << " threads";
    EXPECT_EQ(getStringValue(chunks, 2, 2), "9") << "Value mismatch with " << threads << " threads";
  }
}

TEST_F(IntegrationTest, MultiThread_QuotedFields_1vs4) {
  // quoted_fields.csv has 3 rows with quoted values
  std::string path = testDataPath("quoted/quoted_fields.csv");

  libvroom::CsvOptions opts1;
  opts1.num_threads = 1;
  auto result1 = parseFile(path, opts1);

  libvroom::CsvOptions opts4;
  opts4.num_threads = 4;
  auto result4 = parseFile(path, opts4);

  EXPECT_EQ(result1.chunks.total_rows, result4.chunks.total_rows);
  EXPECT_EQ(result1.schema.size(), result4.schema.size());
}

TEST_F(IntegrationTest, MultiThread_LargeFile_1vs4vs8) {
  // parallel_chunk_boundary.csv is ~2MB, designed to stress chunk boundaries
  std::string path = testDataPath("large/parallel_chunk_boundary.csv");

  size_t expected_rows = 0;
  for (size_t threads : {1u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto [chunks, schema] = parseFile(path, opts);

    EXPECT_GE(chunks.total_rows, 1u) << "No rows parsed with " << threads << " threads";
    EXPECT_GE(schema.size(), 1u) << "No columns with " << threads << " threads";

    if (threads == 1) {
      expected_rows = chunks.total_rows;
    } else {
      EXPECT_EQ(chunks.total_rows, expected_rows)
          << "Row count differs between 1 and " << threads << " threads: " << "expected "
          << expected_rows << ", got " << chunks.total_rows;
    }
  }
}

// =============================================================================
// 3. In-memory Buffer Parsing Tests
// =============================================================================

TEST_F(IntegrationTest, InMemoryBuffer_BasicCSV) {
  std::string csv = "id,name,score\n1,Alice,95\n2,Bob,87\n3,Charlie,92\n";
  auto buffer = libvroom::AlignedBuffer::allocate(csv.size(), LIBVROOM_PADDING);
  std::memcpy(buffer.data(), csv.data(), csv.size());

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open_from_buffer(std::move(buffer));
  ASSERT_TRUE(open_result.ok) << "Failed to open from buffer: " << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << "Failed to read: " << read_result.error;

  EXPECT_EQ(read_result.value.total_rows, 3u);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "id");
  EXPECT_EQ(reader.schema()[1].name, "name");
  EXPECT_EQ(reader.schema()[2].name, "score");
}

TEST_F(IntegrationTest, InMemoryBuffer_QuotedFields) {
  std::string csv = "Name,Address,City\n"
                    "\"John Doe\",\"123 Main St\",\"Springfield\"\n"
                    "\"Jane Smith\",\"456 Oak Ave\",\"Portland\"\n";
  auto buffer = libvroom::AlignedBuffer::allocate(csv.size(), LIBVROOM_PADDING);
  std::memcpy(buffer.data(), csv.data(), csv.size());

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open_from_buffer(std::move(buffer));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  EXPECT_EQ(read_result.value.total_rows, 2u);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "Name");
}

TEST_F(IntegrationTest, InMemoryBuffer_EscapedQuotes) {
  // RFC 4180: "" inside quoted field becomes literal "
  std::string csv = "Text,Description\n"
                    "\"He said \"\"Hello\"\"\",\"A greeting\"\n"
                    "\"She replied \"\"Hi there\"\"\",\"A response\"\n";
  auto buffer = libvroom::AlignedBuffer::allocate(csv.size(), LIBVROOM_PADDING);
  std::memcpy(buffer.data(), csv.data(), csv.size());

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open_from_buffer(std::move(buffer));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  EXPECT_EQ(read_result.value.total_rows, 2u);
  ASSERT_EQ(reader.schema().size(), 2u);
  EXPECT_EQ(reader.schema()[0].name, "Text");
  EXPECT_EQ(reader.schema()[1].name, "Description");
}

// =============================================================================
// 4. Schema Verification Tests
// =============================================================================

TEST_F(IntegrationTest, Schema_ColumnNamesMatchHeader) {
  auto [chunks, schema] = parseContent("Name,Age,City\nalice,30,NYC\n");

  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(schema[0].name, "Name");
  EXPECT_EQ(schema[1].name, "Age");
  EXPECT_EQ(schema[2].name, "City");
}

TEST_F(IntegrationTest, Schema_TypeInferenceProducesNonUnknown) {
  // financial.csv has dates, floats, and integers
  auto [chunks, schema] = parseFile(testDataPath("real_world/financial.csv"));

  ASSERT_EQ(schema.size(), 6u);
  for (size_t i = 0; i < schema.size(); ++i) {
    EXPECT_NE(schema[i].type, libvroom::DataType::UNKNOWN)
        << "Column " << schema[i].name << " at index " << i << " has UNKNOWN type";
  }

  // Date column should be detected as DATE
  EXPECT_EQ(schema[0].name, "Date");
  EXPECT_EQ(schema[0].type, libvroom::DataType::DATE);

  // Volume should be numeric
  EXPECT_EQ(schema[5].name, "Volume");
  EXPECT_TRUE(schema[5].type == libvroom::DataType::INT32 ||
              schema[5].type == libvroom::DataType::INT64)
      << "Volume type: " << libvroom::type_name(schema[5].type);
}

TEST_F(IntegrationTest, Schema_WideCSV_20Columns) {
  auto [chunks, schema] = parseFile(testDataPath("basic/wide_columns.csv"));

  ASSERT_EQ(schema.size(), 20u);
  EXPECT_EQ(schema[0].name, "C1");
  EXPECT_EQ(schema[9].name, "C10");
  EXPECT_EQ(schema[19].name, "C20");
  EXPECT_EQ(chunks.total_rows, 3u);
}

// =============================================================================
// 5. Error Handling Integration Tests
// =============================================================================

TEST_F(IntegrationTest, ErrorHandling_UnclosedQuote_Permissive) {
  // malformed/unclosed_quote.csv has an unclosed quote on row 2
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(testDataPath("malformed/unclosed_quote.csv"));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  // Parsing should complete in permissive mode
  EXPECT_TRUE(read_result.ok) << read_result.error;

  // Should have collected errors about the unclosed quote
  EXPECT_TRUE(reader.has_errors()) << "Should detect errors in malformed file";
  EXPECT_GT(reader.errors().size(), 0u);
}

TEST_F(IntegrationTest, ErrorHandling_InconsistentColumns_Permissive) {
  // malformed/inconsistent_columns.csv has rows with varying field counts
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(testDataPath("malformed/inconsistent_columns.csv"));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  EXPECT_TRUE(read_result.ok) << read_result.error;

  // Should have collected errors for inconsistent column counts
  EXPECT_TRUE(reader.has_errors()) << "Should detect inconsistent column count";
}

TEST_F(IntegrationTest, ErrorHandling_ValidFile_NoErrors) {
  // simple.csv is well-formed; with error collection enabled, no errors
  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(testDataPath("basic/simple.csv"));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  EXPECT_FALSE(reader.has_errors()) << "Valid file should produce no errors";
  EXPECT_EQ(reader.errors().size(), 0u);
  EXPECT_EQ(read_result.value.total_rows, 3u);
}

// =============================================================================
// 6. Real-world Data Tests
// =============================================================================

TEST_F(IntegrationTest, RealWorld_FinancialData) {
  auto [chunks, schema] = parseFile(testDataPath("real_world/financial.csv"));

  // financial.csv: Date,Open,High,Low,Close,Volume with 5 rows
  ASSERT_EQ(schema.size(), 6u);
  EXPECT_EQ(schema[0].name, "Date");
  EXPECT_EQ(schema[1].name, "Open");
  EXPECT_EQ(schema[2].name, "High");
  EXPECT_EQ(schema[3].name, "Low");
  EXPECT_EQ(schema[4].name, "Close");
  EXPECT_EQ(schema[5].name, "Volume");
  EXPECT_EQ(chunks.total_rows, 5u);
}

TEST_F(IntegrationTest, RealWorld_UnicodeData) {
  auto [chunks, schema] = parseFile(testDataPath("real_world/unicode.csv"));

  // unicode.csv: Name,City,Country,Description with 5 rows of UTF-8 content
  ASSERT_EQ(schema.size(), 4u);
  EXPECT_EQ(schema[0].name, "Name");
  EXPECT_EQ(schema[1].name, "City");
  EXPECT_EQ(schema[2].name, "Country");
  EXPECT_EQ(schema[3].name, "Description");
  EXPECT_EQ(chunks.total_rows, 5u);
}

TEST_F(IntegrationTest, RealWorld_ProductCatalog) {
  auto [chunks, schema] = parseFile(testDataPath("real_world/product_catalog.csv"));

  // product_catalog.csv: SKU,Name,Category,Price,Stock,Description
  ASSERT_EQ(schema.size(), 6u);
  EXPECT_EQ(schema[0].name, "SKU");
  EXPECT_EQ(schema[1].name, "Name");
  EXPECT_EQ(schema[2].name, "Category");
  EXPECT_EQ(schema[3].name, "Price");
  EXPECT_EQ(schema[4].name, "Stock");
  EXPECT_EQ(schema[5].name, "Description");
  EXPECT_GE(chunks.total_rows, 1u);
}

// =============================================================================
// 7. Edge Case Tests
// =============================================================================

TEST_F(IntegrationTest, EdgeCase_EmptyFile) {
  // Empty file should fail to open (no header)
  test_util::TempCsvFile csv("");
  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(csv.path());
  EXPECT_FALSE(open_result.ok) << "Empty file should fail to open (no header)";
}

TEST_F(IntegrationTest, EdgeCase_SingleCellFile) {
  auto [chunks, schema] = parseFile(testDataPath("edge_cases/single_cell.csv"));

  // single_cell.csv has a single column header "Value"
  ASSERT_EQ(schema.size(), 1u);
  EXPECT_EQ(schema[0].name, "Value");
}

TEST_F(IntegrationTest, EdgeCase_CRLFLineEndings) {
  auto [chunks, schema] = parseFile(testDataPath("line_endings/crlf.csv"));

  // crlf.csv: A,B,C with 2 data rows using \r\n line endings
  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(chunks.total_rows, 2u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "1");
  EXPECT_EQ(getStringValue(chunks, 2, 1), "6");
}
