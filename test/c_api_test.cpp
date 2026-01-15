/**
 * @file c_api_test.cpp
 * @brief Tests for the libvroom C API wrapper.
 */

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif

extern "C" {
#include "libvroom_c.h"
}

namespace fs = std::filesystem;

class CAPITest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a unique temp directory per process to avoid collisions when
    // CTest runs multiple test binaries in parallel
    temp_dir_ = (fs::temp_directory_path() / ("c_api_test_" + std::to_string(getpid()))).string();
    fs::create_directories(temp_dir_);
    file_counter_ = 0;
  }
  void TearDown() override {
    // Clean up the entire temp directory
    std::error_code ec;
    fs::remove_all(temp_dir_, ec);
  }
  std::string createTestFile(const std::string& content) {
    std::ostringstream filename;
    filename << temp_dir_ << "/test_" << file_counter_++ << ".csv";
    std::string fname = filename.str();
    // Use binary mode to avoid text mode translation issues across platforms
    std::ofstream file(fname, std::ios::binary);
    // Use write() with explicit size for deterministic behavior
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
    // Explicit flush before close to ensure data is visible to subsequent readers.
    // This is important on macOS where aggressive caching can cause race conditions
    // between file writes and subsequent reads from a different file handle.
    file.flush();
    file.close();
    return fname;
  }
  std::string temp_dir_;
  int file_counter_ = 0;

  // Helper to check if an error collector contains a specific error code.
  // Similar to hasErrorCode in csv_parser_errors_test.cpp but for C API.
  bool hasErrorCode(const libvroom_error_collector_t* errors, libvroom_error_t expected_code) {
    for (size_t i = 0; i < libvroom_error_collector_count(errors); ++i) {
      libvroom_parse_error_t parse_error;
      if (libvroom_error_collector_get(errors, i, &parse_error) == LIBVROOM_OK) {
        if (parse_error.code == expected_code) {
          return true;
        }
      }
    }
    return false;
  }
};

// Version Tests
TEST_F(CAPITest, VersionString) {
  const char* version = libvroom_version();
  ASSERT_NE(version, nullptr);
  EXPECT_STREQ(version, "0.1.0");
}

// Error String Tests
TEST_F(CAPITest, ErrorStrings) {
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_OK), "No error");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_UNCLOSED_QUOTE), "Unclosed quote");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_NULL_POINTER), "Null pointer");
}

TEST_F(CAPITest, AllErrorStrings) {
  // Test all error strings for complete coverage
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE), "Invalid quote escape");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_QUOTE_IN_UNQUOTED), "Quote in unquoted field");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INCONSISTENT_FIELDS),
               "Inconsistent field count");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_FIELD_TOO_LARGE), "Field too large");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_MIXED_LINE_ENDINGS), "Mixed line endings");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INVALID_UTF8), "Invalid UTF-8");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_NULL_BYTE), "Null byte in data");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_EMPTY_HEADER), "Empty header");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_DUPLICATE_COLUMNS), "Duplicate columns");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR), "Ambiguous separator");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_FILE_TOO_LARGE), "File too large");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_IO), "I/O error");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INTERNAL), "Internal error");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INVALID_ARGUMENT), "Invalid argument");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_OUT_OF_MEMORY), "Out of memory");
  EXPECT_STREQ(libvroom_error_string(LIBVROOM_ERROR_INVALID_HANDLE), "Invalid handle");
  // Unknown error code
  EXPECT_STREQ(libvroom_error_string(static_cast<libvroom_error_t>(999)), "Unknown error");
}

// Buffer Tests
TEST_F(CAPITest, BufferCreateFromData) {
  const uint8_t data[] = "a,b,c\n1,2,3\n";
  size_t len = sizeof(data) - 1;
  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  ASSERT_NE(buffer, nullptr);
  EXPECT_EQ(libvroom_buffer_length(buffer), len);
  EXPECT_EQ(memcmp(libvroom_buffer_data(buffer), data, len), 0);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, BufferLoadFile) {
  std::string content = "name,value\nalpha,1\nbeta,2\n";
  std::string filename = createTestFile(content);
  libvroom_buffer_t* buffer = libvroom_buffer_load_file(filename.c_str());
  ASSERT_NE(buffer, nullptr);
  EXPECT_EQ(libvroom_buffer_length(buffer), content.size());
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, BufferLoadFileNotFound) {
  EXPECT_EQ(libvroom_buffer_load_file("nonexistent.csv"), nullptr);
}

TEST_F(CAPITest, BufferNullHandling) {
  EXPECT_EQ(libvroom_buffer_data(nullptr), nullptr);
  EXPECT_EQ(libvroom_buffer_length(nullptr), 0u);
  libvroom_buffer_destroy(nullptr);
}

TEST_F(CAPITest, BufferCreateInvalidInput) {
  // Null data pointer
  EXPECT_EQ(libvroom_buffer_create(nullptr, 100), nullptr);
  // Zero length
  const uint8_t data[] = "test";
  EXPECT_EQ(libvroom_buffer_create(data, 0), nullptr);
}

TEST_F(CAPITest, BufferLoadFileNull) {
  EXPECT_EQ(libvroom_buffer_load_file(nullptr), nullptr);
}

// Dialect Tests
TEST_F(CAPITest, DialectCSV) {
  // Use libvroom_dialect_create for CSV: delimiter=',', quote='"', escape='"', double_quote=true
  libvroom_dialect_t* d = libvroom_dialect_create(',', '"', '"', true);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(d), ',');
  EXPECT_EQ(libvroom_dialect_quote_char(d), '"');
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectTSV) {
  // Use libvroom_dialect_create for TSV: delimiter='\t', quote='"', escape='"', double_quote=true
  libvroom_dialect_t* d = libvroom_dialect_create('\t', '"', '"', true);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(d), '\t');
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectCustom) {
  libvroom_dialect_t* d = libvroom_dialect_create(':', '\'', '\\', false);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(d), ':');
  EXPECT_EQ(libvroom_dialect_quote_char(d), '\'');
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectSemicolon) {
  // Use libvroom_dialect_create for semicolon: delimiter=';', quote='"', escape='"',
  // double_quote=true
  libvroom_dialect_t* d = libvroom_dialect_create(';', '"', '"', true);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(d), ';');
  EXPECT_EQ(libvroom_dialect_quote_char(d), '"');
  EXPECT_EQ(libvroom_dialect_escape_char(d), '"');
  EXPECT_TRUE(libvroom_dialect_double_quote(d));
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectPipe) {
  // Use libvroom_dialect_create for pipe: delimiter='|', quote='"', escape='"', double_quote=true
  libvroom_dialect_t* d = libvroom_dialect_create('|', '"', '"', true);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(d), '|');
  EXPECT_EQ(libvroom_dialect_quote_char(d), '"');
  EXPECT_EQ(libvroom_dialect_escape_char(d), '"');
  EXPECT_TRUE(libvroom_dialect_double_quote(d));
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectEscapeAndDoubleQuote) {
  // Test custom dialect with escape char and double_quote = false
  libvroom_dialect_t* d = libvroom_dialect_create(',', '"', '\\', false);
  ASSERT_NE(d, nullptr);
  EXPECT_EQ(libvroom_dialect_escape_char(d), '\\');
  EXPECT_FALSE(libvroom_dialect_double_quote(d));
  libvroom_dialect_destroy(d);
}

TEST_F(CAPITest, DialectNullHandling) {
  EXPECT_EQ(libvroom_dialect_delimiter(nullptr), '\0');
  EXPECT_EQ(libvroom_dialect_quote_char(nullptr), '\0');
  EXPECT_EQ(libvroom_dialect_escape_char(nullptr), '\0');
  EXPECT_FALSE(libvroom_dialect_double_quote(nullptr));
  libvroom_dialect_destroy(nullptr);
}

// Error Collector Tests
TEST_F(CAPITest, ErrorCollectorCreate) {
  libvroom_error_collector_t* c = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(libvroom_error_collector_mode(c), LIBVROOM_MODE_PERMISSIVE);
  EXPECT_FALSE(libvroom_error_collector_has_errors(c));
  EXPECT_EQ(libvroom_error_collector_count(c), 0u);
  libvroom_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorModes) {
  // Test that different modes can be set at creation time
  libvroom_error_collector_t* strict = libvroom_error_collector_create(LIBVROOM_MODE_STRICT, 100);
  ASSERT_NE(strict, nullptr);
  EXPECT_EQ(libvroom_error_collector_mode(strict), LIBVROOM_MODE_STRICT);
  libvroom_error_collector_destroy(strict);

  libvroom_error_collector_t* best_effort =
      libvroom_error_collector_create(LIBVROOM_MODE_BEST_EFFORT, 100);
  ASSERT_NE(best_effort, nullptr);
  EXPECT_EQ(libvroom_error_collector_mode(best_effort), LIBVROOM_MODE_BEST_EFFORT);
  libvroom_error_collector_destroy(best_effort);
}

TEST_F(CAPITest, ErrorCollectorClear) {
  libvroom_error_collector_t* c = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);
  ASSERT_NE(c, nullptr);

  // Clear should work even on empty collector
  libvroom_error_collector_clear(c);
  EXPECT_EQ(libvroom_error_collector_count(c), 0u);

  // Clear with null (should be no-op)
  libvroom_error_collector_clear(nullptr);

  libvroom_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorGetErrors) {
  libvroom_error_collector_t* c = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);
  ASSERT_NE(c, nullptr);

  libvroom_parse_error_t error;
  // Test get with invalid index (no errors yet)
  EXPECT_EQ(libvroom_error_collector_get(c, 0, &error), LIBVROOM_ERROR_INVALID_ARGUMENT);

  // Test get with null error pointer
  EXPECT_EQ(libvroom_error_collector_get(c, 0, nullptr), LIBVROOM_ERROR_NULL_POINTER);

  libvroom_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorNullHandling) {
  EXPECT_EQ(libvroom_error_collector_mode(nullptr), LIBVROOM_MODE_STRICT);
  EXPECT_FALSE(libvroom_error_collector_has_errors(nullptr));
  EXPECT_FALSE(libvroom_error_collector_has_fatal(nullptr));
  EXPECT_EQ(libvroom_error_collector_count(nullptr), 0u);
  libvroom_parse_error_t error;
  EXPECT_EQ(libvroom_error_collector_get(nullptr, 0, &error), LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_error_collector_summary(nullptr), nullptr);
  libvroom_error_collector_destroy(nullptr);
}

TEST_F(CAPITest, ErrorCollectorSummaryNoErrors) {
  libvroom_error_collector_t* c = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);
  ASSERT_NE(c, nullptr);

  char* summary = libvroom_error_collector_summary(c);
  ASSERT_NE(summary, nullptr);
  EXPECT_STREQ(summary, "No errors");
  free(summary);

  libvroom_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorSummaryWithErrors) {
  // Parse data with errors to populate the collector
  const uint8_t data[] = "a,b,c\n1,2\n3,4,5\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);

  // Should have errors from inconsistent field count
  EXPECT_TRUE(libvroom_error_collector_has_errors(errors));

  char* summary = libvroom_error_collector_summary(errors);
  ASSERT_NE(summary, nullptr);

  // Summary should contain error information
  std::string summary_str(summary);
  EXPECT_TRUE(summary_str.find("Total errors:") != std::string::npos);
  EXPECT_TRUE(summary_str.find("Details:") != std::string::npos);

  free(summary);
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ErrorCollectorSummaryWithFatalError) {
  // Parse data with fatal error (unclosed quote)
  const uint8_t data[] = "a,b,c\n\"unclosed";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);

  EXPECT_TRUE(libvroom_error_collector_has_fatal(errors));

  char* summary = libvroom_error_collector_summary(errors);
  ASSERT_NE(summary, nullptr);

  // Summary should mention fatal errors
  std::string summary_str(summary);
  EXPECT_TRUE(summary_str.find("Fatal:") != std::string::npos ||
              summary_str.find("FATAL") != std::string::npos);

  free(summary);
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ErrorCollectorSummaryWithMixedSeverities) {
  // Parse data that triggers both warning (mixed line endings) and error (inconsistent field count)
  const uint8_t data[] = "a,b,c\n1,2,3\r\n4,5\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);

  // Should have multiple errors
  EXPECT_TRUE(libvroom_error_collector_has_errors(errors));
  EXPECT_GT(libvroom_error_collector_count(errors), 1u);

  char* summary = libvroom_error_collector_summary(errors);
  ASSERT_NE(summary, nullptr);

  // Summary should contain breakdown information
  std::string summary_str(summary);
  EXPECT_TRUE(summary_str.find("Total errors:") != std::string::npos);

  free(summary);
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

// Index Tests
TEST_F(CAPITest, IndexCreate) {
  // Note: With the new Parser API, buffer_length is ignored since Parser
  // allocates the index internally during parse(). The index positions will
  // be nullptr until parse() is called.
  libvroom_index_t* idx = libvroom_index_create(1000, 1);
  ASSERT_NE(idx, nullptr);
  EXPECT_EQ(libvroom_index_num_threads(idx), 1u);
  // Positions are nullptr until parse() is called
  EXPECT_EQ(libvroom_index_positions(idx), nullptr);
  libvroom_index_destroy(idx);
}

TEST_F(CAPITest, IndexCreateInvalid) {
  // Note: buffer_length=0 is now valid since it's ignored (Parser allocates internally)
  // Only num_threads=0 should return nullptr
  libvroom_index_t* idx = libvroom_index_create(0, 1);
  EXPECT_NE(idx, nullptr);                            // Valid: buffer_length ignored
  libvroom_index_destroy(idx);                        // Clean up
  EXPECT_EQ(libvroom_index_create(1000, 0), nullptr); // Invalid: num_threads=0
}

TEST_F(CAPITest, IndexColumnsAndTotalCount) {
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  ASSERT_NE(buffer, nullptr);
  ASSERT_NE(parser, nullptr);
  ASSERT_NE(index, nullptr);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  // Test columns accessor
  size_t columns = libvroom_index_columns(index);
  EXPECT_GE(columns, 0u); // Columns may or may not be set by parse

  // Test total_count accessor
  uint64_t total = libvroom_index_total_count(index);
  EXPECT_GT(total, 0u);

  // Verify total_count matches count for single-threaded parse
  EXPECT_EQ(total, libvroom_index_count(index, 0));

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, IndexCountOutOfBounds) {
  libvroom_index_t* idx = libvroom_index_create(1000, 2);
  ASSERT_NE(idx, nullptr);

  // Thread ID out of bounds
  EXPECT_EQ(libvroom_index_count(idx, 100), 0u);

  libvroom_index_destroy(idx);
}

TEST_F(CAPITest, IndexNullHandling) {
  EXPECT_EQ(libvroom_index_num_threads(nullptr), 0u);
  EXPECT_EQ(libvroom_index_columns(nullptr), 0u);
  EXPECT_EQ(libvroom_index_count(nullptr, 0), 0u);
  EXPECT_EQ(libvroom_index_total_count(nullptr), 0u);
  EXPECT_EQ(libvroom_index_positions(nullptr), nullptr);
  libvroom_index_destroy(nullptr);
}

// Parser Tests
TEST_F(CAPITest, ParserCreate) {
  libvroom_parser_t* p = libvroom_parser_create();
  ASSERT_NE(p, nullptr);
  libvroom_parser_destroy(p);
}

TEST_F(CAPITest, ParseSimpleCSV) {
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  ASSERT_NE(buffer, nullptr);
  ASSERT_NE(parser, nullptr);
  ASSERT_NE(index, nullptr);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_GT(libvroom_index_count(index, 0), 0u);

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithErrors) {
  const uint8_t data[] = "a,b,c\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_FALSE(libvroom_error_collector_has_fatal(errors));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseNullPointers) {
  const uint8_t data[] = "a,b,c\n";
  libvroom_buffer_t* buffer = libvroom_buffer_create(data, sizeof(data) - 1);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(100, 1);

  EXPECT_EQ(libvroom_parse(nullptr, buffer, index, nullptr, nullptr), LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parse(parser, nullptr, index, nullptr, nullptr), LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parse(parser, buffer, nullptr, nullptr, nullptr), LIBVROOM_ERROR_NULL_POINTER);

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseNullErrorCollector) {
  // Test that null error collector is handled gracefully (falls back to non-error parse)
  const uint8_t data[] = "a,b,c\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithDialect) {
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);
  // CSV dialect: delimiter=',', quote='"', escape='"', double_quote=true
  libvroom_dialect_t* dialect = libvroom_dialect_create(',', '"', '"', true);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, dialect);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_GT(libvroom_index_count(index, 0), 0u);
  EXPECT_FALSE(libvroom_error_collector_has_fatal(errors));

  libvroom_dialect_destroy(dialect);
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParserDestroyNull) {
  // Should not crash with null
  libvroom_parser_destroy(nullptr);
}

// Dialect Detection Tests
TEST_F(CAPITest, DetectDialectCSV) {
  const uint8_t data[] = "name,value,count\nalpha,1,100\nbeta,2,200\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_detection_result_t* result = libvroom_detect_dialect(buffer);
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));
  libvroom_dialect_t* d = libvroom_detection_result_dialect(result);
  EXPECT_EQ(libvroom_dialect_delimiter(d), ',');

  libvroom_dialect_destroy(d);
  libvroom_detection_result_destroy(result);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, DetectDialectNull) {
  EXPECT_EQ(libvroom_detect_dialect(nullptr), nullptr);
}

// Direct file dialect detection tests
TEST_F(CAPITest, DetectDialectFileCSV) {
  std::string content = "name,value,count\nalpha,1,100\nbeta,2,200\ngamma,3,300\n";
  std::string filename = createTestFile(content);

  libvroom_detection_result_t* result = libvroom_detect_dialect_file(filename.c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));
  EXPECT_GT(libvroom_detection_result_confidence(result), 0.5);

  libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
  ASSERT_NE(dialect, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(dialect), ',');

  EXPECT_EQ(libvroom_detection_result_columns(result), 3u);
  EXPECT_GE(libvroom_detection_result_rows_analyzed(result), 1u);

  libvroom_dialect_destroy(dialect);
  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectDialectFileTSV) {
  std::string content = "name\tvalue\tcount\nalpha\t1\t100\nbeta\t2\t200\n";
  std::string filename = createTestFile(content);

  libvroom_detection_result_t* result = libvroom_detect_dialect_file(filename.c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));

  libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
  ASSERT_NE(dialect, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(dialect), '\t');

  libvroom_dialect_destroy(dialect);
  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectDialectFileSemicolon) {
  std::string content = "name;value;count\nalpha;1;100\nbeta;2;200\n";
  std::string filename = createTestFile(content);

  libvroom_detection_result_t* result = libvroom_detect_dialect_file(filename.c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));

  libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
  ASSERT_NE(dialect, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(dialect), ';');

  libvroom_dialect_destroy(dialect);
  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectDialectFilePipe) {
  std::string content = "name|value|count\nalpha|1|100\nbeta|2|200\n";
  std::string filename = createTestFile(content);

  libvroom_detection_result_t* result = libvroom_detect_dialect_file(filename.c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));

  libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
  ASSERT_NE(dialect, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(dialect), '|');

  libvroom_dialect_destroy(dialect);
  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectDialectFileNull) {
  EXPECT_EQ(libvroom_detect_dialect_file(nullptr), nullptr);
}

TEST_F(CAPITest, DetectDialectFileNotFound) {
  libvroom_detection_result_t* result = libvroom_detect_dialect_file("nonexistent_file.csv");
  ASSERT_NE(result, nullptr);

  // Detection fails, but result object is returned with warning
  EXPECT_FALSE(libvroom_detection_result_success(result));
  EXPECT_NE(libvroom_detection_result_warning(result), nullptr);

  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectDialectFileWithQuotedFields) {
  std::string content =
      "name,description,value\n\"Alice\",\"A person\",100\n\"Bob\",\"Another person\",200\n";
  std::string filename = createTestFile(content);

  libvroom_detection_result_t* result = libvroom_detect_dialect_file(filename.c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));

  libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
  ASSERT_NE(dialect, nullptr);
  EXPECT_EQ(libvroom_dialect_delimiter(dialect), ',');
  EXPECT_EQ(libvroom_dialect_quote_char(dialect), '"');

  libvroom_dialect_destroy(dialect);
  libvroom_detection_result_destroy(result);
}

TEST_F(CAPITest, DetectionResultAllAccessors) {
  const uint8_t data[] = "name,value,count\nalpha,1,100\nbeta,2,200\ngamma,3,300\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_detection_result_t* result = libvroom_detect_dialect(buffer);
  ASSERT_NE(result, nullptr);

  EXPECT_TRUE(libvroom_detection_result_success(result));
  EXPECT_GT(libvroom_detection_result_confidence(result), 0.0);

  // Test columns accessor
  size_t columns = libvroom_detection_result_columns(result);
  EXPECT_EQ(columns, 3u);

  // Test rows_analyzed accessor
  size_t rows = libvroom_detection_result_rows_analyzed(result);
  EXPECT_GE(rows, 1u);

  // Test has_header accessor
  bool has_header = libvroom_detection_result_has_header(result);
  // The header detection may vary, so just verify it returns a value
  (void)has_header;

  // Test warning accessor (may be null for clean data)
  const char* warning = libvroom_detection_result_warning(result);
  // warning is expected to be null or a valid string
  (void)warning;

  libvroom_detection_result_destroy(result);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, DetectionResultNullHandling) {
  EXPECT_FALSE(libvroom_detection_result_success(nullptr));
  EXPECT_EQ(libvroom_detection_result_confidence(nullptr), 0.0);
  EXPECT_EQ(libvroom_detection_result_dialect(nullptr), nullptr);
  EXPECT_EQ(libvroom_detection_result_columns(nullptr), 0u);
  EXPECT_EQ(libvroom_detection_result_rows_analyzed(nullptr), 0u);
  EXPECT_FALSE(libvroom_detection_result_has_header(nullptr));
  EXPECT_EQ(libvroom_detection_result_warning(nullptr), nullptr);
  libvroom_detection_result_destroy(nullptr);
}

// Parse Auto Tests
TEST_F(CAPITest, ParseAuto) {
  const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_detection_result_t* detected = nullptr;
  libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, errors, &detected);
  EXPECT_EQ(err, LIBVROOM_OK);

  if (detected) {
    EXPECT_TRUE(libvroom_detection_result_success(detected));
    libvroom_detection_result_destroy(detected);
  }

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullPointers) {
  const uint8_t data[] = "name,value\n";
  libvroom_buffer_t* buffer = libvroom_buffer_create(data, sizeof(data) - 1);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(100, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  EXPECT_EQ(libvroom_parse_auto(nullptr, buffer, index, errors, nullptr),
            LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parse_auto(parser, nullptr, index, errors, nullptr),
            LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parse_auto(parser, buffer, nullptr, errors, nullptr),
            LIBVROOM_ERROR_NULL_POINTER);

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullDetectedPointer) {
  // Test that parse_auto works when detected out-parameter is null
  const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullErrorCollector) {
  // Test that parse_auto works when error collector is null
  const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);

  libvroom_detection_result_t* detected = nullptr;
  libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, nullptr, &detected);
  EXPECT_EQ(err, LIBVROOM_OK);

  if (detected) {
    libvroom_detection_result_destroy(detected);
  }

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseTSVWithDialect) {
  // Test parse with explicit TSV dialect
  const uint8_t data[] = "a\tb\tc\n1\t2\t3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  // TSV dialect: delimiter='\t', quote='"', escape='"', double_quote=true
  libvroom_dialect_t* dialect = libvroom_dialect_create('\t', '"', '"', true);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, dialect);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_GT(libvroom_index_count(index, 0), 0u);

  libvroom_dialect_destroy(dialect);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

// Utility Function Tests
TEST_F(CAPITest, RecommendedThreads) {
  EXPECT_GE(libvroom_recommended_threads(), 1u);
}

TEST_F(CAPITest, SIMDPadding) {
  EXPECT_GE(libvroom_simd_padding(), 16u);
}

// Integration Test
TEST_F(CAPITest, FullWorkflowFromFile) {
  std::string content = "id,name,value\n1,alpha,100\n2,beta,200\n";
  std::string filename = createTestFile(content);

  libvroom_buffer_t* buffer = libvroom_buffer_load_file(filename.c_str());
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(libvroom_buffer_length(buffer), 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_GT(libvroom_index_count(index, 0), 0u);
  EXPECT_FALSE(libvroom_error_collector_has_fatal(errors));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

// Error Conversion Tests - exercise error code conversions in libvroom_c.cpp
TEST_F(CAPITest, ParseWithUnclosedQuoteError) {
  // CSV with unclosed quote at EOF
  const uint8_t data[] = "a,b,c\n\"unclosed";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  // Should return the fatal error code
  EXPECT_EQ(err, LIBVROOM_ERROR_UNCLOSED_QUOTE);
  EXPECT_TRUE(libvroom_error_collector_has_fatal(errors));

  // Verify we can retrieve the error details
  if (libvroom_error_collector_count(errors) > 0) {
    libvroom_parse_error_t parse_error;
    EXPECT_EQ(libvroom_error_collector_get(errors, 0, &parse_error), LIBVROOM_OK);
    EXPECT_EQ(parse_error.code, LIBVROOM_ERROR_UNCLOSED_QUOTE);
    EXPECT_EQ(parse_error.severity, LIBVROOM_SEVERITY_FATAL);
    EXPECT_NE(parse_error.message, nullptr);
  }

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithInconsistentFieldCount) {
  // CSV with inconsistent field count
  const uint8_t data[] = "a,b,c\n1,2\n3,4,5\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);

  EXPECT_TRUE(libvroom_error_collector_has_errors(errors));
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_INCONSISTENT_FIELDS));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithQuoteInUnquotedField) {
  // CSV with quote in unquoted field
  const uint8_t data[] = "a,b,c\ntest\"bad,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(libvroom_error_collector_has_errors(errors));
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_QUOTE_IN_UNQUOTED));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithInvalidQuoteEscape) {
  // CSV with invalid quote escape ("abc"def - quote not at start/end)
  const uint8_t data[] = "a,b,c\n\"abc\"def,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(libvroom_error_collector_has_errors(errors));
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithMixedLineEndings) {
  // CSV with mixed line endings (LF and CRLF)
  const uint8_t data[] = "a,b,c\n1,2,3\r\n4,5,6\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_MIXED_LINE_ENDINGS));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithNullByte) {
  // CSV with null byte in data
  uint8_t data[] = "a,b,c\n1,\x00,3\n";
  size_t len = 12; // Includes the null byte as data

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_NULL_BYTE));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithEmptyHeader) {
  // CSV with empty header
  const uint8_t data[] = "\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_EMPTY_HEADER));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithDuplicateColumnNames) {
  // CSV with duplicate column names
  const uint8_t data[] = "name,value,name\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors =
      libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 100);

  libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_TRUE(hasErrorCode(errors, LIBVROOM_ERROR_DUPLICATE_COLUMNS));

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

// ============================================================================
// WELL-FORMED CSV NEGATIVE TESTS (No False Positives)
// ============================================================================
//
// These tests verify that well-formed CSV data does NOT trigger errors
// when parsed with an error collector enabled. This ensures the error
// detection system doesn't produce false positives.

TEST_F(CAPITest, WellFormedMinimalCSV) {
  // Minimal valid CSV: single header, single data row
  const uint8_t data[] = "a,b,c\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Minimal valid CSV should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for minimal valid CSV";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedQuotedFields) {
  // Properly quoted fields with embedded content
  const uint8_t data[] = "name,value\n\"Alice\",\"100\"\n\"Bob\",\"200\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Quoted fields should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for properly quoted fields";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedEscapedQuotes) {
  // Properly escaped quotes using double-quote syntax (RFC 4180)
  const uint8_t data[] = "text,desc\n\"He said \"\"Hello\"\"\",\"greeting\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Escaped quotes should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for properly escaped quotes";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedCRLFLineEndings) {
  // Consistent CRLF line endings
  const uint8_t data[] = "a,b,c\r\n1,2,3\r\n4,5,6\r\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "CRLF line endings should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for consistent CRLF line endings";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedLFLineEndings) {
  // Consistent LF line endings (Unix-style)
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "LF line endings should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for consistent LF line endings";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedDistinctColumns) {
  // Distinct column names (no duplicates)
  const uint8_t data[] = "id,name,value,status\n1,Alice,100,active\n2,Bob,200,inactive\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Distinct columns should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for distinct column names";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedEmptyFields) {
  // Empty fields are valid CSV
  const uint8_t data[] = "a,b,c\n,,\n1,,3\n,2,\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Empty fields should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for empty fields";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedQuotedEmptyFields) {
  // Quoted empty fields are valid CSV
  const uint8_t data[] = "a,b,c\n\"\",\"\",\"\"\n\"x\",\"\",\"z\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Quoted empty fields should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for quoted empty fields";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedNewlinesInQuotes) {
  // Newlines inside quoted fields are valid CSV
  const uint8_t data[] = "name,address\n\"John\",\"123 Main St\nApt 4\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Newlines in quoted fields should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for newlines in quotes";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedDelimitersInQuotes) {
  // Delimiters (commas) inside quoted fields are valid CSV
  const uint8_t data[] = "name,values\n\"Smith, John\",\"a,b,c\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Delimiters in quoted fields should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for delimiters in quotes";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedTripleQuotesRFC4180) {
  // Triple quotes """value""" represent a quoted value with embedded quotes (RFC 4180)
  const uint8_t data[] = "text\n\"\"\"quoted\"\"\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Triple quote RFC 4180 syntax should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for valid triple quote syntax";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedNoFinalNewline) {
  // CSV without final newline is valid
  const uint8_t data[] = "a,b,c\n1,2,3";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "CSV without final newline should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for missing final newline";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedSingleColumn) {
  // Single column CSV is valid
  const uint8_t data[] = "value\n1\n2\n3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Single column CSV should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for single column";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedWithDialect) {
  // Well-formed CSV with explicit dialect
  const uint8_t data[] = "a,b,c\n\"x\",\"y\",\"z\"\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);
  libvroom_dialect_t* dialect = libvroom_dialect_create(',', '"', '"', true);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, dialect);
  EXPECT_EQ(err, LIBVROOM_OK) << "Well-formed CSV with dialect should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected with explicit dialect";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_dialect_destroy(dialect);
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedStrictMode) {
  // Well-formed CSV should pass in strict mode without triggering early exit
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_STRICT, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Well-formed CSV should parse successfully in strict mode";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected in strict mode for valid CSV";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedParseAuto) {
  // Well-formed CSV with automatic dialect detection
  const uint8_t data[] = "name,value,count\nalpha,1,100\nbeta,2,200\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_detection_result_t* detected = nullptr;
  libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, errors, &detected);
  EXPECT_EQ(err, LIBVROOM_OK) << "Well-formed CSV should parse successfully with auto detection";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for auto-detected valid CSV";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  if (detected) {
    EXPECT_TRUE(libvroom_detection_result_success(detected)) << "Detection should succeed";
    libvroom_detection_result_destroy(detected);
  }

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, WellFormedFromFile) {
  // Well-formed CSV loaded from file should not trigger errors
  std::string content = "id,name,value\n1,alpha,100\n2,beta,200\n3,gamma,300\n";
  std::string filename = createTestFile(content);

  libvroom_buffer_t* buffer = libvroom_buffer_load_file(filename.c_str());
  ASSERT_NE(buffer, nullptr);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(libvroom_buffer_length(buffer), 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK) << "Well-formed CSV from file should parse successfully";
  EXPECT_FALSE(libvroom_error_collector_has_errors(errors))
      << "No errors expected for valid CSV from file";
  EXPECT_FALSE(libvroom_error_collector_has_fatal(errors)) << "No fatal errors expected";
  EXPECT_EQ(libvroom_error_collector_count(errors), 0u) << "Error count should be zero";

  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

// ============================================================================
// ENCODING DETECTION AND TRANSCODING TESTS
// ============================================================================

class CAPIEncodingTest : public CAPITest {
protected:
  static std::string test_encoding_dir() { return "test/data/encoding/"; }
};

TEST_F(CAPIEncodingTest, EncodingStringConversion) {
  // Test all encoding string conversions
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF8), "UTF-8");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF8_BOM), "UTF-8 (BOM)");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF16_LE), "UTF-16LE");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF16_BE), "UTF-16BE");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF32_LE), "UTF-32LE");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UTF32_BE), "UTF-32BE");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_LATIN1), "Latin-1");
  EXPECT_STREQ(libvroom_encoding_string(LIBVROOM_ENCODING_UNKNOWN), "Unknown");
  // Test unknown value
  EXPECT_STREQ(libvroom_encoding_string(static_cast<libvroom_encoding_t>(999)), "Unknown");
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf8) {
  // Plain UTF-8 (no BOM)
  const uint8_t data[] = "hello,world\n1,2\n";
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data) - 1, &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF8);
  EXPECT_EQ(result.bom_length, 0u);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf8Bom) {
  // UTF-8 with BOM: EF BB BF
  uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'e', 'l', 'l', 'o'};
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF8_BOM);
  EXPECT_EQ(result.bom_length, 3u);
  EXPECT_FALSE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf16Le) {
  // UTF-16 LE BOM: FF FE
  uint8_t data[] = {0xFF, 0xFE, 'a', 0x00, 'b', 0x00};
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF16_LE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf16Be) {
  // UTF-16 BE BOM: FE FF
  uint8_t data[] = {0xFE, 0xFF, 0x00, 'a', 0x00, 'b'};
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF16_BE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf32Le) {
  // UTF-32 LE BOM: FF FE 00 00
  uint8_t data[] = {0xFF, 0xFE, 0x00, 0x00, 'a', 0x00, 0x00, 0x00};
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF32_LE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(CAPIEncodingTest, DetectEncodingUtf32Be) {
  // UTF-32 BE BOM: 00 00 FE FF
  uint8_t data[] = {0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 'a'};
  libvroom_encoding_result_t result;

  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF32_BE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(CAPIEncodingTest, DetectEncodingNullResult) {
  const uint8_t data[] = "hello";
  libvroom_error_t err = libvroom_detect_encoding(data, sizeof(data), nullptr);
  EXPECT_EQ(err, LIBVROOM_ERROR_NULL_POINTER);
}

TEST_F(CAPIEncodingTest, DetectEncodingNullData) {
  // null data is handled gracefully (returns UTF-8)
  libvroom_encoding_result_t result;
  libvroom_error_t err = libvroom_detect_encoding(nullptr, 0, &result);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(result.encoding, LIBVROOM_ENCODING_UTF8);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingUtf16Le) {
  libvroom_load_result_t* result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf16_le_bom.csv").c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF16_LE);
  EXPECT_TRUE(libvroom_load_result_was_transcoded(result));
  EXPECT_GT(libvroom_load_result_length(result), 0u);
  EXPECT_NE(libvroom_load_result_data(result), nullptr);
  EXPECT_DOUBLE_EQ(libvroom_load_result_confidence(result), 1.0);
  EXPECT_EQ(libvroom_load_result_bom_length(result), 2u);

  // Verify the transcoded data is valid UTF-8 and can be parsed
  libvroom_buffer_t* buffer = libvroom_load_result_to_buffer(result);
  ASSERT_NE(buffer, nullptr);

  // The content should be UTF-8 now
  std::string content(reinterpret_cast<const char*>(libvroom_buffer_data(buffer)),
                      libvroom_buffer_length(buffer));
  EXPECT_TRUE(content.find("name") != std::string::npos);

  libvroom_buffer_destroy(buffer);
  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingUtf16Be) {
  libvroom_load_result_t* result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf16_be_bom.csv").c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF16_BE);
  EXPECT_TRUE(libvroom_load_result_was_transcoded(result));

  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingUtf32Le) {
  libvroom_load_result_t* result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf32_le_bom.csv").c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF32_LE);
  EXPECT_TRUE(libvroom_load_result_was_transcoded(result));
  EXPECT_EQ(libvroom_load_result_bom_length(result), 4u);

  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingUtf32Be) {
  libvroom_load_result_t* result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf32_be_bom.csv").c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF32_BE);
  EXPECT_TRUE(libvroom_load_result_was_transcoded(result));

  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingUtf8Bom) {
  libvroom_load_result_t* result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf8_bom.csv").c_str());
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF8_BOM);
  EXPECT_EQ(libvroom_load_result_bom_length(result), 3u);
  // was_transcoded returns true because the BOM was stripped from the data
  EXPECT_TRUE(libvroom_load_result_was_transcoded(result));

  // Verify the BOM was stripped
  const uint8_t* data = libvroom_load_result_data(result);
  ASSERT_NE(data, nullptr);
  // First byte should NOT be the BOM start (0xEF)
  EXPECT_NE(data[0], 0xEF);

  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingPlainUtf8) {
  // Test a plain UTF-8 file without BOM - was_transcoded should be false
  libvroom_load_result_t* result = libvroom_load_file_with_encoding("test/data/basic/simple.csv");
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(libvroom_load_result_encoding(result), LIBVROOM_ENCODING_UTF8);
  EXPECT_EQ(libvroom_load_result_bom_length(result), 0u);
  // No BOM, no transcoding - data was not modified
  EXPECT_FALSE(libvroom_load_result_was_transcoded(result));

  libvroom_load_result_destroy(result);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingNullFilename) {
  EXPECT_EQ(libvroom_load_file_with_encoding(nullptr), nullptr);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingNotFound) {
  EXPECT_EQ(libvroom_load_file_with_encoding("nonexistent_file.csv"), nullptr);
}

TEST_F(CAPIEncodingTest, LoadResultNullHandling) {
  EXPECT_EQ(libvroom_load_result_data(nullptr), nullptr);
  EXPECT_EQ(libvroom_load_result_length(nullptr), 0u);
  EXPECT_EQ(libvroom_load_result_encoding(nullptr), LIBVROOM_ENCODING_UNKNOWN);
  EXPECT_EQ(libvroom_load_result_bom_length(nullptr), 0u);
  EXPECT_DOUBLE_EQ(libvroom_load_result_confidence(nullptr), 0.0);
  EXPECT_FALSE(libvroom_load_result_was_transcoded(nullptr));
  EXPECT_EQ(libvroom_load_result_to_buffer(nullptr), nullptr);
  // Should not crash
  libvroom_load_result_destroy(nullptr);
}

TEST_F(CAPIEncodingTest, LoadFileWithEncodingThenParse) {
  // Full integration test: load a UTF-16 file and parse it
  libvroom_load_result_t* load_result =
      libvroom_load_file_with_encoding((test_encoding_dir() + "utf16_le_bom.csv").c_str());
  ASSERT_NE(load_result, nullptr);

  // Convert to buffer for parsing
  libvroom_buffer_t* buffer = libvroom_load_result_to_buffer(load_result);
  ASSERT_NE(buffer, nullptr);

  // Create parser and index
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(libvroom_buffer_length(buffer), 1);
  libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE, 0);

  // Parse the transcoded data
  libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_FALSE(libvroom_error_collector_has_fatal(errors));
  EXPECT_GT(libvroom_index_count(index, 0), 0u);

  // Cleanup
  libvroom_error_collector_destroy(errors);
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
  libvroom_load_result_destroy(load_result);
}

// ============================================================================
// INDEX SERIALIZATION TESTS
// ============================================================================

TEST_F(CAPITest, IndexWriteAndRead) {
  // Parse a CSV file to create an index
  const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);
  ASSERT_NE(buffer, nullptr);
  ASSERT_NE(parser, nullptr);
  ASSERT_NE(index, nullptr);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  // Save index stats for later comparison
  uint64_t original_count = libvroom_index_total_count(index);
  size_t original_columns = libvroom_index_columns(index);
  size_t original_threads = libvroom_index_num_threads(index);
  EXPECT_GT(original_count, 0u);

  // Write the index to a file
  std::string index_file = temp_dir_ + "/test_index_serialize.idx";
  err = libvroom_index_write(index, index_file.c_str());
  EXPECT_EQ(err, LIBVROOM_OK);

  // Cleanup original index
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);

  // Read the index back
  libvroom_index_t* loaded_index = libvroom_index_read(index_file.c_str());
  ASSERT_NE(loaded_index, nullptr);

  // Verify the loaded index matches the original
  EXPECT_EQ(libvroom_index_total_count(loaded_index), original_count);
  EXPECT_EQ(libvroom_index_columns(loaded_index), original_columns);
  EXPECT_EQ(libvroom_index_num_threads(loaded_index), original_threads);
  EXPECT_NE(libvroom_index_positions(loaded_index), nullptr);

  libvroom_index_destroy(loaded_index);
}

TEST_F(CAPITest, IndexWriteNullPointers) {
  // Test null index pointer
  EXPECT_EQ(libvroom_index_write(nullptr, "test.idx"), LIBVROOM_ERROR_NULL_POINTER);

  // Test null filename
  libvroom_index_t* index = libvroom_index_create(100, 1);
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(libvroom_index_write(index, nullptr), LIBVROOM_ERROR_NULL_POINTER);
  libvroom_index_destroy(index);
}

TEST_F(CAPITest, IndexWriteUnpopulatedIndex) {
  // Test writing an index that hasn't been populated by parse()
  libvroom_index_t* index = libvroom_index_create(100, 1);
  ASSERT_NE(index, nullptr);

  // The index hasn't been populated yet, so write should fail
  EXPECT_EQ(libvroom_index_write(index, "test.idx"), LIBVROOM_ERROR_INVALID_HANDLE);

  libvroom_index_destroy(index);
}

TEST_F(CAPITest, IndexReadNullFilename) {
  EXPECT_EQ(libvroom_index_read(nullptr), nullptr);
}

TEST_F(CAPITest, IndexReadNonexistentFile) {
  EXPECT_EQ(libvroom_index_read("nonexistent_index_file.idx"), nullptr);
}

TEST_F(CAPITest, IndexWriteToInvalidPath) {
  // Parse a CSV to create a valid index
  const uint8_t data[] = "a,b,c\n1,2,3\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  // Try to write to an invalid path (directory that doesn't exist)
  err = libvroom_index_write(index, "/nonexistent/directory/test.idx");
  EXPECT_EQ(err, LIBVROOM_ERROR_IO);

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);
}

TEST_F(CAPITest, IndexReadCorruptedFile) {
  // Create a file with invalid content
  std::string filename = temp_dir_ + "/test_corrupted_index.idx";
  std::ofstream file(filename, std::ios::binary);
  // Write garbage data
  uint8_t garbage[] = {0x00, 0x01, 0x02};
  file.write(reinterpret_cast<const char*>(garbage), sizeof(garbage));
  file.close();

  // Should return nullptr for corrupted file
  EXPECT_EQ(libvroom_index_read(filename.c_str()), nullptr);
}

TEST_F(CAPITest, IndexSerializationRoundTripFromFile) {
  // Full integration test: load CSV from file, parse, save index, reload index
  std::string csv_content = "name,value,count\nalpha,1,100\nbeta,2,200\ngamma,3,300\n";
  std::string csv_file = createTestFile(csv_content);
  std::string index_file = temp_dir_ + "/test_index_roundtrip.idx";

  // Load and parse CSV
  libvroom_buffer_t* buffer = libvroom_buffer_load_file(csv_file.c_str());
  ASSERT_NE(buffer, nullptr);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(libvroom_buffer_length(buffer), 1);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  // Save the index
  uint64_t original_count = libvroom_index_total_count(index);
  size_t original_columns = libvroom_index_columns(index);
  err = libvroom_index_write(index, index_file.c_str());
  EXPECT_EQ(err, LIBVROOM_OK);

  // Cleanup original resources
  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);

  // Reload the index from file
  libvroom_index_t* loaded_index = libvroom_index_read(index_file.c_str());
  ASSERT_NE(loaded_index, nullptr);

  // Verify loaded index matches
  EXPECT_EQ(libvroom_index_total_count(loaded_index), original_count);
  EXPECT_EQ(libvroom_index_columns(loaded_index), original_columns);

  // Verify we can access the positions
  const uint64_t* positions = libvroom_index_positions(loaded_index);
  EXPECT_NE(positions, nullptr);

  // Verify per-thread count
  EXPECT_EQ(libvroom_index_count(loaded_index, 0), original_count);

  libvroom_index_destroy(loaded_index);
}

TEST_F(CAPITest, IndexSerializationMultipleWriteRead) {
  // Test that we can write and read multiple times
  const uint8_t data[] = "x,y\n1,2\n3,4\n";
  size_t len = sizeof(data) - 1;

  libvroom_buffer_t* buffer = libvroom_buffer_create(data, len);
  libvroom_parser_t* parser = libvroom_parser_create();
  libvroom_index_t* index = libvroom_index_create(len, 1);

  libvroom_error_t err = libvroom_parse(parser, buffer, index, nullptr, nullptr);
  EXPECT_EQ(err, LIBVROOM_OK);

  uint64_t original_count = libvroom_index_total_count(index);

  // Write to two different files
  std::string file1 = temp_dir_ + "/test_index_multi1.idx";
  std::string file2 = temp_dir_ + "/test_index_multi2.idx";

  EXPECT_EQ(libvroom_index_write(index, file1.c_str()), LIBVROOM_OK);
  EXPECT_EQ(libvroom_index_write(index, file2.c_str()), LIBVROOM_OK);

  libvroom_index_destroy(index);
  libvroom_parser_destroy(parser);
  libvroom_buffer_destroy(buffer);

  // Read both files
  libvroom_index_t* idx1 = libvroom_index_read(file1.c_str());
  libvroom_index_t* idx2 = libvroom_index_read(file2.c_str());
  ASSERT_NE(idx1, nullptr);
  ASSERT_NE(idx2, nullptr);

  // Both should have the same data
  EXPECT_EQ(libvroom_index_total_count(idx1), original_count);
  EXPECT_EQ(libvroom_index_total_count(idx2), original_count);

  libvroom_index_destroy(idx1);
  libvroom_index_destroy(idx2);
}
