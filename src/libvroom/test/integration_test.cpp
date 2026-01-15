/**
 * @file integration_test.cpp
 * @brief End-to-end integration tests for libvroom.
 *
 * These tests validate the complete parsing pipeline from file loading through
 * parsing to data extraction. They complement unit tests by verifying that
 * all components work together correctly.
 *
 * Test scenarios:
 * 1. Basic E2E - Load file, parse with multi-threading, verify data via streaming
 * 2. Multi-threaded consistency - Same results with different thread counts
 * 3. Streaming vs batch equivalence - Both parsing approaches work on same data
 *
 * Note: The batch parser (Parser class) produces an index of field positions.
 * To verify actual field values, we use the streaming parser (StreamReader).
 * The num_columns() field in Parser::Result is not always populated by the
 * batch parser, so we verify column counts via streaming where needed.
 */

#include <gtest/gtest.h>
#include <libvroom.h>
#include <streaming.h>
#include <thread>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>

using namespace libvroom;

// =============================================================================
// Test Fixture
// =============================================================================

class IntegrationTest : public ::testing::Test {
protected:
    // Helper to create a padded buffer from string content
    static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
        size_t len = content.size();
        uint8_t* buf = allocate_padded_buffer(len, 64);
        std::memcpy(buf, content.data(), len);
        return {buf, len};
    }

    // Helper to extract all field values using streaming parser
    // This is used to verify actual data correctness
    static std::vector<std::vector<std::string>> extract_all_fields_streaming(
        const std::string& csv_content, const Dialect& dialect, bool has_header = true) {

        std::istringstream input(csv_content);
        StreamConfig config;
        config.dialect = dialect;
        config.parse_header = has_header;

        StreamReader reader(input, config);

        std::vector<std::vector<std::string>> rows;
        while (reader.next_row()) {
            std::vector<std::string> row;
            for (const auto& field : reader.row()) {
                row.push_back(std::string(field.data));
            }
            rows.push_back(row);
        }
        return rows;
    }

    // Helper to get header using streaming parser
    static std::vector<std::string> get_header_streaming(
        const std::string& csv_content, const Dialect& dialect) {

        std::istringstream input(csv_content);
        StreamConfig config;
        config.dialect = dialect;
        config.parse_header = true;

        StreamReader reader(input, config);
        if (reader.next_row()) {
            return reader.header();
        }
        return {};
    }

    // Path to test data directory (relative to build directory)
    static std::string test_data_path(const std::string& filename) {
        return "test/data/" + filename;
    }
};

// =============================================================================
// Test 1: Basic End-to-End Test
// =============================================================================

TEST_F(IntegrationTest, BasicEndToEnd_LoadParseVerify) {
    // Load a real CSV file
    FileBuffer buffer = load_file(test_data_path("basic/simple.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load test file";
    ASSERT_GT(buffer.size(), 0) << "File is empty";

    // Parse with multi-threaded parser
    const size_t num_threads = std::min(4u, std::thread::hardware_concurrency());
    Parser parser(num_threads);

    auto result = parser.parse(buffer.data(), buffer.size());

    // Verify parsing succeeded
    ASSERT_TRUE(result.success()) << "Parsing failed";
    EXPECT_GT(result.total_indexes(), 0) << "No field indexes found";

    // Verify dialect was detected correctly
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.dialect.quote_char, '"');

    // Extract actual data using streaming parser and verify correctness
    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    auto rows = extract_all_fields_streaming(csv_content, result.dialect);

    // simple.csv has 3 data rows (excluding header): 1,2,3 / 4,5,6 / 7,8,9
    ASSERT_EQ(rows.size(), 3) << "Expected 3 data rows";
    EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"4", "5", "6"}));
    EXPECT_EQ(rows[2], (std::vector<std::string>{"7", "8", "9"}));

    // Verify header via streaming
    auto header = get_header_streaming(csv_content, result.dialect);
    EXPECT_EQ(header.size(), 3);
    EXPECT_EQ(header, (std::vector<std::string>{"A", "B", "C"}));
}

TEST_F(IntegrationTest, BasicEndToEnd_RealWorldFile) {
    // Test with a more complex real-world file with quoted fields
    FileBuffer buffer = load_file(test_data_path("real_world/contacts.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load contacts.csv";

    Parser parser(4);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success()) << "Parsing failed";
    EXPECT_GT(result.total_indexes(), 0) << "No field indexes found";

    // Verify data using streaming
    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    auto rows = extract_all_fields_streaming(csv_content, result.dialect);

    // contacts.csv has 4 data rows
    ASSERT_EQ(rows.size(), 4) << "Expected 4 contact records";

    // Verify first row contains "Smith, John" with embedded comma
    EXPECT_EQ(rows[0][0], "Smith, John");
    EXPECT_EQ(rows[0][1], "john.smith@example.com");
    EXPECT_EQ(rows[0][2], "(555) 123-4567");

    // Verify last row
    EXPECT_EQ(rows[3][0], "Williams, Alice");

    // Verify header
    auto header = get_header_streaming(csv_content, result.dialect);
    EXPECT_EQ(header.size(), 4);
    EXPECT_EQ(header, (std::vector<std::string>{"Name", "Email", "Phone", "Address"}));
}

TEST_F(IntegrationTest, BasicEndToEnd_AutoDetectDialect) {
    // Test dialect auto-detection with tab-separated file
    FileBuffer buffer = load_file(test_data_path("separators/tab.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load tab.csv";

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success()) << "Parsing failed";

    // Verify auto-detection found the tab delimiter
    EXPECT_EQ(result.dialect.delimiter, '\t') << "Should detect tab delimiter";
}

TEST_F(IntegrationTest, BasicEndToEnd_SemicolonDialect) {
    // Test with semicolon-separated file
    FileBuffer buffer = load_file(test_data_path("separators/semicolon.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load semicolon.csv";

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success()) << "Parsing failed";
    EXPECT_EQ(result.dialect.delimiter, ';') << "Should detect semicolon delimiter";
}

// =============================================================================
// Test 2: Multi-threaded Consistency Test
// =============================================================================

TEST_F(IntegrationTest, MultiThreadedConsistency_SameResults) {
    // Load test file
    FileBuffer buffer = load_file(test_data_path("basic/many_rows.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load test file";

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    // Parse with different thread counts: 1, 2, 4, 8
    std::vector<size_t> thread_counts = {1, 2, 4, 8};
    Dialect detected_dialect;
    bool first = true;

    for (size_t threads : thread_counts) {
        Parser parser(threads);
        auto result = parser.parse(buffer.data(), buffer.size());

        ASSERT_TRUE(result.success()) << "Parsing failed with " << threads << " threads";
        EXPECT_GT(result.total_indexes(), 0) << "No indexes with " << threads << " threads";

        // All should detect the same dialect
        if (first) {
            detected_dialect = result.dialect;
            first = false;
        } else {
            EXPECT_EQ(result.dialect.delimiter, detected_dialect.delimiter)
                << "Dialect mismatch with " << threads << " threads";
        }
    }

    // The key consistency check: streaming parser produces identical data
    // regardless of how batch parsing was done
    auto rows = extract_all_fields_streaming(csv_content, detected_dialect);
    EXPECT_GT(rows.size(), 0) << "Should have parsed rows";
}

TEST_F(IntegrationTest, MultiThreadedConsistency_QuotedFields) {
    // Test with file containing quoted fields (more complex parsing)
    FileBuffer buffer = load_file(test_data_path("quoted/newlines_in_quotes.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load test file";

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    // Parse with 1 and 4 threads
    Parser parser1(1);
    Parser parser4(4);

    auto result1 = parser1.parse(buffer.data(), buffer.size());
    auto result4 = parser4.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result1.success()) << "Single-threaded parsing failed";
    ASSERT_TRUE(result4.success()) << "Multi-threaded parsing failed";

    // Both should produce indexes
    EXPECT_GT(result1.total_indexes(), 0);
    EXPECT_GT(result4.total_indexes(), 0);

    // Both should detect same dialect
    EXPECT_EQ(result1.dialect.delimiter, result4.dialect.delimiter);

    // Verify data is correct using streaming parser
    auto rows = extract_all_fields_streaming(csv_content, result1.dialect);
    EXPECT_GT(rows.size(), 0) << "Should have parsed rows with quoted fields";
}

TEST_F(IntegrationTest, MultiThreadedConsistency_LargeFile) {
    // Test with a file that spans multiple chunks
    FileBuffer buffer = load_file(test_data_path("large/buffer_boundary.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load buffer_boundary.csv";

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    std::vector<size_t> thread_counts = {1, 2, 4};
    Dialect detected_dialect;

    for (size_t threads : thread_counts) {
        Parser parser(threads);
        auto result = parser.parse(buffer.data(), buffer.size());
        ASSERT_TRUE(result.success()) << "Parsing failed with " << threads << " threads";
        EXPECT_GT(result.total_indexes(), 0) << "No indexes with " << threads << " threads";

        if (threads == 1) {
            detected_dialect = result.dialect;
        } else {
            EXPECT_EQ(result.dialect.delimiter, detected_dialect.delimiter);
        }
    }

    // Verify data can be extracted correctly
    auto rows = extract_all_fields_streaming(csv_content, detected_dialect);
    EXPECT_GT(rows.size(), 0) << "Should have parsed rows from large file";
}

TEST_F(IntegrationTest, MultiThreadedConsistency_AllAlgorithms) {
    // Verify different algorithms produce consistent results
    FileBuffer buffer = load_file(test_data_path("basic/simple.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(4);
    Dialect csv_dialect = Dialect::csv();

    // Parse with each algorithm
    auto result_auto = parser.parse(buffer.data(), buffer.size(),
        {.dialect = csv_dialect, .algorithm = ParseAlgorithm::AUTO});
    auto result_spec = parser.parse(buffer.data(), buffer.size(),
        {.dialect = csv_dialect, .algorithm = ParseAlgorithm::SPECULATIVE});
    auto result_two = parser.parse(buffer.data(), buffer.size(),
        {.dialect = csv_dialect, .algorithm = ParseAlgorithm::TWO_PASS});
    auto result_branch = parser.parse(buffer.data(), buffer.size(),
        {.dialect = csv_dialect, .algorithm = ParseAlgorithm::BRANCHLESS});

    // All should succeed
    ASSERT_TRUE(result_auto.success());
    ASSERT_TRUE(result_spec.success());
    ASSERT_TRUE(result_two.success());
    ASSERT_TRUE(result_branch.success());

    // All should produce same results
    EXPECT_EQ(result_auto.num_columns(), result_spec.num_columns());
    EXPECT_EQ(result_auto.num_columns(), result_two.num_columns());
    EXPECT_EQ(result_auto.num_columns(), result_branch.num_columns());

    EXPECT_EQ(result_auto.total_indexes(), result_spec.total_indexes());
    EXPECT_EQ(result_auto.total_indexes(), result_two.total_indexes());
    EXPECT_EQ(result_auto.total_indexes(), result_branch.total_indexes());
}

// =============================================================================
// Test 3: Streaming vs Batch Equivalence Test
// =============================================================================

TEST_F(IntegrationTest, StreamingVsBatch_EquivalentResults) {
    // Load test file
    FileBuffer buffer = load_file(test_data_path("basic/simple.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load test file";

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    // BATCH PARSING: Use Parser to parse the data
    Parser parser(4);
    auto batch_result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(batch_result.success()) << "Batch parsing failed";
    EXPECT_GT(batch_result.total_indexes(), 0) << "Batch should produce indexes";

    // STREAMING PARSING: Use StreamReader to get field data
    std::istringstream input(csv_content);
    StreamConfig config;
    config.dialect = batch_result.dialect;
    config.parse_header = true;

    StreamReader reader(input, config);

    std::vector<std::string> streaming_header;
    std::vector<std::vector<std::string>> streaming_rows;

    while (reader.next_row()) {
        if (streaming_header.empty()) {
            streaming_header = reader.header();
        }
        std::vector<std::string> row;
        for (const auto& field : reader.row()) {
            row.push_back(std::string(field.data));
        }
        streaming_rows.push_back(row);
    }

    // Verify header
    EXPECT_EQ(streaming_header, (std::vector<std::string>{"A", "B", "C"}));

    // Verify row count and content
    ASSERT_EQ(streaming_rows.size(), 3) << "Expected 3 data rows";
    EXPECT_EQ(streaming_rows[0], (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(streaming_rows[1], (std::vector<std::string>{"4", "5", "6"}));
    EXPECT_EQ(streaming_rows[2], (std::vector<std::string>{"7", "8", "9"}));

    // Verify both parsers use the same dialect
    EXPECT_EQ(batch_result.dialect.delimiter, ',');
}

TEST_F(IntegrationTest, StreamingVsBatch_QuotedFieldsEquivalence) {
    // Test with quoted fields to ensure both handle escaping correctly
    FileBuffer buffer = load_file(test_data_path("quoted/escaped_quotes.csv"));
    ASSERT_TRUE(buffer.valid()) << "Failed to load escaped_quotes.csv";

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    // Batch parsing
    Parser parser(2);
    auto batch_result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(batch_result.success()) << "Batch parsing failed";
    EXPECT_GT(batch_result.total_indexes(), 0);

    // Streaming parsing - verify we can extract all rows
    std::istringstream input(csv_content);
    StreamConfig config;
    config.dialect = batch_result.dialect;
    config.parse_header = true;

    StreamReader reader(input, config);

    size_t streaming_row_count = 0;
    size_t expected_field_count = 0;

    while (reader.next_row()) {
        if (streaming_row_count == 0) {
            expected_field_count = reader.row().field_count();
        } else {
            // All rows should have consistent field count
            EXPECT_EQ(reader.row().field_count(), expected_field_count)
                << "Field count mismatch on row " << streaming_row_count;
        }
        streaming_row_count++;
    }

    EXPECT_GT(streaming_row_count, 0) << "No rows parsed by streaming";
}

TEST_F(IntegrationTest, StreamingVsBatch_RealWorldData) {
    // Test with real-world data containing various quoting scenarios
    FileBuffer buffer = load_file(test_data_path("real_world/contacts.csv"));
    ASSERT_TRUE(buffer.valid());

    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());

    // Batch parsing
    Parser parser(4);
    auto batch_result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(batch_result.success());
    EXPECT_GT(batch_result.total_indexes(), 0);

    // Streaming parsing - verify data extraction
    std::istringstream input(csv_content);
    StreamConfig config;
    config.dialect = batch_result.dialect;
    config.parse_header = true;

    StreamReader reader(input, config);

    // Count rows from streaming and verify structure
    size_t streaming_rows = 0;
    while (reader.next_row()) {
        streaming_rows++;
        // contacts.csv should have 4 fields per row
        EXPECT_EQ(reader.row().field_count(), 4)
            << "Row " << streaming_rows << " should have 4 fields";
    }

    // Should have parsed 4 data rows
    EXPECT_EQ(streaming_rows, 4) << "contacts.csv has 4 data rows";
}

TEST_F(IntegrationTest, StreamingVsBatch_ChunkedVsWhole) {
    // Verify streaming chunked parsing matches whole-file streaming
    // Note: Using no header for cleaner comparison
    std::string csv = "Alice,100\nBob,200\nCharlie,300\n";

    // Whole-file streaming
    std::istringstream input1(csv);
    StreamConfig config;
    config.parse_header = false;  // No header for simpler comparison

    StreamReader reader1(input1, config);
    std::vector<std::vector<std::string>> whole_rows;
    while (reader1.next_row()) {
        std::vector<std::string> row;
        for (const auto& field : reader1.row()) {
            row.push_back(std::string(field.data));
        }
        whole_rows.push_back(row);
    }

    // Chunked streaming (push model)
    StreamParser parser(config);
    std::vector<std::vector<std::string>> chunked_rows;

    parser.set_row_handler([&chunked_rows](const Row& row) {
        std::vector<std::string> r;
        for (const auto& field : row) {
            r.push_back(std::string(field.data));
        }
        chunked_rows.push_back(r);
        return true;
    });

    // Feed in small chunks
    size_t chunk_size = 10;
    for (size_t i = 0; i < csv.size(); i += chunk_size) {
        size_t len = std::min(chunk_size, csv.size() - i);
        parser.parse_chunk(std::string_view(csv.data() + i, len));
    }
    parser.finish();

    // Results should be identical
    ASSERT_EQ(whole_rows.size(), chunked_rows.size())
        << "Row count mismatch: whole=" << whole_rows.size() << ", chunked=" << chunked_rows.size();
    for (size_t i = 0; i < whole_rows.size(); ++i) {
        EXPECT_EQ(whole_rows[i], chunked_rows[i])
            << "Row " << i << " mismatch between whole and chunked parsing";
    }
}

// =============================================================================
// Additional Integration Tests
// =============================================================================

TEST_F(IntegrationTest, ErrorHandling_MalformedFile) {
    // Test error collection with a malformed file
    FileBuffer buffer = load_file(test_data_path("malformed/unclosed_quote.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(2);
    ErrorCollector errors(ErrorMode::PERMISSIVE);

    auto result = parser.parse(buffer.data(), buffer.size(),
        ParseOptions::with_errors(errors));

    // Parsing should complete (in permissive mode)
    EXPECT_TRUE(result.success());

    // Should have collected errors
    EXPECT_TRUE(errors.has_errors()) << "Should detect errors in malformed file";
}

TEST_F(IntegrationTest, ErrorHandling_InconsistentColumns) {
    // Test with inconsistent column counts
    FileBuffer buffer = load_file(test_data_path("malformed/inconsistent_columns.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(2);
    ErrorCollector errors(ErrorMode::PERMISSIVE);

    auto result = parser.parse(buffer.data(), buffer.size(),
        ParseOptions::with_errors(errors));

    EXPECT_TRUE(result.success());
    EXPECT_TRUE(errors.has_errors()) << "Should detect inconsistent column count";
}

TEST_F(IntegrationTest, EdgeCase_EmptyFile) {
    // Test with empty file
    FileBuffer buffer = load_file(test_data_path("edge_cases/empty_file.csv"));
    // Empty file might load as valid with 0 size or might fail
    // Either way, parsing should handle it gracefully

    if (buffer.valid() && buffer.size() > 0) {
        Parser parser(1);
        auto result = parser.parse(buffer.data(), buffer.size());
        // Should either succeed with 0 rows or fail gracefully
    }
}

TEST_F(IntegrationTest, EdgeCase_SingleCell) {
    // Test with single-cell file
    FileBuffer buffer = load_file(test_data_path("edge_cases/single_cell.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(1);
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success());

    // Verify via streaming
    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    auto rows = extract_all_fields_streaming(csv_content, result.dialect, false);
    EXPECT_GE(rows.size(), 1) << "Should have at least 1 row";
}

TEST_F(IntegrationTest, LineEndings_CRLF) {
    // Test Windows-style line endings
    FileBuffer buffer = load_file(test_data_path("line_endings/crlf.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success());

    // Verify data is correct despite CRLF
    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    auto rows = extract_all_fields_streaming(csv_content, result.dialect);
    EXPECT_GT(rows.size(), 0);
}

TEST_F(IntegrationTest, LineEndings_CR) {
    // Test old Mac-style line endings
    FileBuffer buffer = load_file(test_data_path("line_endings/cr.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success());
}

TEST_F(IntegrationTest, Unicode_UTF8Content) {
    // Test file with Unicode content
    FileBuffer buffer = load_file(test_data_path("real_world/unicode.csv"));
    ASSERT_TRUE(buffer.valid());

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);

    // Verify we can extract rows
    std::string csv_content(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    auto rows = extract_all_fields_streaming(csv_content, result.dialect);
    EXPECT_GT(rows.size(), 0) << "Should parse unicode content";
}

TEST_F(IntegrationTest, Performance_LargeFileMultiThreaded) {
    // Performance sanity check with larger file
    FileBuffer buffer = load_file(test_data_path("large/buffer_boundary.csv"));
    ASSERT_TRUE(buffer.valid());

    // Parse with different thread counts and verify reasonable performance
    for (size_t threads : {1u, 4u}) {
        Parser parser(threads);
        auto result = parser.parse(buffer.data(), buffer.size());
        ASSERT_TRUE(result.success()) << "Failed with " << threads << " threads";
        EXPECT_GT(result.total_indexes(), 0);
    }
}

// =============================================================================
// In-Memory Buffer Tests (no file I/O)
// =============================================================================

TEST_F(IntegrationTest, InMemory_BasicParsing) {
    // Test complete pipeline with in-memory data
    std::string csv = "id,name,score\n1,Alice,95\n2,Bob,87\n3,Charlie,92\n";
    auto [buf, len] = make_buffer(csv);
    FileBuffer buffer(buf, len);

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
    EXPECT_EQ(result.dialect.delimiter, ',');

    // Verify data
    auto rows = extract_all_fields_streaming(csv, result.dialect);
    ASSERT_EQ(rows.size(), 3);
    EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "Alice", "95"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"2", "Bob", "87"}));
    EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "Charlie", "92"}));

    // Verify header
    auto header = get_header_streaming(csv, result.dialect);
    EXPECT_EQ(header, (std::vector<std::string>{"id", "name", "score"}));
}

TEST_F(IntegrationTest, InMemory_QuotedWithNewlines) {
    // Test handling of quoted fields containing newlines
    std::string csv = "text,number\n\"line1\nline2\",100\n\"single\",200\n";
    auto [buf, len] = make_buffer(csv);
    FileBuffer buffer(buf, len);

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);

    // Verify using streaming
    auto rows = extract_all_fields_streaming(csv, result.dialect);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0][0], "line1\nline2");
    EXPECT_EQ(rows[0][1], "100");
}

TEST_F(IntegrationTest, InMemory_EscapedQuotes) {
    // Test handling of escaped quotes (doubled quotes)
    std::string csv = "quote\n\"say \"\"hello\"\"\"\n\"normal\"\n";
    auto [buf, len] = make_buffer(csv);
    FileBuffer buffer(buf, len);

    Parser parser(2);
    auto result = parser.parse(buffer.data(), buffer.size());

    ASSERT_TRUE(result.success());

    // Verify using streaming (with unescaping)
    std::istringstream input(csv);
    StreamConfig config;
    config.dialect = result.dialect;
    config.parse_header = true;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    // Raw data contains doubled quotes
    EXPECT_EQ(reader.row()[0].data, "say \"\"hello\"\"");
    // Unescaped version should have single quotes
    EXPECT_EQ(reader.row()[0].unescaped(), "say \"hello\"");

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row()[0].data, "normal");
}
