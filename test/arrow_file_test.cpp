/**
 * @file arrow_file_test.cpp
 * @brief Arrow conversion tests using real CSV test files.
 *
 * These tests load actual CSV files from test/data/ and convert them to Arrow
 * tables, validating that the conversion produces expected results. This covers
 * real-world scenarios that may not be adequately tested with inline CSV strings.
 *
 * Issue #86: Add Arrow conversion tests using real CSV test data
 */

#ifdef LIBVROOM_ENABLE_ARROW
#include <gtest/gtest.h>
#include <arrow/api.h>
#include "arrow_output.h"
#include "io_util.h"
#include "dialect.h"
#include <string>

namespace libvroom {

class ArrowFileTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& category, const std::string& filename) {
        return "test/data/" + category + "/" + filename;
    }
};

// ============================================================================
// REAL WORLD CSV FILES
// ============================================================================

TEST_F(ArrowFileTest, RealWorldContacts) {
    // contacts.csv: Quoted fields with embedded commas
    std::string path = getTestDataPath("real_world", "contacts.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 4);  // Name,Email,Phone,Address
    EXPECT_EQ(result.num_rows, 4);

    // Verify column names
    EXPECT_EQ(result.schema->field(0)->name(), "Name");
    EXPECT_EQ(result.schema->field(1)->name(), "Email");
    EXPECT_EQ(result.schema->field(2)->name(), "Phone");
    EXPECT_EQ(result.schema->field(3)->name(), "Address");

    // All columns should be STRING type
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(result.schema->field(i)->type()->id(), arrow::Type::STRING)
            << "Column " << i << " should be STRING";
    }
}

TEST_F(ArrowFileTest, RealWorldFinancial) {
    // financial.csv: Date column and numeric data
    std::string path = getTestDataPath("real_world", "financial.csv");
    ArrowConvertOptions opts;
    opts.infer_types = true;

    auto result = csv_to_arrow(path, opts);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 6);  // Date,Open,High,Low,Close,Volume
    EXPECT_EQ(result.num_rows, 5);

    // Verify column names
    EXPECT_EQ(result.schema->field(0)->name(), "Date");
    EXPECT_EQ(result.schema->field(1)->name(), "Open");
    EXPECT_EQ(result.schema->field(5)->name(), "Volume");

    // Date is STRING, numeric columns should be DOUBLE or INT64
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);

    // Open,High,Low,Close are doubles (e.g., 100.50)
    EXPECT_EQ(result.schema->field(1)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(result.schema->field(2)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(result.schema->field(3)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(result.schema->field(4)->type()->id(), arrow::Type::DOUBLE);

    // Volume is integer (e.g., 1000000)
    EXPECT_EQ(result.schema->field(5)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowFileTest, RealWorldProductCatalog) {
    // product_catalog.csv: Escaped quotes and newlines in quoted fields
    std::string path = getTestDataPath("real_world", "product_catalog.csv");
    ArrowConvertOptions opts;
    opts.infer_types = true;

    auto result = csv_to_arrow(path, opts);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 6);  // SKU,Name,Category,Price,Stock,Description
    EXPECT_EQ(result.num_rows, 5);

    // SKU is STRING, Price is DOUBLE, Stock is INT64
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);  // SKU
    EXPECT_EQ(result.schema->field(3)->type()->id(), arrow::Type::DOUBLE);  // Price
    EXPECT_EQ(result.schema->field(4)->type()->id(), arrow::Type::INT64);   // Stock
}

TEST_F(ArrowFileTest, RealWorldUnicode) {
    // unicode.csv: International characters (UTF-8)
    std::string path = getTestDataPath("real_world", "unicode.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 4);  // Name,City,Country,Description
    EXPECT_EQ(result.num_rows, 5);     // José, 山田, Müller, Αλέξανδρος, Владимир

    // All columns should be STRING type
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(result.schema->field(i)->type()->id(), arrow::Type::STRING);
    }
}

// ============================================================================
// QUOTED FIELD TESTS
// ============================================================================

TEST_F(ArrowFileTest, QuotedEmbeddedSeparators) {
    // embedded_separators.csv: Quoted fields containing commas
    std::string path = getTestDataPath("quoted", "embedded_separators.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A,B,C
    EXPECT_EQ(result.num_rows, 3);

    // Verify table structure is correct despite embedded commas
    EXPECT_TRUE(result.table != nullptr);
}

TEST_F(ArrowFileTest, QuotedEscapedQuotes) {
    // escaped_quotes.csv: Double-quote escaping
    std::string path = getTestDataPath("quoted", "escaped_quotes.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 2);  // Text,Description
    EXPECT_EQ(result.num_rows, 5);
}

TEST_F(ArrowFileTest, QuotedNewlinesInQuotes) {
    // newlines_in_quotes.csv: Newlines inside quoted fields
    std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A,B,C
    EXPECT_EQ(result.num_rows, 3);
}

TEST_F(ArrowFileTest, QuotedMixed) {
    // mixed_quoted.csv: Mix of quoted and unquoted fields (4 columns, 4 rows)
    std::string path = getTestDataPath("quoted", "mixed_quoted.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 4);  // ID,Name,Value,Description
    EXPECT_EQ(result.num_rows, 4);
}

// ============================================================================
// SEPARATOR DIALECT TESTS
// ============================================================================

TEST_F(ArrowFileTest, SeparatorSemicolon) {
    // semicolon.csv: European-style semicolon separator
    std::string path = getTestDataPath("separators", "semicolon.csv");
    auto result = csv_to_arrow(path, ArrowConvertOptions(), Dialect::semicolon());

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A;B;C
    EXPECT_EQ(result.num_rows, 3);

    // Verify column names are correctly parsed with semicolon delimiter
    EXPECT_EQ(result.schema->field(0)->name(), "A");
    EXPECT_EQ(result.schema->field(1)->name(), "B");
    EXPECT_EQ(result.schema->field(2)->name(), "C");
}

TEST_F(ArrowFileTest, SeparatorTab) {
    // tab.csv: Tab-separated values
    std::string path = getTestDataPath("separators", "tab.csv");
    auto result = csv_to_arrow(path, ArrowConvertOptions(), Dialect::tsv());

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A\tB\tC
    EXPECT_EQ(result.num_rows, 3);
}

TEST_F(ArrowFileTest, SeparatorPipe) {
    // pipe.csv: Pipe-separated values
    std::string path = getTestDataPath("separators", "pipe.csv");
    auto result = csv_to_arrow(path, ArrowConvertOptions(), Dialect::pipe());

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A|B|C
    EXPECT_EQ(result.num_rows, 3);
}

// ============================================================================
// LINE ENDING TESTS
// ============================================================================

TEST_F(ArrowFileTest, LineEndingCRLF) {
    // crlf.csv: Windows-style line endings
    std::string path = getTestDataPath("line_endings", "crlf.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowFileTest, LineEndingLF) {
    // lf.csv: Unix-style line endings (A,B,C with 2 data rows)
    std::string path = getTestDataPath("line_endings", "lf.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowFileTest, LineEndingCR) {
    // cr.csv: Classic Mac-style line endings (CR only)
    // This is a tricky format that may be parsed differently
    std::string path = getTestDataPath("line_endings", "cr.csv");
    auto result = csv_to_arrow(path);

    // Just verify it can be loaded without error
    // CR-only line endings may result in different parsing
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_GE(result.num_columns, 1);
}

TEST_F(ArrowFileTest, LineEndingNoFinalNewline) {
    // no_final_newline.csv: File not ending with newline (2 data rows)
    std::string path = getTestDataPath("line_endings", "no_final_newline.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 2);
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

TEST_F(ArrowFileTest, EdgeCaseEmptyFields) {
    // empty_fields.csv: CSV with empty fields
    std::string path = getTestDataPath("edge_cases", "empty_fields.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A,B,C
    EXPECT_EQ(result.num_rows, 4);

    // Check that empty fields result in nulls
    auto col_b = result.table->column(1);
    EXPECT_GT(col_b->null_count(), 0) << "Empty fields should produce null values";
}

TEST_F(ArrowFileTest, EdgeCaseSingleRowHeaderOnly) {
    // single_row_header_only.csv: Header (A,B,C) with no data rows
    std::string path = getTestDataPath("edge_cases", "single_row_header_only.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 0);
}

TEST_F(ArrowFileTest, EdgeCaseSingleCell) {
    // single_cell.csv: Minimal CSV with single header "Value" and no data
    std::string path = getTestDataPath("edge_cases", "single_cell.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 1);
    EXPECT_EQ(result.num_rows, 0);
}

TEST_F(ArrowFileTest, EdgeCaseWhitespaceFields) {
    // whitespace_fields.csv: Fields with leading/trailing whitespace (3 rows)
    std::string path = getTestDataPath("edge_cases", "whitespace_fields.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 3);
}

// ============================================================================
// BASIC CSV FILES
// ============================================================================

TEST_F(ArrowFileTest, BasicSimple) {
    // simple.csv: Basic 3x3 CSV (header + 3 data rows)
    std::string path = getTestDataPath("basic", "simple.csv");
    ArrowConvertOptions opts;
    opts.infer_types = true;

    auto result = csv_to_arrow(path, opts);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);  // A,B,C
    EXPECT_EQ(result.num_rows, 3);     // 3 data rows
}

TEST_F(ArrowFileTest, BasicSingleColumn) {
    // single_column.csv: Single column CSV (Value column, 5 data rows)
    std::string path = getTestDataPath("basic", "single_column.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 1);
    EXPECT_EQ(result.num_rows, 5);
}

TEST_F(ArrowFileTest, BasicWideColumns) {
    // wide_columns.csv: 20 columns (C1-C20), 3 data rows
    std::string path = getTestDataPath("basic", "wide_columns.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 20);  // C1 through C20
    EXPECT_EQ(result.num_rows, 3);
}

TEST_F(ArrowFileTest, BasicManyRows) {
    // many_rows.csv: 3 columns (ID,Value,Label), 20 data rows
    std::string path = getTestDataPath("basic", "many_rows.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 20);
}

// ============================================================================
// TYPE INFERENCE WITH REAL DATA
// ============================================================================

TEST_F(ArrowFileTest, TypeInferenceFinancialData) {
    // Test type inference on financial.csv with custom options
    std::string path = getTestDataPath("real_world", "financial.csv");

    // Without type inference - all strings
    ArrowConvertOptions opts_no_infer;
    opts_no_infer.infer_types = false;
    auto result_string = csv_to_arrow(path, opts_no_infer);

    ASSERT_TRUE(result_string.ok()) << result_string.error_message;
    for (int i = 0; i < result_string.num_columns; i++) {
        EXPECT_EQ(result_string.schema->field(i)->type()->id(), arrow::Type::STRING)
            << "Without inference, all columns should be STRING";
    }

    // With type inference - proper types
    ArrowConvertOptions opts_infer;
    opts_infer.infer_types = true;
    auto result_typed = csv_to_arrow(path, opts_infer);

    ASSERT_TRUE(result_typed.ok()) << result_typed.error_message;
    // Volume should be INT64, prices should be DOUBLE
    EXPECT_EQ(result_typed.schema->field(5)->type()->id(), arrow::Type::INT64);  // Volume
    EXPECT_EQ(result_typed.schema->field(1)->type()->id(), arrow::Type::DOUBLE); // Open
}

// ============================================================================
// ARROW TABLE VALIDATION
// ============================================================================

TEST_F(ArrowFileTest, ValidateTableStructure) {
    // Comprehensive validation of Arrow table structure
    std::string path = getTestDataPath("real_world", "contacts.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;
    ASSERT_NE(result.table, nullptr);
    ASSERT_NE(result.schema, nullptr);

    // Table and schema should match
    EXPECT_EQ(result.table->schema()->num_fields(), result.schema->num_fields());
    EXPECT_EQ(result.table->num_columns(), result.num_columns);
    EXPECT_EQ(result.table->num_rows(), result.num_rows);

    // Each column should have the correct number of rows
    for (int64_t i = 0; i < result.table->num_columns(); i++) {
        auto col = result.table->column(i);
        EXPECT_EQ(col->length(), result.num_rows)
            << "Column " << i << " has incorrect length";
    }
}

TEST_F(ArrowFileTest, ValidateColumnChunking) {
    // Ensure columns are properly chunked
    std::string path = getTestDataPath("basic", "many_rows.csv");
    auto result = csv_to_arrow(path);

    ASSERT_TRUE(result.ok()) << result.error_message;

    for (int64_t i = 0; i < result.table->num_columns(); i++) {
        auto col = result.table->column(i);
        EXPECT_GE(col->num_chunks(), 1) << "Column " << i << " should have at least one chunk";

        // Total length across chunks should match
        int64_t total_length = 0;
        for (int j = 0; j < col->num_chunks(); j++) {
            total_length += col->chunk(j)->length();
        }
        EXPECT_EQ(total_length, result.num_rows);
    }
}

}  // namespace libvroom

#else
#include <gtest/gtest.h>
TEST(ArrowFileTest, ArrowNotEnabled) { GTEST_SKIP() << "Arrow not enabled"; }
#endif
