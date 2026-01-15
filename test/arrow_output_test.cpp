#ifdef LIBVROOM_ENABLE_ARROW
#include "arrow_output.h"
#include "io_util.h"
#include "mem_util.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <cstring>
#include <gtest/gtest.h>

#ifdef LIBVROOM_ENABLE_PARQUET
#include <parquet/arrow/reader.h>
#endif

namespace libvroom {

struct TestBuffer {
  uint8_t* data;
  size_t len;
  explicit TestBuffer(const std::string& content) {
    len = content.size();
    data = allocate_padded_buffer(len, 64);
    std::memcpy(data, content.data(), len);
  }
  ~TestBuffer() {
    if (data)
      aligned_free(data);
  }
  TestBuffer(const TestBuffer&) = delete;
  TestBuffer& operator=(const TestBuffer&) = delete;
};

class ArrowOutputTest : public ::testing::Test {
protected:
  ArrowConvertResult parseAndConvert(const std::string& csv,
                                     const ArrowConvertOptions& opts = ArrowConvertOptions()) {
    TestBuffer buf(csv);
    two_pass parser;
    index idx = parser.init(buf.len, 1);
    parser.parse(buf.data, idx, buf.len);
    ArrowConverter converter(opts);
    return converter.convert(buf.data, buf.len, idx);
  }
};

TEST_F(ArrowOutputTest, BasicConversion) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 2);
  EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, TypeInferenceInteger) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("id,count\n1,100\n2,200\n", opts);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, TypeInferenceDouble) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n1.5\n2.7\n", opts);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, TypeInferenceBoolean) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("flag\ntrue\nfalse\n", opts);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

TEST_F(ArrowOutputTest, ColumnTypeHelpers) {
  EXPECT_EQ(column_type_to_arrow(ColumnType::STRING)->id(), arrow::Type::STRING);
  EXPECT_EQ(column_type_to_arrow(ColumnType::INT64)->id(), arrow::Type::INT64);
  EXPECT_STREQ(column_type_to_string(ColumnType::STRING), "STRING");
}

// Null value tests
TEST_F(ArrowOutputTest, NullValues) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("id,value\n1,NA\n2,\n3,NULL\n4,100\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_rows, 4);
  // The value column should have nulls
  auto col = result.table->column(1);
  EXPECT_EQ(col->null_count(), 3); // NA, empty, and NULL are all null values
}

TEST_F(ArrowOutputTest, NullValueCustom) {
  ArrowConvertOptions opts;
  opts.null_values = {"MISSING", "-999"};
  opts.infer_types = true;
  auto result = parseAndConvert("id,value\n1,MISSING\n2,-999\n3,100\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  auto col = result.table->column(1);
  EXPECT_EQ(col->null_count(), 2);
}

// Boolean tests
TEST_F(ArrowOutputTest, BooleanCaseInsensitive) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("flag\nTRUE\ntrue\nTrue\nFALSE\nfalse\nFalse\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
  EXPECT_EQ(result.num_rows, 6);
}

TEST_F(ArrowOutputTest, BooleanNumeric) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("flag\n1\n0\n1\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

TEST_F(ArrowOutputTest, BooleanYesNo) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("flag\nyes\nno\nYES\nNO\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

// Boolean type promotion tests (Issue #176)
// These tests explicitly verify type promotion rules when boolean-like values
// (0, 1) appear alongside other numeric values.
TEST_F(ArrowOutputTest, BooleanIntPromotion) {
  // When "0" and "1" (which could be boolean) appear with other integers,
  // the column should be promoted to INT64
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n0\n1\n42\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, BooleanDoublePromotion) {
  // When "0" and "1" (which could be boolean) appear with doubles,
  // the column should be promoted to DOUBLE
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n1\n0\n3.14\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

// Bidirectional type promotion tests (Issue #251)
// These tests verify that type promotion works correctly regardless of value order.
TEST_F(ArrowOutputTest, BooleanIntPromotionReverse) {
  // Integer first, then boolean-like values - should still promote to INT64
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n42\n0\n1\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, BooleanToIntToDoubleChain) {
  // Three-way promotion chain: BOOLEAN -> INT64 -> DOUBLE
  // Values that could be boolean (0, 1), then integer (42), then double (3.14)
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n0\n1\n42\n3.14\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, MultipleBooleanWithInt) {
  // Multiple boolean-like values (0, 1) repeated, then an integer
  // Should promote to INT64 regardless of boolean repetition count
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n0\n1\n0\n1\n42\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, DoubleFirstThenBoolean) {
  // Double value first, then boolean-like values - should be DOUBLE
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n3.14\n0\n1\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

// Edge case tests
TEST_F(ArrowOutputTest, SingleColumn) {
  auto result = parseAndConvert("name\nAlice\nBob\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 1);
  EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, SingleRow) {
  auto result = parseAndConvert("a,b,c\n1,2,3\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, EmptyFields) {
  ArrowConvertOptions opts;
  opts.infer_types = false; // Treat all as strings
  auto result = parseAndConvert("a,b,c\n,,\n1,,3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 2);
}

// Type inference edge cases
TEST_F(ArrowOutputTest, MixedIntDouble) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n1\n2.5\n3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  // Mixed int/double should promote to DOUBLE
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, MixedTypesToString) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  // Mix of numbers and text should become STRING
  auto result = parseAndConvert("value\n1\nhello\n3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);
}

// Quoted field tests
TEST_F(ArrowOutputTest, QuotedFields) {
  auto result = parseAndConvert("name,address\n\"John Doe\",\"123 Main St\"\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 2);
  EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, QuotedWithCommas) {
  auto result = parseAndConvert("a,b,c\n1,\"A,B,C\",2\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 1);
}

// Special double values
TEST_F(ArrowOutputTest, SpecialDoubleValues) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\ninf\n-inf\nnan\n1.5\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
  EXPECT_EQ(result.num_rows, 4);
}

// Large integer test
TEST_F(ArrowOutputTest, LargeIntegers) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("id\n9223372036854775807\n-9223372036854775808\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

// Column name inference
TEST_F(ArrowOutputTest, AutoGeneratedColumnNames) {
  // When no header is properly parsed or columns exceed header count
  auto result = parseAndConvert("a,b\n1,2,3\n"); // Extra column in data
  ASSERT_TRUE(result.ok()) << result.error_message;
  // Should still work with auto-generated names for extra columns
  EXPECT_GE(result.num_columns, 2);
}

// Disable type inference
TEST_F(ArrowOutputTest, NoTypeInference) {
  ArrowConvertOptions opts;
  opts.infer_types = false;
  auto result = parseAndConvert("id,value\n1,100\n2,200\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  // All columns should be STRING when type inference is disabled
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);
  EXPECT_EQ(result.schema->field(1)->type()->id(), arrow::Type::STRING);
}

// Whitespace handling
TEST_F(ArrowOutputTest, WhitespaceInNumbers) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("value\n  42  \n  3.14  \n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  // Should still parse numbers with leading/trailing whitespace
  EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

// Bounds validation tests (Issue #85)
// These tests verify that extract_field handles edge cases safely
TEST_F(ArrowOutputTest, FieldRangeStartEqualsEnd) {
  // When start == end, should return empty string_view without crashing
  auto result = parseAndConvert("a,b,c\n,,\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  // Empty fields are handled gracefully
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, ConsecutiveDelimiters) {
  // Tests multiple consecutive delimiters creating zero-length fields
  auto result = parseAndConvert("a,b,c\n1,,3\n,2,\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 2);
}

// Error handling - empty data
TEST_F(ArrowOutputTest, EmptyData) {
  auto result = parseAndConvert("");
  EXPECT_FALSE(result.ok());
}

TEST_F(ArrowOutputTest, HeaderOnly) {
  auto result = parseAndConvert("a,b,c\n");
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_rows, 0);
}

// Security limit tests
TEST_F(ArrowOutputTest, MaxColumnsLimit) {
  ArrowConvertOptions opts;
  opts.max_columns = 2; // Only allow 2 columns
  auto result = parseAndConvert("a,b,c\n1,2,3\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Column count") != std::string::npos);
  EXPECT_TRUE(result.error_message.find("exceeds maximum") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxColumnsLimitAllowed) {
  ArrowConvertOptions opts;
  opts.max_columns = 3; // Allow exactly 3 columns
  auto result = parseAndConvert("a,b,c\n1,2,3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
}

TEST_F(ArrowOutputTest, MaxColumnsUnlimited) {
  ArrowConvertOptions opts;
  opts.max_columns = 0; // Unlimited columns
  auto result = parseAndConvert("a,b,c,d,e\n1,2,3,4,5\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 5);
}

TEST_F(ArrowOutputTest, MaxRowsLimit) {
  ArrowConvertOptions opts;
  opts.max_rows = 2; // Only allow 2 rows
  auto result = parseAndConvert("a,b\n1,2\n3,4\n5,6\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Row count") != std::string::npos);
  EXPECT_TRUE(result.error_message.find("exceeds maximum") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxRowsLimitAllowed) {
  ArrowConvertOptions opts;
  opts.max_rows = 2; // Allow exactly 2 rows
  auto result = parseAndConvert("a,b\n1,2\n3,4\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, MaxRowsDefaultUnlimited) {
  ArrowConvertOptions opts;
  // Default max_rows is 0 (unlimited)
  EXPECT_EQ(opts.max_rows, 0U);
  auto result = parseAndConvert("a\n1\n2\n3\n4\n5\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_rows, 5);
}

TEST_F(ArrowOutputTest, DefaultMaxColumns) {
  ArrowConvertOptions opts;
  // Default max_columns is 10000
  EXPECT_EQ(opts.max_columns, 10000U);
}

TEST_F(ArrowOutputTest, TypeInferenceRowsExceedsMax) {
  ArrowConvertOptions opts;
  opts.type_inference_rows = ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS + 1;
  // Constructor should throw when type_inference_rows exceeds maximum
  EXPECT_THROW(ArrowConverter converter(opts), std::invalid_argument);
}

TEST_F(ArrowOutputTest, TypeInferenceRowsAtMax) {
  ArrowConvertOptions opts;
  opts.type_inference_rows = ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS;
  // Should not throw when exactly at maximum
  EXPECT_NO_THROW(ArrowConverter converter(opts));
}

TEST_F(ArrowOutputTest, TypeInferenceRowsNormalValue) {
  ArrowConvertOptions opts;
  opts.type_inference_rows = 500; // A normal value within limits
  auto result = parseAndConvert("a\n1\n2\n3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
}

// Total cell count limit tests (Issue #91)
TEST_F(ArrowOutputTest, MaxTotalCellsLimit) {
  ArrowConvertOptions opts;
  opts.max_total_cells = 5; // 3 columns × 2 rows = 6 cells exceeds limit
  auto result = parseAndConvert("a,b,c\n1,2,3\n4,5,6\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Total cell count") != std::string::npos);
  EXPECT_TRUE(result.error_message.find("exceeds maximum") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxTotalCellsLimitAllowed) {
  ArrowConvertOptions opts;
  opts.max_total_cells = 6; // 3 columns × 2 rows = 6 cells exactly at limit
  auto result = parseAndConvert("a,b,c\n1,2,3\n4,5,6\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 3);
  EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, MaxTotalCellsUnlimited) {
  ArrowConvertOptions opts;
  opts.max_total_cells = 0; // Unlimited cells
  auto result = parseAndConvert("a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 5);
  EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, DefaultMaxTotalCells) {
  ArrowConvertOptions opts;
  // Default max_total_cells is 100M
  EXPECT_EQ(opts.max_total_cells, 100000000U);
}

TEST_F(ArrowOutputTest, MaxTotalCellsWithLargeColumnsSmallRows) {
  // Tests that high column × low row count is caught
  ArrowConvertOptions opts;
  opts.max_columns = 0;      // Disable column limit for this test
  opts.max_total_cells = 10; // Only allow 10 total cells
  // 5 columns × 3 rows = 15 cells > 10
  auto result = parseAndConvert("a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n11,12,13,14,15\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Total cell count") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxTotalCellsWithSmallColumnsLargeRows) {
  // Tests that low column × high row count is caught
  ArrowConvertOptions opts;
  opts.max_total_cells = 5; // Only allow 5 total cells
  // 2 columns × 4 rows = 8 cells > 5
  auto result = parseAndConvert("a,b\n1,2\n3,4\n5,6\n7,8\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Total cell count") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxTotalCellsInteractionWithColumnLimit) {
  // Both column limit and total cell limit are enforced
  ArrowConvertOptions opts;
  opts.max_columns = 2;       // Only allow 2 columns
  opts.max_total_cells = 100; // Plenty of cell room
  // 3 columns should fail on column limit first
  auto result = parseAndConvert("a,b,c\n1,2,3\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Column count") != std::string::npos);
}

TEST_F(ArrowOutputTest, MaxTotalCellsInteractionWithRowLimit) {
  // Both row limit and total cell limit are enforced
  ArrowConvertOptions opts;
  opts.max_rows = 2;          // Only allow 2 rows
  opts.max_total_cells = 100; // Plenty of cell room
  // 3 rows should fail on row limit first
  auto result = parseAndConvert("a,b\n1,2\n3,4\n5,6\n", opts);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.error_message.find("Row count") != std::string::npos);
}

// Memory conversion function test
TEST_F(ArrowOutputTest, FromMemoryConversion) {
  std::string csv = "name,age\nAlice,30\nBob,25\n";
  auto result = csv_to_arrow_from_memory(reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
  ASSERT_TRUE(result.ok()) << result.error_message;
  EXPECT_EQ(result.num_columns, 2);
  EXPECT_EQ(result.num_rows, 2);
}

// =============================================================================
// Columnar Format Export Tests (Parquet/Feather)
// =============================================================================

TEST_F(ArrowOutputTest, DetectFormatFromExtensionParquet) {
  EXPECT_EQ(detect_format_from_extension("data.parquet"), ColumnarFormat::PARQUET);
  EXPECT_EQ(detect_format_from_extension("data.pq"), ColumnarFormat::PARQUET);
  EXPECT_EQ(detect_format_from_extension("/path/to/file.parquet"), ColumnarFormat::PARQUET);
  EXPECT_EQ(detect_format_from_extension("data.PARQUET"), ColumnarFormat::PARQUET);
}

TEST_F(ArrowOutputTest, DetectFormatFromExtensionFeather) {
  EXPECT_EQ(detect_format_from_extension("data.feather"), ColumnarFormat::FEATHER);
  EXPECT_EQ(detect_format_from_extension("data.arrow"), ColumnarFormat::FEATHER);
  EXPECT_EQ(detect_format_from_extension("data.ipc"), ColumnarFormat::FEATHER);
  EXPECT_EQ(detect_format_from_extension("/path/to/file.FEATHER"), ColumnarFormat::FEATHER);
}

TEST_F(ArrowOutputTest, DetectFormatFromExtensionUnknown) {
  EXPECT_EQ(detect_format_from_extension("data.csv"), ColumnarFormat::AUTO);
  EXPECT_EQ(detect_format_from_extension("data.txt"), ColumnarFormat::AUTO);
  EXPECT_EQ(detect_format_from_extension("data"), ColumnarFormat::AUTO);
  EXPECT_EQ(detect_format_from_extension(""), ColumnarFormat::AUTO);
  EXPECT_EQ(detect_format_from_extension("data."), ColumnarFormat::AUTO);
}

TEST_F(ArrowOutputTest, WriteFeatherBasic) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok()) << result.error_message;

  // Write to temporary file
  std::string tmp_path = "/tmp/test_output_basic.feather";
  auto write_result = write_feather(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;
  EXPECT_GT(write_result.bytes_written, 0);

  // Clean up
  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteFeatherWithTypes) {
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("id,value,flag\n1,1.5,true\n2,2.5,false\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;

  std::string tmp_path = "/tmp/test_output_types.feather";
  auto write_result = write_feather(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteFeatherNullTable) {
  std::shared_ptr<arrow::Table> null_table;
  auto write_result = write_feather(null_table, "/tmp/null_table.feather");
  EXPECT_FALSE(write_result.ok());
  EXPECT_TRUE(write_result.error_message.find("null") != std::string::npos ||
              write_result.error_message.find("Table") != std::string::npos);
}

TEST_F(ArrowOutputTest, WriteFeatherInvalidPath) {
  auto result = parseAndConvert("a,b\n1,2\n");
  ASSERT_TRUE(result.ok());

  // Try to write to invalid path
  auto write_result = write_feather(result.table, "/nonexistent/directory/file.feather");
  EXPECT_FALSE(write_result.ok());
}

#ifdef LIBVROOM_ENABLE_PARQUET

TEST_F(ArrowOutputTest, WriteParquetBasic) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok()) << result.error_message;

  std::string tmp_path = "/tmp/test_output_basic.parquet";
  auto write_result = write_parquet(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;
  EXPECT_GT(write_result.bytes_written, 0);

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteParquetWithCompression) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok());

  std::string tmp_path = "/tmp/test_output_compressed.parquet";

  // Test different compression codecs
  ParquetWriteOptions opts;

  opts.compression = ParquetWriteOptions::Compression::SNAPPY;
  auto write_result = write_parquet(result.table, tmp_path, opts);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;
  int64_t snappy_size = write_result.bytes_written;

  opts.compression = ParquetWriteOptions::Compression::UNCOMPRESSED;
  write_result = write_parquet(result.table, tmp_path, opts);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;
  int64_t uncompressed_size = write_result.bytes_written;

  // Uncompressed should generally be larger or equal
  EXPECT_GE(uncompressed_size, snappy_size);

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteParquetZstd) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok());

  std::string tmp_path = "/tmp/test_output_zstd.parquet";
  ParquetWriteOptions opts;
  opts.compression = ParquetWriteOptions::Compression::ZSTD;

  auto write_result = write_parquet(result.table, tmp_path, opts);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteParquetGzip) {
  auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
  ASSERT_TRUE(result.ok());

  std::string tmp_path = "/tmp/test_output_gzip.parquet";
  ParquetWriteOptions opts;
  opts.compression = ParquetWriteOptions::Compression::GZIP;

  auto write_result = write_parquet(result.table, tmp_path, opts);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteParquetNullTable) {
  std::shared_ptr<arrow::Table> null_table;
  auto write_result = write_parquet(null_table, "/tmp/null_table.parquet");
  EXPECT_FALSE(write_result.ok());
}

TEST_F(ArrowOutputTest, CsvToParquetDirect) {
  // Create a temp CSV file
  std::string csv_path = "/tmp/test_input.csv";
  std::string parquet_path = "/tmp/test_output.parquet";

  // Write test CSV
  FILE* f = fopen(csv_path.c_str(), "w");
  ASSERT_NE(f, nullptr);
  fprintf(f, "name,age\nAlice,30\nBob,25\n");
  fclose(f);

  auto write_result = csv_to_parquet(csv_path, parquet_path);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;
  EXPECT_GT(write_result.bytes_written, 0);

  std::remove(csv_path.c_str());
  std::remove(parquet_path.c_str());
}

#endif // LIBVROOM_ENABLE_PARQUET

TEST_F(ArrowOutputTest, CsvToFeatherDirect) {
  // Create a temp CSV file
  std::string csv_path = "/tmp/test_input.csv";
  std::string feather_path = "/tmp/test_output.feather";

  // Write test CSV
  FILE* f = fopen(csv_path.c_str(), "w");
  ASSERT_NE(f, nullptr);
  fprintf(f, "name,age\nAlice,30\nBob,25\n");
  fclose(f);

  auto write_result = csv_to_feather(csv_path, feather_path);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;
  EXPECT_GT(write_result.bytes_written, 0);

  std::remove(csv_path.c_str());
  std::remove(feather_path.c_str());
}

TEST_F(ArrowOutputTest, WriteColumnarAutoDetectParquet) {
  auto result = parseAndConvert("a,b\n1,2\n");
  ASSERT_TRUE(result.ok());

  std::string tmp_path = "/tmp/test_auto.parquet";
  auto write_result = write_columnar(result.table, tmp_path);

#ifdef LIBVROOM_ENABLE_PARQUET
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;
#else
  // Without Parquet support, should fail with appropriate message
  EXPECT_FALSE(write_result.ok());
  EXPECT_TRUE(write_result.error_message.find("not available") != std::string::npos);
#endif

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteColumnarAutoDetectFeather) {
  auto result = parseAndConvert("a,b\n1,2\n");
  ASSERT_TRUE(result.ok());

  std::string tmp_path = "/tmp/test_auto.feather";
  auto write_result = write_columnar(result.table, tmp_path);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, WriteColumnarExplicitFormat) {
  auto result = parseAndConvert("a,b\n1,2\n");
  ASSERT_TRUE(result.ok());

  // Test that explicit format parameter overrides auto-detection from extension
  // This verifies the format parameter takes precedence when specified
  std::string tmp_path = "/tmp/test_explicit.feather";
  auto write_result = write_columnar(result.table, tmp_path, ColumnarFormat::FEATHER);
  EXPECT_TRUE(write_result.ok()) << write_result.error_message;

  std::remove(tmp_path.c_str());
}

// =============================================================================
// Round-Trip Tests - Write and Read Back
// =============================================================================

TEST_F(ArrowOutputTest, RoundTripFeather) {
  // Parse CSV to Arrow table
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("name,age,score\nAlice,30,95.5\nBob,25,87.3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;
  ASSERT_EQ(result.num_rows, 2);
  ASSERT_EQ(result.num_columns, 3);

  // Write to Feather
  std::string tmp_path = "/tmp/test_roundtrip.feather";
  auto write_result = write_feather(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;

  // Read back using Arrow IPC reader
  auto input_result = arrow::io::ReadableFile::Open(tmp_path);
  ASSERT_TRUE(input_result.ok()) << input_result.status().ToString();
  auto input_file = *input_result;

  auto reader_result = arrow::ipc::RecordBatchFileReader::Open(input_file);
  ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
  auto reader = *reader_result;

  // Verify schema
  auto read_schema = reader->schema();
  EXPECT_EQ(read_schema->num_fields(), 3);
  EXPECT_EQ(read_schema->field(0)->name(), "name");
  EXPECT_EQ(read_schema->field(1)->name(), "age");
  EXPECT_EQ(read_schema->field(2)->name(), "score");

  // Verify row count
  int64_t total_rows = 0;
  for (int i = 0; i < reader->num_record_batches(); ++i) {
    auto batch_result = reader->ReadRecordBatch(i);
    ASSERT_TRUE(batch_result.ok());
    total_rows += (*batch_result)->num_rows();
  }
  EXPECT_EQ(total_rows, 2);

  std::remove(tmp_path.c_str());
}

TEST_F(ArrowOutputTest, RoundTripFeatherWithNulls) {
  // Test round-trip with null values
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("id,value\n1,100\n2,NA\n3,\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;

  std::string tmp_path = "/tmp/test_roundtrip_nulls.feather";
  auto write_result = write_feather(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;

  // Read back
  auto input_result = arrow::io::ReadableFile::Open(tmp_path);
  ASSERT_TRUE(input_result.ok());
  auto reader_result = arrow::ipc::RecordBatchFileReader::Open(*input_result);
  ASSERT_TRUE(reader_result.ok());

  // The value column should preserve null count
  auto batch_result = (*reader_result)->ReadRecordBatch(0);
  ASSERT_TRUE(batch_result.ok());
  auto batch = *batch_result;
  EXPECT_EQ(batch->num_rows(), 3);
  // Value column (index 1) should have 2 nulls (NA and empty)
  EXPECT_EQ(batch->column(1)->null_count(), 2);

  std::remove(tmp_path.c_str());
}

#ifdef LIBVROOM_ENABLE_PARQUET
TEST_F(ArrowOutputTest, RoundTripParquet) {
  // Parse CSV to Arrow table
  ArrowConvertOptions opts;
  opts.infer_types = true;
  auto result = parseAndConvert("name,age,score\nAlice,30,95.5\nBob,25,87.3\n", opts);
  ASSERT_TRUE(result.ok()) << result.error_message;

  // Write to Parquet
  std::string tmp_path = "/tmp/test_roundtrip.parquet";
  auto write_result = write_parquet(result.table, tmp_path);
  ASSERT_TRUE(write_result.ok()) << write_result.error_message;

  // Read back using Parquet reader
  auto input_result = arrow::io::ReadableFile::Open(tmp_path);
  ASSERT_TRUE(input_result.ok()) << input_result.status().ToString();

  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  auto status =
      parquet::arrow::OpenFile(*input_result, arrow::default_memory_pool(), &parquet_reader);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::shared_ptr<arrow::Table> read_table;
  status = parquet_reader->ReadTable(&read_table);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Verify dimensions
  EXPECT_EQ(read_table->num_rows(), 2);
  EXPECT_EQ(read_table->num_columns(), 3);

  // Verify column names
  EXPECT_EQ(read_table->schema()->field(0)->name(), "name");
  EXPECT_EQ(read_table->schema()->field(1)->name(), "age");
  EXPECT_EQ(read_table->schema()->field(2)->name(), "score");

  std::remove(tmp_path.c_str());
}
#endif // LIBVROOM_ENABLE_PARQUET

} // namespace libvroom

#else
#include <gtest/gtest.h>
TEST(ArrowOutputTest, ArrowNotEnabled) {
  GTEST_SKIP() << "Arrow not enabled";
}
#endif
